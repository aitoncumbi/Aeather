//! The S3 API, implemented on the engine through the `s3s` adapter.
//!
//! `s3s` parses requests, verifies signatures and serializes responses; this
//! module maps each operation onto engine calls and owns S3-specific rules
//! such as checksum verification and conditional requests.

pub(crate) mod convert;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use sync_wrapper::SyncStream;

use self::convert::*;
use crate::engine::Engine;
use crate::meta::{ListPage, ListQuery, ObjectAttrs, ObjectRecord};

/// S3 limit on keys per DeleteObjects request.
const MAX_DELETE_KEYS: usize = 1000;

pub struct AetherS3 {
    engine: Arc<Engine>,
    region: String,
}

impl AetherS3 {
    pub fn new(engine: Arc<Engine>, region: String) -> Self {
        Self { engine, region }
    }

    fn owner() -> Owner {
        Owner { display_name: Some("aether".into()), id: Some("aether".into()) }
    }

    async fn list(&self, bucket: &str, query: ListQuery) -> S3Result<ListPage> {
        self.engine.list_objects(bucket, query).await.map_err(s3_error)
    }
}

/// Fields shared by GetObject and HeadObject responses.
struct ObjectHeaders {
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_disposition: Option<String>,
    content_language: Option<String>,
    cache_control: Option<String>,
    expires: Option<String>,
    metadata: Option<Metadata>,
    checksum: Checksum,
}

impl ObjectHeaders {
    /// `response-*` query parameters override the stored headers. Checksums
    /// are returned only when requested and only for whole-object reads.
    #[allow(clippy::too_many_arguments)]
    fn new(
        record: &ObjectRecord,
        whole_object: bool,
        checksum_mode: Option<&ChecksumMode>,
        response_content_type: Option<String>,
        response_content_encoding: Option<String>,
        response_content_disposition: Option<String>,
        response_content_language: Option<String>,
        response_cache_control: Option<String>,
        response_expires: Option<Timestamp>,
    ) -> Self {
        let attrs = &record.attrs;
        let wants_checksum = checksum_mode.is_some_and(|m| m.as_str().eq_ignore_ascii_case(ChecksumMode::ENABLED));
        let expires = match response_expires {
            Some(ts) => {
                let mut buf = Vec::new();
                ts.format(TimestampFormat::HttpDate, &mut buf).ok().and_then(|_| String::from_utf8(buf).ok())
            }
            None => attrs.expires.clone(),
        };
        Self {
            content_type: response_content_type.or_else(|| attrs.content_type.clone()),
            content_encoding: response_content_encoding.or_else(|| attrs.content_encoding.clone()),
            content_disposition: response_content_disposition.or_else(|| attrs.content_disposition.clone()),
            content_language: response_content_language.or_else(|| attrs.content_language.clone()),
            cache_control: response_cache_control.or_else(|| attrs.cache_control.clone()),
            expires,
            metadata: (!attrs.user_metadata.is_empty())
                .then(|| attrs.user_metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            checksum: if whole_object && wants_checksum {
                checksum_from_map(&attrs.checksums)
            } else {
                Checksum::default()
            },
        }
    }
}

#[async_trait::async_trait]
impl S3 for AetherS3 {
    // ---- buckets ----

    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let prefix = req.input.prefix.unwrap_or_default();
        let buckets = self.engine.list_buckets().await.map_err(s3_error)?;
        let buckets = buckets
            .into_iter()
            .filter(|b| b.name.starts_with(&prefix))
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(timestamp(b.created_at)),
                bucket_region: Some(self.region.clone()),
                ..Default::default()
            })
            .collect();
        let output = ListBucketsOutput { buckets: Some(buckets), owner: Some(Self::owner()), ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;
        if input.object_lock_enabled_for_bucket == Some(true) {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        self.engine.create_bucket(&input.bucket).await.map_err(s3_error)?;
        let output = CreateBucketOutput { location: Some(format!("/{}", input.bucket)), ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.engine.delete_bucket(&req.input.bucket).await.map_err(s3_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        let output = HeadBucketOutput { bucket_region: Some(self.region.clone()), ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        // S3 reports us-east-1 as an empty location constraint.
        let location_constraint =
            (self.region != "us-east-1").then(|| BucketLocationConstraint::from(self.region.clone()));
        Ok(S3Response::new(GetBucketLocationOutput { location_constraint }))
    }

    // ---- objects ----

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let S3Request { input, headers, trailing_headers, .. } = req;

        if input.tagging.is_some() {
            return Err(s3_error!(NotImplemented, "object tagging is not supported yet"));
        }
        if input.sse_customer_key.is_some() || input.server_side_encryption.is_some() {
            return Err(s3_error!(NotImplemented, "server-side encryption is not supported yet"));
        }
        if input.object_lock_mode.is_some() || input.object_lock_legal_hold_status.is_some() {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        if input.write_offset_bytes.is_some() {
            return Err(s3_error!(NotImplemented, "appends are not supported"));
        }
        if let Some(class) = &input.storage_class
            && class.as_str() != StorageClass::STANDARD
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        // Canned ACLs (e.g. `x-amz-acl: private`, sent by many tools) are accepted
        // and ignored: access is governed by access keys, not ACLs.

        let condition = write_condition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        self.engine.precheck_put(&input.bucket, &input.key, &condition).await.map_err(s3_error)?;

        let mut sent = Checksum {
            checksum_crc32: input.checksum_crc32,
            checksum_crc32c: input.checksum_crc32c,
            checksum_crc64nvme: input.checksum_crc64nvme,
            checksum_md5: input.checksum_md5,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            checksum_sha512: input.checksum_sha512,
            checksum_xxhash3: input.checksum_xxhash3,
            checksum_xxhash64: input.checksum_xxhash64,
            checksum_xxhash128: input.checksum_xxhash128,
            checksum_type: None,
        };
        let trailer = headers.get("x-amz-trailer").and_then(|v| v.to_str().ok());
        let mut hasher = checksum_hasher(&sent, input.checksum_algorithm.as_ref(), trailer)?;

        let body = input.body.unwrap_or_else(|| StreamingBlob::from(Bytes::new()));
        let staged = self.engine.stage(body.inspect_ok(|chunk| hasher.update(chunk))).await.map_err(s3_error)?;

        verify_content_md5(input.content_md5.as_deref(), &staged.md5)?;
        if let Some(trailers) = trailing_headers.and_then(|t| t.take()) {
            add_trailer_checksums(&mut sent, &trailers)?;
        }
        let checksums = verify_checksums(hasher.finalize(), &sent)?;

        let attrs = ObjectAttrs {
            content_type: input.content_type,
            content_encoding: input.content_encoding,
            content_disposition: input.content_disposition,
            content_language: input.content_language,
            cache_control: input.cache_control,
            expires: input.expires,
            user_metadata: input.metadata.map(BTreeMap::from_iter).unwrap_or_default(),
            checksums,
        };
        let record =
            self.engine.commit_put(&input.bucket, &input.key, staged, attrs, condition).await.map_err(s3_error)?;

        let mut output = PutObjectOutput { e_tag: Some(etag(&record)), ..Default::default() };
        set_checksums!(output, checksum_from_map(&record.attrs.checksums));
        Ok(S3Response::new(output))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        if input.sse_customer_key.is_some() {
            return Err(s3_error!(NotImplemented, "server-side encryption is not supported yet"));
        }
        check_version_id(input.version_id.as_deref())?;
        check_part_number(input.part_number)?;
        let record = self.engine.get_object(&input.bucket, &input.key).await.map_err(s3_error)?;
        check_read_conditions(
            &record,
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )?;
        let (range, content_range) = resolve_range(input.range.as_ref(), record.size)?;
        let headers = ObjectHeaders::new(
            &record,
            content_range.is_none(),
            input.checksum_mode.as_ref(),
            input.response_content_type,
            input.response_content_encoding,
            input.response_content_disposition,
            input.response_content_language,
            input.response_cache_control,
            input.response_expires,
        );

        let body = SyncStream::new(self.engine.read_object(&record, range.clone()));
        let mut output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(body)),
            content_length: Some((range.end - range.start) as i64),
            content_range,
            accept_ranges: Some("bytes".into()),
            e_tag: Some(etag(&record)),
            last_modified: Some(timestamp(record.last_modified)),
            content_type: headers.content_type,
            content_encoding: headers.content_encoding,
            content_disposition: headers.content_disposition,
            content_language: headers.content_language,
            cache_control: headers.cache_control,
            expires: headers.expires,
            metadata: headers.metadata,
            ..Default::default()
        };
        set_checksums!(output, headers.checksum);
        Ok(S3Response::new(output))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        check_version_id(input.version_id.as_deref())?;
        check_part_number(input.part_number)?;
        let record = self.engine.get_object(&input.bucket, &input.key).await.map_err(s3_error)?;
        check_read_conditions(
            &record,
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )?;
        let (range, content_range) = resolve_range(input.range.as_ref(), record.size)?;
        let headers = ObjectHeaders::new(
            &record,
            content_range.is_none(),
            input.checksum_mode.as_ref(),
            input.response_content_type,
            input.response_content_encoding,
            input.response_content_disposition,
            input.response_content_language,
            input.response_cache_control,
            input.response_expires,
        );
        let mut output = HeadObjectOutput {
            content_length: Some((range.end - range.start) as i64),
            content_range,
            accept_ranges: Some("bytes".into()),
            e_tag: Some(etag(&record)),
            last_modified: Some(timestamp(record.last_modified)),
            content_type: headers.content_type,
            content_encoding: headers.content_encoding,
            content_disposition: headers.content_disposition,
            content_language: headers.content_language,
            cache_control: headers.cache_control,
            expires: headers.expires,
            metadata: headers.metadata,
            ..Default::default()
        };
        set_checksums!(output, headers.checksum);
        Ok(S3Response::new(output))
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        check_version_id(input.version_id.as_deref())?;
        if input.if_match.is_some() || input.if_match_last_modified_time.is_some() || input.if_match_size.is_some() {
            return Err(s3_error!(NotImplemented, "conditional deletes are not supported yet"));
        }
        self.engine.delete_object(&input.bucket, &input.key).await.map_err(s3_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let objects = input.delete.objects;
        if objects.len() > MAX_DELETE_KEYS {
            return Err(s3_error!(MalformedXML, "a request can delete at most {MAX_DELETE_KEYS} keys"));
        }

        let mut keys = Vec::with_capacity(objects.len());
        let mut errors = Vec::new();
        for object in objects {
            match check_version_id(object.version_id.as_deref()) {
                Ok(()) => keys.push(object.key),
                Err(_) => errors.push(Error {
                    key: Some(object.key),
                    version_id: object.version_id,
                    code: Some("NoSuchVersion".into()),
                    message: Some("versioning is not enabled".into()),
                }),
            }
        }
        self.engine.delete_objects(&input.bucket, keys.clone()).await.map_err(s3_error)?;

        let quiet = input.delete.quiet == Some(true);
        let deleted = (!quiet)
            .then(|| keys.into_iter().map(|key| DeletedObject { key: Some(key), ..Default::default() }).collect());
        let output = DeleteObjectsOutput { deleted, errors: Some(errors), ..Default::default() };
        Ok(S3Response::new(output))
    }

    // ---- listing ----

    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let url = wants_url_encoding(input.encoding_type.as_ref())?;
        let prefix = input.prefix.unwrap_or_default();
        let start = match (&input.continuation_token, &input.start_after) {
            (Some(token), _) => Some(decode_token(token)?),
            (None, Some(after)) if !after.is_empty() => Some(start_after(after)),
            _ => None,
        };
        let max_keys = max_keys(input.max_keys)?;
        let query = ListQuery { prefix: prefix.clone(), delimiter: input.delimiter.clone(), start, max_keys };
        let page = self.list(&input.bucket, query).await?;

        let encode = |s: String| if url { url_encode(&s) } else { s };
        let owner = (input.fetch_owner == Some(true)).then(Self::owner);
        let key_count = page.objects.len() + page.common_prefixes.len();
        let output = ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: Some(encode(prefix)),
            delimiter: input.delimiter.map(encode),
            start_after: input.start_after.map(encode),
            max_keys: Some(max_keys as i32),
            key_count: Some(key_count as i32),
            continuation_token: input.continuation_token,
            next_continuation_token: page.next_start.as_deref().map(encode_token),
            is_truncated: Some(page.next_start.is_some()),
            contents: Some(
                page.objects
                    .into_iter()
                    .map(|o| Object {
                        key: Some(encode(o.key)),
                        size: Some(o.size as i64),
                        e_tag: Some(ETag::Strong(o.etag)),
                        last_modified: Some(timestamp(o.last_modified)),
                        storage_class: Some(ObjectStorageClass::from_static(ObjectStorageClass::STANDARD)),
                        owner: owner.clone(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            common_prefixes: Some(
                page.common_prefixes.into_iter().map(|p| CommonPrefix { prefix: Some(encode(p)) }).collect(),
            ),
            encoding_type: input.encoding_type,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        let url = wants_url_encoding(input.encoding_type.as_ref())?;
        let prefix = input.prefix.unwrap_or_default();
        let start = input
            .marker
            .as_deref()
            .filter(|m| !m.is_empty())
            .map(|m| marker_start(m, &prefix, input.delimiter.as_deref()));
        let max_keys = max_keys(input.max_keys)?;
        let query = ListQuery { prefix: prefix.clone(), delimiter: input.delimiter.clone(), start, max_keys };
        let page = self.list(&input.bucket, query).await?;

        let truncated = page.next_start.is_some();
        // S3 returns NextMarker only with a delimiter; clients otherwise use the last key.
        let next_marker = if truncated && input.delimiter.is_some() {
            let last_key = page.objects.last().map(|o| o.key.clone());
            let last_prefix = page.common_prefixes.last().cloned();
            last_key.max(last_prefix)
        } else {
            None
        };

        let encode = |s: String| if url { url_encode(&s) } else { s };
        let output = ListObjectsOutput {
            name: Some(input.bucket),
            prefix: Some(encode(prefix)),
            delimiter: input.delimiter.map(encode),
            marker: input.marker.map(encode),
            next_marker: next_marker.map(encode),
            max_keys: Some(max_keys as i32),
            is_truncated: Some(truncated),
            contents: Some(
                page.objects
                    .into_iter()
                    .map(|o| Object {
                        key: Some(encode(o.key)),
                        size: Some(o.size as i64),
                        e_tag: Some(ETag::Strong(o.etag)),
                        last_modified: Some(timestamp(o.last_modified)),
                        storage_class: Some(ObjectStorageClass::from_static(ObjectStorageClass::STANDARD)),
                        owner: Some(Self::owner()),
                        ..Default::default()
                    })
                    .collect(),
            ),
            common_prefixes: Some(
                page.common_prefixes.into_iter().map(|p| CommonPrefix { prefix: Some(encode(p)) }).collect(),
            ),
            encoding_type: input.encoding_type,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
