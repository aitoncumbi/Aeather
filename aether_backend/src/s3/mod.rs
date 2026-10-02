//! The S3 API, implemented on the engine through the `s3s` adapter.
//!
//! `s3s` parses requests, verifies signatures and serializes responses; this
//! module maps each operation onto engine calls and owns S3-specific rules
//! such as checksum verification and conditional requests.

pub(crate) mod convert;
mod copy;
pub mod cors;
pub mod host;
mod multipart;
mod tags;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, TrailingHeaders, s3_error};
use sync_wrapper::SyncStream;

use self::convert::*;
use crate::auth::Authorizer;
use crate::engine::{Engine, StagedObject, VersionRef, Versioning, VersionsQuery};
use crate::error::Error as EngineError;
use crate::meta::{ListPage, ListQuery, ObjectRecord, UploadRecord};

/// S3 limit on keys per DeleteObjects request.
const MAX_DELETE_KEYS: usize = 1000;

pub struct AetherS3 {
    engine: Arc<Engine>,
    auth: Arc<Authorizer>,
    region: String,
}

impl AetherS3 {
    pub fn new(engine: Arc<Engine>, auth: Arc<Authorizer>, region: String) -> Self {
        Self { engine, auth, region }
    }

    /// Requests may ask for SSE-S3 (`AES256`), which applies to every new
    /// object once a master key is configured. SSE-KMS and SSE-C are refused.
    fn check_sse(&self, requested: Option<&ServerSideEncryption>, customer_key: bool) -> S3Result<()> {
        if customer_key {
            return Err(s3_error!(NotImplemented, "SSE-C (customer-provided keys) is not supported yet"));
        }
        match requested.map(|s| s.as_str()) {
            None => Ok(()),
            Some(ServerSideEncryption::AES256) if self.engine.config().keyring.is_some() => Ok(()),
            Some(ServerSideEncryption::AES256) => Err(s3_error!(
                NotImplemented,
                "encryption at rest is off on this server: set encryption.master_key to enable SSE-S3"
            )),
            Some(other) => Err(s3_error!(NotImplemented, "only SSE-S3 (AES256) is supported, not {other}")),
        }
    }

    fn owner() -> Owner {
        Owner { display_name: Some("aether".into()), id: Some("aether".into()) }
    }

    async fn list(&self, bucket: &str, query: ListQuery) -> S3Result<ListPage> {
        let max_keys = query.max_keys;
        let mut page = self.engine.list_objects(bucket, query).await.map_err(s3_error)?;
        // Asking for nothing gets a complete (empty) answer, as in S3.
        if max_keys == 0 {
            page.next_start = None;
        }
        Ok(page)
    }

    /// Streams an upload body to disk and verifies every integrity check the
    /// client sent. Returns the staged body and the checksums to store with it.
    async fn stage_body(
        &self,
        body: Option<StreamingBlob>,
        checks: BodyChecks<'_>,
        part_of: Option<&UploadRecord>,
    ) -> S3Result<(StagedObject, BTreeMap<String, String>)> {
        let BodyChecks { mut sent, algorithm, also, content_md5, trailer, trailing_headers } = checks;
        let mut hasher = checksum_hasher(&sent, algorithm, trailer)?;
        if let Some(alg) = also {
            enable_checksum(&mut hasher, alg)?;
        }
        let body = body.unwrap_or_else(|| StreamingBlob::from(Bytes::new()));
        let stream = body.inspect_ok(|chunk| hasher.update(chunk));
        let staged = match part_of {
            Some(upload) => self.engine.stage_part(stream, upload).await,
            None => self.engine.stage(stream).await,
        };
        let staged = staged.map_err(s3_error)?;

        verify_content_md5(content_md5, &staged.md5)?;
        if let Some(trailers) = trailing_headers.and_then(|t| t.take()) {
            add_trailer_checksums(&mut sent, &trailers)?;
        }
        let checksums = verify_checksums(hasher.finalize(), &sent)?;
        Ok((staged, checksums))
    }
}

/// Integrity checks a client can attach to an upload.
struct BodyChecks<'a> {
    /// `x-amz-checksum-*` header values.
    sent: Checksum,
    /// `x-amz-sdk-checksum-algorithm`: compute this even without a value.
    algorithm: Option<&'a ChecksumAlgorithm>,
    /// Also compute this algorithm (a multipart upload's).
    also: Option<&'a str>,
    content_md5: Option<&'a str>,
    /// `x-amz-trailer`: checksums that will follow the body.
    trailer: Option<&'a str>,
    trailing_headers: Option<TrailingHeaders>,
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
    checksum_type: Option<ChecksumType>,
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
        let with_checksum = whole_object && wants_checksum;
        let expires = match response_expires {
            Some(ts) => {
                let mut buf = Vec::new();
                ts.format(TimestampFormat::HttpDate, &mut buf).ok().and_then(|_| String::from_utf8(buf).ok())
            }
            None => attrs.expires.clone(),
        };
        Self {
            // S3's type for objects uploaded without one.
            content_type: response_content_type
                .or_else(|| attrs.content_type.clone())
                .or_else(|| Some("binary/octet-stream".into())),
            content_encoding: response_content_encoding.or_else(|| attrs.content_encoding.clone()),
            content_disposition: response_content_disposition.or_else(|| attrs.content_disposition.clone()),
            content_language: response_content_language.or_else(|| attrs.content_language.clone()),
            cache_control: response_cache_control.or_else(|| attrs.cache_control.clone()),
            expires,
            metadata: (!attrs.user_metadata.is_empty())
                .then(|| attrs.user_metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            checksum: if with_checksum { checksum_from_map(&attrs.checksums) } else { Checksum::default() },
            checksum_type: if with_checksum { stored_checksum_type(attrs) } else { None },
        }
    }
}

#[async_trait::async_trait]
impl S3 for AetherS3 {
    // ---- buckets ----

    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let identity = self.auth.caller(&req)?;
        let input = req.input;
        let limit = match input.max_buckets {
            None => usize::MAX,
            Some(n @ 1..=10_000) => n as usize,
            Some(_) => return Err(s3_error!(InvalidArgument, "max-buckets must be between 1 and 10000")),
        };
        let after = match &input.continuation_token {
            Some(token) => Some(String::from_utf8(decode_token(token)?).map_err(|_| s3_error!(InvalidToken))?),
            None => None,
        };
        let prefix = input.prefix.clone().unwrap_or_default();
        let other_region = input.bucket_region.as_ref().is_some_and(|r| *r != self.region);
        let buckets = self.engine.list_buckets().await.map_err(s3_error)?;
        // Keys only see the buckets they have some permission on. Names are sorted.
        let mut visible = buckets.into_iter().filter(|b| {
            !other_region
                && b.name.starts_with(&prefix)
                && after.as_ref().is_none_or(|after| b.name > *after)
                && !identity.grant(&b.name).is_empty()
        });
        let page: Vec<_> = visible.by_ref().take(limit).collect();
        let continuation_token = match (visible.next(), page.last()) {
            (Some(_), Some(last)) => Some(encode_token(last.name.as_bytes())),
            _ => None,
        };
        let buckets = page
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(timestamp(b.created_at)),
                bucket_region: Some(self.region.clone()),
                ..Default::default()
            })
            .collect();
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Self::owner()),
            continuation_token,
            prefix: input.prefix,
        };
        Ok(S3Response::new(output))
    }

    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let identity = self.auth.caller(&req)?;
        let input = req.input;
        if input.object_lock_enabled_for_bucket == Some(true) {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        let has_access = !identity.grant(&input.bucket).is_empty();
        if !identity.can_create_buckets() {
            // Applications often "create" their bucket at startup and ignore
            // BucketAlreadyOwnedByYou, so answer that when the key can use it.
            if has_access && self.engine.get_bucket(&input.bucket).await.is_ok() {
                return Err(s3_error!(BucketAlreadyOwnedByYou));
            }
            return Err(s3_error!(AccessDenied, "this access key may not create buckets"));
        }
        let created = match identity.owner_key() {
            Some(owner) => self.engine.create_bucket_for(&input.bucket, owner).await,
            None => self.engine.create_bucket(&input.bucket).await,
        };
        match created {
            Ok(_) => {}
            Err(EngineError::BucketAlreadyExists) if !has_access => return Err(s3_error!(BucketAlreadyExists)),
            Err(e) => return Err(s3_error(e)),
        }
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

        self.check_sse(input.server_side_encryption.as_ref(), input.sse_customer_key.is_some())?;
        if input.object_lock_mode.is_some() || input.object_lock_legal_hold_status.is_some() {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        if input.write_offset_bytes.is_some() {
            return Err(s3_error!(NotImplemented, "appends are not supported"));
        }
        check_storage_class(input.storage_class.as_ref())?;
        // Canned ACLs (e.g. `x-amz-acl: private`, sent by many tools) are accepted
        // and ignored: access is governed by access keys, not ACLs.

        let condition = write_condition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let size = input.content_length.and_then(|n| u64::try_from(n).ok());
        self.engine.precheck_put(&input.bucket, &input.key, &condition, size).await.map_err(s3_error)?;

        let (bucket, key, body) = (input.bucket.clone(), input.key.clone(), input.body);
        let (algorithm, content_md5) = (input.checksum_algorithm.clone(), input.content_md5.clone());
        let checks = BodyChecks {
            sent: sent_checksum!(input),
            algorithm: algorithm.as_ref(),
            also: None,
            content_md5: content_md5.as_deref(),
            trailer: headers.get("x-amz-trailer").and_then(|v| v.to_str().ok()),
            trailing_headers,
        };
        let (staged, checksums) = self.stage_body(body, checks, None).await?;

        let mut attrs = object_attrs!(input);
        attrs.checksums = checksums;
        let record = self.engine.commit_put(&bucket, &key, staged, attrs, condition).await.map_err(s3_error)?;

        let mut output = PutObjectOutput {
            version_id: version_id_of(&record),
            server_side_encryption: sse_of(&record),
            e_tag: Some(etag(&record)),
            checksum_type: stored_checksum_type(&record.attrs),
            ..Default::default()
        };
        set_checksums!(output, checksum_from_map(&record.attrs.checksums));
        Ok(S3Response::new(output))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        self.check_sse(None, input.sse_customer_key.is_some())?;
        let version = parse_version(input.version_id.as_deref())?;
        let record = self.engine.get_version(&input.bucket, &input.key, version).await.map_err(s3_error)?;
        if record.delete_marker {
            return Err(delete_marker_error(&record, version.is_some()));
        }
        check_read_conditions(
            &record,
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )?;
        let Selection { range, content_range, parts_count } = select(&record, input.part_number, input.range.as_ref())?;
        let headers = ObjectHeaders::new(
            &record,
            content_range.is_none() && parts_count.is_none(),
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
            version_id: version_id_of(&record),
            server_side_encryption: sse_of(&record),
            body: Some(StreamingBlob::wrap(body)),
            content_length: Some((range.end - range.start) as i64),
            content_range,
            parts_count,
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
            checksum_type: headers.checksum_type,
            tag_count: tags::count(&record.attrs.tags),
            ..Default::default()
        };
        set_checksums!(output, headers.checksum);
        Ok(S3Response::new(output))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let version = parse_version(input.version_id.as_deref())?;
        let record = self.engine.get_version(&input.bucket, &input.key, version).await.map_err(s3_error)?;
        if record.delete_marker {
            return Err(delete_marker_error(&record, version.is_some()));
        }
        check_read_conditions(
            &record,
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )?;
        let Selection { range, content_range, parts_count } = select(&record, input.part_number, input.range.as_ref())?;
        let headers = ObjectHeaders::new(
            &record,
            content_range.is_none() && parts_count.is_none(),
            input.checksum_mode.as_ref(),
            input.response_content_type,
            input.response_content_encoding,
            input.response_content_disposition,
            input.response_content_language,
            input.response_cache_control,
            input.response_expires,
        );
        let mut output = HeadObjectOutput {
            version_id: version_id_of(&record),
            server_side_encryption: sse_of(&record),
            content_length: Some((range.end - range.start) as i64),
            content_range,
            parts_count,
            checksum_type: headers.checksum_type,
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
        let mut response = S3Response::new(output);
        // Not in S3's HeadObject model, but sent by Ceph's S3 server and read by some clients.
        if let Some(count) = tags::count(&record.attrs.tags) {
            response.headers.insert("x-amz-tagging-count", count.into());
        }
        Ok(response)
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let version = parse_version(input.version_id.as_deref())?;
        let condition =
            delete_condition(input.if_match.as_ref(), input.if_match_last_modified_time.as_ref(), input.if_match_size)?;
        let deleted =
            self.engine.delete_object_if(&input.bucket, &input.key, version, condition).await.map_err(s3_error)?;
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: deleted.delete_marker.then_some(true),
            version_id: deleted.version.map(|v| v.to_string()),
            ..Default::default()
        }))
    }

    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let objects = input.delete.objects;
        if objects.len() > MAX_DELETE_KEYS {
            return Err(s3_error!(MalformedXML, "a request can delete at most {MAX_DELETE_KEYS} keys"));
        }

        let mut targets = Vec::with_capacity(objects.len());
        let mut errors = Vec::new();
        for object in objects {
            let error = |code: &str, message: &str| Error {
                key: Some(object.key.clone()),
                version_id: object.version_id.clone(),
                code: Some(code.into()),
                message: Some(message.into()),
            };
            let Ok(version) = parse_version(object.version_id.as_deref()) else {
                errors.push(error("InvalidArgument", "Invalid version id specified"));
                continue;
            };
            let if_match = object.e_tag.clone().map(ETagCondition::ETag);
            match delete_condition(if_match.as_ref(), object.last_modified_time.as_ref(), object.size) {
                Ok(condition) => targets.push((object.key, version, condition)),
                Err(e) => errors.push(error("InvalidArgument", e.message().unwrap_or("invalid condition"))),
            }
        }
        let results = self.engine.delete_objects_if(&input.bucket, targets.clone()).await.map_err(s3_error)?;

        let mut deleted = Vec::with_capacity(targets.len());
        for ((key, version, _), result) in targets.into_iter().zip(results) {
            let Some(result) = result else {
                errors.push(Error {
                    key: Some(key),
                    version_id: version.map(|v| v.to_string()),
                    code: Some("PreconditionFailed".into()),
                    message: Some("At least one of the pre-conditions you specified did not hold".into()),
                });
                continue;
            };
            // A version removed is echoed back; a delete marker created is named.
            let (version_id, marker_version_id) = match version {
                Some(v) => (Some(v.to_string()), None),
                None => (None, result.delete_marker.then(|| result.version.map(|v| v.to_string())).flatten()),
            };
            deleted.push(DeletedObject {
                key: Some(key),
                version_id,
                delete_marker: result.delete_marker.then_some(true),
                delete_marker_version_id: marker_version_id,
            });
        }
        let quiet = input.delete.quiet == Some(true);
        let output =
            DeleteObjectsOutput { deleted: (!quiet).then_some(deleted), errors: Some(errors), ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        self.copy_object_op(req).await
    }

    // ---- multipart uploads ----

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.create_multipart_upload_op(req).await
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        self.upload_part_op(req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.upload_part_copy_op(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.complete_multipart_upload_op(req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.abort_multipart_upload_op(req).await
    }

    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        self.list_parts_op(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.list_multipart_uploads_op(req).await
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
        let delimiter = input.delimiter.filter(|d| !d.is_empty());
        let query = ListQuery { prefix: prefix.clone(), delimiter: delimiter.clone(), start, max_keys };
        let page = self.list(&input.bucket, query).await?;

        let encode = |s: String| if url { url_encode(&s) } else { s };
        let owner = (input.fetch_owner == Some(true)).then(Self::owner);
        let key_count = page.objects.len() + page.common_prefixes.len();
        let output = ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: Some(encode(prefix)),
            delimiter: delimiter.map(encode),
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

    /// Without versioning (v2), every object has exactly one version, `null`,
    /// and there are no delete markers. Cleanup tools rely on this call.
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let input = req.input;
        let url = wants_url_encoding(input.encoding_type.as_ref())?;
        let version_marker = match input.version_id_marker.as_deref().filter(|m| !m.is_empty()) {
            None => None,
            Some(m) => {
                Some(VersionRef::parse(m).ok_or_else(|| s3_error!(InvalidArgument, "invalid version-id-marker"))?)
            }
        };
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter.filter(|d| !d.is_empty());
        let key_marker = input.key_marker.clone().filter(|m| !m.is_empty());
        if version_marker.is_some() && key_marker.is_none() {
            return Err(s3_error!(InvalidArgument, "a version-id-marker needs a key-marker"));
        }
        let max_keys = max_keys(input.max_keys)?;
        let query = VersionsQuery {
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            key_marker,
            version_marker,
            max_keys,
        };
        let mut page = self.engine.list_versions(&input.bucket, query).await.map_err(s3_error)?;
        // Asking for nothing gets a complete (empty) answer, as in S3.
        if max_keys == 0 {
            page.next = None;
        }

        let encode = |s: String| if url { url_encode(&s) } else { s };
        let (mut versions, mut markers) = (Vec::new(), Vec::new());
        for entry in page.versions {
            let record = entry.record;
            if record.delete_marker {
                markers.push(DeleteMarkerEntry {
                    key: Some(encode(entry.key)),
                    version_id: Some(record.version().to_string()),
                    is_latest: Some(entry.is_latest),
                    last_modified: Some(timestamp(record.last_modified)),
                    owner: Some(Self::owner()),
                });
            } else {
                versions.push(ObjectVersion {
                    key: Some(encode(entry.key)),
                    version_id: Some(record.version().to_string()),
                    is_latest: Some(entry.is_latest),
                    size: Some(record.size as i64),
                    e_tag: Some(ETag::Strong(record.etag)),
                    last_modified: Some(timestamp(record.last_modified)),
                    storage_class: Some(ObjectVersionStorageClass::from_static(ObjectVersionStorageClass::STANDARD)),
                    owner: Some(Self::owner()),
                    ..Default::default()
                });
            }
        }
        let truncated = page.next.is_some();
        let (next_key_marker, next_version_id_marker) = match page.next {
            Some((key, version)) => (Some(encode(key)), version.map(|v| v.to_string())),
            None => (None, None),
        };
        let output = ListObjectVersionsOutput {
            name: Some(input.bucket),
            prefix: Some(encode(prefix)),
            delimiter: delimiter.map(encode),
            key_marker: input.key_marker.map(encode),
            version_id_marker: input.version_id_marker,
            next_key_marker,
            next_version_id_marker,
            max_keys: Some(max_keys as i32),
            is_truncated: Some(truncated),
            versions: Some(versions),
            delete_markers: Some(markers),
            common_prefixes: Some(
                page.common_prefixes.into_iter().map(|p| CommonPrefix { prefix: Some(encode(p)) }).collect(),
            ),
            encoding_type: input.encoding_type,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// Versioning has never been enabled on any bucket (it arrives in v2).
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let bucket = self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        let status = match bucket.versioning {
            Versioning::Unversioned => None,
            Versioning::Enabled => Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
            Versioning::Suspended => Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)),
        };
        Ok(S3Response::new(GetBucketVersioningOutput { status, ..Default::default() }))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let input = req.input;
        let config = input.versioning_configuration;
        if config.mfa_delete.as_ref().is_some_and(|m| m.as_str() == MFADelete::ENABLED) {
            return Err(s3_error!(NotImplemented, "MFA delete is not supported"));
        }
        let versioning = match config.status.as_ref().map(|s| s.as_str()) {
            Some(BucketVersioningStatus::ENABLED) => Versioning::Enabled,
            Some(BucketVersioningStatus::SUSPENDED) => Versioning::Suspended,
            _ => return Err(s3_error!(MalformedXML, "the versioning status must be Enabled or Suspended")),
        };
        self.engine.set_bucket_versioning(&input.bucket, versioning).await.map_err(s3_error)?;
        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }

    /// Tags cannot be set yet (such requests are refused), so every object's
    /// tag set is empty. Copy tools read it to carry tags over to the copy.
    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let input = req.input;
        let version = parse_version(input.version_id.as_deref())?;
        let record = self.engine.get_version(&input.bucket, &input.key, version).await.map_err(s3_error)?;
        if record.delete_marker {
            return Err(delete_marker_error(&record, version.is_some()));
        }
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set: tags::to_tag_set(&record.attrs.tags),
            version_id: version_id_of(&record),
        }))
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let input = req.input;
        let version = parse_version(input.version_id.as_deref())?;
        let tags = tags::from_tag_set(input.tagging.tag_set)?;
        self.engine.set_object_tags(&input.bucket, &input.key, version, tags).await.map_err(s3_error)?;
        Ok(S3Response::new(PutObjectTaggingOutput { version_id: input.version_id }))
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let input = req.input;
        let version = parse_version(input.version_id.as_deref())?;
        self.engine.set_object_tags(&input.bucket, &input.key, version, Default::default()).await.map_err(s3_error)?;
        Ok(S3Response::new(DeleteObjectTaggingOutput { version_id: input.version_id }))
    }

    /// No bucket has tags; S3 answers NoSuchTagSet for an untagged bucket.
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let input = req.input;
        let rules = cors::rules_from_s3(input.cors_configuration)?;
        self.engine.set_bucket_cors(&input.bucket, rules).await.map_err(s3_error)?;
        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    async fn get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let bucket = self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        if bucket.cors.is_empty() {
            return Err(s3_error!(NoSuchCORSConfiguration));
        }
        Ok(S3Response::new(GetBucketCorsOutput { cors_rules: Some(cors::rules_to_s3(bucket.cors)) }))
    }

    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        self.engine.set_bucket_cors(&req.input.bucket, Vec::new()).await.map_err(s3_error)?;
        Ok(S3Response::new(DeleteBucketCorsOutput::default()))
    }

    /// With a master key, every bucket encrypts new objects with SSE-S3.
    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        if self.engine.config().keyring.is_none() {
            return Err(s3_error!(ServerSideEncryptionConfigurationNotFoundError));
        }
        let rule = ServerSideEncryptionRule {
            apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                kms_master_key_id: None,
            }),
            ..Default::default()
        };
        let config = ServerSideEncryptionConfiguration { rules: vec![rule] };
        Ok(S3Response::new(GetBucketEncryptionOutput { server_side_encryption_configuration: Some(config) }))
    }

    async fn get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        self.engine.get_bucket(&req.input.bucket).await.map_err(s3_error)?;
        Err(s3_error!(NoSuchTagSet))
    }

    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        let url = wants_url_encoding(input.encoding_type.as_ref())?;
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter.filter(|d| !d.is_empty());
        let start =
            input.marker.as_deref().filter(|m| !m.is_empty()).map(|m| marker_start(m, &prefix, delimiter.as_deref()));
        let max_keys = max_keys(input.max_keys)?;
        let query = ListQuery { prefix: prefix.clone(), delimiter: delimiter.clone(), start, max_keys };
        let page = self.list(&input.bucket, query).await?;

        let truncated = page.next_start.is_some();
        // S3 returns NextMarker only with a delimiter; clients otherwise use the last key.
        let next_marker = if truncated && delimiter.is_some() {
            let last_key = page.objects.last().map(|o| o.key.clone());
            let last_prefix = page.common_prefixes.last().cloned();
            last_key.max(last_prefix)
        } else {
            None
        };

        let encode = |s: String| if url { url_encode(&s) } else { s };
        // Clients decode Marker but not Prefix in v1 responses (unlike v2).
        let output = ListObjectsOutput {
            name: Some(input.bucket),
            prefix: Some(prefix),
            delimiter: delimiter.map(encode),
            marker: Some(encode(input.marker.unwrap_or_default())),
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
