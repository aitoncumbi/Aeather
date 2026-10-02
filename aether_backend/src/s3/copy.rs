//! CopyObject and UploadPartCopy.
//!
//! CopyObject shares the source's blocks, so copies of any size are instant.
//! UploadPartCopy copies bytes, because a byte range rarely lines up with
//! block boundaries.

use futures::TryStreamExt;
use s3s::checksum::ChecksumHasher;
use s3s::dto::*;
use s3s::{S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::AetherS3;
use super::convert::*;
use crate::error::Error as EngineError;
use crate::meta::{ObjectAttrs, ObjectRecord};

/// S3's limit on one part, and so on one UploadPartCopy.
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
/// A copy whose source changes while it runs is retried this many times.
const COPY_ATTEMPTS: usize = 3;

/// `x-amz-copy-source-if-*` headers.
struct SourceConditions {
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
}

impl SourceConditions {
    /// Same rules as conditional GET, except that every failure is a 412.
    fn check(&self, source: &ObjectRecord) -> S3Result<()> {
        check_read_conditions(
            source,
            self.if_match.as_ref(),
            self.if_none_match.as_ref(),
            self.if_modified_since.as_ref(),
            self.if_unmodified_since.as_ref(),
        )
        .map_err(|e| if *e.code() == S3ErrorCode::NotModified { s3_error!(PreconditionFailed) } else { e })
    }
}

impl AetherS3 {
    pub(super) async fn copy_object_op(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        // Tags follow their own directive: copied from the source unless REPLACE.
        let new_tags = match input.tagging_directive.as_ref().map(|d| d.as_str()) {
            None | Some(TaggingDirective::COPY) => None,
            Some(TaggingDirective::REPLACE) => Some(super::tags::from_header(input.tagging.as_deref())?),
            Some(other) => return Err(s3_error!(InvalidArgument, "unknown tagging directive {other}")),
        };
        self.check_sse(
            input.server_side_encryption.as_ref(),
            input.sse_customer_key.is_some() || input.copy_source_sse_customer_key.is_some(),
        )?;
        if input.object_lock_mode.is_some() || input.object_lock_legal_hold_status.is_some() {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        check_storage_class(input.storage_class.as_ref())?;

        let (src_bucket, src_key) = copy_source(&input.copy_source)?;
        let replace = match input.metadata_directive.as_ref().map(|d| d.as_str()) {
            None | Some(MetadataDirective::COPY) => false,
            Some(MetadataDirective::REPLACE) => true,
            Some(other) => return Err(s3_error!(InvalidArgument, "unknown metadata directive {other}")),
        };
        if !replace && src_bucket == input.bucket && src_key == input.key {
            return Err(s3_error!(
                InvalidRequest,
                "this copy request is illegal because it is trying to copy an object to itself without \
                 changing the object's metadata"
            ));
        }
        let condition = write_condition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let conditions = SourceConditions {
            if_match: input.copy_source_if_match.clone(),
            if_none_match: input.copy_source_if_none_match.clone(),
            if_modified_since: input.copy_source_if_modified_since.clone(),
            if_unmodified_since: input.copy_source_if_unmodified_since.clone(),
        };
        let (dst_bucket, dst_key) = (input.bucket.clone(), input.key.clone());
        let new_attrs = if replace { Some(object_attrs!(input)) } else { None };

        let mut attempt = 0;
        let record = loop {
            attempt += 1;
            let source = self.engine.get_object(&src_bucket, &src_key).await.map_err(s3_error)?;
            conditions.check(&source)?;
            // The data does not change, so neither do its checksums.
            let mut attrs = match &new_attrs {
                Some(attrs) => ObjectAttrs {
                    checksums: source.attrs.checksums.clone(),
                    checksum_type: source.attrs.checksum_type.clone(),
                    ..attrs.clone()
                },
                None => source.attrs.clone(),
            };
            attrs.tags = new_tags.clone().unwrap_or_else(|| source.attrs.tags.clone());
            let copied = self
                .engine
                .copy_object(&src_bucket, &src_key, &source, &dst_bucket, &dst_key, attrs, condition.clone())
                .await;
            match copied {
                Ok(record) => break record,
                Err(EngineError::Conflict(_)) if attempt < COPY_ATTEMPTS => continue,
                Err(e) => return Err(s3_error(e)),
            }
        };

        let mut result = CopyObjectResult {
            e_tag: Some(etag(&record)),
            last_modified: Some(timestamp(record.last_modified)),
            checksum_type: stored_checksum_type(&record.attrs),
            ..Default::default()
        };
        set_copy_checksums!(result, checksum_from_map(&record.attrs.checksums));
        let sse = sse_of(&record);
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(result),
            server_side_encryption: sse,
            ..Default::default()
        }))
    }

    pub(super) async fn upload_part_copy_op(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;
        self.check_sse(None, input.sse_customer_key.is_some() || input.copy_source_sse_customer_key.is_some())?;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let number = part_number(input.part_number)?;
        let upload = self.engine.get_upload(&input.bucket, &input.key, upload_id).await.map_err(s3_error)?;

        let (src_bucket, src_key) = copy_source(&input.copy_source)?;
        let source = self.engine.get_object(&src_bucket, &src_key).await.map_err(s3_error)?;
        let conditions = SourceConditions {
            if_match: input.copy_source_if_match,
            if_none_match: input.copy_source_if_none_match,
            if_modified_since: input.copy_source_if_modified_since,
            if_unmodified_since: input.copy_source_if_unmodified_since,
        };
        conditions.check(&source)?;
        let range = copy_source_range(input.copy_source_range.as_deref(), source.size)?;
        if range.end - range.start > MAX_PART_SIZE {
            return Err(s3_error!(EntityTooLarge, "a part can be at most 5 GiB"));
        }

        let mut hasher = ChecksumHasher::default();
        if let Some(alg) = upload.checksum_algorithm.as_deref() {
            enable_checksum(&mut hasher, alg)?;
        }
        let data = self.engine.read_object(&source, range).inspect_ok(|chunk| hasher.update(chunk));
        // Failing to read our own source is a server error, not a client one.
        let staged = self.engine.stage_part(data, &upload).await.map_err(|e| match e {
            EngineError::Body(source) => s3_error!(InternalError, "reading the copy source failed: {source}"),
            other => s3_error(other),
        })?;
        let checksums = checksum_to_map(hasher.finalize());
        let part = self
            .engine
            .commit_part(&input.bucket, &input.key, upload_id, number, staged, checksums)
            .await
            .map_err(s3_error)?;

        let mut result = CopyPartResult {
            e_tag: Some(ETag::Strong(part.etag)),
            last_modified: Some(timestamp(part.last_modified)),
            ..Default::default()
        };
        set_copy_checksums!(result, checksum_from_map(&part.checksums));
        Ok(S3Response::new(UploadPartCopyOutput { copy_part_result: Some(result), ..Default::default() }))
    }
}

/// The source bucket and key. Only the `null` version exists until versioning.
fn copy_source(source: &CopySource) -> S3Result<(String, String)> {
    match source {
        CopySource::Bucket { bucket, key, version_id } => {
            check_version_id(version_id.as_deref())?;
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(s3_error!(NotImplemented, "access point and outpost copy sources are not supported")),
    }
}
