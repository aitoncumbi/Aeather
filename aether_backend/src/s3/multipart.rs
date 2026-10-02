//! Multipart upload operations.
//!
//! Parts are verified like single uploads (Content-MD5, `x-amz-checksum-*`,
//! trailers). If the upload was created with a checksum algorithm, every part
//! is hashed with it, so the object's checksum can be derived on completion:
//! `COMPOSITE` hashes the parts' checksums, `FULL_OBJECT` combines the parts'
//! CRCs into the CRC of the whole object.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use s3s::checksum::ChecksumHasher;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result, s3_error};

use super::convert::*;
use super::{AetherS3, BodyChecks};
use crate::engine::Completion;
use crate::error::Error as EngineError;
use crate::meta::{ObjectRecord, PartsPage, UploadListQuery, UploadRecord};

/// Algorithms S3 accepts for multipart uploads.
const MULTIPART_ALGORITHMS: &[&str] = &["crc32", "crc32c", "crc64nvme", "sha1", "sha256"];

impl AetherS3 {
    pub(super) async fn create_multipart_upload_op(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        if input.server_side_encryption.is_some() || input.sse_customer_key.is_some() {
            return Err(s3_error!(NotImplemented, "server-side encryption is not supported yet"));
        }
        if input.object_lock_mode.is_some() || input.object_lock_legal_hold_status.is_some() {
            return Err(s3_error!(NotImplemented, "object lock is not supported"));
        }
        check_storage_class(input.storage_class.as_ref())?;

        let algorithm = input.checksum_algorithm.as_ref().map(|a| a.as_str().to_ascii_lowercase());
        let requested_type = input.checksum_type.as_ref().map(|t| t.as_str().to_ascii_uppercase());
        let checksum_type = multipart_checksum_type(algorithm.as_deref(), requested_type.as_deref())?;

        let (bucket, key) = (input.bucket.clone(), input.key.clone());
        let attrs = object_attrs!(input);
        let upload =
            self.engine.create_upload(&bucket, &key, attrs, algorithm, checksum_type).await.map_err(s3_error)?;
        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id_string(&upload)),
            checksum_algorithm: upload.checksum_algorithm.as_deref().map(checksum_algorithm),
            checksum_type: upload.checksum_type.clone().map(ChecksumType::from),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn upload_part_op(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let S3Request { input, headers, trailing_headers, .. } = req;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let number = part_number(input.part_number)?;
        // Fail before reading the body if the upload does not exist, or if the
        // object could no longer fit the bucket's quota.
        let upload = self.engine.get_upload(&input.bucket, &input.key, upload_id).await.map_err(s3_error)?;
        let size = input.content_length.and_then(|n| u64::try_from(n).ok());
        self.engine.precheck_part(&input.bucket, &input.key, upload_id, number, size).await.map_err(s3_error)?;

        let (bucket, key, body) = (input.bucket.clone(), input.key.clone(), input.body);
        let (algorithm, content_md5) = (input.checksum_algorithm.clone(), input.content_md5.clone());
        let checks = BodyChecks {
            sent: sent_checksum!(input),
            algorithm: algorithm.as_ref(),
            also: upload.checksum_algorithm.as_deref(),
            content_md5: content_md5.as_deref(),
            trailer: headers.get("x-amz-trailer").and_then(|v| v.to_str().ok()),
            trailing_headers,
        };
        let (staged, checksums) = self.stage_body(body, checks, true).await?;
        let part =
            self.engine.commit_part(&bucket, &key, upload_id, number, staged, checksums).await.map_err(s3_error)?;

        let mut output = UploadPartOutput { e_tag: Some(ETag::Strong(part.etag)), ..Default::default() };
        set_checksums!(output, checksum_from_map(&part.checksums));
        Ok(S3Response::new(output))
    }

    pub(super) async fn complete_multipart_upload_op(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let condition = write_condition(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let (bucket, key, expected_size) = (input.bucket.clone(), input.key.clone(), input.mpu_object_size);
        let parts = input.multipart_upload.clone().and_then(|m| m.parts).unwrap_or_default();
        if parts.is_empty() {
            return Err(s3_error!(MalformedXML, "the request must list at least one part"));
        }
        let mut requested = Vec::with_capacity(parts.len());
        for part in &parts {
            let number = part.part_number.ok_or_else(|| s3_error!(MalformedXML, "every part needs a PartNumber"))?;
            let etag = part.e_tag.as_ref().ok_or_else(|| s3_error!(MalformedXML, "every part needs an ETag"))?;
            requested.push((part_number(number)?, etag.value().to_owned()));
        }

        let completion = match self.engine.prepare_completion(&bucket, &key, upload_id, &requested).await {
            Ok(completion) => completion,
            Err(EngineError::NoSuchUpload) => return self.already_completed(bucket, key, upload_id).await,
            Err(e) => return Err(s3_error(e)),
        };
        if let Some(expected) = expected_size
            && u64::try_from(expected).ok() != Some(completion.size())
        {
            return Err(s3_error!(InvalidRequest, "the parts add up to {} bytes, not {expected}", completion.size()));
        }
        let whole_object = sent_checksum!(input);
        let (checksums, checksum_type) = multipart_checksum(&completion, parts, &whole_object)?;
        let record =
            match self.engine.complete_upload(&bucket, &key, completion, checksums, checksum_type, condition).await {
                Ok(record) => record,
                Err(EngineError::NoSuchUpload) => return self.already_completed(bucket, key, upload_id).await,
                Err(e) => return Err(s3_error(e)),
            };
        Ok(S3Response::new(completed_output(bucket, key, &record)))
    }

    /// Clients retry CompleteMultipartUpload when a response is lost, by which
    /// time the upload is gone; S3 answers success if it made the object.
    async fn already_completed(
        &self,
        bucket: String,
        key: String,
        upload_id: uuid::Uuid,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        match self.engine.completed_upload(&bucket, &key, upload_id).await.map_err(s3_error)? {
            Some(record) => Ok(S3Response::new(completed_output(bucket, key, &record))),
            None => Err(s3_error!(NoSuchUpload)),
        }
    }

    pub(super) async fn abort_multipart_upload_op(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        if input.if_match_initiated_time.is_some() {
            return Err(s3_error!(NotImplemented, "conditional aborts are not supported"));
        }
        let upload_id = parse_upload_id(&input.upload_id)?;
        self.engine.abort_upload(&input.bucket, &input.key, upload_id).await.map_err(s3_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    pub(super) async fn list_parts_op(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let upload_id = parse_upload_id(&input.upload_id)?;
        let after = non_negative(input.part_number_marker.unwrap_or(0), "part-number-marker")? as u32;
        let max = non_negative(input.max_parts.unwrap_or(1000), "max-parts")? as usize;
        let PartsPage { upload, parts, truncated } =
            self.engine.list_parts(&input.bucket, &input.key, upload_id, after, max).await.map_err(s3_error)?;

        let next_marker = parts.last().map(|(n, _)| *n as i32);
        let parts = parts
            .into_iter()
            .map(|(number, part)| {
                let mut out = Part {
                    part_number: Some(number as i32),
                    e_tag: Some(ETag::Strong(part.etag)),
                    size: Some(part.size as i64),
                    last_modified: Some(timestamp(part.last_modified)),
                    ..Default::default()
                };
                set_checksums!(out, checksum_from_map(&part.checksums));
                out
            })
            .collect();
        let output = ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id_string(&upload)),
            part_number_marker: input.part_number_marker,
            next_part_number_marker: next_marker,
            max_parts: Some(max.min(crate::engine::MAX_LIST_PARTS) as i32),
            is_truncated: Some(truncated),
            parts: Some(parts),
            owner: Some(Self::owner()),
            initiator: Some(initiator()),
            storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
            checksum_algorithm: upload.checksum_algorithm.as_deref().map(checksum_algorithm),
            checksum_type: upload.checksum_type.clone().map(ChecksumType::from),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn list_multipart_uploads_op(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let url = wants_url_encoding(input.encoding_type.as_ref())?;
        let max = non_negative(input.max_uploads.unwrap_or(1000), "max-uploads")? as usize;
        let key_marker = input.key_marker.clone().filter(|k| !k.is_empty());
        // S3 ignores the upload id marker without a key marker.
        let upload_id_marker = match (&key_marker, input.upload_id_marker.as_deref()) {
            (Some(_), Some(id)) if !id.is_empty() => {
                Some(uuid::Uuid::try_parse(id).map_err(|_| s3_error!(InvalidArgument, "invalid upload-id-marker"))?)
            }
            _ => None,
        };
        let query = UploadListQuery {
            prefix: input.prefix.clone().unwrap_or_default(),
            delimiter: input.delimiter.clone(),
            key_marker,
            upload_id_marker,
            max_uploads: max,
        };
        let page = self.engine.list_uploads(&input.bucket, query).await.map_err(s3_error)?;

        let encode = |s: String| if url { url_encode(&s) } else { s };
        let uploads = page
            .uploads
            .iter()
            .map(|upload| MultipartUpload {
                key: Some(encode(upload.key.clone())),
                upload_id: Some(upload_id_string(upload)),
                initiated: Some(timestamp(upload.initiated)),
                storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                owner: Some(Self::owner()),
                initiator: Some(initiator()),
                checksum_algorithm: upload.checksum_algorithm.as_deref().map(checksum_algorithm),
                checksum_type: upload.checksum_type.clone().map(ChecksumType::from),
            })
            .collect();
        let output = ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix.map(encode),
            delimiter: input.delimiter.map(encode),
            key_marker: input.key_marker.map(encode),
            upload_id_marker: input.upload_id_marker,
            next_key_marker: page.next_key_marker.map(encode),
            next_upload_id_marker: page.next_upload_id_marker.map(|id| id.simple().to_string()),
            max_uploads: Some(max.min(crate::engine::MAX_LIST_PARTS) as i32),
            is_truncated: Some(page.truncated),
            uploads: Some(uploads),
            common_prefixes: Some(
                page.common_prefixes.into_iter().map(|p| CommonPrefix { prefix: Some(encode(p)) }).collect(),
            ),
            encoding_type: input.encoding_type,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

fn upload_id_string(upload: &UploadRecord) -> String {
    upload.upload_id.simple().to_string()
}

fn initiator() -> Initiator {
    let owner = AetherS3::owner();
    Initiator { display_name: owner.display_name, id: owner.id }
}

fn checksum_algorithm(lowercase: &str) -> ChecksumAlgorithm {
    ChecksumAlgorithm::from(lowercase.to_ascii_uppercase())
}

fn non_negative(value: i32, name: &str) -> S3Result<i32> {
    if value < 0 { Err(s3_error!(InvalidArgument, "{name} must not be negative")) } else { Ok(value) }
}

/// Validates the checksum settings of CreateMultipartUpload and fills in
/// S3's default type: `FULL_OBJECT` for CRC64NVME, `COMPOSITE` otherwise.
fn multipart_checksum_type(algorithm: Option<&str>, requested: Option<&str>) -> S3Result<Option<String>> {
    const FULL: &str = ChecksumType::FULL_OBJECT;
    const COMPOSITE: &str = ChecksumType::COMPOSITE;
    match (algorithm, requested) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(s3_error!(InvalidRequest, "x-amz-checksum-type requires x-amz-checksum-algorithm")),
        (Some(alg), _) if !MULTIPART_ALGORITHMS.contains(&alg) => {
            Err(s3_error!(InvalidRequest, "{alg} is not supported for multipart uploads"))
        }
        (Some("crc64nvme"), None) => Ok(Some(FULL.to_owned())),
        (Some(_), None) => Ok(Some(COMPOSITE.to_owned())),
        (Some(alg), Some(FULL)) if alg.starts_with("crc") => Ok(Some(FULL.to_owned())),
        (Some(alg), Some(COMPOSITE)) if alg != "crc64nvme" => Ok(Some(COMPOSITE.to_owned())),
        (Some(alg), Some(ty)) => Err(s3_error!(InvalidRequest, "checksum type {ty} is not supported with {alg}")),
    }
}

/// The completed object's checksum, derived from the parts' checksums.
/// Checksums the client lists for parts, or for the whole object, must match.
fn multipart_checksum(
    completion: &Completion,
    client_parts: Vec<CompletedPart>,
    whole_object: &Checksum,
) -> S3Result<(BTreeMap<String, String>, Option<String>)> {
    let upload = &completion.upload;
    let Some(alg) = upload.checksum_algorithm.as_deref() else { return Ok((BTreeMap::new(), None)) };
    let ty = upload.checksum_type.clone().unwrap_or_else(|| ChecksumType::COMPOSITE.to_owned());

    let mut parts = Vec::with_capacity(completion.parts.len());
    for (client, (number, part)) in client_parts.into_iter().zip(&completion.parts) {
        let Some(stored) = part.checksums.get(alg) else {
            return Err(s3_error!(InvalidPart, "part {number} has no {alg} checksum"));
        };
        if let Some(sent) = checksum_to_map(sent_checksum!(client)).get(alg)
            && sent != stored
        {
            return Err(s3_error!(InvalidPart, "the {alg} checksum of part {number} does not match"));
        }
        parts.push((decode_checksum(stored)?, part.size));
    }

    let value = if ty == ChecksumType::FULL_OBJECT { combine_crcs(alg, &parts)? } else { composite(alg, &parts)? };
    if let Some(sent) = checksum_to_map(whole_object.clone()).get(alg)
        && *sent != value
        && value.split('-').next() != Some(sent.as_str())
    {
        return Err(s3_error!(BadDigest, "the {alg} checksum you specified did not match the data"));
    }
    Ok((BTreeMap::from([(alg.to_owned(), value)]), Some(ty)))
}

fn decode_checksum(value: &str) -> S3Result<Vec<u8>> {
    STANDARD.decode(value).map_err(|_| s3_error!(InternalError, "a stored part checksum is not valid base64"))
}

/// `COMPOSITE`: the checksum of the concatenated part checksums, then `-<count>`.
fn composite(alg: &str, parts: &[(Vec<u8>, u64)]) -> S3Result<String> {
    let mut hasher = ChecksumHasher::default();
    enable_checksum(&mut hasher, alg)?;
    for (checksum, _) in parts {
        hasher.update(checksum);
    }
    let digest = checksum_to_map(hasher.finalize()).remove(alg).expect("algorithm was enabled");
    Ok(format!("{digest}-{}", parts.len()))
}

/// `FULL_OBJECT`: the parts' CRCs combined into the CRC of the whole object.
fn combine_crcs(alg: &str, parts: &[(Vec<u8>, u64)]) -> S3Result<String> {
    use crc_fast::CrcAlgorithm;
    let (algorithm, width) = match alg {
        "crc32" => (CrcAlgorithm::Crc32IsoHdlc, 4),
        "crc32c" => (CrcAlgorithm::Crc32Iscsi, 4),
        "crc64nvme" => (CrcAlgorithm::Crc64Nvme, 8),
        other => return Err(s3_error!(InvalidRequest, "{other} cannot be combined into a full-object checksum")),
    };
    let mut combined: Option<u64> = None;
    for (checksum, len) in parts {
        if checksum.len() != width {
            return Err(s3_error!(InternalError, "a stored part checksum has the wrong length"));
        }
        let crc = checksum.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
        combined = Some(match combined {
            None => crc,
            Some(acc) => crc_fast::checksum_combine(algorithm, acc, crc, *len),
        });
    }
    let bytes = combined.unwrap_or(0).to_be_bytes();
    Ok(STANDARD.encode(&bytes[8 - width..]))
}

fn completed_output(bucket: String, key: String, record: &ObjectRecord) -> CompleteMultipartUploadOutput {
    let mut output = CompleteMultipartUploadOutput {
        location: Some(format!("/{bucket}/{key}")),
        bucket: Some(bucket),
        key: Some(key),
        e_tag: Some(etag(record)),
        checksum_type: stored_checksum_type(&record.attrs),
        ..Default::default()
    };
    set_checksums!(output, checksum_from_map(&record.attrs.checksums));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc(alg: &str, data: &[u8]) -> Vec<u8> {
        let mut hasher = ChecksumHasher::default();
        enable_checksum(&mut hasher, alg).unwrap();
        hasher.update(data);
        decode_checksum(&checksum_to_map(hasher.finalize())[alg]).unwrap()
    }

    #[test]
    fn full_object_crcs_match_hashing_the_whole_object() {
        let parts: [&[u8]; 4] = [b"hello ", b"", b"multipart ", b"world"];
        let whole = parts.concat();
        for alg in ["crc32", "crc32c", "crc64nvme"] {
            let per_part: Vec<_> = parts.iter().map(|p| (crc(alg, p), p.len() as u64)).collect();
            assert_eq!(combine_crcs(alg, &per_part).unwrap(), STANDARD.encode(crc(alg, &whole)), "{alg}");
        }
    }

    #[test]
    fn composite_checksum_hashes_part_checksums() {
        let parts = vec![(crc("sha256", b"a"), 1), (crc("sha256", b"b"), 1)];
        let expected = crc("sha256", &[parts[0].0.clone(), parts[1].0.clone()].concat());
        assert_eq!(composite("sha256", &parts).unwrap(), format!("{}-2", STANDARD.encode(expected)));
    }

    #[test]
    fn checksum_type_defaults_and_validation() {
        assert_eq!(multipart_checksum_type(None, None).unwrap(), None);
        assert_eq!(multipart_checksum_type(Some("crc32"), None).unwrap().as_deref(), Some("COMPOSITE"));
        assert_eq!(multipart_checksum_type(Some("crc64nvme"), None).unwrap().as_deref(), Some("FULL_OBJECT"));
        assert_eq!(
            multipart_checksum_type(Some("crc32c"), Some("FULL_OBJECT")).unwrap().as_deref(),
            Some("FULL_OBJECT")
        );
        assert!(multipart_checksum_type(Some("sha256"), Some("FULL_OBJECT")).is_err());
        assert!(multipart_checksum_type(Some("crc64nvme"), Some("COMPOSITE")).is_err());
        assert!(multipart_checksum_type(Some("md5"), None).is_err());
        assert!(multipart_checksum_type(None, Some("COMPOSITE")).is_err());
    }
}
