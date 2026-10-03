//! Conversions between engine types and s3s DTOs.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::ops::Range;

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use s3s::checksum::ChecksumHasher;
use s3s::dto::{self, Checksum, ChecksumAlgorithm, ETag, ETagCondition, Timestamp, TimestampFormat};
use s3s::{BodySizeLimitExceeded, S3Error, S3ErrorCode, S3Result, s3_error};
use tracing::error;

use crate::error::Error;
use crate::meta::{DeleteCondition, Millis, ObjectAttrs, ObjectRecord, WriteCondition, prefix_successor};

pub fn s3_error(e: Error) -> S3Error {
    match e {
        Error::NoSuchBucket => S3Error::new(S3ErrorCode::NoSuchBucket),
        Error::BucketAlreadyExists => S3Error::new(S3ErrorCode::BucketAlreadyOwnedByYou),
        Error::BucketNotEmpty => S3Error::new(S3ErrorCode::BucketNotEmpty),
        Error::NoSuchKey => S3Error::new(S3ErrorCode::NoSuchKey),
        Error::NoSuchVersion => S3Error::new(S3ErrorCode::NoSuchVersion),
        Error::MethodNotAllowed(msg) => S3Error::with_message(S3ErrorCode::MethodNotAllowed, msg),
        // 400 for a wrong key too, as Ceph and the s3-tests expect.
        Error::CustomerKey(problem) => S3Error::with_message(S3ErrorCode::InvalidRequest, problem.to_string()),
        Error::PreconditionFailed => S3Error::new(S3ErrorCode::PreconditionFailed),
        Error::InvalidBucketName(name) => {
            S3Error::with_message(S3ErrorCode::InvalidBucketName, format!("invalid bucket name: {name}"))
        }
        Error::InvalidArgument(msg) => S3Error::with_message(S3ErrorCode::InvalidArgument, msg),
        // Not an AWS error (S3 has no quotas); the code Ceph's S3 server uses.
        Error::QuotaExceeded(msg) => {
            let mut e = S3Error::with_message(S3ErrorCode::Custom("QuotaExceeded".into()), msg);
            e.set_status_code(StatusCode::FORBIDDEN);
            e
        }
        Error::NoSuchUpload => S3Error::new(S3ErrorCode::NoSuchUpload),
        Error::InvalidPart(msg) => S3Error::with_message(S3ErrorCode::InvalidPart, msg),
        Error::InvalidPartOrder => S3Error::new(S3ErrorCode::InvalidPartOrder),
        Error::EntityTooSmall(part) => S3Error::with_message(
            S3ErrorCode::EntityTooSmall,
            format!("part {part} is smaller than the minimum allowed size; only the last part may be smaller"),
        ),
        Error::Conflict(msg) => S3Error::with_message(S3ErrorCode::OperationAborted, msg),
        Error::BodyTimeout => S3Error::with_message(
            S3ErrorCode::RequestTimeout,
            "your socket connection to the server was not read from or written to within the timeout period",
        ),
        // The body stream fails with s3s errors (bad chunk signature, size limit)
        // or I/O errors when the client goes away.
        Error::Body(source) => match source.downcast::<S3Error>() {
            Ok(e) => *e,
            Err(source) if source.is::<BodySizeLimitExceeded>() => S3Error::new(S3ErrorCode::EntityTooLarge),
            Err(source) => S3Error::with_message(S3ErrorCode::IncompleteBody, source.to_string()),
        },
        other => {
            error!(error = %other, "internal error");
            S3Error::internal_error(other)
        }
    }
}

pub fn timestamp(millis: Millis) -> Timestamp {
    let nanos = i128::from(millis) * 1_000_000;
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap_or(time::OffsetDateTime::UNIX_EPOCH).into()
}

pub fn millis(ts: &Timestamp) -> Millis {
    (time::OffsetDateTime::from(ts.clone()).unix_timestamp_nanos() / 1_000_000) as Millis
}

pub fn etag(record: &ObjectRecord) -> ETag {
    ETag::Strong(record.etag.clone())
}

/// Versioning arrives in v2; until then only the implicit `null` version exists.
/// A `versionId` from a request: `null`, or one of ours.
pub fn parse_version(version_id: Option<&str>) -> S3Result<Option<crate::meta::VersionRef>> {
    match version_id.filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => crate::meta::VersionRef::parse(v)
            .map(Some)
            .ok_or_else(|| s3_error!(InvalidArgument, "Invalid version id specified")),
    }
}

/// `x-amz-version-id` for a response: set for versions written while
/// versioning was enabled; absent for `null` versions.
pub fn version_id_of(record: &crate::meta::ObjectRecord) -> Option<String> {
    record.versioned.then(|| record.version().to_string())
}

/// The error for reading a delete marker: 404 when it is the current version,
/// 405 when asked for by version id, with `x-amz-delete-marker: true`.
pub fn delete_marker_error(record: &crate::meta::ObjectRecord, by_version: bool) -> S3Error {
    let mut e = if by_version {
        S3Error::with_message(
            S3ErrorCode::MethodNotAllowed,
            "The specified method is not allowed against this resource.",
        )
    } else {
        S3Error::new(S3ErrorCode::NoSuchKey)
    };
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-delete-marker", HeaderValue::from_static("true"));
    if let Ok(v) = HeaderValue::from_str(&record.version().to_string()) {
        headers.insert("x-amz-version-id", v);
    }
    e.set_headers(headers);
    e
}

/// `If-Match` or `If-None-Match` on a write (one of them, not both).
pub fn write_condition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<WriteCondition> {
    match (if_match, if_none_match) {
        (None, None) => Ok(WriteCondition::None),
        (Some(ETagCondition::Any), None) => Ok(WriteCondition::IfMatchAny),
        (Some(ETagCondition::ETag(etag)), None) => Ok(WriteCondition::IfMatch(etag.value().to_owned())),
        (None, Some(ETagCondition::Any)) => Ok(WriteCondition::IfNoneMatchAny),
        (None, Some(ETagCondition::ETag(etag))) => Ok(WriteCondition::IfNoneMatch(etag.value().to_owned())),
        (Some(_), Some(_)) => Err(s3_error!(InvalidArgument, "If-Match and If-None-Match cannot be combined")),
    }
}

/// `If-Match`, `x-amz-if-match-last-modified-time` and `x-amz-if-match-size`
/// on a delete. `If-Match: *` adds nothing: deleting a missing key succeeds anyway.
pub fn delete_condition(
    if_match: Option<&ETagCondition>,
    last_modified: Option<&Timestamp>,
    size: Option<i64>,
) -> S3Result<DeleteCondition> {
    let size = size.map(u64::try_from).transpose().map_err(|_| s3_error!(InvalidArgument, "negative size"))?;
    Ok(DeleteCondition {
        etag: if_match.and_then(ETagCondition::as_etag).map(|etag| etag.value().to_owned()),
        last_modified: last_modified.map(millis),
        size,
    })
}

/// HTTP conditional-read rules (RFC 9110 §13.2.2), as S3 applies them to GET and HEAD.
pub fn check_read_conditions(
    record: &ObjectRecord,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<()> {
    let matches = |cond: &ETagCondition| match cond {
        ETagCondition::Any => true,
        ETagCondition::ETag(etag) => etag.value() == record.etag,
    };
    // HTTP dates have one-second resolution.
    let modified = record.last_modified.div_euclid(1000);
    let seconds = |ts: &Timestamp| time::OffsetDateTime::from(ts.clone()).unix_timestamp();

    if let Some(cond) = if_match {
        if !matches(cond) {
            return Err(s3_error!(PreconditionFailed));
        }
    } else if let Some(ts) = if_unmodified_since
        && modified > seconds(ts)
    {
        return Err(s3_error!(PreconditionFailed));
    }
    if let Some(cond) = if_none_match {
        if matches(cond) {
            return Err(not_modified(record));
        }
    } else if let Some(ts) = if_modified_since
        && modified <= seconds(ts)
    {
        return Err(not_modified(record));
    }
    Ok(())
}

/// A 304 carries the validators the client compares against (RFC 9110 §15.4.5).
fn not_modified(record: &ObjectRecord) -> S3Error {
    let mut error = s3_error!(NotModified);
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&format!("\"{}\"", record.etag)) {
        headers.insert(header::ETAG, value);
    }
    let mut date = Vec::new();
    if timestamp(record.last_modified).format(TimestampFormat::HttpDate, &mut date).is_ok()
        && let Ok(value) = HeaderValue::from_bytes(&date)
    {
        headers.insert(header::LAST_MODIFIED, value);
    }
    error.set_headers(headers);
    error
}

/// What a GET or HEAD returns: a byte range, or one part of a multipart object.
pub struct Selection {
    pub range: Range<u64>,
    /// Set for partial responses (206).
    pub content_range: Option<String>,
    /// Set when a part was requested from a multipart object.
    pub parts_count: Option<i32>,
}

pub fn select(record: &ObjectRecord, part_number: Option<i32>, range: Option<&dto::Range>) -> S3Result<Selection> {
    let size = record.size;
    let content_range = |r: &Range<u64>| (r.start < r.end).then(|| format!("bytes {}-{}/{}", r.start, r.end - 1, size));
    match (part_number, range) {
        (None, None) => Ok(Selection { range: 0..size, content_range: None, parts_count: None }),
        (None, Some(range)) => {
            let r = range.check(size)?;
            Ok(Selection { content_range: content_range(&r), range: r, parts_count: None })
        }
        (Some(_), Some(_)) => Err(s3_error!(InvalidRequest, "cannot specify both Range and partNumber")),
        // A single-part object is its own part 1.
        (Some(1), None) if record.part_sizes.is_empty() => {
            Ok(Selection { range: 0..size, content_range: None, parts_count: None })
        }
        (Some(n), None) => {
            let index = usize::try_from(n).ok().filter(|&n| n >= 1 && n <= record.part_sizes.len());
            let Some(index) = index else {
                let mut e = S3Error::with_message(
                    S3ErrorCode::Custom("InvalidPartNumber".into()),
                    "the requested part number is not satisfiable",
                );
                e.set_status_code(axum::http::StatusCode::RANGE_NOT_SATISFIABLE);
                return Err(e);
            };
            let start: u64 = record.part_sizes[..index - 1].iter().sum();
            let r = start..start + record.part_sizes[index - 1];
            Ok(Selection {
                content_range: content_range(&r),
                range: r,
                parts_count: Some(record.part_sizes.len() as i32),
            })
        }
    }
}

/// Only the STANDARD storage class exists.
pub fn check_storage_class(class: Option<&dto::StorageClass>) -> S3Result<()> {
    match class {
        Some(class) if class.as_str() != dto::StorageClass::STANDARD => Err(s3_error!(InvalidStorageClass)),
        _ => Ok(()),
    }
}

/// Parses `x-amz-copy-source-range` (`bytes=first-last`) against the source size.
pub fn copy_source_range(range: Option<&str>, size: u64) -> S3Result<Range<u64>> {
    let Some(range) = range else { return Ok(0..size) };
    let parsed = range
        .strip_prefix("bytes=")
        .and_then(|r| r.split_once('-'))
        .and_then(|(first, last)| Some((first.trim().parse::<u64>().ok()?, last.trim().parse::<u64>().ok()?)));
    match parsed {
        Some((first, last)) if first <= last && last < size => Ok(first..last + 1),
        Some((first, last)) if first <= last => {
            Err(s3_error!(InvalidRange, "the copy source range is outside the source object ({size} bytes)"))
        }
        _ => Err(s3_error!(InvalidArgument, "invalid copy source range {range:?}")),
    }
}

pub fn parse_upload_id(upload_id: &str) -> S3Result<uuid::Uuid> {
    uuid::Uuid::try_parse(upload_id).map_err(|_| s3_error!(NoSuchUpload))
}

pub fn part_number(n: i32) -> S3Result<u32> {
    u32::try_from(n)
        .ok()
        .filter(|n| (1..=crate::engine::MAX_PARTS).contains(n))
        .ok_or_else(|| s3_error!(InvalidArgument, "part numbers must be between 1 and {}", crate::engine::MAX_PARTS))
}

/// `aws-chunked` describes how the request body was framed, not the object,
/// so it is not stored (S3 drops it too).
pub fn stored_content_encoding(encoding: Option<String>) -> Option<String> {
    let encoding = encoding?;
    let kept: Vec<&str> = encoding
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    (!kept.is_empty()).then(|| kept.join(", "))
}

/// Attributes for a new object from request headers.
macro_rules! object_attrs {
    ($input:ident) => {
        $crate::meta::ObjectAttrs {
            content_type: $input.content_type,
            content_encoding: $crate::s3::convert::stored_content_encoding($input.content_encoding),
            content_disposition: $input.content_disposition,
            content_language: $input.content_language,
            cache_control: $input.cache_control,
            expires: $input.expires,
            user_metadata: $input.metadata.map(std::collections::BTreeMap::from_iter).unwrap_or_default(),
            checksums: std::collections::BTreeMap::new(),
            checksum_type: None,
            tags: $crate::s3::tags::from_header($input.tagging.as_deref())?,
        }
    };
}

/// The `x-amz-checksum-*` values of a request (or a `CompletedPart`).
macro_rules! sent_checksum {
    ($input:ident) => {
        s3s::dto::Checksum {
            checksum_crc32: $input.checksum_crc32,
            checksum_crc32c: $input.checksum_crc32c,
            checksum_crc64nvme: $input.checksum_crc64nvme,
            checksum_md5: $input.checksum_md5,
            checksum_sha1: $input.checksum_sha1,
            checksum_sha256: $input.checksum_sha256,
            checksum_sha512: $input.checksum_sha512,
            checksum_xxhash3: $input.checksum_xxhash3,
            checksum_xxhash64: $input.checksum_xxhash64,
            checksum_xxhash128: $input.checksum_xxhash128,
            checksum_type: None,
        }
    };
}

pub(crate) use {object_attrs, sent_checksum};

/// The checksum type reported with stored checksums: whatever a multipart
/// upload recorded, otherwise `FULL_OBJECT`.
pub fn stored_checksum_type(attrs: &ObjectAttrs) -> Option<dto::ChecksumType> {
    if attrs.checksums.is_empty() {
        return None;
    }
    let ty = attrs.checksum_type.clone().unwrap_or_else(|| dto::ChecksumType::FULL_OBJECT.to_owned());
    Some(dto::ChecksumType::from(ty))
}

// ---- checksums ----

/// Calls `$mac!` with every (algorithm, s3s field) pair.
macro_rules! for_each_checksum {
    ($mac:ident) => {
        $mac! {
            crc32 => checksum_crc32,
            crc32c => checksum_crc32c,
            crc64nvme => checksum_crc64nvme,
            md5 => checksum_md5,
            sha1 => checksum_sha1,
            sha256 => checksum_sha256,
            sha512 => checksum_sha512,
            xxhash3 => checksum_xxhash3,
            xxhash64 => checksum_xxhash64,
            xxhash128 => checksum_xxhash128,
        }
    };
}

/// Copies stored checksums into a DTO that only has the five checksums S3
/// supports for copies (`CopyObjectResult`, `CopyPartResult`).
macro_rules! set_copy_checksums {
    ($out:expr, $checksum:expr) => {{
        let checksum: s3s::dto::Checksum = $checksum;
        $out.checksum_crc32 = checksum.checksum_crc32;
        $out.checksum_crc32c = checksum.checksum_crc32c;
        $out.checksum_crc64nvme = checksum.checksum_crc64nvme;
        $out.checksum_sha1 = checksum.checksum_sha1;
        $out.checksum_sha256 = checksum.checksum_sha256;
    }};
}

/// Copies stored checksums into the checksum fields of an output DTO.
macro_rules! set_checksums {
    ($out:expr, $checksum:expr) => {{
        let checksum: s3s::dto::Checksum = $checksum;
        $out.checksum_crc32 = checksum.checksum_crc32;
        $out.checksum_crc32c = checksum.checksum_crc32c;
        $out.checksum_crc64nvme = checksum.checksum_crc64nvme;
        $out.checksum_md5 = checksum.checksum_md5;
        $out.checksum_sha1 = checksum.checksum_sha1;
        $out.checksum_sha256 = checksum.checksum_sha256;
        $out.checksum_sha512 = checksum.checksum_sha512;
        $out.checksum_xxhash3 = checksum.checksum_xxhash3;
        $out.checksum_xxhash64 = checksum.checksum_xxhash64;
        $out.checksum_xxhash128 = checksum.checksum_xxhash128;
    }};
}

pub(crate) use {set_checksums, set_copy_checksums};

pub fn checksum_to_map(checksum: Checksum) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    macro_rules! collect {
        ($($name:ident => $field:ident,)*) => {
            $( if let Some(value) = checksum.$field { map.insert(stringify!($name).to_owned(), value); } )*
        };
    }
    for_each_checksum!(collect);
    map
}

pub fn checksum_from_map(map: &BTreeMap<String, String>) -> Checksum {
    let mut checksum = Checksum::default();
    macro_rules! fill {
        ($($name:ident => $field:ident,)*) => {
            $( checksum.$field = map.get(stringify!($name)).cloned(); )*
        };
    }
    for_each_checksum!(fill);
    checksum
}

/// Hashers for every algorithm the client asked for: a value in a header, the
/// `x-amz-sdk-checksum-algorithm` header, or a checksum announced in `x-amz-trailer`.
pub fn checksum_hasher(
    sent: &Checksum,
    algorithm: Option<&ChecksumAlgorithm>,
    trailer: Option<&str>,
) -> S3Result<ChecksumHasher> {
    let mut requested: Vec<String> = checksum_to_map(sent.clone()).into_keys().collect();
    if let Some(alg) = algorithm {
        requested.push(alg.as_str().to_ascii_lowercase());
    }
    for name in trailer.unwrap_or_default().split(',') {
        if let Some(alg) = name.trim().to_ascii_lowercase().strip_prefix("x-amz-checksum-") {
            requested.push(alg.to_owned());
        }
    }

    let mut hasher = ChecksumHasher::default();
    for alg in requested {
        enable_checksum(&mut hasher, &alg)?;
    }
    Ok(hasher)
}

/// Turns on one algorithm (lowercase name) in a hasher.
pub fn enable_checksum(hasher: &mut ChecksumHasher, algorithm: &str) -> S3Result<()> {
    match algorithm {
        "crc32" => hasher.crc32 = Some(Default::default()),
        "crc32c" => hasher.crc32c = Some(Default::default()),
        "crc64nvme" => hasher.crc64nvme = Some(Default::default()),
        "md5" => hasher.md5 = Some(Default::default()),
        "sha1" => hasher.sha1 = Some(Default::default()),
        "sha256" => hasher.sha256 = Some(Default::default()),
        "sha512" => hasher.sha512 = Some(Default::default()),
        "xxhash3" => hasher.xxhash3 = Some(Default::default()),
        "xxhash64" => hasher.xxhash64 = Some(Default::default()),
        "xxhash128" => hasher.xxhash128 = Some(Default::default()),
        other => return Err(s3_error!(InvalidRequest, "unsupported checksum algorithm: {other}")),
    }
    Ok(())
}

/// Adds checksum values sent as aws-chunked trailers to those sent as headers.
pub fn add_trailer_checksums(sent: &mut Checksum, trailers: &axum::http::HeaderMap) -> S3Result<()> {
    macro_rules! read {
        ($($name:ident => $field:ident,)*) => {$(
            if let Some(value) = trailers.get(concat!("x-amz-checksum-", stringify!($name))) {
                let value = value.to_str().map_err(|_| s3_error!(InvalidArgument, "invalid trailing checksum"))?;
                sent.$field = Some(value.to_owned());
            }
        )*};
    }
    for_each_checksum!(read);
    Ok(())
}

/// Every checksum the client sent must equal the one computed over the body.
/// Returns the computed checksums, which are stored with the object.
pub fn verify_checksums(computed: Checksum, sent: &Checksum) -> S3Result<BTreeMap<String, String>> {
    let computed = checksum_to_map(computed);
    for (alg, value) in checksum_to_map(sent.clone()) {
        if computed.get(&alg) != Some(&value) {
            return Err(s3_error!(BadDigest, "the {alg} checksum you specified did not match the data"));
        }
    }
    Ok(computed)
}

pub fn verify_content_md5(content_md5: Option<&str>, md5: &[u8; 16]) -> S3Result<()> {
    let Some(content_md5) = content_md5 else { return Ok(()) };
    let sent = STANDARD.decode(content_md5).map_err(|_| s3_error!(InvalidDigest))?;
    if sent.len() != 16 {
        return Err(s3_error!(InvalidDigest));
    }
    if sent != md5 {
        return Err(s3_error!(BadDigest, "the Content-MD5 you specified did not match the data"));
    }
    Ok(())
}

// ---- listing ----

/// `max-keys` defaults to and is capped at 1000.
pub fn max_keys(max_keys: Option<i32>) -> S3Result<usize> {
    match max_keys {
        None => Ok(crate::engine::MAX_LIST_KEYS),
        Some(n) if n < 0 => Err(s3_error!(InvalidArgument, "max-keys must not be negative")),
        Some(n) => Ok((n as usize).min(crate::engine::MAX_LIST_KEYS)),
    }
}

pub fn encode_token(start: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(start)
}

pub fn decode_token(token: &str) -> S3Result<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(token).map_err(|_| s3_error!(InvalidArgument, "invalid continuation token"))
}

/// The inclusive start that skips `key` itself.
pub fn start_after(key: &str) -> Vec<u8> {
    let mut start = key.as_bytes().to_vec();
    start.push(0);
    start
}

/// Start for a v1 `marker`. A marker that is itself a common prefix (as
/// returned in `NextMarker`) skips every key under that prefix.
pub fn marker_start(marker: &str, prefix: &str, delimiter: Option<&str>) -> Vec<u8> {
    if let Some(delim) = delimiter.filter(|d| !d.is_empty())
        && let Some(rest) = marker.strip_prefix(prefix)
        && rest.find(delim).map(|i| i + delim.len()) == Some(rest.len())
        && let Some(next) = prefix_successor(marker.as_bytes())
    {
        return next;
    }
    start_after(marker)
}

pub fn wants_url_encoding(encoding_type: Option<&dto::EncodingType>) -> S3Result<bool> {
    match encoding_type.map(|e| e.as_str()) {
        None => Ok(false),
        Some(e) if e.eq_ignore_ascii_case("url") => Ok(true),
        Some(_) => Err(s3_error!(InvalidArgument, "invalid encoding-type")),
    }
}

/// Percent-encodes everything except unreserved characters and `/`.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => out.push(b as char),
            _ => write!(out, "%{b:02X}").expect("writing to a String cannot fail"),
        }
    }
    out
}

/// `x-amz-server-side-encryption` for an object encrypted at rest.
pub fn sse_of(record: &crate::meta::ObjectRecord) -> Option<dto::ServerSideEncryption> {
    record.sse.as_ref().map(|_| dto::ServerSideEncryption::from_static(dto::ServerSideEncryption::AES256))
}

/// SSE-C response headers: the algorithm and the key's MD5.
pub fn sse_customer_of(record: &crate::meta::ObjectRecord) -> (Option<String>, Option<String>) {
    match &record.sse_customer {
        Some(stored) => (Some("AES256".into()), Some(stored.key_md5.clone())),
        None => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_chunked_is_not_stored_as_content_encoding() {
        let stored = |e: &str| stored_content_encoding(Some(e.to_owned()));
        assert_eq!(stored("gzip").as_deref(), Some("gzip"));
        assert_eq!(stored("deflate, gzip").as_deref(), Some("deflate, gzip"));
        assert_eq!(stored("gzip, aws-chunked").as_deref(), Some("gzip"));
        assert_eq!(stored("aws-chunked,gzip").as_deref(), Some("gzip"));
        assert_eq!(stored("aws-chunked"), None);
        assert_eq!(stored("aws-chunked, aws-chunked"), None);
        assert_eq!(stored_content_encoding(None), None);
    }

    #[test]
    fn marker_skips_common_prefix_only_when_it_is_one() {
        assert_eq!(marker_start("photos/", "", Some("/")), b"photos0".to_vec());
        assert_eq!(marker_start("a/b/", "a/", Some("/")), b"a/b0".to_vec());
        assert_eq!(marker_start("photos/x", "", Some("/")), b"photos/x\0".to_vec());
        assert_eq!(marker_start("photos/", "", None), b"photos/\0".to_vec());
    }

    #[test]
    fn url_encoding() {
        assert_eq!(url_encode("a b+c/ü.txt"), "a%20b%2Bc/%C3%BC.txt");
    }

    #[test]
    fn checksum_maps_round_trip() {
        let checksum = Checksum {
            checksum_crc32: Some("AAAAAA==".into()),
            checksum_sha256: Some("x".into()),
            ..Default::default()
        };
        let map = checksum_to_map(checksum.clone());
        assert_eq!(map.keys().collect::<Vec<_>>(), ["crc32", "sha256"]);
        assert_eq!(checksum_from_map(&map), checksum);
    }

    #[test]
    fn verifies_sent_checksums() {
        let mut hasher = checksum_hasher(&Checksum::default(), None, Some("x-amz-checksum-crc32")).unwrap();
        hasher.update(b"hello");
        let computed = hasher.finalize();
        let good = computed.clone();
        assert_eq!(verify_checksums(computed.clone(), &good).unwrap().len(), 1);
        assert!(verify_checksums(computed.clone(), &Checksum::default()).is_ok());
        let bad = Checksum { checksum_crc32: Some("AAAAAA==".into()), ..Default::default() };
        assert!(verify_checksums(computed, &bad).is_err());
    }

    #[test]
    fn content_md5() {
        let md5: [u8; 16] = hex::decode("5d41402abc4b2a76b9719d911017c592").unwrap().try_into().unwrap();
        assert!(verify_content_md5(Some("XUFAKrxLKna5cZ2REBfFkg=="), &md5).is_ok());
        assert!(verify_content_md5(Some("AAAAAAAAAAAAAAAAAAAAAA=="), &md5).is_err());
        assert!(verify_content_md5(Some("not base64!"), &md5).is_err());
        assert!(verify_content_md5(None, &md5).is_ok());
    }
}
