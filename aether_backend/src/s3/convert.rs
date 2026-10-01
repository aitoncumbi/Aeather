//! Conversions between engine types and s3s DTOs.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::ops::Range;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use s3s::checksum::ChecksumHasher;
use s3s::dto::{self, Checksum, ChecksumAlgorithm, ETag, ETagCondition, Timestamp};
use s3s::{BodySizeLimitExceeded, S3Error, S3ErrorCode, S3Result, s3_error};
use tracing::error;

use crate::error::Error;
use crate::meta::{Millis, ObjectRecord, WriteCondition, prefix_successor};

pub fn s3_error(e: Error) -> S3Error {
    match e {
        Error::NoSuchBucket => S3Error::new(S3ErrorCode::NoSuchBucket),
        Error::BucketAlreadyExists => S3Error::new(S3ErrorCode::BucketAlreadyOwnedByYou),
        Error::BucketNotEmpty => S3Error::new(S3ErrorCode::BucketNotEmpty),
        Error::NoSuchKey => S3Error::new(S3ErrorCode::NoSuchKey),
        Error::PreconditionFailed => S3Error::new(S3ErrorCode::PreconditionFailed),
        Error::InvalidBucketName(name) => {
            S3Error::with_message(S3ErrorCode::InvalidBucketName, format!("invalid bucket name: {name}"))
        }
        Error::InvalidArgument(msg) => S3Error::with_message(S3ErrorCode::InvalidArgument, msg),
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

pub fn etag(record: &ObjectRecord) -> ETag {
    ETag::Strong(record.etag.clone())
}

/// Versioning arrives in v2; until then only the implicit `null` version exists.
pub fn check_version_id(version_id: Option<&str>) -> S3Result<()> {
    match version_id {
        None | Some("null") => Ok(()),
        Some(_) => Err(s3_error!(NoSuchVersion)),
    }
}

/// S3 supports `If-None-Match: *` and `If-Match: <etag>` on writes.
pub fn write_condition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<WriteCondition> {
    match (if_match, if_none_match) {
        (None, None) => Ok(WriteCondition::None),
        (None, Some(cond)) if cond.is_any() => Ok(WriteCondition::IfNoneMatchAny),
        (Some(ETagCondition::ETag(etag)), None) => Ok(WriteCondition::IfMatch(etag.value().to_owned())),
        _ => Err(s3_error!(NotImplemented, "only If-None-Match: * or If-Match: <etag> are supported on writes")),
    }
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
            return Err(s3_error!(NotModified));
        }
    } else if let Some(ts) = if_modified_since
        && modified <= seconds(ts)
    {
        return Err(s3_error!(NotModified));
    }
    Ok(())
}

/// Objects are single-part until multipart upload exists, so only part 1 is valid.
pub fn check_part_number(part_number: Option<i32>) -> S3Result<()> {
    match part_number {
        None | Some(1) => Ok(()),
        Some(_) => {
            let mut e = S3Error::with_message(
                S3ErrorCode::Custom("InvalidPartNumber".into()),
                "the requested part number is not satisfiable",
            );
            e.set_status_code(axum::http::StatusCode::RANGE_NOT_SATISFIABLE);
            Err(e)
        }
    }
}

/// The byte range to send and, for partial responses, the `Content-Range` value.
pub fn resolve_range(range: Option<&dto::Range>, size: u64) -> S3Result<(Range<u64>, Option<String>)> {
    match range {
        None => Ok((0..size, None)),
        Some(range) => {
            let r = range.check(size)?;
            let content_range = format!("bytes {}-{}/{}", r.start, r.end - 1, size);
            Ok((r, Some(content_range)))
        }
    }
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

pub(crate) use set_checksums;

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
        match alg.as_str() {
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
    }
    Ok(hasher)
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

#[cfg(test)]
mod tests {
    use super::*;

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
