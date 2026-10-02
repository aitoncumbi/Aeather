//! Object tags: the `x-amz-tagging` header and the tagging operations, with
//! S3's limits.

use std::collections::BTreeMap;

use s3s::dto::{Tag, TagSet};
use s3s::{S3Result, s3_error};

const MAX_TAGS: usize = 10;
const MAX_KEY_CHARS: usize = 128;
const MAX_VALUE_CHARS: usize = 256;

/// Tags from an `x-amz-tagging` header (`key1=value1&key2=value2`, URL-encoded).
pub fn from_header(header: Option<&str>) -> S3Result<BTreeMap<String, String>> {
    let Some(header) = header.filter(|h| !h.is_empty()) else {
        return Ok(BTreeMap::new());
    };
    let pairs: Vec<(String, String)> = form_urlencoded::parse(header.as_bytes()).into_owned().collect();
    validate(pairs)
}

/// Tags from a `PutObjectTagging` body.
pub fn from_tag_set(set: TagSet) -> S3Result<BTreeMap<String, String>> {
    validate(set.into_iter().map(|tag| (tag.key.unwrap_or_default(), tag.value.unwrap_or_default())).collect())
}

pub fn to_tag_set(tags: &BTreeMap<String, String>) -> TagSet {
    tags.iter().map(|(k, v)| Tag { key: Some(k.clone()), value: Some(v.clone()) }).collect()
}

/// `x-amz-tagging-count` for GET and HEAD: absent when there are no tags.
pub fn count(tags: &BTreeMap<String, String>) -> Option<i32> {
    (!tags.is_empty()).then_some(tags.len() as i32)
}

fn validate(pairs: Vec<(String, String)>) -> S3Result<BTreeMap<String, String>> {
    if pairs.len() > MAX_TAGS {
        return Err(s3_error!(InvalidTag, "Object tags cannot be greater than {MAX_TAGS}"));
    }
    let mut tags = BTreeMap::new();
    for (key, value) in pairs {
        if key.is_empty() || key.chars().count() > MAX_KEY_CHARS {
            return Err(s3_error!(InvalidTag, "A tag key must be 1 to {MAX_KEY_CHARS} characters"));
        }
        if value.chars().count() > MAX_VALUE_CHARS {
            return Err(s3_error!(InvalidTag, "A tag value may be at most {MAX_VALUE_CHARS} characters"));
        }
        if key.starts_with("aws:") {
            return Err(s3_error!(InvalidTag, "Tag keys starting with aws: are reserved"));
        }
        if tags.insert(key, value).is_some() {
            return Err(s3_error!(InvalidTag, "Cannot provide multiple Tags with the same key"));
        }
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_are_url_encoded_pairs() {
        let tags = from_header(Some("project=aether&owner=ops%20team&empty=")).unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags["owner"], "ops team");
        assert_eq!(tags["empty"], "");
        assert!(from_header(None).unwrap().is_empty());
        assert!(from_header(Some("")).unwrap().is_empty());
    }

    #[test]
    fn s3_limits_apply() {
        let many = (0..11).map(|i| format!("k{i}=v")).collect::<Vec<_>>().join("&");
        assert!(from_header(Some(&many)).is_err());
        assert!(from_header(Some(&format!("{}=v", "k".repeat(129)))).is_err());
        assert!(from_header(Some(&format!("k={}", "v".repeat(257)))).is_err());
        assert!(from_header(Some(&format!("{}={}", "é".repeat(128), "é".repeat(256)))).is_ok());
        assert!(from_header(Some("a=1&a=2")).is_err());
        assert!(from_header(Some("=v")).is_err());
        assert!(from_header(Some("aws:created=1")).is_err());
    }
}
