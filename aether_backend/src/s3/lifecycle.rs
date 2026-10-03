//! Lifecycle configurations: S3's rules, validated as S3 does, to and from
//! the stored form. The rules themselves are applied by the engine's
//! lifecycle job.

use s3s::dto::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration,
    LifecycleRule as S3Rule, LifecycleRuleAndOperator, LifecycleRuleFilter, NoncurrentVersionExpiration, Tag,
};
use s3s::{S3Result, s3_error};

use super::convert::timestamp;
use crate::meta::{LifecycleFilter, LifecycleRule, Millis};

const MAX_RULES: usize = 1000;

fn positive(value: Option<i32>, what: &str) -> S3Result<Option<u32>> {
    match value {
        None => Ok(None),
        Some(n) if n > 0 => Ok(Some(n as u32)),
        Some(_) => Err(s3_error!(InvalidArgument, "'{what}' must be a positive integer")),
    }
}

fn filter_from_s3(rule: &S3Rule) -> S3Result<LifecycleFilter> {
    let size = |n: Option<i64>| n.map(|n| u64::try_from(n).unwrap_or(0));
    let tags = |tags: &[Tag]| {
        tags.iter().map(|t| (t.key.clone().unwrap_or_default(), t.value.clone().unwrap_or_default())).collect()
    };
    let Some(filter) = &rule.filter else {
        // The deprecated top-level <Prefix>, or nothing: the whole bucket.
        return Ok(LifecycleFilter { prefix: rule.prefix.clone().unwrap_or_default(), ..Default::default() });
    };
    // AWS wants several conditions inside <And>; Ceph also takes them side by
    // side. Either way every condition given must hold.
    let LifecycleRuleFilter { and, object_size_greater_than, object_size_less_than, prefix, tag, .. } = filter;
    let mut out = LifecycleFilter {
        prefix: prefix.clone().unwrap_or_default(),
        tags: tag.as_ref().map(|t| tags(std::slice::from_ref(t))).unwrap_or_default(),
        size_greater_than: size(*object_size_greater_than),
        size_less_than: size(*object_size_less_than),
    };
    if let Some(and) = and {
        if out.prefix.is_empty() {
            out.prefix = and.prefix.clone().unwrap_or_default();
        } else if and.prefix.as_ref().is_some_and(|p| *p != out.prefix) {
            return Err(s3_error!(MalformedXML, "a lifecycle filter has one prefix"));
        }
        out.tags.extend(and.tags.as_deref().map(tags).unwrap_or_default());
        out.size_greater_than = out.size_greater_than.or(size(and.object_size_greater_than));
        out.size_less_than = out.size_less_than.or(size(and.object_size_less_than));
    }
    Ok(out)
}

pub fn rules_from_s3(config: BucketLifecycleConfiguration) -> S3Result<Vec<LifecycleRule>> {
    if config.rules.is_empty() || config.rules.len() > MAX_RULES {
        return Err(s3_error!(MalformedXML, "a lifecycle configuration has between 1 and {MAX_RULES} rules"));
    }
    let mut ids = std::collections::HashSet::new();
    config
        .rules
        .into_iter()
        .map(|rule| {
            if let Some(id) = &rule.id {
                if id.len() > 255 {
                    return Err(s3_error!(InvalidArgument, "a rule ID may be at most 255 characters"));
                }
                if !ids.insert(id.clone()) {
                    return Err(s3_error!(InvalidArgument, "rule IDs must be unique"));
                }
            }
            let enabled = match rule.status.as_str() {
                ExpirationStatus::ENABLED => true,
                ExpirationStatus::DISABLED => false,
                _ => return Err(s3_error!(MalformedXML, "a rule's Status is Enabled or Disabled")),
            };
            if rule.transitions.as_ref().is_some_and(|t| !t.is_empty())
                || rule.noncurrent_version_transitions.as_ref().is_some_and(|t| !t.is_empty())
            {
                return Err(s3_error!(NotImplemented, "transitions are not supported: Aether has one storage class"));
            }
            let filter = filter_from_s3(&rule)?;
            // S3 names rules that come without an ID.
            let id = Some(rule.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()));
            let legacy_prefix = rule.filter.is_none() && rule.prefix.is_some();
            let mut out = LifecycleRule { id, enabled, filter, legacy_prefix, ..Default::default() };
            if let Some(exp) = &rule.expiration {
                let marker = exp.expired_object_delete_marker == Some(true);
                if [exp.days.is_some(), exp.date.is_some(), marker].iter().filter(|g| **g).count() != 1 {
                    return Err(s3_error!(
                        MalformedXML,
                        "an Expiration has exactly one of Days, Date or ExpiredObjectDeleteMarker"
                    ));
                }
                out.expiration_days = positive(exp.days, "Days")?;
                if let Some(date) = &exp.date {
                    let date = time::OffsetDateTime::from(date.clone());
                    if date.time() != time::Time::MIDNIGHT || date.offset() != time::UtcOffset::UTC {
                        return Err(s3_error!(InvalidArgument, "'Date' must be at midnight UTC"));
                    }
                    out.expiration_date = Some((date.unix_timestamp_nanos() / 1_000_000) as Millis);
                }
                out.expired_object_delete_marker = marker;
                if marker && !out.filter.tags.is_empty() {
                    return Err(s3_error!(
                        InvalidArgument,
                        "ExpiredObjectDeleteMarker cannot be used with tag filters"
                    ));
                }
            }
            if let Some(nc) = &rule.noncurrent_version_expiration {
                out.noncurrent_days = positive(nc.noncurrent_days, "NoncurrentDays")?;
                out.newer_noncurrent_versions = positive(nc.newer_noncurrent_versions, "NewerNoncurrentVersions")?;
                if out.noncurrent_days.is_none() {
                    return Err(s3_error!(MalformedXML, "NoncurrentVersionExpiration needs NoncurrentDays"));
                }
            }
            if let Some(abort) = &rule.abort_incomplete_multipart_upload {
                out.abort_upload_days = positive(abort.days_after_initiation, "DaysAfterInitiation")?;
                if !out.filter.tags.is_empty() {
                    return Err(s3_error!(
                        InvalidRequest,
                        "AbortIncompleteMultipartUpload cannot be used with tag filters"
                    ));
                }
            }
            let acts = out.expiration_days.is_some()
                || out.expiration_date.is_some()
                || out.expired_object_delete_marker
                || out.noncurrent_days.is_some()
                || out.abort_upload_days.is_some();
            if !acts {
                return Err(s3_error!(InvalidRequest, "a lifecycle rule needs at least one action"));
            }
            Ok(out)
        })
        .collect()
}

pub fn rules_to_s3(rules: Vec<LifecycleRule>) -> Vec<S3Rule> {
    rules
        .into_iter()
        .map(|rule| {
            let f = rule.filter;
            let tags: Vec<Tag> = f.tags.into_iter().map(|(k, v)| Tag { key: Some(k), value: Some(v) }).collect();
            let size = |n: Option<u64>| n.map(|n| n as i64);
            let simple = tags.is_empty() && f.size_greater_than.is_none() && f.size_less_than.is_none();
            let legacy = rule.legacy_prefix && simple;
            let legacy_prefix = legacy.then(|| f.prefix.clone());
            let filter = if simple {
                LifecycleRuleFilter { prefix: Some(f.prefix), ..Default::default() }
            } else {
                LifecycleRuleFilter {
                    and: Some(LifecycleRuleAndOperator {
                        prefix: (!f.prefix.is_empty()).then_some(f.prefix),
                        tags: (!tags.is_empty()).then_some(tags),
                        object_size_greater_than: size(f.size_greater_than),
                        object_size_less_than: size(f.size_less_than),
                    }),
                    ..Default::default()
                }
            };
            let expiration =
                (rule.expiration_days.is_some() || rule.expiration_date.is_some() || rule.expired_object_delete_marker)
                    .then(|| LifecycleExpiration {
                        days: rule.expiration_days.map(|d| d as i32),
                        date: rule.expiration_date.map(timestamp),
                        expired_object_delete_marker: rule.expired_object_delete_marker.then_some(true),
                    });
            S3Rule {
                id: rule.id,
                status: ExpirationStatus::from_static(if rule.enabled {
                    ExpirationStatus::ENABLED
                } else {
                    ExpirationStatus::DISABLED
                }),
                filter: (!legacy).then_some(filter),
                expiration,
                noncurrent_version_expiration: rule.noncurrent_days.map(|d| NoncurrentVersionExpiration {
                    noncurrent_days: Some(d as i32),
                    newer_noncurrent_versions: rule.newer_noncurrent_versions.map(|n| n as i32),
                }),
                abort_incomplete_multipart_upload: rule
                    .abort_upload_days
                    .map(|d| AbortIncompleteMultipartUpload { days_after_initiation: Some(d as i32) }),
                prefix: legacy_prefix,
                transitions: None,
                noncurrent_version_transitions: None,
            }
        })
        .collect()
}
