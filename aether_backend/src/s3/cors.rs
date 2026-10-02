//! Per-bucket CORS, as in S3: the rules set with `PutBucketCors` decide which
//! browser origins may call the S3 API on a bucket.
//!
//! `s3s` handles the three configuration calls but not the browser side, so
//! [`layer`] answers `OPTIONS` preflights and adds `Access-Control-*` headers
//! to responses. CORS only lets a browser *read* responses; every request
//! still needs a valid signature.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use s3s::dto::{CORSConfiguration, CORSRule};
use s3s::{S3Result, s3_error};

use crate::engine::Engine;
use crate::meta::CorsRule;
use crate::observe::RequestFacts;

/// S3's limit on rules per bucket.
const MAX_RULES: usize = 100;
const METHODS: [&str; 5] = ["GET", "PUT", "POST", "DELETE", "HEAD"];

/// Validates a `PutBucketCors` configuration the way S3 does.
pub fn rules_from_s3(config: CORSConfiguration) -> S3Result<Vec<CorsRule>> {
    let rules = config.cors_rules;
    if rules.is_empty() || rules.len() > MAX_RULES {
        return Err(s3_error!(MalformedXML, "a CORS configuration has between 1 and {MAX_RULES} rules"));
    }
    rules
        .into_iter()
        .map(|rule| {
            if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() {
                return Err(s3_error!(MalformedXML, "every CORS rule needs an AllowedOrigin and an AllowedMethod"));
            }
            if let Some(method) = rule.allowed_methods.iter().find(|m| !METHODS.contains(&m.as_str())) {
                return Err(s3_error!(
                    InvalidRequest,
                    "Found unsupported HTTP method in CORS config. Unsupported method is {method}"
                ));
            }
            let headers = rule.allowed_headers.unwrap_or_default();
            for value in rule.allowed_origins.iter().chain(&headers) {
                if value.matches('*').count() > 1 {
                    return Err(s3_error!(
                        InvalidRequest,
                        "AllowedOrigin and AllowedHeader values may contain at most one wildcard: {value}"
                    ));
                }
            }
            let max_age_secs = match rule.max_age_seconds {
                Some(secs) => {
                    Some(u32::try_from(secs).map_err(|_| s3_error!(InvalidArgument, "MaxAgeSeconds is negative"))?)
                }
                None => None,
            };
            Ok(CorsRule {
                id: rule.id,
                allowed_origins: rule.allowed_origins,
                allowed_methods: rule.allowed_methods,
                allowed_headers: headers.iter().map(|h| h.to_ascii_lowercase()).collect(),
                expose_headers: rule.expose_headers.unwrap_or_default(),
                max_age_secs,
            })
        })
        .collect()
}

pub fn rules_to_s3(rules: Vec<CorsRule>) -> Vec<CORSRule> {
    rules
        .into_iter()
        .map(|rule| CORSRule {
            id: rule.id,
            allowed_origins: rule.allowed_origins,
            allowed_methods: rule.allowed_methods,
            allowed_headers: (!rule.allowed_headers.is_empty()).then_some(rule.allowed_headers),
            expose_headers: (!rule.expose_headers.is_empty()).then_some(rule.expose_headers),
            max_age_seconds: rule.max_age_secs.map(|s| i32::try_from(s).unwrap_or(i32::MAX)),
        })
        .collect()
}

/// `pattern` with at most one `*`, which matches any run of characters.
fn wildcard(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((prefix, suffix)) => {
            value.len() >= prefix.len() + suffix.len() && value.starts_with(prefix) && value.ends_with(suffix)
        }
    }
}

/// The first rule that allows `origin` to use `method` and send `headers`
/// (lowercased), as S3 evaluates them: in order, first match wins.
fn matching<'a>(rules: &'a [CorsRule], origin: &str, method: &str, headers: &[String]) -> Option<&'a CorsRule> {
    rules.iter().find(|rule| {
        rule.allowed_origins.iter().any(|o| wildcard(o, origin))
            && rule.allowed_methods.iter().any(|m| m == method)
            && headers.iter().all(|h| rule.allowed_headers.iter().any(|p| wildcard(p, h)))
    })
}

/// The bucket a path-style request addresses, if any.
fn bucket_of(path: &str) -> Option<&str> {
    let name = path.strip_prefix('/')?.split('/').next()?;
    // Empty: the service root. `_`: the health endpoints, not a bucket.
    (!name.is_empty() && !name.starts_with('_')).then_some(name)
}

pub async fn layer(State(engine): State<Arc<Engine>>, req: Request, next: Next) -> Response {
    let Some(bucket) = bucket_of(req.uri().path()).map(str::to_owned) else {
        return next.run(req).await;
    };
    if req.method() == Method::OPTIONS {
        if let Some(facts) = req.extensions().get::<RequestFacts>() {
            facts.record_operation("CorsPreflight");
        }
        return preflight(&engine, &bucket, req.headers()).await;
    }
    let Some(origin) = header_str(req.headers(), header::ORIGIN).map(str::to_owned) else {
        return next.run(req).await;
    };
    let method = req.method().as_str().to_owned();
    let rules = engine.get_bucket(&bucket).await.map(|b| b.cors).unwrap_or_default();
    let mut response = next.run(req).await;
    if rules.is_empty() {
        return response;
    }
    let headers = response.headers_mut();
    headers.append(header::VARY, HeaderValue::from_static("Origin"));
    if let Some(rule) = matching(&rules, &origin, &method, &[]) {
        allow(headers, rule, &origin);
    }
    response
}

async fn preflight(engine: &Engine, bucket: &str, request: &HeaderMap) -> Response {
    let (Some(origin), Some(method)) =
        (header_str(request, header::ORIGIN), header_str(request, header::ACCESS_CONTROL_REQUEST_METHOD))
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "Insufficient information. Origin and Access-Control-Request-Method headers needed.",
        );
    };
    let asked: Vec<String> = header_str(request, header::ACCESS_CONTROL_REQUEST_HEADERS)
        .map(|v| v.split(',').map(|h| h.trim().to_ascii_lowercase()).filter(|h| !h.is_empty()).collect())
        .unwrap_or_default();
    let rules = engine.get_bucket(bucket).await.map(|b| b.cors).unwrap_or_default();
    let Some(rule) = matching(&rules, origin, method, &asked) else {
        return error(StatusCode::FORBIDDEN, "AccessForbidden", "CORSResponse: This CORS request is not allowed.");
    };

    let mut response = Response::new(Body::empty());
    let headers = response.headers_mut();
    allow(headers, rule, origin);
    if !asked.is_empty() {
        insert(headers, header::ACCESS_CONTROL_ALLOW_HEADERS, &asked.join(", "));
    }
    if let Some(secs) = rule.max_age_secs {
        insert(headers, header::ACCESS_CONTROL_MAX_AGE, &secs.to_string());
    }
    for name in ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method"] {
        headers.append(header::VARY, HeaderValue::from_static(name));
    }
    response
}

/// The headers a matching rule grants, on preflights and actual responses.
fn allow(headers: &mut HeaderMap, rule: &CorsRule, origin: &str) {
    if rule.allowed_origins.iter().any(|o| o == "*") {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    } else {
        insert(headers, header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
    }
    insert(headers, header::ACCESS_CONTROL_ALLOW_METHODS, &rule.allowed_methods.join(", "));
    if !rule.expose_headers.is_empty() {
        insert(headers, header::ACCESS_CONTROL_EXPOSE_HEADERS, &rule.expose_headers.join(", "));
    }
}

fn insert(headers: &mut HeaderMap, name: header::HeaderName, value: &str) {
    // Values come from validated rules or the request's own headers.
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn header_str(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{message}</Message></Error>"
    );
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .expect("static response")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(origins: &[&str], methods: &[&str], headers: &[&str]) -> CorsRule {
        CorsRule {
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_methods: methods.iter().map(|s| s.to_string()).collect(),
            allowed_headers: headers.iter().map(|s| s.to_string()).collect(),
            ..CorsRule::default()
        }
    }

    #[test]
    fn wildcards_match_one_run_of_characters() {
        assert!(wildcard("*", "https://app.example.com"));
        assert!(wildcard("https://*.example.com", "https://app.example.com"));
        assert!(!wildcard("https://*.example.com", "https://example.com"));
        assert!(!wildcard("https://*.example.com", "http://app.example.com"));
        assert!(wildcard("x-amz-*", "x-amz-date"));
        assert!(wildcard("https://app.example.com", "https://app.example.com"));
        assert!(!wildcard("https://app.example.com", "https://app.example.com.evil.net"));
    }

    #[test]
    fn the_first_rule_allowing_origin_method_and_headers_wins() {
        let rules = [
            rule(&["https://app.example.com"], &["GET"], &[]),
            rule(&["https://*.example.com"], &["GET", "PUT"], &["content-type", "x-amz-*"]),
        ];
        let headers = |hs: &[&str]| hs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(matching(&rules, "https://app.example.com", "GET", &[]), Some(&rules[0]));
        assert_eq!(matching(&rules, "https://app.example.com", "PUT", &headers(&["x-amz-date"])), Some(&rules[1]));
        assert_eq!(matching(&rules, "https://app.example.com", "GET", &headers(&["authorization"])), None);
        assert_eq!(matching(&rules, "https://app.example.com", "DELETE", &[]), None);
        assert_eq!(matching(&rules, "https://other.net", "GET", &[]), None);
    }

    #[test]
    fn requests_are_mapped_to_buckets() {
        assert_eq!(bucket_of("/photos/2026/a.jpg"), Some("photos"));
        assert_eq!(bucket_of("/photos"), Some("photos"));
        assert_eq!(bucket_of("/"), None);
        assert_eq!(bucket_of("/_aether/health/live"), None);
    }

    #[test]
    fn configurations_are_validated() {
        let config = |rules: Vec<CORSRule>| CORSConfiguration { cors_rules: rules };
        let s3_rule = |origins: &[&str], methods: &[&str]| CORSRule {
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_methods: methods.iter().map(|s| s.to_string()).collect(),
            allowed_headers: Some(vec!["Content-Type".into()]),
            ..CORSRule::default()
        };
        let rules = rules_from_s3(config(vec![s3_rule(&["*"], &["GET", "PUT"])])).unwrap();
        assert_eq!(rules[0].allowed_headers, ["content-type"]);
        assert!(rules_from_s3(config(vec![])).is_err());
        assert!(rules_from_s3(config(vec![s3_rule(&["*"], &["PATCH"])])).is_err());
        assert!(rules_from_s3(config(vec![s3_rule(&["https://*.*.com"], &["GET"])])).is_err());
        assert!(rules_from_s3(config(vec![s3_rule(&[], &["GET"])])).is_err());
    }
}
