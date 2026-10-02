//! Virtual-hosted-style addressing: `bucket.s3.example.com/key` as well as
//! path-style `s3.example.com/bucket/key`.
//!
//! A request whose host is a subdomain of a configured domain addresses that
//! bucket; every other host (the domain itself, an IP address, `localhost`,
//! any other name) is path-style, as before. Unlike `s3s`'s parsers, unknown
//! hosts are never taken as bucket names and never rejected, so adding a
//! domain cannot break clients that reach the server another way.

use s3s::S3Result;
use s3s::host::{S3Host, VirtualHost};

#[derive(Clone, Debug, Default)]
pub struct Domains(Vec<String>);

impl Domains {
    pub fn new(domains: &[String]) -> Self {
        Self(domains.iter().map(|d| d.trim_end_matches('.').to_ascii_lowercase()).collect())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The bucket named by `host`, if it is a subdomain of a configured domain.
    pub fn bucket_in_host(&self, host: &str) -> Option<String> {
        let name = strip_port(host).to_ascii_lowercase();
        self.0.iter().find_map(|domain| {
            let bucket = name.strip_suffix(domain.as_str())?.strip_suffix('.')?;
            (!bucket.is_empty()).then(|| bucket.to_owned())
        })
    }

    /// The bucket a request addresses: from the host, or else the first path segment.
    pub fn bucket_of(&self, host: Option<&str>, path: &str) -> Option<String> {
        if let Some(bucket) = host.and_then(|h| self.bucket_in_host(h)) {
            return Some(bucket);
        }
        let name = path.strip_prefix('/')?.split('/').next()?;
        // Empty: the service root. `_`: the health endpoints, not a bucket.
        (!name.is_empty() && !name.starts_with('_')).then(|| name.to_owned())
    }
}

impl S3Host for Domains {
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>> {
        Ok(match self.bucket_in_host(host) {
            Some(bucket) => VirtualHost::new(host).with_bucket(bucket),
            None => VirtualHost::new(host),
        })
    }
}

/// `host` without a `:port` suffix; IPv6 literals keep their brackets.
fn strip_port(host: &str) -> &str {
    match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) && !name.ends_with(':') => name,
        _ => host,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subdomains_of_configured_domains_name_buckets() {
        let d = Domains::new(&["s3.example.com".into(), "storage.internal.".into()]);
        assert_eq!(d.bucket_in_host("photos.s3.example.com"), Some("photos".into()));
        assert_eq!(d.bucket_in_host("Photos.S3.Example.com:9000"), Some("photos".into()));
        assert_eq!(d.bucket_in_host("logs.2026.storage.internal"), Some("logs.2026".into()));
        assert_eq!(d.bucket_in_host("s3.example.com"), None);
        assert_eq!(d.bucket_in_host("evils3.example.com"), None);
        assert_eq!(d.bucket_in_host("127.0.0.1:9000"), None);
        assert_eq!(d.bucket_in_host("[::1]:9000"), None);
        assert_eq!(d.bucket_in_host("localhost"), None);
    }

    #[test]
    fn requests_are_mapped_to_buckets() {
        let d = Domains::new(&["s3.test".into()]);
        assert_eq!(d.bucket_of(Some("web.s3.test"), "/a/b.txt"), Some("web".into()));
        assert_eq!(d.bucket_of(Some("s3.test"), "/photos/2026/a.jpg"), Some("photos".into()));
        assert_eq!(d.bucket_of(None, "/photos"), Some("photos".into()));
        assert_eq!(d.bucket_of(Some("s3.test"), "/"), None);
        assert_eq!(d.bucket_of(Some("s3.test"), "/_aether/health/live"), None);
    }

    #[test]
    fn ports_are_stripped() {
        assert_eq!(strip_port("example.com:9000"), "example.com");
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("[::1]:9000"), "[::1]");
        assert_eq!(strip_port("[::1]"), "[::1]");
    }
}
