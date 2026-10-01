//! Presigned GET URLs (AWS Signature Version 4, query-string form) for the
//! console's share links. The link is verified by the S3 API like any other
//! presigned request, so it works only while the signing key may read the
//! object.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// SigV4's limit on `X-Amz-Expires`: seven days.
pub const MAX_EXPIRES_SECS: u64 = 7 * 24 * 60 * 60;

pub struct Signer<'a> {
    pub access_key_id: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
}

/// `endpoint` is `http(s)://host[:port]`. Returns `None` if it is not.
pub fn presign_get(
    signer: &Signer<'_>,
    endpoint: &str,
    bucket: &str,
    key: &str,
    expires_secs: u64,
    now: OffsetDateTime,
) -> Option<String> {
    let (scheme, authority) = split_endpoint(endpoint)?;
    let uri = format!("/{}/{}", uri_encode(bucket, false), uri_encode(key, true));
    let signed = sign_query(signer, &authority, &uri, expires_secs, now);
    Some(format!("{scheme}://{authority}{uri}?{signed}"))
}

/// Scheme and the `Host` value clients will send (default ports dropped).
fn split_endpoint(endpoint: &str) -> Option<(&str, String)> {
    let (scheme, rest) = endpoint.trim_end_matches('/').split_once("://")?;
    if !matches!(scheme, "http" | "https") || rest.is_empty() || rest.contains(['/', '?', '#', '@']) {
        return None;
    }
    let default_port = if scheme == "https" { ":443" } else { ":80" };
    Some((scheme, rest.strip_suffix(default_port).unwrap_or(rest).to_ascii_lowercase()))
}

/// The query string of a presigned GET of `uri` (already encoded), ending in
/// `X-Amz-Signature`.
fn sign_query(signer: &Signer<'_>, host: &str, uri: &str, expires_secs: u64, now: OffsetDateTime) -> String {
    let date = format!("{:04}{:02}{:02}", now.year(), u8::from(now.month()), now.day());
    let amz_date = format!("{date}T{:02}{:02}{:02}Z", now.hour(), now.minute(), now.second());
    let scope = format!("{date}/{}/s3/aws4_request", signer.region);
    let credential = format!("{}/{scope}", signer.access_key_id);
    // Already in the sorted order SigV4 requires.
    let query = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
        ("X-Amz-Credential", credential),
        ("X-Amz-Date", amz_date.clone()),
        ("X-Amz-Expires", expires_secs.to_string()),
        ("X-Amz-SignedHeaders", "host".to_owned()),
    ]
    .iter()
    .map(|(name, value)| format!("{name}={}", uri_encode(value, false)))
    .collect::<Vec<_>>()
    .join("&");

    let canonical_request = format!("GET\n{uri}\n{query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex::encode(Sha256::digest(canonical_request)));
    let mut key = hmac(format!("AWS4{}", signer.secret_key).as_bytes(), date.as_bytes());
    for part in [signer.region, "s3", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
    format!("{query}&X-Amz-Signature={signature}")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// SigV4 URI encoding: everything but unreserved characters (and `/` in
/// paths) is percent-encoded, as UTF-8 bytes.
fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example in AWS's "Authenticating Requests: Using Query
    /// Parameters" documentation.
    #[test]
    fn matches_the_aws_documentation_example() {
        let signer = Signer {
            access_key_id: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
        };
        let now = time::Date::from_calendar_date(2013, time::Month::May, 24).unwrap().midnight().assume_utc();
        let query = sign_query(&signer, "examplebucket.s3.amazonaws.com", "/test.txt", 86400, now);
        assert!(
            query.ends_with("X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"),
            "{query}"
        );
    }

    #[test]
    fn keys_are_encoded_and_default_ports_dropped() {
        let signer = Signer { access_key_id: "AK", secret_key: "s", region: "us-east-1" };
        let now = OffsetDateTime::UNIX_EPOCH;
        let url = presign_get(&signer, "https://S3.example.com:443/", "bkt", "a b/ü+.txt", 60, now).unwrap();
        assert!(url.starts_with("https://s3.example.com/bkt/a%20b/%C3%BC%2B.txt?X-Amz-Algorithm="), "{url}");
        assert!(url.contains("X-Amz-Credential=AK%2F19700101%2Fus-east-1%2Fs3%2Faws4_request"));
        assert!(presign_get(&signer, "http://host:9000", "b", "k", 60, now).unwrap().starts_with("http://host:9000/"));
        assert_eq!(presign_get(&signer, "ftp://host", "b", "k", 60, now), None);
        assert_eq!(presign_get(&signer, "https://host/prefix", "b", "k", 60, now), None);
    }
}
