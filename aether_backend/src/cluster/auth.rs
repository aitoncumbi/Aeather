//! Authentication of requests between nodes.
//!
//! Every node knows `cluster.secret`. A request carries the sending node's id,
//! the time, a random nonce and the SHA-256 of its body, plus an HMAC-SHA256
//! over those, the method and the path. The receiver checks the MAC, that the
//! time is within [`MAX_SKEW`] of its own clock, and that it has not seen the
//! nonce before, so a captured request is accepted at most once. The secret
//! proves that the sender belongs to the cluster, not which node it is: the
//! node id is what the sender says it is.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use axum::http::HeaderMap;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

pub const NODE: &str = "x-aether-node";
pub const DATE: &str = "x-aether-date";
pub const NONCE: &str = "x-aether-nonce";
pub const CONTENT_SHA256: &str = "x-aether-content-sha256";
pub const SIGNATURE: &str = "x-aether-signature";

/// How far a request's time may be from the receiver's clock.
pub const MAX_SKEW: Duration = Duration::from_secs(300);
/// Nonces remembered at most; past this, requests are refused until old ones expire.
const MAX_NONCES: usize = 1_000_000;

type HmacSha256 = Hmac<Sha256>;

/// The key requests are signed with, derived from `cluster.secret`.
#[derive(Clone)]
pub struct ClusterKey([u8; 32]);

/// What a signature covers.
pub struct Signed<'a> {
    pub method: &'a str,
    /// Path and query, as sent.
    pub target: &'a str,
    pub node: &'a str,
    /// Unix milliseconds.
    pub date: i64,
    pub nonce: &'a str,
    /// Hex SHA-256 of the body.
    pub content_sha256: &'a str,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("the request is not signed for the internal API")]
    Missing,
    #[error("the body does not match its signed hash")]
    BodyMismatch,
    #[error("the signature does not match: cluster.secret must be the same on every node")]
    BadSignature,
    #[error("the request's time is more than 5 minutes from this node's clock; check the clocks (NTP)")]
    Skew,
    #[error("the request was already received once")]
    Replayed,
    #[error("too many recent requests to check for replays")]
    Busy,
}

impl ClusterKey {
    pub fn derive(secret: &str) -> Self {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC takes any key length");
        mac.update(b"aether internal api v1");
        Self(mac.finalize().into_bytes().into())
    }

    fn mac(&self, request: &Signed<'_>) -> HmacSha256 {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(&self.0).expect("32-byte key");
        let Signed { method, target, node, date, nonce, content_sha256 } = request;
        mac.update(
            format!("AETHER-INTERNAL-V1\n{method}\n{target}\n{node}\n{date}\n{nonce}\n{content_sha256}").as_bytes(),
        );
        mac
    }

    /// The `x-aether-signature` value for `request`.
    pub fn sign(&self, request: &Signed<'_>) -> String {
        hex::encode(self.mac(request).finalize().into_bytes())
    }

    /// Checks a request whose body is in memory; returns the node id it
    /// claims to come from.
    pub fn verify(
        &self,
        method: &str,
        target: &str,
        headers: &HeaderMap,
        body: &[u8],
        now: i64,
        replays: &ReplayGuard,
    ) -> Result<String, AuthError> {
        self.verify_headers(method, target, headers, now)?.verify_body(body, now, replays)
    }

    /// The first half of [`verify`](Self::verify), before the body is read:
    /// the signature (which covers the body's hash) and the time. Requests
    /// that are not signed are refused without reading their bodies.
    pub fn verify_headers(
        &self,
        method: &str,
        target: &str,
        headers: &HeaderMap,
        now: i64,
    ) -> Result<SignedHeaders, AuthError> {
        let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).ok_or(AuthError::Missing);
        let node = header(NODE)?;
        let date: i64 = header(DATE)?.parse().map_err(|_| AuthError::Missing)?;
        let nonce = header(NONCE)?;
        let content_sha256 = header(CONTENT_SHA256)?;
        let signature = hex::decode(header(SIGNATURE)?).map_err(|_| AuthError::BadSignature)?;
        let token_ok = |s: &str, max: usize| !s.is_empty() && s.len() <= max && s.bytes().all(|b| b.is_ascii_graphic());
        if !token_ok(node, 64) || !(16..=64).contains(&nonce.len()) || !nonce.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(AuthError::Missing);
        }
        let signed = Signed { method, target, node, date, nonce, content_sha256 };
        self.mac(&signed).verify_slice(&signature).map_err(|_| AuthError::BadSignature)?;
        if date.abs_diff(now) > MAX_SKEW.as_millis() as u64 {
            return Err(AuthError::Skew);
        }
        Ok(SignedHeaders { node: node.to_owned(), nonce: nonce.to_owned(), content_sha256: content_sha256.to_owned() })
    }
}

/// A request whose headers are signed; its body is still to be checked.
pub struct SignedHeaders {
    node: String,
    nonce: String,
    content_sha256: String,
}

impl SignedHeaders {
    /// The second half of [`ClusterKey::verify`]: the body matches the signed
    /// hash, and the request was not received before. Replays are checked
    /// last, so requests that fail anything else do not use up nonces.
    pub fn verify_body(self, body: &[u8], now: i64, replays: &ReplayGuard) -> Result<String, AuthError> {
        if body_sha256(body) != self.content_sha256 {
            return Err(AuthError::BodyMismatch);
        }
        replays.check(&self.nonce, now)?;
        Ok(self.node)
    }
}

/// Hex SHA-256, as `x-aether-content-sha256` carries it.
pub fn body_sha256(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// A fresh random nonce (128 bits, hex).
pub fn new_nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Remembers the nonces of accepted requests for as long as their time is
/// within the skew window, so each request is accepted at most once.
#[derive(Default)]
pub struct ReplayGuard {
    /// Nonce → when it may be forgotten (Unix milliseconds).
    seen: Mutex<HashMap<String, i64>>,
}

impl ReplayGuard {
    fn check(&self, nonce: &str, now: i64) -> Result<(), AuthError> {
        let mut seen = self.seen.lock().expect("replay guard lock poisoned");
        if seen.len() >= MAX_NONCES {
            seen.retain(|_, forget_at| *forget_at > now);
            if seen.len() >= MAX_NONCES {
                return Err(AuthError::Busy);
            }
        }
        // The request's time is within MAX_SKEW of `now`, so it stops being
        // accepted at most 2 × MAX_SKEW from now.
        let forget_at = now + 2 * MAX_SKEW.as_millis() as i64;
        match seen.insert(nonce.to_owned(), forget_at) {
            Some(previous) if previous > now => Err(AuthError::Replayed),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    const NOW: i64 = 1_800_000_000_000;

    fn signed_headers(key: &ClusterKey, method: &str, target: &str, body: &[u8], date: i64) -> HeaderMap {
        let nonce = new_nonce();
        let content = body_sha256(body);
        let signature =
            key.sign(&Signed { method, target, node: "node-a", date, nonce: &nonce, content_sha256: &content });
        let mut headers = HeaderMap::new();
        for (name, value) in [
            (NODE, "node-a".to_owned()),
            (DATE, date.to_string()),
            (NONCE, nonce),
            (CONTENT_SHA256, content),
            (SIGNATURE, signature),
        ] {
            headers.insert(name, HeaderValue::from_str(&value).unwrap());
        }
        headers
    }

    #[test]
    fn signed_requests_are_accepted_once() {
        let key = ClusterKey::derive("a-cluster-secret-of-32-characters!");
        let guard = ReplayGuard::default();
        let headers = signed_headers(&key, "POST", "/internal/v1/x?y=1", b"body", NOW);
        assert_eq!(key.verify("POST", "/internal/v1/x?y=1", &headers, b"body", NOW + 1000, &guard).unwrap(), "node-a");
        assert_eq!(
            key.verify("POST", "/internal/v1/x?y=1", &headers, b"body", NOW + 2000, &guard),
            Err(AuthError::Replayed)
        );
    }

    #[test]
    fn anything_changed_is_refused() {
        let key = ClusterKey::derive("a-cluster-secret-of-32-characters!");
        let guard = ReplayGuard::default();
        let headers = signed_headers(&key, "POST", "/internal/v1/x", b"body", NOW);
        let verify = |method, target, headers: &HeaderMap, body: &[u8], now| {
            key.verify(method, target, headers, body, now, &guard)
        };
        assert_eq!(verify("GET", "/internal/v1/x", &headers, b"body", NOW), Err(AuthError::BadSignature));
        assert_eq!(verify("POST", "/internal/v1/y", &headers, b"body", NOW), Err(AuthError::BadSignature));
        assert_eq!(verify("POST", "/internal/v1/x", &headers, b"other", NOW), Err(AuthError::BodyMismatch));
        let skew = MAX_SKEW.as_millis() as i64;
        assert_eq!(verify("POST", "/internal/v1/x", &headers, b"body", NOW + skew + 1), Err(AuthError::Skew));
        assert_eq!(verify("POST", "/internal/v1/x", &headers, b"body", NOW - skew - 1), Err(AuthError::Skew));

        let mut tampered = headers.clone();
        tampered.insert(NODE, HeaderValue::from_static("node-b"));
        assert_eq!(verify("POST", "/internal/v1/x", &tampered, b"body", NOW), Err(AuthError::BadSignature));
        let mut tampered = headers.clone();
        tampered.insert(DATE, HeaderValue::from_str(&(NOW + 1).to_string()).unwrap());
        assert_eq!(verify("POST", "/internal/v1/x", &tampered, b"body", NOW), Err(AuthError::BadSignature));
        let mut unsigned = headers.clone();
        unsigned.remove(SIGNATURE);
        assert_eq!(verify("POST", "/internal/v1/x", &unsigned, b"body", NOW), Err(AuthError::Missing));

        let other = ClusterKey::derive("another-secret-also-32-characters!");
        assert_eq!(
            other.verify("POST", "/internal/v1/x", &headers, b"body", NOW, &guard),
            Err(AuthError::BadSignature)
        );
        // None of those used up the nonce.
        assert!(verify("POST", "/internal/v1/x", &headers, b"body", NOW).is_ok());
    }

    #[test]
    fn nonces_are_forgotten_once_their_requests_expire() {
        let guard = ReplayGuard::default();
        guard.check("00112233445566778899aabbccddeeff", NOW).unwrap();
        let later = NOW + 2 * MAX_SKEW.as_millis() as i64 + 1;
        guard.check("00112233445566778899aabbccddeeff", later).unwrap();
    }
}
