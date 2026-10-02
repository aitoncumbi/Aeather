//! Web console sign-in: an admin signs in once with a key and secret and gets
//! an HttpOnly, SameSite=Strict session cookie, so the secret never stays in
//! the browser. Sessions live in memory: a restart signs everyone out.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderValue, header};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::error::{Error, Result};

pub const COOKIE: &str = "aether_session";
/// Every request the console makes carries this header. Browsers do not let
/// other sites set custom headers on cross-origin requests without a CORS
/// preflight (which this API never approves), so requiring it on
/// cookie-authenticated writes rules out cross-site request forgery even
/// where SameSite cookies are not enforced.
pub const CONSOLE_HEADER: &str = "x-aether-console";

/// A session ends after this long without a request...
const IDLE: Duration = Duration::from_secs(12 * 60 * 60);
/// ...and this long after sign-in in any case.
const MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Old sessions are dropped first once this many exist.
const MAX_SESSIONS: usize = 10_000;

#[derive(Debug)]
struct Session {
    access_key_id: String,
    created: Instant,
    last_used: Instant,
}

impl Session {
    fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.last_used) > IDLE || now.duration_since(self.created) > MAX_AGE
    }
}

#[derive(Debug, Default)]
pub struct Sessions {
    by_token: Mutex<HashMap<String, Session>>,
}

impl Sessions {
    /// Starts a session for an access key that has already been authenticated.
    pub fn create(&self, access_key_id: &str) -> Result<String> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|e| Error::Io(std::io::Error::other(format!("no randomness: {e}"))))?;
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let now = Instant::now();
        let mut sessions = self.by_token.lock().expect("sessions lock poisoned");
        sessions.retain(|_, s| !s.expired(now));
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = sessions.iter().min_by_key(|(_, s)| s.last_used).map(|(t, _)| t.clone())
        {
            sessions.remove(&oldest);
        }
        let session = Session { access_key_id: access_key_id.to_owned(), created: now, last_used: now };
        sessions.insert(token.clone(), session);
        Ok(token)
    }

    /// The access key a live session belongs to; also marks it as used.
    pub fn resolve(&self, token: &str) -> Option<String> {
        let now = Instant::now();
        let mut sessions = self.by_token.lock().expect("sessions lock poisoned");
        let session = sessions.get_mut(token)?;
        if session.expired(now) {
            sessions.remove(token);
            return None;
        }
        session.last_used = now;
        Some(session.access_key_id.clone())
    }

    pub fn remove(&self, token: &str) {
        self.by_token.lock().expect("sessions lock poisoned").remove(token);
    }
}

/// The session token from the `Cookie` header, if any.
pub fn token(headers: &HeaderMap) -> Option<String> {
    headers.get_all(header::COOKIE).iter().filter_map(|v| v.to_str().ok()).flat_map(|v| v.split(';')).find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        (name == COOKIE && !value.is_empty()).then(|| value.to_owned())
    })
}

/// `Set-Cookie` for a new session. Without `Max-Age` it is a browser-session
/// cookie: closing the browser signs out too.
pub fn set_cookie(token: &str, secure: bool) -> HeaderValue {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!("{COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict{secure}"))
        .expect("tokens are URL-safe base64")
}

pub fn clear_cookie(secure: bool) -> HeaderValue {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!("{COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict{secure}"))
        .expect("static cookie")
}

pub fn from_console(headers: &HeaderMap) -> bool {
    headers.contains_key(CONSOLE_HEADER)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_resolve_until_removed() {
        let sessions = Sessions::default();
        let token = sessions.create("AK1").unwrap();
        assert_eq!(token.len(), 43);
        assert_eq!(sessions.resolve(&token).as_deref(), Some("AK1"));
        assert_ne!(sessions.create("AK1").unwrap(), token);
        sessions.remove(&token);
        assert_eq!(sessions.resolve(&token), None);
        assert_eq!(sessions.resolve("made-up"), None);
    }

    #[test]
    fn sessions_expire_when_idle_or_old() {
        let now = Instant::now();
        let session = Session { access_key_id: "AK1".into(), created: now, last_used: now };
        assert!(!session.expired(now + IDLE));
        assert!(session.expired(now + IDLE + Duration::from_secs(1)));
        let busy = Session { last_used: now + MAX_AGE, ..session };
        assert!(busy.expired(now + MAX_AGE + Duration::from_secs(1)));
    }

    #[test]
    fn the_session_cookie_is_found_among_others() {
        let mut headers = HeaderMap::new();
        headers.append(header::COOKIE, HeaderValue::from_static("theme=dark; aether_session=abc123"));
        assert_eq!(token(&headers).as_deref(), Some("abc123"));
        headers.insert(header::COOKIE, HeaderValue::from_static("aether_session="));
        assert_eq!(token(&headers), None);
        let cookie = set_cookie("abc", true);
        assert_eq!(cookie, "aether_session=abc; Path=/; HttpOnly; SameSite=Strict; Secure");
    }
}
