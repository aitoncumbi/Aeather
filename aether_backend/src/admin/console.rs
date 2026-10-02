//! Serves the web console, a static Next.js export embedded at build time
//! (see `build.rs`), from the admin listener. Being on the same origin as the
//! admin API, it needs no CORS and can sign in with a SameSite cookie.

use axum::body::Body;
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

mod assets {
    include!(concat!(env!("OUT_DIR"), "/console_assets.rs"));
}

/// The export inlines small scripts and styles, hence `'unsafe-inline'`;
/// everything else must come from this origin.
const CSP: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; \
                   img-src 'self' data: blob:; font-src 'self'; connect-src 'self'; object-src 'none'; \
                   base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

const NOT_BUILT: &str = include_str!("console_not_built.html");

/// Whether this binary was built with the console.
pub fn embedded() -> bool {
    !assets::ASSETS.is_empty()
}

fn find(path: &str) -> Option<&'static [u8]> {
    let i = assets::ASSETS.binary_search_by(|(p, _)| p.cmp(&path)).ok()?;
    Some(assets::ASSETS[i].1)
}

/// `GET` and `HEAD` of any path outside `/api/`.
pub async fn serve(method: Method, uri: Uri) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if !embedded() {
        return page(StatusCode::OK, "text/html; charset=utf-8", NOT_BUILT.as_bytes(), false);
    }
    let Some(decoded) = percent_decode(uri.path()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let path = decoded.trim_start_matches('/');
    // Pages are exported as `name/index.html` (trailingSlash), so `/keys`,
    // `/keys/` and `/keys/index.html` all work.
    let candidates = [
        path.to_owned(),
        format!(
            "{}index.html",
            if path.is_empty() || path.ends_with('/') { path.to_owned() } else { format!("{path}/") }
        ),
        format!("{path}.html"),
    ];
    for candidate in &candidates {
        if candidate.ends_with('/') || candidate.contains("..") {
            continue;
        }
        if let Some(bytes) = find(candidate) {
            let immutable = candidate.starts_with("_next/static/");
            return page(StatusCode::OK, content_type(candidate), bytes, immutable);
        }
    }
    match find("404.html") {
        Some(bytes) => page(StatusCode::NOT_FOUND, "text/html; charset=utf-8", bytes, false),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Decodes `%XX` escapes (the export's file names include characters such
/// as `!` and `(` that some clients escape). `None` for invalid UTF-8.
fn percent_decode(path: &str) -> Option<String> {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = path.get(i + 1..i + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).ok()
}

fn page(status: StatusCode, content_type: &'static str, bytes: &'static [u8], immutable: bool) -> Response {
    let mut response = (status, Body::from(bytes)).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if immutable { "public, max-age=31536000, immutable" } else { "no-cache" }),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP));
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("same-origin"));
    response
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("txt") => "text/plain; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("map") => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assets_are_sorted_for_lookup() {
        assert!(assets::ASSETS.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn paths_are_percent_decoded() {
        assert_eq!(percent_decode("/a/__next.%21KGN%28x%29.txt").as_deref(), Some("/a/__next.!KGN(x).txt"));
        assert_eq!(percent_decode("/plain").as_deref(), Some("/plain"));
        assert_eq!(percent_decode("/bad%zz").as_deref(), Some("/bad%zz"));
        assert_eq!(percent_decode("/%ff"), None);
    }

    #[test]
    fn content_types_by_extension() {
        assert_eq!(content_type("_next/static/chunks/app.js"), "text/javascript; charset=utf-8");
        assert_eq!(content_type("keys/index.html"), "text/html; charset=utf-8");
        assert_eq!(content_type("README"), "application/octet-stream");
    }
}
