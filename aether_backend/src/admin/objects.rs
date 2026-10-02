//! Object browsing for the web console: list, inspect, download, upload,
//! delete and share. Object data goes through this API, on the admin
//! listener, rather than to the S3 port with presigned URLs: the admin port is
//! often reached through an SSH tunnel, from which the S3 port is not
//! reachable, and the console then needs no CORS rules.
//!
//! Keys can hold any character, `/` included, so they travel in the query
//! string (`?key=`), not the path.

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::presign::{self, MAX_EXPIRES_SECS, Signer};
use super::{AdminState, ApiError, ApiResult, Caller, rfc3339};
use crate::error::{BoxError, Error};
use crate::meta::{ListQuery, ObjectAttrs, ObjectData, ObjectRecord, WriteCondition};

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    prefix: String,
    /// `/` to list one level of a "folder" tree; empty for a flat listing.
    #[serde(default)]
    delimiter: String,
    /// From a previous page's `continuation`.
    continuation: Option<String>,
    max_keys: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectList {
    pub prefix: String,
    pub objects: Vec<ObjectItem>,
    /// Common prefixes ("folders") one level below `prefix`.
    pub prefixes: Vec<String>,
    /// Pass back to get the next page; absent on the last page.
    pub continuation: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectItem {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectDetail {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    pub user_metadata: BTreeMap<String, String>,
    pub checksums: BTreeMap<String, String>,
    /// Number of parts for objects made by a multipart upload, else 0.
    pub parts: usize,
    /// `inline` (stored in the metadata record) or `blocks`.
    pub storage: &'static str,
    pub blocks: usize,
}

#[derive(Debug, Deserialize)]
pub struct KeyParam {
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUploadRequest {
    key: String,
    content_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadView {
    pub upload_id: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartView {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteRequest {
    key: String,
    parts: Vec<CompletedPart>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedPart {
    part_number: u32,
    etag: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareRequest {
    key: String,
    expires_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShareLink {
    pub url: String,
    pub expires_at: String,
}

fn item(key: String, record: &ObjectRecord) -> ObjectItem {
    ObjectItem { key, size: record.size, etag: record.etag.clone(), last_modified: rfc3339(record.last_modified) }
}

pub async fn list(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<ObjectList>> {
    let start = match &params.continuation {
        Some(token) => Some(URL_SAFE_NO_PAD.decode(token).map_err(|_| ApiError::bad_request("invalid continuation"))?),
        None => None,
    };
    let query = ListQuery {
        prefix: params.prefix.clone(),
        delimiter: Some(params.delimiter).filter(|d| !d.is_empty()),
        start,
        max_keys: params.max_keys.unwrap_or(crate::engine::MAX_LIST_KEYS).max(1),
    };
    let page = s.engine.list_objects(&bucket, query).await?;
    Ok(Json(ObjectList {
        prefix: params.prefix,
        objects: page
            .objects
            .into_iter()
            .map(|o| ObjectItem { key: o.key, size: o.size, etag: o.etag, last_modified: rfc3339(o.last_modified) })
            .collect(),
        prefixes: page.common_prefixes,
        continuation: page.next_start.map(|start| URL_SAFE_NO_PAD.encode(start)),
    }))
}

pub async fn head(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    Query(KeyParam { key }): Query<KeyParam>,
) -> ApiResult<Json<ObjectDetail>> {
    let record = s.engine.get_object(&bucket, &key).await?;
    let attrs = &record.attrs;
    let (storage, blocks) = match &record.data {
        ObjectData::Inline(_) => ("inline", 0),
        ObjectData::Blocks(blocks) => ("blocks", blocks.len()),
    };
    Ok(Json(ObjectDetail {
        key,
        size: record.size,
        etag: record.etag.clone(),
        last_modified: rfc3339(record.last_modified),
        content_type: attrs.content_type.clone(),
        content_encoding: attrs.content_encoding.clone(),
        content_disposition: attrs.content_disposition.clone(),
        content_language: attrs.content_language.clone(),
        cache_control: attrs.cache_control.clone(),
        expires: attrs.expires.clone(),
        user_metadata: attrs.user_metadata.clone(),
        checksums: attrs.checksums.clone(),
        parts: record.part_sizes.len(),
        storage,
        blocks,
    }))
}

/// Always an attachment, and never with the stored `Content-Type`: an
/// uploaded HTML file must not run as a page of the console's origin.
pub async fn download(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    Query(KeyParam { key }): Query<KeyParam>,
) -> ApiResult<Response> {
    let record = s.engine.get_object(&bucket, &key).await?;
    let body = Body::from_stream(s.engine.read_object(&record, 0..record.size));
    let name = key.rsplit('/').next().filter(|n| !n.is_empty()).unwrap_or("download");
    let mut response = body.into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(record.size));
    headers.insert(header::CONTENT_DISPOSITION, attachment(name));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("sandbox"));
    if let Ok(etag) = HeaderValue::from_str(&format!("\"{}\"", record.etag)) {
        headers.insert(header::ETAG, etag);
    }
    Ok(response)
}

/// `attachment; filename="…"; filename*=UTF-8''…` (RFC 6266).
fn attachment(name: &str) -> HeaderValue {
    let ascii: String =
        name.chars().map(|c| if c.is_ascii_graphic() && c != '"' && c != '\\' || c == ' ' { c } else { '_' }).collect();
    let mut encoded = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    HeaderValue::from_str(&format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}"))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment"))
}

pub async fn delete(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    Query(KeyParam { key }): Query<KeyParam>,
) -> ApiResult<StatusCode> {
    s.engine.delete_object(&bucket, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Uploads a whole object in one request. The console uses multipart
/// uploads for anything large.
pub async fn put(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    Query(KeyParam { key }): Query<KeyParam>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<(StatusCode, Json<ObjectItem>)> {
    let max = s.settings.max_object_size;
    check_length(&headers, max)?;
    let attrs = ObjectAttrs { content_type: content_type(&headers), ..Default::default() };
    let record = s.engine.put_object(&bucket, &key, limited(body, max), attrs, WriteCondition::None).await?;
    Ok((StatusCode::CREATED, Json(item(key, &record))))
}

pub async fn create_upload(
    State(s): State<AdminState>,
    Path(bucket): Path<String>,
    body: Result<Json<CreateUploadRequest>, JsonRejection>,
) -> ApiResult<(StatusCode, Json<UploadView>)> {
    let Json(req) = body?;
    s.engine.get_bucket(&bucket).await?;
    let attrs = ObjectAttrs { content_type: req.content_type.filter(|t| !t.is_empty()), ..Default::default() };
    let upload = s.engine.create_upload(&bucket, &req.key, attrs, None, None).await?;
    Ok((StatusCode::CREATED, Json(UploadView { upload_id: upload.upload_id.to_string(), key: upload.key })))
}

pub async fn upload_part(
    State(s): State<AdminState>,
    Path((bucket, upload_id, part_number)): Path<(String, String, u32)>,
    Query(KeyParam { key }): Query<KeyParam>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Json<PartView>> {
    let upload_id = upload_id_of(&upload_id)?;
    crate::engine::validate_part_number(part_number)?;
    let max = s.settings.max_object_size;
    check_length(&headers, max)?;
    s.engine.get_upload(&bucket, &key, upload_id).await?;
    let staged = s.engine.stage_part(limited(body, max)).await?;
    let part = s.engine.commit_part(&bucket, &key, upload_id, part_number, staged, BTreeMap::new()).await?;
    Ok(Json(PartView { part_number, etag: part.etag, size: part.size }))
}

pub async fn complete_upload(
    State(s): State<AdminState>,
    Path((bucket, upload_id)): Path<(String, String)>,
    body: Result<Json<CompleteRequest>, JsonRejection>,
) -> ApiResult<Json<ObjectItem>> {
    let Json(req) = body?;
    let upload_id = upload_id_of(&upload_id)?;
    let requested: Vec<(u32, String)> = req.parts.into_iter().map(|p| (p.part_number, p.etag)).collect();
    let completion = s.engine.prepare_completion(&bucket, &req.key, upload_id, &requested).await?;
    let record =
        s.engine.complete_upload(&bucket, &req.key, completion, BTreeMap::new(), None, WriteCondition::None).await?;
    Ok(Json(item(req.key, &record)))
}

pub async fn abort_upload(
    State(s): State<AdminState>,
    Path((bucket, upload_id)): Path<(String, String)>,
    Query(KeyParam { key }): Query<KeyParam>,
) -> ApiResult<StatusCode> {
    s.engine.abort_upload(&bucket, &key, upload_id_of(&upload_id)?).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// A presigned GET URL on the S3 endpoint, signed with the caller's key.
pub async fn share(
    State(s): State<AdminState>,
    Extension(caller): Extension<Caller>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ShareRequest>, JsonRejection>,
) -> ApiResult<Json<ShareLink>> {
    let Json(req) = body?;
    if !(1..=MAX_EXPIRES_SECS).contains(&req.expires_secs) {
        return Err(ApiError::bad_request(format!("expires_secs must be between 1 and {MAX_EXPIRES_SECS}")));
    }
    s.engine.get_object(&bucket, &req.key).await?;
    let secret = s.auth.secret_of(&caller.access_key_id).ok_or(Error::NoSuchAccessKey)?;
    let signer = Signer { access_key_id: &caller.access_key_id, secret_key: &secret, region: &s.settings.region };
    let endpoint = s.settings.s3_endpoint(&headers);
    let now = time::OffsetDateTime::now_utc();
    let url = presign::presign_get(&signer, &endpoint, &bucket, &req.key, req.expires_secs, now)
        .ok_or_else(|| ApiError::bad_request(format!("s3.public_url ({endpoint}) is not http(s)://host[:port]")))?;
    let expires = now + time::Duration::seconds(req.expires_secs as i64);
    let expires_at = expires.format(&time::format_description::well_known::Rfc3339).unwrap_or_default();
    Ok(Json(ShareLink { url, expires_at }))
}

fn upload_id_of(id: &str) -> Result<Uuid, Error> {
    Uuid::try_parse(id).map_err(|_| Error::NoSuchUpload)
}

fn content_type(headers: &HeaderMap) -> Option<String> {
    headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).filter(|v| !v.is_empty()).map(str::to_owned)
}

fn check_length(headers: &HeaderMap, max: u64) -> ApiResult<()> {
    let length = headers.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
    match length {
        Some(n) if n > max => Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "EntityTooLarge",
            format!("the body is larger than the {max}-byte limit (s3.max_object_size)"),
        )),
        _ => Ok(()),
    }
}

/// The body as a stream that fails once it passes `max` bytes (for requests
/// without a `Content-Length`).
fn limited(body: Body, max: u64) -> impl futures::Stream<Item = Result<bytes::Bytes, BoxError>> + Unpin {
    let mut seen = 0u64;
    body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(BoxError::from)?;
        seen += chunk.len() as u64;
        if seen > max {
            return Err(BoxError::from(format!("the body is larger than the {max}-byte limit")));
        }
        Ok(chunk)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_names_survive_any_characters() {
        assert_eq!(attachment("q3.pdf"), "attachment; filename=\"q3.pdf\"; filename*=UTF-8''q3.pdf");
        assert_eq!(
            attachment("naïve \"x\".txt"),
            "attachment; filename=\"na_ve _x_.txt\"; filename*=UTF-8''na%C3%AFve%20%22x%22.txt"
        );
    }
}
