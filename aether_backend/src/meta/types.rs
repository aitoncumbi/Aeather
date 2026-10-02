use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};

/// Unix time in milliseconds.
pub type Millis = i64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRecord {
    /// Prefix for this bucket's keys in the `objects` table.
    pub id: Uuid,
    pub name: String,
    pub created_at: Millis,
    /// Which browser origins may call the S3 API on this bucket. Empty: none.
    #[serde(default)]
    pub cors: Vec<CorsRule>,
    #[serde(default)]
    pub quota: Quota,
}

/// Limits on a bucket's usage. `None` is no limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub max_objects: Option<u64>,
}

impl Quota {
    /// Fails if a write would take usage from `before` to `after` past a
    /// limit. Writes that do not grow usage pass even when the bucket is over
    /// a limit (for example after it was lowered), so they can free space.
    pub fn check(&self, before: BucketUsage, after: BucketUsage) -> Result<()> {
        if let Some(max) = self.max_bytes
            && after.bytes > max
            && after.bytes > before.bytes
        {
            return Err(Error::QuotaExceeded(format!("the bucket's quota of {max} bytes would be exceeded")));
        }
        if let Some(max) = self.max_objects
            && after.objects > max
            && after.objects > before.objects
        {
            return Err(Error::QuotaExceeded(format!("the bucket's quota of {max} objects would be exceeded")));
        }
        Ok(())
    }
}

/// One rule of a bucket's CORS configuration, as in S3's `PutBucketCors`.
/// Origins and headers may contain one `*` wildcard.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsRule {
    #[serde(default)]
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    /// Some of GET, PUT, POST, DELETE and HEAD.
    pub allowed_methods: Vec<String>,
    /// Request headers a preflight may ask for; lowercased.
    #[serde(default)]
    pub allowed_headers: Vec<String>,
    /// Response headers scripts may read.
    #[serde(default)]
    pub expose_headers: Vec<String>,
    #[serde(default)]
    pub max_age_secs: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRecord {
    /// Unique per write. Exposed once bucket versioning exists (v2).
    pub version_id: Uuid,
    pub size: u64,
    /// Hex digest without quotes.
    pub etag: String,
    pub last_modified: Millis,
    pub attrs: ObjectAttrs,
    pub data: ObjectData,
    /// Size of each part, for objects created by a multipart upload.
    /// Empty for single-part objects.
    #[serde(default)]
    pub part_sizes: Vec<u64>,
    /// The multipart upload that created the object, so that a retried
    /// CompleteMultipartUpload can be answered after the upload is gone.
    #[serde(default)]
    pub upload_id: Option<Uuid>,
    /// The data key when the object is encrypted at rest (SSE-S3).
    #[serde(default)]
    pub sse: Option<crate::crypto::SealedKey>,
}

/// Attributes supplied by the client when the object was written.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectAttrs {
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub content_encoding: Option<String>,
    #[serde(default)]
    pub content_disposition: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default)]
    pub cache_control: Option<String>,
    /// Raw `Expires` header value; S3 stores it verbatim.
    #[serde(default)]
    pub expires: Option<String>,
    /// `x-amz-meta-*` headers, without the prefix.
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    /// Full-object checksums the client sent, keyed by lowercase algorithm
    /// name (`crc32c`, `sha256`, ...), base64-encoded as in S3 headers.
    #[serde(default)]
    pub checksums: BTreeMap<String, String>,
    /// `COMPOSITE` or `FULL_OBJECT` for multipart checksums; `None` otherwise.
    #[serde(default)]
    pub checksum_type: Option<String>,
    /// Object tags (S3 allows up to 10).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectData {
    /// Small objects live in the metadata record itself.
    Inline(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Everything else is a sequence of immutable blocks, in order.
    Blocks(Vec<BlockRef>),
}

impl ObjectData {
    pub fn blocks(&self) -> &[BlockRef] {
        match self {
            ObjectData::Inline(_) => &[],
            ObjectData::Blocks(blocks) => blocks,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRef {
    pub id: Uuid,
    /// Plaintext length.
    pub len: u64,
    /// CRC32C of the file's bytes as stored (ciphertext when encrypted).
    pub crc32c: u32,
    /// File size when encrypted (`len` plus a tag per frame); absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_len: Option<u64>,
}

impl BlockRef {
    pub fn stored_len(&self) -> u64 {
        self.stored_len.unwrap_or(self.len)
    }
}

/// Credentials for one application, and what they may access.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessKeyRecord {
    pub access_key_id: String,
    /// SigV4 verification needs the plaintext. With encryption at rest it is
    /// stored empty and kept in `sealed_secret` instead; the engine's key
    /// cache holds it decrypted.
    pub secret_key: String,
    /// The secret sealed under the master key, bound to the access key id.
    #[serde(default)]
    pub sealed_secret: Option<crate::crypto::SealedKey>,
    /// What the key is for, e.g. the application's name.
    pub name: String,
    pub created_at: Millis,
    /// Full access to every bucket and to the admin API.
    #[serde(default)]
    pub admin: bool,
    /// May create buckets; it becomes their owner.
    #[serde(default)]
    pub can_create_buckets: bool,
    /// Disabled keys are rejected but kept, so they can be re-enabled.
    #[serde(default)]
    pub disabled: bool,
    /// Bucket name → permissions. Removed when the bucket is deleted.
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketGrant>,
}

impl fmt::Debug for AccessKeyRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessKeyRecord")
            .field("access_key_id", &self.access_key_id)
            .field("name", &self.name)
            .field("admin", &self.admin)
            .field("can_create_buckets", &self.can_create_buckets)
            .field("disabled", &self.disabled)
            .field("buckets", &self.buckets)
            .finish_non_exhaustive()
    }
}

/// Permissions of one key on one bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketGrant {
    /// Get and list objects.
    #[serde(default)]
    pub read: bool,
    /// Put, copy and delete objects; multipart uploads.
    #[serde(default)]
    pub write: bool,
    /// Delete the bucket and change its settings.
    #[serde(default)]
    pub owner: bool,
}

impl BucketGrant {
    pub const FULL: Self = Self { read: true, write: true, owner: true };

    pub fn is_empty(&self) -> bool {
        !(self.read || self.write || self.owner)
    }
}

/// Changes to a key's settings; `None` leaves a field unchanged.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUpdate {
    pub name: Option<String>,
    pub admin: Option<bool>,
    pub can_create_buckets: Option<bool>,
    pub disabled: Option<bool>,
}

/// Object count and total size of a bucket, kept up to date by every write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketUsage {
    pub objects: u64,
    pub bytes: u64,
}

/// A multipart upload in progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRecord {
    /// Time-ordered (UUIDv7), so uploads of one key list oldest first.
    pub upload_id: Uuid,
    pub key: String,
    pub initiated: Millis,
    /// Attributes the completed object will have.
    pub attrs: ObjectAttrs,
    /// Checksum algorithm chosen at creation (lowercase), computed for every part.
    #[serde(default)]
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub checksum_type: Option<String>,
    /// Every part is encrypted under this data key; the object inherits it.
    #[serde(default)]
    pub sse: Option<crate::crypto::SealedKey>,
}

/// One uploaded part. Parts are never stored inline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRecord {
    pub size: u64,
    /// Hex MD5 of the part, without quotes.
    pub etag: String,
    pub last_modified: Millis,
    pub blocks: Vec<BlockRef>,
    #[serde(default)]
    pub checksums: BTreeMap<String, String>,
}

/// The fields a listing needs, without the data location or attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: Millis,
}

/// Precondition on a write, evaluated inside the commit transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteCondition {
    #[default]
    None,
    /// `If-None-Match: *` — only create, never overwrite.
    IfNoneMatchAny,
    /// `If-None-Match: <etag>` — write unless the key holds this exact object.
    IfNoneMatch(String),
    /// `If-Match: *` — only overwrite an existing object.
    IfMatchAny,
    /// `If-Match: <etag>` — only overwrite this exact object.
    IfMatch(String),
}

impl WriteCondition {
    /// `existing` is the object the write would replace.
    pub fn check(&self, existing: Option<&ObjectRecord>) -> Result<()> {
        match (self, existing) {
            (Self::IfMatchAny | Self::IfMatch(_), None) => Err(Error::NoSuchKey),
            (Self::IfNoneMatchAny, Some(_)) => Err(Error::PreconditionFailed),
            (Self::IfNoneMatch(etag), Some(prev)) if *etag == prev.etag => Err(Error::PreconditionFailed),
            (Self::IfMatch(etag), Some(prev)) if *etag != prev.etag => Err(Error::PreconditionFailed),
            _ => Ok(()),
        }
    }
}

/// Precondition on a delete. Deleting a key that does not exist succeeds
/// whatever the condition, as in S3.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteCondition {
    pub etag: Option<String>,
    /// Compared at the one-second resolution of HTTP dates.
    pub last_modified: Option<Millis>,
    pub size: Option<u64>,
}

impl DeleteCondition {
    pub fn check(&self, existing: &ObjectRecord) -> Result<()> {
        let etag = self.etag.as_ref().is_none_or(|etag| *etag == existing.etag);
        let modified =
            self.last_modified.is_none_or(|ms| ms.div_euclid(1000) == existing.last_modified.div_euclid(1000));
        let size = self.size.is_none_or(|size| size == existing.size);
        if etag && modified && size { Ok(()) } else { Err(Error::PreconditionFailed) }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListQuery {
    pub prefix: String,
    /// Empty or `None` means no grouping.
    pub delimiter: Option<String>,
    /// Inclusive lower bound on keys, as raw bytes. Built from `start-after`,
    /// a v1 marker or a continuation token.
    pub start: Option<Vec<u8>>,
    pub max_keys: usize,
}

/// One page of an upload's parts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartsPage {
    pub upload: UploadRecord,
    /// (part number, part), in ascending order.
    pub parts: Vec<(u32, PartRecord)>,
    /// More parts follow.
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UploadListQuery {
    pub prefix: String,
    pub delimiter: Option<String>,
    /// List uploads after this key (or, with `upload_id_marker`, after that upload of this key).
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<Uuid>,
    pub max_uploads: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UploadPage {
    pub uploads: Vec<UploadRecord>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<Uuid>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ObjectSummary>,
    pub common_prefixes: Vec<String>,
    /// Inclusive start of the next page; `Some` means the listing was truncated.
    pub next_start: Option<Vec<u8>>,
}
