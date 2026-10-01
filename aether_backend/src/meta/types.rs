use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unix time in milliseconds.
pub type Millis = i64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRecord {
    /// Prefix for this bucket's keys in the `objects` table.
    pub id: Uuid,
    pub name: String,
    pub created_at: Millis,
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
    pub len: u64,
    pub crc32c: u32,
}

/// Credentials for one application, and what they may access.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessKeyRecord {
    pub access_key_id: String,
    /// Stored in plaintext: SigV4 verification needs it. Encrypted under the
    /// master key once encryption at rest exists (v2).
    pub secret_key: String,
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
    /// `If-Match: <etag>` — only overwrite this exact object.
    IfMatch(String),
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
