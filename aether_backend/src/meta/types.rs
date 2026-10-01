use std::collections::BTreeMap;

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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ObjectSummary>,
    pub common_prefixes: Vec<String>,
    /// Inclusive start of the next page; `Some` means the listing was truncated.
    pub next_start: Option<Vec<u8>>,
}
