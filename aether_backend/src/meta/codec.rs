//! Value encoding: one format-version byte, then MessagePack with named fields.
//! Named fields let new `#[serde(default)]` fields be added without migrations.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

const FORMAT_V1: u8 = 1;

pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut out = vec![FORMAT_V1];
    // Writing to a Vec cannot fail, and our types have no fallible Serialize impls.
    rmp_serde::encode::write_named(&mut out, value).expect("metadata value serializes");
    out
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    match bytes.split_first() {
        Some((&FORMAT_V1, rest)) => {
            rmp_serde::from_slice(rest).map_err(|e| Error::Corrupt(format!("undecodable value: {e}")))
        }
        Some((version, _)) => Err(Error::Corrupt(format!("unknown value format {version}"))),
        None => Err(Error::Corrupt("empty value".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::types::*;
    use uuid::Uuid;

    #[test]
    fn object_record_round_trips() {
        let record = ObjectRecord {
            version_id: Uuid::now_v7(),
            versioned: true,
            delete_marker: false,
            size: 3,
            etag: "abc".into(),
            last_modified: 42,
            attrs: ObjectAttrs {
                content_type: Some("text/plain".into()),
                user_metadata: [("k".to_string(), "v".to_string())].into(),
                ..Default::default()
            },
            data: ObjectData::Blocks(vec![BlockRef { id: Uuid::new_v4(), len: 3, crc32c: 7, stored_len: None }]),
            part_sizes: vec![3],
            upload_id: Some(Uuid::now_v7()),
            sse: None,
            sse_customer: None,
            part_checksums: vec![[("crc32".to_string(), "AAAAAA==".to_string())].into()],
        };
        let back: ObjectRecord = decode(&encode(&record)).unwrap();
        assert_eq!(back, record);

        let inline = ObjectData::Inline(b"hi".to_vec());
        assert_eq!(decode::<ObjectData>(&encode(&inline)).unwrap(), inline);
    }

    #[test]
    fn missing_fields_take_defaults() {
        #[derive(Serialize)]
        struct Old {
            content_type: Option<String>,
        }
        let attrs: ObjectAttrs = decode(&encode(&Old { content_type: Some("a/b".into()) })).unwrap();
        assert_eq!(attrs.content_type.as_deref(), Some("a/b"));
        assert!(attrs.user_metadata.is_empty());
    }

    #[test]
    fn records_from_before_multipart_still_decode() {
        #[derive(Serialize)]
        struct V1Record {
            version_id: Uuid,
            size: u64,
            etag: String,
            last_modified: i64,
            attrs: ObjectAttrs,
            data: ObjectData,
        }
        let old = V1Record {
            version_id: Uuid::now_v7(),
            size: 1,
            etag: "e".into(),
            last_modified: 1,
            attrs: ObjectAttrs::default(),
            data: ObjectData::Inline(b"x".to_vec()),
        };
        let record: ObjectRecord = decode(&encode(&old)).unwrap();
        assert!(record.part_sizes.is_empty());
        assert_eq!(record.attrs.checksum_type, None);
    }

    #[test]
    fn rejects_unknown_format() {
        assert!(matches!(decode::<ObjectAttrs>(&[9, 0]), Err(Error::Corrupt(_))));
    }
}
