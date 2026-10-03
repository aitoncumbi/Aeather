//! Encryption at rest (SSE-S3): a master key from the configuration wraps a
//! random data key per object; each block (and inline object) is encrypted
//! with a key derived from that data key, in 64 KiB AES-256-GCM frames, so a
//! range read decrypts only the frames it covers.
//!
//! Frame `i` of a block is sealed with nonce `0000 ‖ i` (big-endian) under the
//! block's own key, so nonces never repeat under one key, frames cannot be
//! reordered, and a missing frame fails authentication.

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::hkdf::{HKDF_SHA256, Salt};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{Error, Result};

/// Plaintext bytes per frame.
pub const FRAME: u64 = 64 * 1024;
/// Authentication tag appended to every frame.
pub const TAG: u64 = 16;
const KEY_LEN: usize = 32;

/// Stored size of `len` plaintext bytes: every frame (including a final
/// partial one) carries a tag.
pub fn stored_len(len: u64) -> u64 {
    len + len.div_ceil(FRAME) * TAG
}

/// A data key, sealed under a master key, as stored in object metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedKey {
    /// Fingerprint of the master key that sealed it.
    pub key_id: String,
    #[serde(with = "serde_bytes")]
    pub nonce: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub sealed: Vec<u8>,
}

/// The current master key, which seals new data keys, plus earlier ones that
/// can still open what they sealed. Debug output shows fingerprints only.
pub struct Keyring {
    current: (String, LessSafeKey),
    previous: Vec<(String, LessSafeKey)>,
    rng: SystemRandom,
}

/// First 8 bytes of SHA-256 of the key, in hex: names a key without revealing it.
pub fn fingerprint(key: &[u8]) -> String {
    hex::encode(&Sha256::digest(key)[..8])
}

fn aead_key(bytes: &[u8]) -> Result<LessSafeKey> {
    let key = UnboundKey::new(&AES_256_GCM, bytes).map_err(|_| invalid("master keys must be 32 bytes"))?;
    Ok(LessSafeKey::new(key))
}

fn invalid(msg: &str) -> Error {
    Error::InvalidArgument(msg.to_owned())
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let previous: Vec<_> = self.previous.iter().map(|(id, _)| id).collect();
        f.debug_struct("Keyring").field("current", &self.current.0).field("previous", &previous).finish()
    }
}

impl Keyring {
    /// Keys are 32 raw bytes each.
    pub fn new(current: &[u8], previous: &[Vec<u8>]) -> Result<Self> {
        let current = (fingerprint(current), aead_key(current)?);
        let previous = previous.iter().map(|k| Ok((fingerprint(k), aead_key(k)?))).collect::<Result<_>>()?;
        Ok(Self { current, previous, rng: SystemRandom::new() })
    }

    pub fn current_id(&self) -> &str {
        &self.current.0
    }

    pub fn has(&self, key_id: &str) -> bool {
        self.current.0 == key_id || self.previous.iter().any(|(id, _)| id == key_id)
    }

    /// A fresh random data key, and the same key sealed for storage.
    pub fn new_data_key(&self) -> Result<(DataKey, SealedKey)> {
        let mut key = [0u8; KEY_LEN];
        self.rng.fill(&mut key).map_err(|_| Error::Io(std::io::Error::other("no randomness")))?;
        let sealed = self.seal(&key, b"aether data key")?;
        Ok((DataKey(key), sealed))
    }

    /// Seals a small secret under the current master key. `context` is
    /// authenticated with it, so the result only opens for the same context.
    pub fn seal(&self, plaintext: &[u8], context: &[u8]) -> Result<SealedKey> {
        let mut nonce = [0u8; NONCE_LEN];
        self.rng.fill(&mut nonce).map_err(|_| Error::Io(std::io::Error::other("no randomness")))?;
        let mut sealed = plaintext.to_vec();
        self.current
            .1
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::from(context), &mut sealed)
            .map_err(|_| Error::Io(std::io::Error::other("sealing failed")))?;
        Ok(SealedKey { key_id: self.current.0.clone(), nonce: nonce.to_vec(), sealed })
    }

    /// Opens what [`seal`](Self::seal) sealed with the same `context`.
    pub fn unseal(&self, sealed: &SealedKey, context: &[u8]) -> Result<Vec<u8>> {
        let key = std::iter::once(&self.current)
            .chain(&self.previous)
            .find(|(id, _)| *id == sealed.key_id)
            .map(|(_, key)| key)
            .ok_or_else(|| {
                Error::Corrupt(format!("data sealed with master key {} which is not configured", sealed.key_id))
            })?;
        let nonce =
            Nonce::try_assume_unique_for_key(&sealed.nonce).map_err(|_| Error::Corrupt("bad sealed nonce".into()))?;
        let mut buf = sealed.sealed.clone();
        let plain = key
            .open_in_place(nonce, Aad::from(context), &mut buf)
            .map_err(|_| Error::Corrupt(format!("sealed data does not open with master key {}", sealed.key_id)))?;
        Ok(plain.to_vec())
    }

    pub fn open(&self, sealed: &SealedKey) -> Result<DataKey> {
        let plain = self.unseal(sealed, b"aether data key")?;
        Ok(DataKey(plain.try_into().map_err(|_| Error::Corrupt("bad data key length".into()))?))
    }
}

/// A key the client sends with each request (SSE-C), already checked
/// against its MD5. Never stored: objects keep a salted HMAC of it instead.
pub struct CustomerKey {
    key: DataKey,
    raw: [u8; KEY_LEN],
    /// Base64 MD5 of the key, as S3 echoes it in responses.
    pub md5: String,
}

impl std::fmt::Debug for CustomerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomerKey(md5 {})", self.md5)
    }
}

impl CustomerKey {
    /// `key` and `md5` are the base64 values of the request headers.
    pub fn parse(key: &str, md5: &str) -> Result<Self> {
        use base64::Engine as _;
        use md5::Digest as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let raw: [u8; KEY_LEN] =
            b64.decode(key).ok().and_then(|k| k.try_into().ok()).ok_or_else(|| {
                invalid("The secret key was invalid for the specified algorithm: it must be 256 bits")
            })?;
        let actual = b64.encode(md5::Md5::digest(raw));
        if actual != md5 {
            return Err(invalid("The calculated MD5 hash of the key did not match the hash that was provided."));
        }
        Ok(Self { key: DataKey(raw), raw, md5: actual })
    }

    pub fn data_key(&self) -> &DataKey {
        &self.key
    }

    /// What an object stores to recognise this key later.
    pub fn check(&self) -> Result<SseCustomer> {
        let mut salt = [0u8; 16];
        SystemRandom::new().fill(&mut salt).map_err(|_| Error::Io(std::io::Error::other("no randomness")))?;
        Ok(SseCustomer {
            key_md5: self.md5.clone(),
            salt: salt.to_vec(),
            hmac: ring::hmac::sign(&self.hmac_key(), &salt).as_ref().to_vec(),
        })
    }

    fn hmac_key(&self) -> ring::hmac::Key {
        ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &self.raw)
    }
}

/// Stored with an SSE-C object: enough to recognise its key, not to recover it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseCustomer {
    pub key_md5: String,
    #[serde(with = "serde_bytes")]
    salt: Vec<u8>,
    #[serde(with = "serde_bytes")]
    hmac: Vec<u8>,
}

impl SseCustomer {
    pub fn matches(&self, key: &CustomerKey) -> bool {
        ring::hmac::verify(&key.hmac_key(), &self.salt, &self.hmac).is_ok()
    }
}

/// An object's data key, in memory only.
#[derive(Clone)]
pub struct DataKey([u8; KEY_LEN]);

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey(<redacted>)")
    }
}

impl DataKey {
    /// The cipher for one block (or an inline object), named by `id`.
    pub fn for_block(&self, id: &Uuid) -> FrameCipher {
        let prk = Salt::new(HKDF_SHA256, id.as_bytes()).extract(&self.0);
        let okm = prk.expand(&[b"aether block v1"], &AES_256_GCM).expect("output length matches AES-256");
        FrameCipher(LessSafeKey::new(UnboundKey::from(okm)))
    }
}

pub struct FrameCipher(LessSafeKey);

fn frame_nonce(index: u64) -> Nonce {
    let mut nonce = [0u8; NONCE_LEN];
    nonce[4..].copy_from_slice(&index.to_be_bytes());
    Nonce::assume_unique_for_key(nonce)
}

impl FrameCipher {
    /// Encrypts frame `index` (at most `FRAME` bytes) and appends its tag.
    pub fn seal(&self, index: u64, frame: &[u8]) -> Vec<u8> {
        let mut out = frame.to_vec();
        self.0.seal_in_place_append_tag(frame_nonce(index), Aad::empty(), &mut out).expect("frame fits AES-GCM limits");
        out
    }

    /// Decrypts one stored frame (ciphertext and tag).
    pub fn open(&self, index: u64, stored: &mut [u8]) -> std::io::Result<usize> {
        let plain = self.0.open_in_place(frame_nonce(index), Aad::empty(), stored).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("frame {index} failed authentication"))
        })?;
        Ok(plain.len())
    }

    /// Encrypts a whole buffer (an inline object) into frames.
    pub fn seal_all(&self, data: &[u8]) -> Vec<u8> {
        data.chunks(FRAME as usize).enumerate().flat_map(|(i, frame)| self.seal(i as u64, frame)).collect()
    }

    /// Decrypts a whole buffer sealed with [`seal_all`](Self::seal_all).
    pub fn open_all(&self, stored: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(stored.len());
        for (i, frame) in stored.chunks((FRAME + TAG) as usize).enumerate() {
            let mut buf = frame.to_vec();
            let n = self.open(i as u64, &mut buf)?;
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn customer_keys_are_checked_and_recognised() {
        use base64::Engine as _;
        use md5::Digest as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let (key, md5) = (b64.encode([5u8; 32]), b64.encode(md5::Md5::digest([5u8; 32])));
        let customer = CustomerKey::parse(&key, &md5).unwrap();
        assert!(CustomerKey::parse(&key, &b64.encode([0u8; 16])).is_err(), "MD5 mismatch");
        assert!(CustomerKey::parse(&b64.encode([5u8; 16]), &md5).is_err(), "not 256 bits");
        let stored = customer.check().unwrap();
        assert!(stored.matches(&customer));
        let other = CustomerKey::parse(&b64.encode([6u8; 32]), &b64.encode(md5::Md5::digest([6u8; 32]))).unwrap();
        assert!(!stored.matches(&other));
        assert!(!format!("{customer:?}").contains(&key), "debug output hides the key");
    }

    fn keyring() -> Keyring {
        Keyring::new(&[7u8; 32], &[vec![9u8; 32]]).unwrap()
    }

    #[test]
    fn data_keys_seal_and_open_under_current_and_previous_keys() {
        let ring = keyring();
        let (key, sealed) = ring.new_data_key().unwrap();
        assert_eq!(sealed.key_id, fingerprint(&[7u8; 32]));
        let opened = ring.open(&sealed).unwrap();
        assert_eq!(opened.0, key.0);

        // After rotation, the old key still opens what it sealed.
        let rotated = Keyring::new(&[8u8; 32], &[vec![7u8; 32]]).unwrap();
        assert_eq!(rotated.open(&sealed).unwrap().0, key.0);
        // Without it, opening fails clearly.
        let other = Keyring::new(&[8u8; 32], &[]).unwrap();
        assert!(other.open(&sealed).unwrap_err().to_string().contains("not configured"));
        let mut tampered = sealed.clone();
        tampered.sealed[0] ^= 1;
        assert!(ring.open(&tampered).is_err());
        assert!(Keyring::new(&[1u8; 16], &[]).is_err());
    }

    #[test]
    fn frames_round_trip_and_detect_tampering() {
        let (key, _) = keyring().new_data_key().unwrap();
        let block = Uuid::new_v4();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let stored = key.for_block(&block).seal_all(&data);
        assert_eq!(stored.len() as u64, stored_len(data.len() as u64));
        assert_ne!(&stored[..100], &data[..100]);
        assert_eq!(key.for_block(&block).open_all(&stored).unwrap(), data);
        // Another block's key, a flipped bit, or a swapped frame all fail.
        assert!(key.for_block(&Uuid::new_v4()).open_all(&stored).is_err());
        let mut flipped = stored.clone();
        flipped[70_000] ^= 1;
        assert!(key.for_block(&block).open_all(&flipped).is_err());
        let frame = (FRAME + TAG) as usize;
        let mut swapped = stored.clone();
        swapped[..frame].copy_from_slice(&stored[frame..2 * frame]);
        swapped[frame..2 * frame].copy_from_slice(&stored[..frame]);
        assert!(key.for_block(&block).open_all(&swapped).is_err());
        assert_eq!(stored_len(0), 0);
        assert_eq!(stored_len(1), 17);
        assert_eq!(stored_len(FRAME), FRAME + TAG);
        assert_eq!(stored_len(FRAME + 1), FRAME + 1 + 2 * TAG);
    }
}
