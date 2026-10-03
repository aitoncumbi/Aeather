//! Access keys and their permissions.
//!
//! Keys are read on every S3 request, so the engine keeps all of them in
//! memory and reloads the cache after each change it makes.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use super::{Engine, now_millis};
use crate::crypto::{Keyring, SealedKey};
use crate::error::{Error, Result};
use crate::meta::{AccessKeyRecord, BucketGrant, BucketRecord, Command, KeyUpdate, MetaStore, Outcome};

const KEY_ID_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// Settings for a new access key.
#[derive(Clone, Debug, Default)]
pub struct NewKey {
    pub name: String,
    pub admin: bool,
    pub can_create_buckets: bool,
    pub buckets: BTreeMap<String, BucketGrant>,
    /// Import existing credentials (e.g. when migrating from another store)
    /// instead of generating new ones.
    pub access_key_id: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Default)]
pub(super) struct KeyCache {
    keys: RwLock<Arc<HashMap<String, AccessKeyRecord>>>,
    /// Serializes reloads, so the last reload to finish saw the latest commit.
    reload: Mutex<()>,
}

impl KeyCache {
    /// Loads every key, with sealed secrets decrypted.
    pub(super) fn load(meta: &MetaStore, cache: &KeyCache, keyring: Option<&Keyring>) -> Result<()> {
        let _guard = cache.reload.lock().expect("key cache lock poisoned");
        let mut keys = HashMap::new();
        for mut key in meta.list_keys()? {
            if let Some(sealed) = &key.sealed_secret {
                let ring = keyring.ok_or_else(|| {
                    Error::Corrupt(format!("access key secrets are sealed with master key {}", sealed.key_id))
                })?;
                let secret = ring.unseal(sealed, key.access_key_id.as_bytes())?;
                key.secret_key =
                    String::from_utf8(secret).map_err(|_| Error::Corrupt("sealed secret is not UTF-8".into()))?;
            }
            keys.insert(key.access_key_id.clone(), key);
        }
        *cache.keys.write().expect("key cache lock poisoned") = Arc::new(keys);
        Ok(())
    }
}

/// The secret sealed for storage, bound to the key's id.
fn seal_secret(ring: &Keyring, key: &AccessKeyRecord) -> Result<SealedKey> {
    ring.seal(key.secret_key.as_bytes(), key.access_key_id.as_bytes())
}

impl Engine {
    /// A key by id, from memory.
    pub fn access_key(&self, access_key_id: &str) -> Option<AccessKeyRecord> {
        self.keys.keys.read().expect("key cache lock poisoned").get(access_key_id).cloned()
    }

    /// Every key, ordered by id.
    pub fn access_keys(&self) -> Vec<AccessKeyRecord> {
        let keys = Arc::clone(&self.keys.keys.read().expect("key cache lock poisoned"));
        let mut out: Vec<_> = keys.values().cloned().collect();
        out.sort_by(|a, b| a.access_key_id.cmp(&b.access_key_id));
        out
    }

    pub async fn create_key(&self, new: NewKey) -> Result<AccessKeyRecord> {
        validate_key_name(&new.name)?;
        let access_key_id = match new.access_key_id {
            Some(id) => validate_access_key_id(id)?,
            None => random_key_id()?,
        };
        let secret_key = match new.secret_key {
            Some(secret) => validate_secret_key(secret)?,
            None => random_secret()?,
        };
        let key = AccessKeyRecord {
            access_key_id,
            secret_key,
            name: new.name,
            created_at: now_millis(),
            admin: new.admin,
            can_create_buckets: new.can_create_buckets,
            disabled: false,
            buckets: new.buckets.into_iter().filter(|(_, grant)| !grant.is_empty()).collect(),
            sealed_secret: None,
        };
        let mut stored = key.clone();
        if let Some(ring) = &self.config.keyring {
            stored.sealed_secret = Some(seal_secret(ring, &key)?);
            stored.secret_key.clear();
        }
        self.apply_to_keys(Command::CreateKey { key: stored }).await?;
        Ok(key)
    }

    pub async fn update_key(&self, access_key_id: &str, update: KeyUpdate) -> Result<AccessKeyRecord> {
        if let Some(name) = &update.name {
            validate_key_name(name)?;
        }
        let command = Command::UpdateKey { access_key_id: access_key_id.to_owned(), update };
        match self.apply_to_keys(command).await? {
            Outcome::Key(key) => Ok(key),
            other => unreachable!("UpdateKey returned {other:?}"),
        }
    }

    pub async fn delete_key(&self, access_key_id: &str) -> Result<()> {
        self.apply_to_keys(Command::DeleteKey { access_key_id: access_key_id.to_owned() }).await?;
        Ok(())
    }

    /// Sets a key's permissions on a bucket; an empty grant removes them.
    pub async fn set_grant(&self, access_key_id: &str, bucket: &str, grant: BucketGrant) -> Result<AccessKeyRecord> {
        let command = Command::SetGrant { access_key_id: access_key_id.to_owned(), bucket: bucket.to_owned(), grant };
        match self.apply_to_keys(command).await? {
            Outcome::Key(key) => Ok(key),
            other => unreachable!("SetGrant returned {other:?}"),
        }
    }

    /// Creates a bucket owned by `owner`, which gets full permissions on it.
    pub async fn create_bucket_for(&self, name: &str, owner: &str) -> Result<BucketRecord> {
        super::validate_bucket_name(name)?;
        let command = Command::CreateBucket {
            name: name.to_owned(),
            id: uuid::Uuid::new_v4(),
            created_at: now_millis(),
            owner: Some(owner.to_owned()),
        };
        match self.apply_to_keys(command).await? {
            Outcome::Bucket(bucket) => Ok(bucket),
            other => unreachable!("CreateBucket returned {other:?}"),
        }
    }

    /// With encryption at rest, seals secrets still stored in plaintext and
    /// re-seals those sealed under an earlier master key, so it can be retired
    /// once no object needs it. Runs at startup.
    pub(super) async fn seal_key_secrets(&self) -> Result<usize> {
        let Some(ring) = &self.config.keyring else { return Ok(0) };
        let mut sealed = Vec::new();
        for key in self.access_keys() {
            let current = key.sealed_secret.as_ref().is_some_and(|s| s.key_id == ring.current_id());
            if !current {
                sealed.push((key.access_key_id.clone(), seal_secret(ring, &key)?));
            }
        }
        let count = sealed.len();
        if count > 0 {
            self.apply_to_keys(Command::SealKeySecrets { sealed }).await?;
        }
        Ok(count)
    }

    /// Applies a command that changes keys, then refreshes the cache before
    /// returning, so the change applies to the very next request.
    pub(super) async fn apply_to_keys(&self, command: Command) -> Result<Outcome> {
        let outcome = self.apply(command).await?;
        self.reload_keys().await?;
        Ok(outcome)
    }

    async fn reload_keys(&self) -> Result<()> {
        let meta = Arc::clone(&self.meta);
        let cache = Arc::clone(&self.keys);
        let keyring = self.config.keyring.clone();
        tokio::task::spawn_blocking(move || KeyCache::load(&meta, &cache, keyring.as_deref())).await?
    }
}

fn validate_key_name(name: &str) -> Result<()> {
    if name.trim().is_empty() || name.len() > 256 {
        return Err(Error::InvalidArgument("key names must be 1 to 256 characters".into()));
    }
    Ok(())
}

/// Imported ids: 3 to 128 letters, digits, `.`, `_` or `-`.
fn validate_access_key_id(id: String) -> Result<String> {
    let valid = (3..=128).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(id)
    } else {
        Err(Error::InvalidArgument("access key ids must be 3 to 128 letters, digits, '.', '_' or '-'".into()))
    }
}

/// Imported secrets: 8 to 128 printable ASCII characters, no spaces.
fn validate_secret_key(secret: String) -> Result<String> {
    let valid = (8..=128).contains(&secret.len()) && secret.bytes().all(|b| b.is_ascii_graphic());
    if valid {
        Ok(secret)
    } else {
        Err(Error::InvalidArgument("secret keys must be 8 to 128 printable characters".into()))
    }
}

fn random_bytes(len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    getrandom::fill(&mut buf).map_err(|e| Error::Io(std::io::Error::other(format!("no system randomness: {e}"))))?;
    Ok(buf)
}

/// `AK` and 18 random upper-case letters or digits, like AWS's 20-character ids.
fn random_key_id() -> Result<String> {
    let mut id = String::from("AK");
    while id.len() < 20 {
        for byte in random_bytes(32)? {
            // Reject the top of the byte range so every symbol is equally likely.
            if id.len() < 20 && usize::from(byte) < 252 {
                id.push(char::from(KEY_ID_ALPHABET[usize::from(byte) % KEY_ID_ALPHABET.len()]));
            }
        }
    }
    Ok(id)
}

/// 240 random bits as 40 base64 characters, the length of AWS secrets.
fn random_secret() -> Result<String> {
    Ok(STANDARD.encode(random_bytes(30)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_credentials_look_like_aws_ones() {
        let id = random_key_id().unwrap();
        assert_eq!(id.len(), 20);
        assert!(id.starts_with("AK") && id.bytes().all(|b| KEY_ID_ALPHABET.contains(&b)));
        assert_ne!(id, random_key_id().unwrap());
        assert_eq!(random_secret().unwrap().len(), 40);
    }

    #[test]
    fn imported_credentials_are_validated() {
        assert!(validate_access_key_id("billing-service".into()).is_ok());
        assert!(validate_access_key_id("ab".into()).is_err());
        assert!(validate_access_key_id("has space".into()).is_err());
        assert!(validate_access_key_id("colon:no".into()).is_err());
        assert!(validate_secret_key("s3cr3t/with+chars".into()).is_ok());
        assert!(validate_secret_key("short".into()).is_err());
        assert!(validate_secret_key("has a space".into()).is_err());
    }
}
