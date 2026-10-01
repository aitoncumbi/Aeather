//! Who is calling, and what they may do.
//!
//! Two kinds of identity exist: the root key from the config file, which can
//! do everything, and access keys stored in metadata, each with per-bucket
//! permissions (or `admin`, which grants everything). The same identities sign
//! S3 requests and log in to the admin API.

use std::sync::Arc;

use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::{CopyObjectInput, CopySource, UploadPartCopyInput};
use s3s::path::S3Path;
use s3s::{S3Request, S3Result, s3_error};
use subtle::ConstantTimeEq;

use crate::engine::Engine;
use crate::meta::{AccessKeyRecord, BucketGrant};

pub struct Authorizer {
    engine: Arc<Engine>,
    root_access_key: String,
    root_secret_key: String,
}

#[derive(Clone, Debug)]
pub enum Identity {
    Root,
    Key(AccessKeyRecord),
}

impl Identity {
    /// Root and admin keys may do anything, including use the admin API.
    pub fn is_admin(&self) -> bool {
        match self {
            Identity::Root => true,
            Identity::Key(key) => key.admin,
        }
    }

    pub fn can_create_buckets(&self) -> bool {
        match self {
            Identity::Root => true,
            Identity::Key(key) => key.admin || key.can_create_buckets,
        }
    }

    /// Effective permissions on a bucket.
    pub fn grant(&self, bucket: &str) -> BucketGrant {
        match self {
            Identity::Root => BucketGrant::FULL,
            Identity::Key(key) if key.admin => BucketGrant::FULL,
            Identity::Key(key) => key.buckets.get(bucket).copied().unwrap_or_default(),
        }
    }

    /// The access key to record as a bucket's owner; `None` for root and
    /// admins, who can access every bucket anyway.
    pub fn owner_key(&self) -> Option<&str> {
        match self {
            Identity::Key(key) if !key.admin => Some(&key.access_key_id),
            _ => None,
        }
    }
}

impl Authorizer {
    pub fn new(engine: Arc<Engine>, root_access_key: String, root_secret_key: String) -> Arc<Self> {
        Arc::new(Self { engine, root_access_key, root_secret_key })
    }

    pub fn is_root_key(&self, access_key_id: &str) -> bool {
        access_key_id == self.root_access_key
    }

    /// The identity behind an access key id; `None` if unknown or disabled.
    pub fn identify(&self, access_key_id: &str) -> Option<Identity> {
        if self.is_root_key(access_key_id) {
            return Some(Identity::Root);
        }
        self.engine.access_key(access_key_id).filter(|key| !key.disabled).map(Identity::Key)
    }

    fn secret_key(&self, access_key_id: &str) -> Option<String> {
        match self.identify(access_key_id)? {
            Identity::Root => Some(self.root_secret_key.clone()),
            Identity::Key(key) => Some(key.secret_key),
        }
    }

    /// Checks a plaintext secret (admin API login) in constant time.
    pub fn authenticate(&self, access_key_id: &str, secret_key: &str) -> Option<Identity> {
        let expected = self.secret_key(access_key_id)?;
        let matches: bool = expected.as_bytes().ct_eq(secret_key.as_bytes()).into();
        if matches { self.identify(access_key_id) } else { None }
    }

    /// The identity that signed a request that s3s already authenticated.
    pub fn caller<T>(&self, req: &S3Request<T>) -> S3Result<Identity> {
        let credentials = req.credentials.as_ref().ok_or_else(|| s3_error!(AccessDenied, "Signature is required"))?;
        self.identify(&credentials.access_key).ok_or_else(|| s3_error!(InvalidAccessKeyId))
    }
}

/// What an operation requires on the bucket it addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Required {
    /// The handler decides (ListBuckets filters, CreateBucket checks rights).
    Handler,
    Read,
    Write,
    Owner,
    /// Any permission on the bucket.
    Any,
    /// Read or write: inspecting multipart uploads, which uploaders need.
    ReadOrWrite,
}

fn required(operation: &str) -> Required {
    match operation {
        "ListBuckets" | "CreateBucket" => Required::Handler,
        "HeadBucket" | "GetBucketLocation" => Required::Any,
        "ListObjects" | "ListObjectsV2" | "GetObject" | "HeadObject" => Required::Read,
        "ListMultipartUploads" | "ListParts" => Required::ReadOrWrite,
        "PutObject"
        | "DeleteObject"
        | "DeleteObjects"
        | "CopyObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "UploadPartCopy"
        | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => Required::Write,
        // Everything else (bucket configuration, mostly not implemented yet).
        _ => Required::Owner,
    }
}

fn allowed(grant: BucketGrant, required: Required) -> bool {
    match required {
        Required::Handler => true,
        Required::Read => grant.read,
        Required::Write => grant.write,
        Required::Owner => grant.owner,
        Required::Any => !grant.is_empty(),
        Required::ReadOrWrite => grant.read || grant.write,
    }
}

/// Supplies secrets for SigV4 verification.
pub struct S3Credentials(pub Arc<Authorizer>);

#[async_trait::async_trait]
impl S3Auth for S3Credentials {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.0.secret_key(access_key) {
            Some(secret) => Ok(SecretKey::from(secret)),
            None => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}

/// Enforces per-bucket permissions on every S3 request.
pub struct S3Permissions(pub Arc<Authorizer>);

impl S3Permissions {
    fn check_source(&self, identity: &Identity, source: &CopySource) -> S3Result<()> {
        match source {
            CopySource::Bucket { bucket, .. } if identity.grant(bucket).read => Ok(()),
            CopySource::Bucket { .. } => Err(s3_error!(AccessDenied, "no read access to the copy source")),
            // Rejected later as not implemented.
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl S3Access for S3Permissions {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let credentials = cx.credentials().ok_or_else(|| s3_error!(AccessDenied, "Signature is required"))?;
        let identity = self.0.identify(&credentials.access_key).ok_or_else(|| s3_error!(InvalidAccessKeyId))?;
        let required = required(cx.s3_op().name());
        let permitted = match cx.s3_path() {
            S3Path::Bucket { bucket } | S3Path::Object { bucket, .. } => allowed(identity.grant(bucket), required),
            S3Path::Root => required == Required::Handler || identity.is_admin(),
        };
        if permitted { Ok(()) } else { Err(s3_error!(AccessDenied)) }
    }

    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        let identity = self.0.caller(req)?;
        self.check_source(&identity, &req.input.copy_source)
    }

    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        let identity = self.0.caller(req)?;
        self.check_source(&identity, &req.input.copy_source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_need_the_right_permission() {
        let read = BucketGrant { read: true, ..Default::default() };
        let write = BucketGrant { write: true, ..Default::default() };
        assert!(allowed(read, required("GetObject")));
        assert!(!allowed(write, required("GetObject")));
        assert!(allowed(write, required("PutObject")));
        assert!(!allowed(read, required("DeleteObjects")));
        assert!(allowed(write, required("ListParts")));
        assert!(allowed(read, required("HeadBucket")));
        assert!(!allowed(BucketGrant::default(), required("HeadBucket")));
        assert!(!allowed(BucketGrant { read: true, write: true, owner: false }, required("DeleteBucket")));
        assert!(!allowed(read, required("PutBucketPolicy")), "unknown operations need owner");
    }
}
