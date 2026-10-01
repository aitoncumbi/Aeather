use std::io;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by the storage engine.
///
/// The S3 gateway maps these onto S3 error codes; the admin API onto HTTP statuses.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the specified bucket does not exist")]
    NoSuchBucket,

    #[error("the bucket already exists")]
    BucketAlreadyExists,

    #[error("the bucket is not empty")]
    BucketNotEmpty,

    #[error("the specified key does not exist")]
    NoSuchKey,

    #[error("a precondition did not hold")]
    PreconditionFailed,

    #[error("the specified multipart upload does not exist")]
    NoSuchUpload,

    #[error("invalid part: {0}")]
    InvalidPart(String),

    #[error("parts must be listed in ascending order")]
    InvalidPartOrder,

    #[error("part {0} is smaller than the minimum part size")]
    EntityTooSmall(u32),

    #[error("the access key does not exist")]
    NoSuchAccessKey,

    #[error("an access key with this id already exists")]
    AccessKeyExists,

    /// A concurrent change invalidated the request; retrying may succeed.
    #[error("conflicting concurrent operation: {0}")]
    Conflict(String),

    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("reading the request body failed: {0}")]
    Body(#[source] BoxError),

    #[error("metadata store error: {0}")]
    Meta(#[from] redb::Error),

    #[error("corrupt metadata: {0}")]
    Corrupt(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

// redb splits its errors by operation; they all convert into `redb::Error`.
macro_rules! from_redb {
    ($($ty:ty),*) => {$(
        impl From<$ty> for Error {
            fn from(e: $ty) -> Self {
                Error::Meta(e.into())
            }
        }
    )*};
}

from_redb!(redb::DatabaseError, redb::TransactionError, redb::TableError, redb::StorageError, redb::CommitError);
