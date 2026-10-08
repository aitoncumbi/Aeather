//! Clustering (v3), being built in steps (docs/CLUSTERING.md). So far: the
//! internal API that nodes use to talk to each other, its request
//! authentication, and a client for it.

pub mod api;
pub mod auth;
pub mod client;

pub use self::api::{ClusterSettings, NodeHealth};
pub use self::auth::ClusterKey;
pub use self::client::ClusterClient;
