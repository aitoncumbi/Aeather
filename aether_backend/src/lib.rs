//! Aether: S3-compatible object storage. See `docs/DESIGN.md`.

pub mod admin;
pub mod auth;
pub mod blocks;
pub mod cli;
pub mod config;
pub mod engine;
pub mod error;
pub mod http;
pub mod logger;
pub mod meta;
pub mod metrics;
pub mod observe;
pub mod s3;
pub mod server;
