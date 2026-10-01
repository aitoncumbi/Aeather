//! Configuration: built-in defaults, then a TOML file, then `AETHER_*`
//! environment variables (`__` separates levels, e.g. `AETHER_S3__LISTEN`).

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

use crate::engine::EngineConfig;

const DEFAULT_CONFIG_FILE: &str = "aether.toml";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub storage: StorageConfig,
    pub s3: S3Config,
    pub admin: AdminConfig,
    pub tls: TlsConfig,
    pub http: HttpConfig,
    pub gc: GcConfig,
    pub scrub: ScrubConfig,
    pub log: LogConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Holds the metadata database and block files.
    pub data_dir: PathBuf,
    /// Uploads are split into blocks of at most this many bytes.
    pub block_size: u64,
    /// Objects up to this many bytes are stored in the metadata database.
    pub inline_threshold: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    pub listen: SocketAddr,
    /// Region reported to clients. SDKs sign requests for this region.
    pub region: String,
    /// Root credentials. Required; requests are never accepted unsigned.
    pub access_key: String,
    pub secret_key: Secret,
    /// Largest single PUT, in bytes. S3's own limit is 5 GiB.
    pub max_object_size: u64,
    /// Also accept the legacy Signature Version 2 (HMAC-SHA1, deprecated by
    /// AWS). Off by default; some old clients still sign with it.
    pub allow_sig_v2: bool,
}

/// The JSON admin API (keys, permissions, buckets, usage).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    pub enabled: bool,
    /// Loopback by default; expose it deliberately (e.g. `0.0.0.0:9001` in a container).
    pub listen: SocketAddr,
}

/// HTTPS. Setting both files enables TLS on the S3 port.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert_file: Option<PathBuf>,
    /// PEM private key (PKCS#8, PKCS#1 or SEC1).
    pub key_file: Option<PathBuf>,
    /// Also serve the admin API over TLS, with the same certificate.
    pub admin: bool,
    /// How often the files are checked for changes; a renewed certificate is
    /// picked up without a restart. 0 disables.
    pub reload_secs: u64,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self { cert_file: None, key_file: None, admin: false, reload_secs: 60 }
    }
}

impl TlsConfig {
    pub fn files(&self) -> Option<(&Path, &Path)> {
        Some((self.cert_file.as_deref()?, self.key_file.as_deref()?))
    }
}

/// Connection limits for both listeners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// Time allowed to send the request headers.
    pub header_timeout_secs: u64,
    /// Time allowed to complete a TLS handshake.
    pub handshake_timeout_secs: u64,
    /// An upload that sends no data for this long is aborted.
    pub body_timeout_secs: u64,
    /// On shutdown, how long to wait for in-flight requests before closing.
    pub shutdown_timeout_secs: u64,
}

/// Background verification of stored data.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScrubConfig {
    /// Re-read every block and check its CRC32C this often. 0 disables.
    pub interval_secs: u64,
    /// Read rate limit in bytes per second, so scrubbing does not starve clients. 0: unlimited.
    pub bytes_per_sec: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GcConfig {
    /// How long blocks of deleted or overwritten objects are kept so
    /// in-progress downloads can finish.
    pub grace_secs: u64,
    pub interval_secs: u64,
    /// Unreferenced block files younger than this are never swept.
    pub orphan_grace_secs: u64,
    pub sweep_interval_secs: u64,
    /// Multipart uploads not completed within this time are aborted. 0 disables.
    pub abandoned_upload_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    /// Default filter, e.g. `info` or `aether_backend=debug,info`. `RUST_LOG` overrides it.
    pub level: String,
    pub format: LogFormat,
    /// One line per request: method, path, status, operation, access key, duration.
    pub access_log: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// A string that never appears in logs or debug output.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_empty() { "\"\"" } else { "\"***\"" })
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let engine = EngineConfig::default();
        Self { data_dir: engine.data_dir, block_size: engine.block_size, inline_threshold: engine.inline_threshold }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 9000)),
            region: "us-east-1".into(),
            access_key: String::new(),
            secret_key: Secret::default(),
            max_object_size: 5 * 1024 * 1024 * 1024,
            allow_sig_v2: false,
        }
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self { enabled: true, listen: SocketAddr::from(([127, 0, 0, 1], 9001)) }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self { header_timeout_secs: 30, handshake_timeout_secs: 10, body_timeout_secs: 60, shutdown_timeout_secs: 30 }
    }
}

impl Default for ScrubConfig {
    fn default() -> Self {
        let engine = EngineConfig::default();
        Self { interval_secs: engine.scrub_interval.as_secs(), bytes_per_sec: engine.scrub_rate }
    }
}

impl Default for GcConfig {
    fn default() -> Self {
        let engine = EngineConfig::default();
        Self {
            grace_secs: engine.gc_grace.as_secs(),
            interval_secs: engine.gc_interval.as_secs(),
            orphan_grace_secs: engine.orphan_grace.as_secs(),
            sweep_interval_secs: engine.sweep_interval.as_secs(),
            abandoned_upload_secs: engine.upload_expiry.as_secs(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: "info".into(), format: LogFormat::Text, access_log: true }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file {0} does not exist")]
    Missing(PathBuf),
    #[error(transparent)]
    Parse(#[from] Box<figment::Error>),
    #[error("{0}")]
    Invalid(String),
}

impl Config {
    /// Loads `path`, or `./aether.toml` if it exists when no path is given.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        let file = match path {
            Some(path) if !path.exists() => return Err(ConfigError::Missing(path.to_owned())),
            Some(path) => Some(path),
            // The default file is optional: settings can come from the environment alone.
            None => Some(Path::new(DEFAULT_CONFIG_FILE)).filter(|p| p.exists()),
        };
        if let Some(file) = file {
            figment = figment.merge(Toml::file_exact(file));
        }
        // These AETHER_* variables belong to the CLI, not the server config:
        // the config file path and the admin client's connection settings.
        let cli_variables =
            ["config", "admin_url", "admin_access_key", "admin_secret_key", "admin_ca_cert", "admin_insecure"];
        figment = figment.merge(Env::prefixed("AETHER_").split("__").ignore(&cli_variables));
        let config: Config = figment.extract().map_err(Box::new)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: &str| Err(ConfigError::Invalid(msg.to_owned()));
        if self.s3.access_key.len() < 3 || self.s3.secret_key.expose().len() < 8 {
            return invalid(
                "s3.access_key (3+ characters) and s3.secret_key (8+ characters) must be set; \
                 for example with AETHER_S3__ACCESS_KEY and AETHER_S3__SECRET_KEY",
            );
        }
        if self.storage.block_size < 1024 * 1024 {
            return invalid("storage.block_size must be at least 1 MiB");
        }
        if self.storage.inline_threshold as u64 > self.storage.block_size {
            return invalid("storage.inline_threshold must not exceed storage.block_size");
        }
        if self.gc.interval_secs == 0 || self.gc.sweep_interval_secs == 0 {
            return invalid("gc.interval_secs and gc.sweep_interval_secs must be positive");
        }
        if self.tls.cert_file.is_some() != self.tls.key_file.is_some() {
            return invalid("tls.cert_file and tls.key_file must be set together");
        }
        if self.tls.admin && self.tls.files().is_none() {
            return invalid("tls.admin needs tls.cert_file and tls.key_file");
        }
        let http = &self.http;
        if [http.header_timeout_secs, http.handshake_timeout_secs, http.body_timeout_secs].contains(&0) {
            return invalid("http timeouts must be positive");
        }
        Ok(())
    }

    pub fn engine_config(&self) -> EngineConfig {
        EngineConfig {
            data_dir: self.storage.data_dir.clone(),
            block_size: self.storage.block_size,
            inline_threshold: self.storage.inline_threshold,
            gc_grace: Duration::from_secs(self.gc.grace_secs),
            gc_interval: Duration::from_secs(self.gc.interval_secs),
            orphan_grace: Duration::from_secs(self.gc.orphan_grace_secs),
            sweep_interval: Duration::from_secs(self.gc.sweep_interval_secs),
            upload_expiry: Duration::from_secs(self.gc.abandoned_upload_secs),
            body_timeout: Duration::from_secs(self.http.body_timeout_secs),
            scrub_interval: Duration::from_secs(self.scrub.interval_secs),
            scrub_rate: self.scrub.bytes_per_sec,
            ..EngineConfig::default()
        }
    }
}

#[cfg(test)]
// `figment::Jail` closures return its large error type.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn file_then_env_overrides() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "custom.toml",
                r#"
                [s3]
                listen = "127.0.0.1:9100"
                access_key = "from-file"
                secret_key = "file-secret"

                [storage]
                data_dir = "/srv/aether"
                "#,
            )?;
            jail.set_env("AETHER_S3__SECRET_KEY", "env-secret");
            jail.set_env("AETHER_CONFIG", "custom.toml");

            let config = Config::load(Some(Path::new("custom.toml"))).unwrap();
            assert_eq!(config.s3.listen, "127.0.0.1:9100".parse().unwrap());
            assert_eq!(config.s3.access_key, "from-file");
            assert_eq!(config.s3.secret_key.expose(), "env-secret");
            assert_eq!(config.storage.data_dir, PathBuf::from("/srv/aether"));
            assert_eq!(config.s3.region, "us-east-1");
            assert!(!format!("{config:?}").contains("env-secret"));
            Ok(())
        });
    }

    #[test]
    fn cli_variables_do_not_break_the_server_config() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("AETHER_S3__ACCESS_KEY", "admin");
            jail.set_env("AETHER_S3__SECRET_KEY", "change-me-please");
            jail.set_env("AETHER_ADMIN_URL", "http://127.0.0.1:9001");
            jail.set_env("AETHER_ADMIN_ACCESS_KEY", "admin");
            jail.set_env("AETHER_ADMIN_SECRET_KEY", "change-me-please");
            jail.set_env("AETHER_ADMIN_CA_CERT", "/etc/ca.pem");
            jail.set_env("AETHER_ADMIN_INSECURE", "true");
            jail.set_env("AETHER_ADMIN__LISTEN", "0.0.0.0:9001");
            let config = Config::load(None).unwrap();
            assert_eq!(config.admin.listen, "0.0.0.0:9001".parse().unwrap());
            Ok(())
        });
    }

    #[test]
    fn credentials_are_required() {
        figment::Jail::expect_with(|_| {
            let result = Config::load(None);
            assert!(matches!(result, Err(ConfigError::Invalid(_))), "{result:?}");
            Ok(())
        });
    }

    #[test]
    fn environment_alone_is_enough() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("AETHER_S3__ACCESS_KEY", "admin");
            jail.set_env("AETHER_S3__SECRET_KEY", "change-me-please");
            jail.set_env("AETHER_STORAGE__DATA_DIR", "/data");
            let config = Config::load(None).unwrap();
            assert_eq!(config.storage.data_dir, PathBuf::from("/data"));
            Ok(())
        });
    }

    #[test]
    fn default_file_is_used_when_present() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("aether.toml", "[s3]\naccess_key = \"admin\"\nsecret_key = \"change-me-please\"\n")?;
            assert_eq!(Config::load(None).unwrap().s3.access_key, "admin");
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_and_missing_files_are_errors() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("typo.toml", "[s3]\nlisten_addr = \"0.0.0.0:1\"\n")?;
            assert!(matches!(Config::load(Some(Path::new("typo.toml"))), Err(ConfigError::Parse(_))));
            assert!(matches!(Config::load(Some(Path::new("nope.toml"))), Err(ConfigError::Missing(_))));
            Ok(())
        });
    }
}
