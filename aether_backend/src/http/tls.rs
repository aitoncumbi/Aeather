//! TLS certificates that are reloaded when their files change, so renewed
//! certificates (cert-manager, certbot) apply without a restart.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Makes `ring` the process-wide rustls crypto backend, for HTTPS clients
/// (the CLI). Calling it more than once is harmless.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub struct ReloadingCert {
    cert_file: PathBuf,
    key_file: PathBuf,
    provider: Arc<CryptoProvider>,
    current: RwLock<Arc<CertifiedKey>>,
    loaded_from: RwLock<(Option<SystemTime>, Option<SystemTime>)>,
}

impl fmt::Debug for ReloadingCert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadingCert").field("cert_file", &self.cert_file).finish_non_exhaustive()
    }
}

impl ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.read().expect("certificate lock poisoned")))
    }
}

impl ReloadingCert {
    /// Loads the certificate and key; fails if either is unusable.
    pub fn load(cert_file: &Path, key_file: &Path) -> io::Result<Arc<Self>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let key = certified_key(cert_file, key_file, &provider)?;
        Ok(Arc::new(Self {
            cert_file: cert_file.to_owned(),
            key_file: key_file.to_owned(),
            provider,
            current: RwLock::new(Arc::new(key)),
            loaded_from: RwLock::new((modified(cert_file), modified(key_file))),
        }))
    }

    /// A rustls server configuration that serves this certificate over
    /// HTTP/2 or HTTP/1.1.
    pub fn server_config(self: &Arc<Self>) -> io::Result<Arc<ServerConfig>> {
        let mut config = ServerConfig::builder_with_provider(Arc::clone(&self.provider))
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(self) as Arc<dyn ResolvesServerCert>);
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    /// Reloads the files if they changed since the last load. A broken new
    /// certificate is reported and the previous one stays in use.
    pub fn reload_if_changed(&self) -> io::Result<bool> {
        let stamps = (modified(&self.cert_file), modified(&self.key_file));
        if stamps == *self.loaded_from.read().expect("certificate lock poisoned") {
            return Ok(false);
        }
        let key = certified_key(&self.cert_file, &self.key_file, &self.provider)?;
        *self.current.write().expect("certificate lock poisoned") = Arc::new(key);
        *self.loaded_from.write().expect("certificate lock poisoned") = stamps;
        Ok(true)
    }

    pub fn spawn_reloader(self: &Arc<Self>, every: Duration) -> JoinHandle<()> {
        let cert = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await;
            loop {
                tick.tick().await;
                match cert.reload_if_changed() {
                    Ok(true) => info!(file = %cert.cert_file.display(), "reloaded TLS certificate"),
                    Ok(false) => {}
                    Err(e) => warn!(error = %e, "TLS certificate changed but cannot be loaded; keeping the old one"),
                }
            }
        })
    }
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

fn certified_key(cert_file: &Path, key_file: &Path, provider: &CryptoProvider) -> io::Result<CertifiedKey> {
    let invalid = |what: &str, path: &Path, e: &dyn fmt::Display| {
        io::Error::new(io::ErrorKind::InvalidData, format!("{what} {}: {e}", path.display()))
    };
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_file)
        .and_then(|certs| certs.collect())
        .map_err(|e| invalid("cannot read certificates from", cert_file, &e))?;
    if chain.is_empty() {
        return Err(invalid("no certificates in", cert_file, &"empty"));
    }
    let key =
        PrivateKeyDer::from_pem_file(key_file).map_err(|e| invalid("cannot read private key from", key_file, &e))?;
    let signing_key =
        provider.key_provider.load_private_key(key).map_err(|e| invalid("unusable private key in", key_file, &e))?;
    let certified = CertifiedKey::new(chain, signing_key);
    certified.keys_match().map_err(|e| invalid("certificate does not match the key in", key_file, &e))?;
    Ok(certified)
}
