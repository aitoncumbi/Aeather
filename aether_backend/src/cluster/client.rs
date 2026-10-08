//! Calls another node's internal API, signing every request.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use bytes::Bytes;
use reqwest::Method;

use super::api::NodeHealth;
use super::auth::{self, ClusterKey, Signed};
use crate::engine::now_millis;

pub struct ClusterClient {
    http: reqwest::Client,
    key: ClusterKey,
    /// Who this client signs as: a node id, or a label such as `cli`.
    node: String,
}

impl ClusterClient {
    /// Talks HTTP/2 only (internal listeners always offer it). `ca_file`
    /// holds the certificates other nodes' TLS certificates chain to.
    pub fn new(key: ClusterKey, node: impl Into<String>, ca_file: Option<&Path>) -> anyhow::Result<Self> {
        // reqwest is built without a bundled crypto backend; use the server's.
        crate::http::tls::install_crypto_provider();
        let mut builder = reqwest::Client::builder().http2_prior_knowledge().connect_timeout(Duration::from_secs(5));
        if let Some(path) = ca_file {
            let pem = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
            for cert in reqwest::Certificate::from_pem_bundle(&pem)? {
                builder = builder.add_root_certificate(cert);
            }
        }
        Ok(Self { http: builder.build()?, key, node: node.into() })
    }

    /// `GET /internal/v1/health` on the node at `base` (e.g. `http://node-b:9002`).
    pub async fn health(&self, base: &str) -> anyhow::Result<NodeHealth> {
        let response =
            self.send(Method::GET, base, "/internal/v1/health", Bytes::new(), Duration::from_secs(10)).await?;
        Ok(response.json().await?)
    }

    async fn send(
        &self,
        method: Method,
        base: &str,
        target: &str,
        body: Bytes,
        timeout: Duration,
    ) -> anyhow::Result<reqwest::Response> {
        let url = format!("{}{target}", base.trim_end_matches('/'));
        let date = now_millis();
        let nonce = auth::new_nonce();
        let content_sha256 = auth::body_sha256(&body);
        let signature = self.key.sign(&Signed {
            method: method.as_str(),
            target,
            node: &self.node,
            date,
            nonce: &nonce,
            content_sha256: &content_sha256,
        });
        let response = self
            .http
            .request(method, &url)
            .timeout(timeout)
            .header(auth::NODE, &self.node)
            .header(auth::DATE, date.to_string())
            .header(auth::NONCE, nonce)
            .header(auth::CONTENT_SHA256, content_sha256)
            .header(auth::SIGNATURE, signature)
            .body(body)
            .send()
            .await
            .with_context(|| format!("cannot reach {url}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let value: serde_json::Value = response.json().await.unwrap_or_default();
            let message = value["error"]["message"].as_str().unwrap_or("no details");
            bail!("{url}: {status}: {message}");
        }
        Ok(response)
    }
}
