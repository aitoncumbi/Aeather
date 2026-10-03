//! `aether key`, `aether bucket` and `aether status`: manage a running server
//! through its admin API.
//!
//! Without `--url`, `--access-key` and `--secret-key`, the commands read the
//! server's config file and use its admin address and root key, so they work
//! with no flags on the server host.

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use reqwest::Method;
use serde_json::{Value, json};

use crate::config::Config;
use crate::meta::BucketGrant;

#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Admin API URL [default: from admin.listen in the config file]
    #[arg(long, env = "AETHER_ADMIN_URL", global = true)]
    pub url: Option<String>,
    /// Admin access key id [default: the root key from the config file]
    #[arg(long, env = "AETHER_ADMIN_ACCESS_KEY", global = true)]
    pub access_key: Option<String>,
    /// Admin secret key [default: the root secret from the config file]
    #[arg(long, env = "AETHER_ADMIN_SECRET_KEY", global = true, hide_env_values = true)]
    pub secret_key: Option<String>,
    /// Trust this PEM CA certificate for an https admin URL (e.g. a private CA)
    #[arg(long, env = "AETHER_ADMIN_CA_CERT", global = true)]
    pub ca_cert: Option<std::path::PathBuf>,
    /// Do not verify the admin API's TLS certificate (testing only)
    #[arg(long, env = "AETHER_ADMIN_INSECURE", global = true)]
    pub insecure: bool,
    /// Print the API's JSON instead of a table
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub enum KeyCommand {
    /// List access keys
    List,
    /// Create an access key and print its secret (shown only once)
    Create {
        /// What the key is for, e.g. the application's name
        name: String,
        /// Full access to every bucket and to the admin API
        #[arg(long)]
        admin: bool,
        /// Allow the key to create buckets; it owns the buckets it creates
        #[arg(long)]
        create_buckets: bool,
        /// Permissions on a bucket, e.g. `--grant photos=rw` (r: read, w: write, o: owner); repeatable
        #[arg(long = "grant", value_name = "BUCKET=PERMS")]
        grants: Vec<String>,
        /// Use this access key id instead of generating one (for migrations)
        #[arg(long, requires = "import_secret")]
        import_id: Option<String>,
        /// Use this secret instead of generating one (for migrations)
        #[arg(long, requires = "import_id")]
        import_secret: Option<String>,
    },
    /// Show one access key
    Show { access_key_id: String },
    /// Set a key's permissions on a bucket: any of r (read), w (write), o (owner)
    Grant { access_key_id: String, bucket: String, perms: String },
    /// Remove a key's permissions on a bucket
    Revoke { access_key_id: String, bucket: String },
    /// Change a key's name or roles
    Update {
        access_key_id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        admin: Option<bool>,
        #[arg(long)]
        create_buckets: Option<bool>,
    },
    /// Re-enable a disabled key
    Enable { access_key_id: String },
    /// Reject a key's requests without deleting it
    Disable { access_key_id: String },
    /// Delete a key permanently
    Delete { access_key_id: String },
}

#[derive(Subcommand, Debug)]
pub enum BucketCommand {
    /// List buckets with their object count and size
    List,
    /// Create a bucket
    Create { name: String },
    /// Show a bucket's usage and which keys can access it
    Show { name: String },
    /// Delete an empty bucket
    Delete { name: String },
    /// Turn on versioning (keep every version), or suspend it
    Versioning {
        name: String,
        #[arg(value_parser = ["enable", "suspend"])]
        action: String,
    },
    /// Limit a bucket's size and object count. Unset limits stay as they are
    Quota {
        name: String,
        /// Largest total size, e.g. 50GiB or 10GB, or `none` for no limit
        #[arg(long)]
        max_bytes: Option<String>,
        /// Most objects, or `none` for no limit
        #[arg(long)]
        max_objects: Option<String>,
        /// Remove both limits
        #[arg(long, conflicts_with_all = ["max_bytes", "max_objects"])]
        clear: bool,
    },
}

pub enum Request {
    Key(KeyCommand),
    Bucket(BucketCommand),
    Status,
    Scrub,
    Backup(std::path::PathBuf),
}

pub async fn run(request: Request, args: &ClientArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let client = Client::connect(args, config_path)?;
    let output = match request {
        Request::Key(command) => key(&client, command, args.json).await?,
        Request::Bucket(command) => bucket(&client, command, args.json).await?,
        Request::Status => {
            let status = client.call(Method::GET, "/api/v1/status", None).await?;
            if args.json { pretty(&status) } else { format_status(&status) }
        }
        Request::Scrub => {
            client.call(Method::POST, "/api/v1/scrub", None).await?;
            "Scrub started. Follow its progress with `aether status`.\n".to_owned()
        }
        Request::Backup(dir) => {
            // Relative paths are taken from here, as users expect.
            let dir = if dir.is_absolute() { dir } else { std::env::current_dir()?.join(dir) };
            let report = client.call(Method::POST, "/api/v1/backup", Some(serde_json::json!({ "dir": dir }))).await?;
            if args.json {
                pretty(&report)
            } else {
                format!(
                    "Backed up {} blocks ({}) to {} in {:.1} s.\n\
                     To restore, start Aether with that directory as storage.data_dir (and the same encryption keys).\n",
                    text(&report["blocks"]),
                    bytes(report["bytes"].as_u64().unwrap_or(0)),
                    text(&report["dir"]),
                    report["duration_ms"].as_u64().unwrap_or(0) as f64 / 1000.0
                )
            }
        }
    };
    print!("{output}");
    Ok(())
}

async fn key(client: &Client, command: KeyCommand, json: bool) -> anyhow::Result<String> {
    let path = |id: &str| format!("/api/v1/keys/{}", encode(id));
    let show = |value: Value| if json { pretty(&value) } else { format_key(&value) };
    Ok(match command {
        KeyCommand::List => {
            let keys = client.call(Method::GET, "/api/v1/keys", None).await?;
            if json { pretty(&keys) } else { format_keys(&keys) }
        }
        KeyCommand::Create { name, admin, create_buckets, grants, import_id, import_secret } => {
            let mut buckets = serde_json::Map::new();
            for grant in grants {
                let (bucket, perms) = grant.split_once('=').context("--grant takes BUCKET=PERMS, e.g. photos=rw")?;
                buckets.insert(bucket.to_owned(), serde_json::to_value(parse_perms(perms)?)?);
            }
            let mut body =
                json!({ "name": name, "admin": admin, "can_create_buckets": create_buckets, "buckets": buckets });
            if let (Some(id), Some(secret)) = (import_id, import_secret) {
                body["access_key_id"] = json!(id);
                body["secret_key"] = json!(secret);
            }
            let key = client.call(Method::POST, "/api/v1/keys", Some(body)).await?;
            if json { pretty(&key) } else { format_created_key(&key) }
        }
        KeyCommand::Show { access_key_id } => show(client.call(Method::GET, &path(&access_key_id), None).await?),
        KeyCommand::Grant { access_key_id, bucket, perms } => {
            let body = serde_json::to_value(parse_perms(&perms)?)?;
            let url = format!("{}/buckets/{}", path(&access_key_id), encode(&bucket));
            show(client.call(Method::PUT, &url, Some(body)).await?)
        }
        KeyCommand::Revoke { access_key_id, bucket } => {
            let url = format!("{}/buckets/{}", path(&access_key_id), encode(&bucket));
            client.call(Method::DELETE, &url, None).await?;
            format!("Revoked {access_key_id}'s permissions on {bucket}.\n")
        }
        KeyCommand::Update { access_key_id, name, admin, create_buckets } => {
            let body = json!({ "name": name, "admin": admin, "can_create_buckets": create_buckets });
            show(client.call(Method::PATCH, &path(&access_key_id), Some(body)).await?)
        }
        KeyCommand::Enable { access_key_id } => {
            show(client.call(Method::PATCH, &path(&access_key_id), Some(json!({ "disabled": false }))).await?)
        }
        KeyCommand::Disable { access_key_id } => {
            show(client.call(Method::PATCH, &path(&access_key_id), Some(json!({ "disabled": true }))).await?)
        }
        KeyCommand::Delete { access_key_id } => {
            client.call(Method::DELETE, &path(&access_key_id), None).await?;
            format!("Deleted access key {access_key_id}.\n")
        }
    })
}

async fn bucket(client: &Client, command: BucketCommand, json: bool) -> anyhow::Result<String> {
    let path = |name: &str| format!("/api/v1/buckets/{}", encode(name));
    Ok(match command {
        BucketCommand::List => {
            let buckets = client.call(Method::GET, "/api/v1/buckets", None).await?;
            if json { pretty(&buckets) } else { format_buckets(&buckets) }
        }
        BucketCommand::Create { name } => {
            let bucket = client.call(Method::POST, "/api/v1/buckets", Some(json!({ "name": name }))).await?;
            if json { pretty(&bucket) } else { format!("Created bucket {name}.\n") }
        }
        BucketCommand::Show { name } => {
            let bucket = client.call(Method::GET, &path(&name), None).await?;
            if json { pretty(&bucket) } else { format_bucket(&bucket) }
        }
        BucketCommand::Delete { name } => {
            client.call(Method::DELETE, &path(&name), None).await?;
            format!("Deleted bucket {name}.\n")
        }
        BucketCommand::Versioning { name, action } => {
            let status = if action == "enable" { "Enabled" } else { "Suspended" };
            let body = json!({ "status": status });
            let bucket = client.call(Method::PUT, &format!("{}/versioning", path(&name)), Some(body)).await?;
            if json { pretty(&bucket) } else { format_bucket(&bucket) }
        }
        BucketCommand::Quota { name, max_bytes, max_objects, clear } => {
            let mut quota = client.call(Method::GET, &path(&name), None).await?["quota"].clone();
            if clear {
                quota = json!({ "max_bytes": null, "max_objects": null });
            }
            if let Some(value) = max_bytes {
                quota["max_bytes"] = json!(limit(&value, parse_size)?);
            }
            if let Some(value) = max_objects {
                quota["max_objects"] = json!(limit(&value, |v| v.parse().ok())?);
            }
            let bucket = client.call(Method::PUT, &format!("{}/quota", path(&name)), Some(quota)).await?;
            if json { pretty(&bucket) } else { format_bucket(&bucket) }
        }
    })
}

struct Client {
    http: reqwest::Client,
    base: String,
    access_key: String,
    secret_key: String,
}

impl Client {
    fn connect(args: &ClientArgs, config_path: Option<&Path>) -> anyhow::Result<Self> {
        let (url, access_key, secret_key) = match (&args.url, &args.access_key, &args.secret_key) {
            (Some(url), Some(access_key), Some(secret_key)) => (url.clone(), access_key.clone(), secret_key.clone()),
            _ => {
                let config = Config::load(config_path).context(
                    "pass --url, --access-key and --secret-key, or run where the server's config file can be read",
                )?;
                let scheme = if config.tls.admin { "https" } else { "http" };
                let url = args.url.clone().unwrap_or_else(|| format!("{scheme}://{}", reachable(config.admin.listen)));
                let access_key = args.access_key.clone().unwrap_or(config.s3.access_key);
                let secret_key = args.secret_key.clone().unwrap_or_else(|| config.s3.secret_key.expose().to_owned());
                (url, access_key, secret_key)
            }
        };
        let base = url.trim_end_matches('/').to_owned();
        Ok(Self { http: http_client(args.ca_cert.as_deref(), args.insecure)?, base, access_key, secret_key })
    }

    /// Returns the response body, or `Value::Null` for 204 No Content.
    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> anyhow::Result<Value> {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .basic_auth(&self.access_key, Some(&self.secret_key));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.with_context(|| format!("cannot reach the admin API at {}", self.base))?;
        let status = response.status();
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let value: Value = response.json().await.with_context(|| format!("unexpected response ({status})"))?;
        if !status.is_success() {
            let code = value["error"]["code"].as_str().unwrap_or("Error");
            let message = value["error"]["message"].as_str().unwrap_or("");
            bail!("{code}: {message}");
        }
        Ok(value)
    }
}

fn http_client(ca_cert: Option<&Path>, accept_invalid_certs: bool) -> anyhow::Result<reqwest::Client> {
    // reqwest is built without a bundled crypto backend; use the server's.
    crate::http::tls::install_crypto_provider();
    let mut builder = reqwest::Client::builder().tls_danger_accept_invalid_certs(accept_invalid_certs);
    if let Some(path) = ca_cert {
        let pem = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
        builder = builder.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
    }
    Ok(builder.build()?)
}

/// `aether health`: exits successfully when the S3 listener reports ready.
/// Meant for container health checks, so it trusts any certificate: it
/// connects to its own server over loopback.
pub async fn health(url: Option<&str>, config_path: Option<&Path>) -> anyhow::Result<()> {
    let base = match url {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None => {
            let config = Config::load(config_path)?;
            let scheme = if config.tls.files().is_some() { "https" } else { "http" };
            format!("{scheme}://{}", reachable(config.s3.listen))
        }
    };
    let client = http_client(None, true)?;
    let response = client
        .get(format!("{base}/_aether/health/ready"))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .with_context(|| format!("cannot reach {base}"))?;
    if !response.status().is_success() {
        bail!("not ready: {}", response.status());
    }
    Ok(())
}

/// A connectable address for a listen address (`0.0.0.0` → loopback).
fn reachable(listen: SocketAddr) -> SocketAddr {
    match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => SocketAddr::new(Ipv4Addr::LOCALHOST.into(), listen.port()),
        IpAddr::V6(ip) if ip.is_unspecified() => SocketAddr::new(Ipv6Addr::LOCALHOST.into(), listen.port()),
        _ => listen,
    }
}

/// Percent-encodes a path segment.
fn encode(segment: &str) -> String {
    crate::s3::convert::url_encode(segment).replace('/', "%2F")
}

/// `r`, `w` and `o` in any combination; `none` (or empty) for no permissions.
pub fn parse_perms(perms: &str) -> anyhow::Result<BucketGrant> {
    let mut grant = BucketGrant::default();
    if perms.eq_ignore_ascii_case("none") {
        return Ok(grant);
    }
    for c in perms.chars() {
        match c.to_ascii_lowercase() {
            'r' => grant.read = true,
            'w' => grant.write = true,
            'o' => grant.owner = true,
            _ => bail!("unknown permission {c:?}: use r (read), w (write) and o (owner), e.g. rw"),
        }
    }
    Ok(grant)
}

fn perms(grant: &Value) -> String {
    let flags = [("read", 'r'), ("write", 'w'), ("owner", 'o')];
    let s: String = flags.iter().filter(|(name, _)| grant[name].as_bool() == Some(true)).map(|(_, c)| *c).collect();
    if s.is_empty() { "-".into() } else { s }
}

fn pretty(value: &Value) -> String {
    format!("{}\n", serde_json::to_string_pretty(value).unwrap_or_default())
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// `none`, or a value `parse` accepts.
fn limit(value: &str, parse: impl Fn(&str) -> Option<u64>) -> anyhow::Result<Option<u64>> {
    if value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    parse(value).map(Some).ok_or_else(|| anyhow::anyhow!("invalid limit {value:?}: use a number, e.g. 50GiB, or none"))
}

/// A byte count with an optional unit: KB, MB, GB, TB (powers of 1000) or
/// KiB, MiB, GiB, TiB (powers of 1024).
fn parse_size(value: &str) -> Option<u64> {
    let value = value.trim();
    let split = value.find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let multiplier: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1000,
        "mb" => 1000u64.pow(2),
        "gb" => 1000u64.pow(3),
        "tb" => 1000u64.pow(4),
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        _ => return None,
    };
    number.parse::<u64>().ok()?.checked_mul(multiplier)
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 { format!("{n} B") } else { format!("{size:.1} {}", UNITS[unit]) }
}

fn roles(key: &Value) -> String {
    let mut roles = Vec::new();
    if key["admin"].as_bool() == Some(true) {
        roles.push("admin");
    }
    if key["can_create_buckets"].as_bool() == Some(true) {
        roles.push("creates buckets");
    }
    if key["disabled"].as_bool() == Some(true) {
        roles.push("DISABLED");
    }
    roles.join(", ")
}

fn grants(key: &Value) -> String {
    let Some(buckets) = key["buckets"].as_object() else { return String::new() };
    buckets.iter().map(|(bucket, grant)| format!("{bucket}:{}", perms(grant))).collect::<Vec<_>>().join(" ")
}

fn table(header: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut out = String::new();
    let mut line = |cells: Vec<String>| {
        let padded: Vec<String> = cells.iter().zip(&widths).map(|(c, w)| format!("{c:<w$}")).collect();
        let _ = writeln!(out, "{}", padded.join("  ").trim_end());
    };
    line(header.iter().map(|h| h.to_string()).collect());
    for row in rows {
        line(row);
    }
    out
}

fn format_keys(keys: &Value) -> String {
    let rows = keys
        .as_array()
        .map(|keys| {
            keys.iter().map(|k| vec![text(&k["access_key_id"]), text(&k["name"]), roles(k), grants(k)]).collect()
        })
        .unwrap_or_default();
    table(&["ACCESS KEY ID", "NAME", "ROLES", "BUCKETS"], rows)
}

fn format_key(key: &Value) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Access key id: {}", text(&key["access_key_id"]));
    let _ = writeln!(out, "Name:          {}", text(&key["name"]));
    let _ = writeln!(out, "Created:       {}", text(&key["created_at"]));
    let roles = roles(key);
    let _ = writeln!(out, "Roles:         {}", if roles.is_empty() { "-" } else { &roles });
    let grants = grants(key);
    let _ = writeln!(out, "Buckets:       {}", if grants.is_empty() { "-" } else { &grants });
    out
}

fn format_created_key(key: &Value) -> String {
    let mut out = format_key(key);
    let _ = writeln!(out, "Secret key:    {}", text(&key["secret_key"]));
    out.push_str("\nSave the secret key now: it cannot be shown again.\n");
    out
}

fn format_buckets(buckets: &Value) -> String {
    let rows = buckets
        .as_array()
        .map(|buckets| {
            buckets
                .iter()
                .map(|b| {
                    let size = bytes(b["bytes"].as_u64().unwrap_or(0));
                    vec![text(&b["name"]), text(&b["objects"]), size, text(&b["created_at"])]
                })
                .collect()
        })
        .unwrap_or_default();
    table(&["NAME", "OBJECTS", "SIZE", "CREATED"], rows)
}

fn format_bucket(bucket: &Value) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Bucket:  {}", text(&bucket["name"]));
    let _ = writeln!(out, "Created: {}", text(&bucket["created_at"]));
    let _ = writeln!(out, "Objects: {}", text(&bucket["objects"]));
    let _ = writeln!(out, "Size:    {}", bytes(bucket["bytes"].as_u64().unwrap_or(0)));
    let _ = writeln!(out, "Versioning: {}", text(&bucket["versioning"]));
    if let Some(n) = bucket["noncurrent_objects"].as_u64().filter(|n| *n > 0) {
        let size = bytes(bucket["noncurrent_bytes"].as_u64().unwrap_or(0));
        let _ = writeln!(out, "Older versions: {n} ({size})");
    }
    let quota = &bucket["quota"];
    let limits: Vec<String> =
        [quota["max_bytes"].as_u64().map(bytes), quota["max_objects"].as_u64().map(|n| format!("{n} objects"))]
            .into_iter()
            .flatten()
            .collect();
    let _ = writeln!(out, "Quota:   {}", if limits.is_empty() { "none".into() } else { limits.join(", ") });
    let keys: Vec<Vec<String>> = bucket["keys"]
        .as_array()
        .map(|keys| keys.iter().map(|k| vec![text(&k["access_key_id"]), text(&k["name"]), perms(k)]).collect())
        .unwrap_or_default();
    if keys.is_empty() {
        out.push_str("\nNo access keys have permissions on this bucket (admin keys always do).\n");
    } else {
        out.push('\n');
        out.push_str(&table(&["ACCESS KEY ID", "NAME", "PERMS"], keys));
    }
    out
}

fn format_status(status: &Value) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Version:     {}", text(&status["version"]));
    let _ = writeln!(out, "Uptime:      {}s", text(&status["uptime_secs"]));
    let _ = writeln!(out, "Buckets:     {}", text(&status["buckets"]));
    let _ = writeln!(out, "Objects:     {}", text(&status["objects"]));
    let _ = writeln!(out, "Data:        {}", bytes(status["bytes"].as_u64().unwrap_or(0)));
    let _ = writeln!(out, "Blocks:      {}", text(&status["blocks"]));
    let _ = writeln!(out, "Access keys: {}", text(&status["access_keys"]));
    if let (Some(available), Some(total)) =
        (status["disk_available_bytes"].as_u64(), status["disk_total_bytes"].as_u64())
    {
        let _ = writeln!(out, "Disk:        {} free of {}", bytes(available), bytes(total));
    }
    let scrub = &status["scrub"];
    if scrub.is_null() {
        let _ = writeln!(out, "Scrub:       not run since the server started");
    } else {
        let progress = format!(
            "{} objects, {} blocks, {} checked, {} damaged",
            text(&scrub["objects_checked"]),
            text(&scrub["blocks_checked"]),
            bytes(scrub["bytes_checked"].as_u64().unwrap_or(0)),
            scrub["corrupt"].as_array().map_or(0, Vec::len),
        );
        let state = if scrub["finished_at"].is_null() { "running" } else { "finished" };
        let _ = writeln!(out, "Scrub:       {state}: {progress}");
        for block in scrub["corrupt"].as_array().into_iter().flatten() {
            let _ = writeln!(
                out,
                "  DAMAGED {}/{} (block {}): {}",
                text(&block["bucket"]),
                text(&block["key"]),
                text(&block["block_id"]),
                text(&block["problem"])
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissions_parse() {
        assert_eq!(parse_perms("rw").unwrap(), BucketGrant { read: true, write: true, owner: false });
        assert_eq!(parse_perms("RWO").unwrap(), BucketGrant::FULL);
        assert_eq!(parse_perms("none").unwrap(), BucketGrant::default());
        assert_eq!(parse_perms("").unwrap(), BucketGrant::default());
        assert!(parse_perms("rx").is_err());
        assert_eq!(perms(&json!({ "read": true, "owner": true })), "ro");
        assert_eq!(perms(&json!({})), "-");
    }

    #[test]
    fn sizes_and_addresses() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(5 * 1024 * 1024 + 1024 * 512), "5.5 MiB");
        assert_eq!(reachable("0.0.0.0:9001".parse().unwrap()), "127.0.0.1:9001".parse().unwrap());
        assert_eq!(reachable("10.0.0.5:9001".parse().unwrap()), "10.0.0.5:9001".parse().unwrap());
        assert_eq!(encode("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn quota_limits_parse() {
        assert_eq!(parse_size("50GiB"), Some(50 << 30));
        assert_eq!(parse_size("10 GB"), Some(10_000_000_000));
        assert_eq!(parse_size("2048"), Some(2048));
        assert_eq!(parse_size("1.5GB"), None);
        assert_eq!(parse_size("99999999999TB"), None);
        assert_eq!(limit("none", parse_size).unwrap(), None);
        assert!(limit("lots", parse_size).is_err());
    }

    #[test]
    fn tables_align() {
        let out = table(&["A", "LONG"], vec![vec!["xyz".into(), "1".into()]]);
        assert_eq!(out, "A    LONG\nxyz  1\n");
    }
}
