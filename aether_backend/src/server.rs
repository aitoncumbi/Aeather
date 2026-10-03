use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::error_handling::HandleError;
use axum::extract::State;
use axum::http::{HeaderName, Response, StatusCode};
use axum::middleware;
use axum::routing::get;
use futures::FutureExt;
use s3s::config::StaticConfigProvider;
use s3s::service::S3ServiceBuilder;
use tracing::{error, info, warn};

use crate::admin::AdminSettings;
use crate::auth::{Authorizer, S3Credentials, S3Permissions};
use crate::config::Config;
use crate::engine::Engine;
use crate::history::History;
use crate::http::tls::ReloadingCert;
use crate::http::{self, Limits};
use crate::metrics::Metrics;
use crate::observe::{Observer, observe};
use crate::s3::AetherS3;
use crate::s3::cors::CorsState;
use crate::s3::host::Domains;

/// Opens the engine and serves the S3 and admin APIs until `shutdown` resolves.
pub async fn run(config: Config, shutdown: impl Future<Output = ()> + Send + 'static) -> anyhow::Result<()> {
    let engine = Arc::new(Engine::open(config.engine_config()).await?);
    let mut background = engine.start_background_tasks();
    let routers = routers(engine, &config);
    background.push(routers.history.spawn_sampler());
    let shutdown = shutdown.shared();
    let limits = Limits {
        header_timeout: Duration::from_secs(config.http.header_timeout_secs),
        handshake_timeout: Duration::from_secs(config.http.handshake_timeout_secs),
        shutdown_timeout: Duration::from_secs(config.http.shutdown_timeout_secs),
    };

    let tls = match config.tls.files() {
        Some((cert_file, key_file)) => {
            let cert = ReloadingCert::load(cert_file, key_file)?;
            if config.tls.reload_secs > 0 {
                background.push(cert.spawn_reloader(Duration::from_secs(config.tls.reload_secs)));
            }
            Some(cert.server_config()?)
        }
        None => None,
    };
    let admin_tls = if config.tls.admin { tls.clone() } else { None };

    info!(data_dir = %config.storage.data_dir.display(), region = %config.s3.region, "starting");
    let s3_listener = http::bind(config.s3.listen, "S3 API", tls.is_some()).await?;
    let s3 = http::serve(s3_listener, routers.s3, tls, limits, shutdown.clone());
    if config.admin.enabled {
        if !config.admin.listen.ip().is_loopback() && admin_tls.is_none() {
            warn!("the admin API is reachable from the network without TLS; set tls.admin = true");
        }
        let admin_listener = http::bind(config.admin.listen, "admin API", admin_tls.is_some()).await?;
        let admin = http::serve(admin_listener, routers.admin, admin_tls, limits, shutdown);
        tokio::try_join!(s3, admin)?;
    } else {
        s3.await?;
    }
    info!("shut down");
    Ok(())
}

/// The two HTTP applications, sharing one engine and one set of identities.
pub struct Routers {
    pub s3: Router,
    pub admin: Router,
    /// Feeds the console's charts once its sampler is spawned.
    pub history: Arc<History>,
}

pub fn routers(engine: Arc<Engine>, config: &Config) -> Routers {
    let auth =
        Authorizer::new(Arc::clone(&engine), config.s3.access_key.clone(), config.s3.secret_key.expose().to_owned());
    let metrics = Metrics::new(Arc::clone(&engine));
    let observer = |api, id_header| Observer {
        api,
        id_header: HeaderName::from_static(id_header),
        metrics: Arc::clone(&metrics),
        access_log: config.log.access_log,
    };
    let s3 = s3_router(Arc::clone(&engine), Arc::clone(&auth), config)
        .layer(middleware::from_fn_with_state(observer("s3", "x-amz-request-id"), observe));
    let history = History::new(Arc::clone(&metrics));
    let settings = AdminSettings::from_config(config);
    let admin = crate::admin::router(engine, auth, Arc::clone(&metrics), Arc::clone(&history), settings)
        .layer(middleware::from_fn_with_state(observer("admin", "x-request-id"), observe));
    Routers { s3, admin, history }
}

fn s3_router(engine: Arc<Engine>, auth: Arc<Authorizer>, config: &Config) -> Router {
    let mut s3_config = s3s::config::S3Config::default();
    s3_config.put_object_max_size = Some(config.s3.max_object_size);
    s3_config.enable_sig_v2 = config.s3.allow_sig_v2;

    let sse_c_allowed = config.tls.files().is_some() || config.s3.allow_sse_c_over_http;
    let s3 = AetherS3::new(Arc::clone(&engine), Arc::clone(&auth), config.s3.region.clone(), sse_c_allowed);
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(S3Credentials(Arc::clone(&auth)));
    builder.set_access(S3Permissions(auth));
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(s3_config))));
    let domains = Domains::new(&config.s3.domains);
    if !domains.is_empty() {
        builder.set_host(domains.clone());
    }
    let s3 = HandleError::new(builder.build(), |err: s3s::HttpError| async move {
        error!(error = ?err, "S3 request failed");
        Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty()).unwrap()
    });

    // `_` is not valid in bucket names, so these never shadow S3 paths.
    Router::new()
        .route("/_aether/health/live", get(|| async { "ok" }))
        .route("/_aether/health/ready", get(ready))
        .with_state(Arc::clone(&engine))
        .fallback_service(s3)
        .layer(middleware::from_fn_with_state(CorsState { engine, domains }, crate::s3::cors::layer))
}

/// Ready when the metadata store answers.
async fn ready(State(engine): State<Arc<Engine>>) -> (StatusCode, &'static str) {
    match engine.list_buckets().await {
        Ok(_) => (StatusCode::OK, "ready"),
        Err(e) => {
            error!(error = %e, "readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "metadata store unavailable")
        }
    }
}

/// Resolves on Ctrl-C or SIGTERM (what container runtimes send on stop).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    info!("shutdown requested, finishing in-flight requests");
}
