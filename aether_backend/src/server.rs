use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;

use axum::Router;
use axum::body::Body;
use axum::error_handling::HandleError;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::routing::get;
use s3s::config::StaticConfigProvider;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::auth::{Authorizer, S3Credentials, S3Permissions};
use crate::config::Config;
use crate::engine::Engine;
use crate::s3::AetherS3;

/// Opens the engine and serves the S3 and admin APIs until `shutdown` resolves.
pub async fn run(config: Config, shutdown: impl Future<Output = ()> + Send + 'static) -> anyhow::Result<()> {
    let engine = Arc::new(Engine::open(config.engine_config()).await?);
    let _background = engine.start_background_tasks();
    let routers = routers(engine, &config);
    let shutdown = shutdown.shared();

    let s3_listener = TcpListener::bind(config.s3.listen).await?;
    info!(
        addr = %s3_listener.local_addr()?,
        data_dir = %config.storage.data_dir.display(),
        region = %config.s3.region,
        "S3 API listening"
    );
    let s3 = axum::serve(s3_listener, routers.s3).with_graceful_shutdown(shutdown.clone());

    if config.admin.enabled {
        let admin_listener = TcpListener::bind(config.admin.listen).await?;
        info!(addr = %admin_listener.local_addr()?, "admin API listening");
        let admin = axum::serve(admin_listener, routers.admin).with_graceful_shutdown(shutdown);
        tokio::try_join!(s3.into_future(), admin.into_future())?;
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
}

pub fn routers(engine: Arc<Engine>, config: &Config) -> Routers {
    let auth =
        Authorizer::new(Arc::clone(&engine), config.s3.access_key.clone(), config.s3.secret_key.expose().to_owned());
    Routers { s3: s3_router(Arc::clone(&engine), Arc::clone(&auth), config), admin: crate::admin::router(engine, auth) }
}

fn s3_router(engine: Arc<Engine>, auth: Arc<Authorizer>, config: &Config) -> Router {
    let mut s3_config = s3s::config::S3Config::default();
    s3_config.put_object_max_size = Some(config.s3.max_object_size);

    let s3 = AetherS3::new(Arc::clone(&engine), Arc::clone(&auth), config.s3.region.clone());
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(S3Credentials(Arc::clone(&auth)));
    builder.set_access(S3Permissions(auth));
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(s3_config))));
    let s3 = HandleError::new(builder.build(), |err: s3s::HttpError| async move {
        error!(error = ?err, "S3 request failed");
        Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty()).unwrap()
    });

    // `_` is not valid in bucket names, so these never shadow S3 paths.
    Router::new()
        .route("/_aether/health/live", get(|| async { "ok" }))
        .route("/_aether/health/ready", get(ready))
        .with_state(engine)
        .fallback_service(s3)
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
