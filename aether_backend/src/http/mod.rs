//! The HTTP server: accepts connections (optionally over TLS), enforces
//! connection-level timeouts, and shuts down gracefully within a deadline.

pub mod tls;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ConnectInfo;
use hyper::Request;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;
use tracing::{debug, info, warn};

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Time allowed to send the request headers (slow-loris protection).
    pub header_timeout: Duration,
    pub handshake_timeout: Duration,
    /// On shutdown, how long to wait for in-flight requests.
    pub shutdown_timeout: Duration,
}

/// Serves `app` on `listener` until `shutdown` resolves, then stops accepting
/// and waits up to `limits.shutdown_timeout` for open connections to finish.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<rustls::ServerConfig>>,
    limits: Limits,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let acceptor = tls.map(TlsAcceptor::from);
    let mut builder = Builder::new(TokioExecutor::new());
    builder.http1().timer(TokioTimer::new()).header_read_timeout(limits.header_timeout);
    builder.http2().timer(TokioTimer::new());
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(connection) => connection,
                Err(e) => {
                    // Usually out of file descriptors: back off instead of spinning.
                    warn!(error = %e, "failed to accept a connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let service = hyper::service::service_fn({
            let app = app.clone();
            move |mut request: Request<Incoming>| {
                request.extensions_mut().insert(ConnectInfo(peer));
                app.clone().oneshot(request)
            }
        });
        let (builder, watcher, acceptor) = (builder.clone(), graceful.watcher(), acceptor.clone());
        tokio::spawn(async move {
            let result = match acceptor {
                None => {
                    let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
                    watcher.watch(connection.into_owned()).await
                }
                Some(acceptor) => match tokio::time::timeout(limits.handshake_timeout, acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => {
                        let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
                        watcher.watch(connection.into_owned()).await
                    }
                    Ok(Err(e)) => return debug!(%peer, error = %e, "TLS handshake failed"),
                    Err(_) => return debug!(%peer, "TLS handshake timed out"),
                },
            };
            if let Err(e) = result {
                debug!(%peer, error = %e, "connection closed with an error");
            }
        });
    }

    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => {}
        () = tokio::time::sleep(limits.shutdown_timeout) => {
            info!("in-flight requests did not finish within the shutdown timeout; closing them");
        }
    }
    Ok(())
}

/// Binds `addr`, logging what is served there.
pub async fn bind(addr: SocketAddr, what: &str, tls: bool) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    let scheme = if tls { "https" } else { "http" };
    info!(addr = %listener.local_addr()?, %scheme, "{what} listening");
    Ok(listener)
}
