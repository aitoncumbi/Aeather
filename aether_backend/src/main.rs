use actix_web::HttpServer;
use aether_backend::{get_config, build_app};
use tracing::{info, error};


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = get_config();
    HttpServer::new(|| build_app())
        .bind(("127.0.0.1", config.api_port))?
        .run()
        .await
        .map_err(|e| {
            error!("Failed to start server: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Server failed to start")
        })?;
    info!("Server started on http://127.0.0.1:{}", config.api_port);
    Ok(())
}
