use actix_web::HttpServer;
use aether_backend::{get_config, build_app, db};
use tracing::{info, error};



#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logger once before starting the server
    aether_backend::logger::init_logger();
    
    let config = get_config();
    let db_conn = db::database::initialize_configuration_database().await
        .expect("Failed to initialize database connection");
    
    info!("Starting server on http://127.0.0.1:{}", config.api_port);
    
    HttpServer::new(move || {
        build_app(db_conn.clone())
    })
        .bind(("127.0.0.1", config.api_port))?
        .run()
        .await
        .map_err(|e| {
            error!("Failed to start server: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Server failed to start")
        })?;
    
    Ok(())
}
