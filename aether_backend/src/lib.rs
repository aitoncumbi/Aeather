pub mod logger;
pub mod config;
pub mod services;
pub mod storage;
pub mod api;
pub  mod models;
pub mod db;

use actix_web::{App, web};
use sea_orm::DatabaseConnection;
use tracing::error;
use api::{
    health::health_routes,
    buckets::bucket_scope,
    objects::object_scope
};


pub fn build_app(db: DatabaseConnection) -> App<impl actix_web::dev::ServiceFactory<actix_web::dev::ServiceRequest, Response = actix_web::dev::ServiceResponse, Error = actix_web::Error, Config = (), InitError = ()>> {
    storage::mass_storage_service::Storage::new(&get_config());
    App::new()
    .app_data(
        web::Data::new(db))
    .service(health_routes())
    .service(bucket_scope())
    .service(object_scope())
}



pub fn get_config() -> config::Config {
    match config::init_config() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to initialize configuration: {}", e);
            std::process::exit(1);
        }
    }
}


pub async fn get_db_connection() -> DatabaseConnection {
    match db::database::initialize_configuration_database().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to initialize database connection: {}", e);
            std::process::exit(1);
        }
    }
}