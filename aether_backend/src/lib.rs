pub mod logger;
pub mod config;
pub mod services;
pub mod storage;
pub mod api;

use actix_web::App;
use tracing::error;
use api::{
    health::health_routes,
    buckets::bucket_scope,
};



pub fn build_app() -> App<impl actix_web::dev::ServiceFactory<actix_web::dev::ServiceRequest, Response = actix_web::dev::ServiceResponse, Error = actix_web::Error, Config = (), InitError = ()>> {
    storage::mass_storage_service::Storage::new(&get_config());
    App::new()
    .service(health_routes())
    .service(bucket_scope())

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
