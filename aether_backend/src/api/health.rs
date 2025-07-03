// api/health.rs
use actix_web::{web, Scope};
use crate::services::health::health_check;

pub fn health_routes() -> Scope {
    web::scope("/health")
        .route("", web::get().to(health_check))
}
