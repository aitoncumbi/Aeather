use actix_web::{web, Scope};
use crate::services::buckets::{create_bucket, delete_bucket};

pub fn bucket_scope() -> Scope {
    web::scope("/buckets")
        .route("/{name}", web::post().to(create_bucket))
        .route("/{name}", web::delete().to(delete_bucket))
}