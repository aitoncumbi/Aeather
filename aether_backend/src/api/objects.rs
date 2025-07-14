use actix_web::{web, Scope};
use crate::services::objects::upload_object;

pub fn object_scope() -> Scope {
    web::scope("")
    .app_data(web::PayloadConfig::new(3 * 1024 * 1024 * 1024))
    .route("/bucket/{bucket_name}/object/{object_name}", web::put().to(upload_object))
}