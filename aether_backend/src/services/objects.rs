use actix_web::{web, HttpResponse, Responder};
use sea_orm::DatabaseConnection;
use crate::get_config;
use crate::models::object;
use crate::storage::mass_storage_service::Storage;

pub async fn upload_object(path: web::Path<(String, String)>, body: web::Bytes, db_conn: web::Data<DatabaseConnection>) -> impl Responder {
    let (bucket_name, object_name) = path.into_inner();

    
    let storage = Storage::new(&get_config());
    match storage.save_object(&bucket_name, &object_name, &body) {
        Ok(_) => HttpResponse::Created().body("Object uploaded"),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                HttpResponse::NotFound().body(format!("Bucket not found: {}", bucket_name))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to upload object: {}", e))
            }
        }
    }
}