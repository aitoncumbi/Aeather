
use actix_web::{web, HttpResponse, Responder};
use crate::get_config;
use crate::storage::mass_storage_service::Storage;

pub async fn create_bucket(path: web::Path<String>) -> impl Responder {
    // Extract the bucket name from the path
    let bucket_name = path.into_inner();

    // Create a new folder for the bucket
    let storage = Storage::new(&get_config());
    match storage.new_folder(&bucket_name) {
        Ok(folder_path) => HttpResponse::Created().body(format!("Bucket successfully created")),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                HttpResponse::Conflict().body(format!("Bucket {} already exists", bucket_name))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to create bucket: {}", e))
            }
        },
    }
}

pub async fn delete_bucket(path: web::Path<String>) -> impl Responder {
    // Extract the bucket name from the path
    let bucket_name = path.into_inner();

    // Delete the folder for the bucket
    let storage = Storage::new(&get_config());
    match storage.delete_folder(&bucket_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Bucket deleted: {}", bucket_name)),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                HttpResponse::NotFound().body(format!("Bucket not found: {}", e))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to delete bucket: {}", e))
            }
        },
    }
}