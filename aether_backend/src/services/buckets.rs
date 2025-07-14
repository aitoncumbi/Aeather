use actix_web::{web, HttpResponse, Responder};
use sea_orm::ActiveValue::Set;
use sea_orm::DatabaseConnection;
use crate::get_config;
use crate::models::bucket;
use crate::storage::mass_storage_service::{self, Storage};
use crate::db::bucket::{create_bucket as db_create_bucket, delete_bucket as db_delete_bucket};

pub async fn create_bucket(path: web::Path<String>, db_conn: web::Data<DatabaseConnection>) -> impl Responder {
    // Extract the bucket name from the path
    let bucket_name = path.into_inner();

    // Create a new folder for the bucket
    let storage = Storage::new(&get_config());
    match storage.new_folder(&bucket_name) {
        Ok(_folder_path) => {
            if db_create_bucket(&bucket_name, &db_conn).await {
                HttpResponse::Created().body(format!("Bucket created: {}", bucket_name))
            } else {
               match storage.delete_folder(&bucket_name) {
                    Ok(_) => HttpResponse::InternalServerError().body(format!("Failed to create bucket in database, folder deleted: {}", bucket_name)),
                    Err(e) => HttpResponse::InternalServerError().body(format!("Failed to create bucket and delete folder: {}", e)),
               }
            }
        }

        Err(e) => {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                HttpResponse::Conflict().body(format!("Bucket {} already exists", bucket_name))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to create bucket: {}", e))
            }
        },
    }
}

pub async fn delete_bucket(path: web::Path<String>, db_conn: web::Data<DatabaseConnection>) -> impl Responder {
    // Extract the bucket name from the path
    let bucket_name = path.into_inner();

    // Delete the folder for the bucket
    let storage = Storage::new(&get_config());
    match storage.delete_folder(&bucket_name) {
        Ok(_) => {
            if db_delete_bucket(&bucket_name, &db_conn).await {
                HttpResponse::Ok().body(format!("Bucket deleted: {}", bucket_name))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to delete bucket from database: {}", bucket_name))
            }
        },
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                HttpResponse::NotFound().body(format!("Bucket not found: {}", e))
            } else {
                HttpResponse::InternalServerError().body(format!("Failed to delete bucket: {}", e))
            }
        },
    }
}