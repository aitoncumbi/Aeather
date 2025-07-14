use sea_orm::{Database, DatabaseConnection};
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue;
use std::fs;
use std::path::Path;

pub async fn initialize_configuration_database() -> Result<DatabaseConnection, sea_orm::DbErr> {
    let database_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "./dev/database/".to_string());
    // Ensure the directory exists
    if let Some(parent) = Path::new(&database_path).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|e| sea_orm::DbErr::Custom(format!("Failed to create database directory: {}", e)))?;
        }
    } else {
        fs::create_dir_all(&database_path).map_err(|e| sea_orm::DbErr::Custom(format!("Failed to create database directory: {}", e)))?;
    }
    let db_file_path = format!("{}aether.db", database_path);
    if !Path::new(&db_file_path).exists() {
        fs::File::create(&db_file_path)
            .map_err(|e| sea_orm::DbErr::Custom(format!("Failed to create database file: {}", e)))?;
    }
    let db_url = format!("sqlite:{}", db_file_path);
    Database::connect(&db_url).await
}
