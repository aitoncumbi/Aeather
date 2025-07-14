use sea_orm::ActiveValue::Set;
use sea_orm::{ConnectionTrait, EntityTrait, JsonValue, ColumnTrait, QueryFilter};
use tracing::{info, error};
use uuid::Uuid;
use chrono::Utc;
use crate::db;
use crate::models::bucket;
use sea_orm::DatabaseConnection;

pub async fn initialize_table(db_conn: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    let create_table_sql = r#"
CREATE TABLE IF NOT EXISTS object (
    id TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT,
    updated_at TEXT,
    owner_id TEXT,
    is_public BOOLEAN,
    is_archived BOOLEAN,
    is_deleted BOOLEAN,
    tags TEXT,
    metadata TEXT,
    size INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    version TEXT,
    last_accessed TEXT,
    permissions TEXT,
    checksum TEXT
);
"#;
    sea_orm::DatabaseConnection::execute(
        db_conn,
        sea_orm::Statement::from_string(sea_orm::DatabaseBackend::Sqlite, create_table_sql.to_owned())
    ).await?;
    Ok(())
}


pub async fn create_object(new_object: bucket::ActiveModel, db_conn: &DatabaseConnection) -> bool {
    // Initialize the table if it doesn't exist
    initialize_table(db_conn).await.unwrap_or_else(|e| {
        error!("Failed to initialize object table: {}", e);
    });

    

    match bucket::Entity::insert(new_object.clone()).exec(db_conn).await {
        Ok(_) => true,
        Err(e) => {
            error!("Failed to create object: {}", e);
            false
        }
    }
}
