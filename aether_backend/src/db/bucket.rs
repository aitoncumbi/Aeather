use sea_orm::ActiveValue::Set;
use sea_orm::{ConnectionTrait, EntityTrait, JsonValue, ColumnTrait, QueryFilter};
use tracing::{info, error};
use uuid::Uuid;
use chrono::Utc;
use crate::db;
use crate::models::bucket;
use sea_orm::DatabaseConnection;

pub async fn create_bucket(bucket_name: &str, db_conn: &DatabaseConnection) -> bool {
    let new_bucket = bucket::ActiveModel{
        id: Set(Uuid::new_v4().to_string()),
        name: Set(bucket_name.to_string()),
        description: Set("".to_string()), // Default description
        created_at: Set(Utc::now().to_string()),
        updated_at: Set(Utc::now().to_string()),
        owner_id: Set("default_owner".to_string()), // Replace with actual owner ID logic
        is_public: Set(true),
        is_archived: Set(false),
        is_deleted: Set(false),
        tags: Set(JsonValue::Array(vec![])), // Default empty tags
    };

    initialize_table(db_conn).await.unwrap_or_else(|e| {
        error!("Failed to initialize bucket table: {}", e);
    });

    match bucket::Entity::insert(new_bucket.clone()).exec(db_conn).await {
        Ok(_) => true,
        Err(e) => {
            error!("Failed to create bucket: {}", e);
            false
        }
    }
}


pub async fn initialize_table(db_conn: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    let create_table_sql = r#"
CREATE TABLE IF NOT EXISTS bucket (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT,
    updated_at TEXT,
    owner_id TEXT,
    is_public BOOLEAN,
    is_archived BOOLEAN,
    is_deleted BOOLEAN,
    tags TEXT
);
"#;
    sea_orm::DatabaseConnection::execute(db_conn, sea_orm::Statement::from_string(sea_orm::DatabaseBackend::Sqlite, create_table_sql.to_owned())).await?;
    Ok(())
}

pub async fn delete_bucket(bucket_name: &str, db_conn: &DatabaseConnection) -> bool {
    let bucket_id = match bucket::Entity::find()
        .filter(bucket::Column::Name.eq(bucket_name))
        .one(db_conn)
        .await
    {
        Ok(Some(bucket)) => bucket.id,
        Ok(None) => {
            error!("Bucket not found: {}", bucket_name);
            return false;
        }
        Err(e) => {
            error!("Failed to find bucket: {}", e);
            return false;
        }
    };

    match bucket::Entity::delete_by_id(bucket_id).exec(db_conn).await {
        Ok(_) => {
            info!("Bucket deleted successfully: {}", bucket_name);
            true
        }
        Err(e) => {
            error!("Failed to delete bucket: {}", e);
            false
        }
    }
}