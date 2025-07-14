use sea_orm::entity::prelude::*;
use sea_orm::JsonValue;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "object")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: String,
    pub key: String, // Unique identifier for the object
    pub bucket_id: String, // Reference to the bucket this object belongs to
    pub name: String,
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
    pub owner_id: String,
    pub is_public: bool,
    pub is_archived: bool,
    pub is_deleted: bool,
    pub tags: Option<JsonValue>, // Store as JSON array
    pub metadata: Option<String>, // Optional metadata field for additional information
    pub size: u64, // Size of the object in bytes
    pub content_type: String, // MIME type of the object
    pub version: Option<String>, // Versioning support for the object same as object name (uuid)
    pub last_accessed: Option<String>, // Timestamp of the last access
    pub permissions: Option<JsonValue>, // Store as JSON array
    pub checksum: Option<String>, // Checksum for integrity verification
}

// Required by SeaORM even if there are no relations
#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl sea_orm::RelationTrait for Relation {
    fn def(&self) -> sea_orm::RelationDef {
        panic!("No RelationDef for object::Relation because there are no relations")
    }
}

impl ActiveModelBehavior for ActiveModel {}