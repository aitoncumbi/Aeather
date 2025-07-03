pub struct Object {
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
    pub tags: Vec<String>,
    pub metadata: Option<String>, // Optional metadata field for additional information
    pub size: u64, // Size of the object in bytes
    pub content_type: String, // MIME type of the object
    pub version: Option<String>, // Versioning support for the object
    pub last_accessed: Option<String>, // Timestamp of the last access
    pub permissions: Vec<String>, // List of permissions associated with the object
    pub checksum: Option<String>, // Checksum for integrity verification
}