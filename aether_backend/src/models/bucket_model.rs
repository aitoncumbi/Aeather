pub struct Bucket{
    pub id: String,
    pub name: String,
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
    pub owner_id: String,
    pub is_public: bool,
    pub is_archived: bool,
    pub is_deleted: bool,
    pub tags: Vec<String>,
}