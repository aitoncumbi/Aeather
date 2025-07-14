#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiErrorResponse {
    pub status: String,
    pub code: String,
    pub message: String,
    pub level: String,
}


impl ApiErrorResponse {
    pub fn new(status: &str, code: &str, message: &str, level: &str) -> Self {
        ApiErrorResponse {
            status: status.to_string(),
            code: code.to_string(),
            message: message.to_string(),
            level: level.to_string(),
        }
    }
}