use crate::config::Config;
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn, error};
use infer::Infer;
use uuid::Uuid;

pub struct Storage {
    pub root_path: String,
}


impl Storage {
    pub fn new(config: &Config) -> Self {
        Storage {
            root_path: config.storage_root.clone(),
        }
    }

    pub fn new_folder(&self, folder_name: &str) -> Result<String, std::io::Error> {
        let folder_path = format!("{}/{}", self.root_path, folder_name);
        let path = PathBuf::from(&folder_path);
        if !path.exists() {
            fs::create_dir_all(&path)?;
            info!("Created folder: {}", folder_path);
            return Ok(folder_path);
        } else {
            warn!("Folder {} already exists", folder_name);
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Folder {} already exists", folder_name),
            ));
        }
        
    }

    pub fn delete_folder(&self, folder_name: &str) -> Result<(), std::io::Error> {
        let folder_path = format!("{}/{}", self.root_path, folder_name);
        let path = PathBuf::from(&folder_path);
        if path.exists() {
            fs::remove_dir_all(&path)?;
            info!("Deleted folder: {}", folder_path);
            return Ok(());
        } else {
            error!("Folder {} does not exist", folder_name);
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Folder {} does not exist", folder_name),
            ));
        }
    }

    pub fn save_object(&self, bucket_name: &str, object_name: &str, data: &[u8]) -> Result<String, std::io::Error> {
        let bucket_path = PathBuf::from(format!("{}/{}", self.root_path, bucket_name));
        if !bucket_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Bucket {} not found", bucket_name),
            ));
        }

        // Create a directory with the file name (without extension)
        let file_dir = bucket_path.join(object_name);
        fs::create_dir_all(&file_dir)?;

        // Save the file inside this directory, with the correct extension
        let file_extension = self.get_file_extension(data);
        let file_name_string = uuid::Uuid::new_v4().to_string();
        let file_name = format!("{}.{}", file_name_string, file_extension);
        let file_path = file_dir.join(file_name);

        fs::write(&file_path, data)?;
        info!("Saved object: {}", file_path.display());
        Ok(file_path.to_string_lossy().to_string())
    }

     fn get_mime_type(&self, data: &[u8]) -> String {
        match Infer::new().get(data) {
            Some(infer) => infer.mime_type().to_string(),
            None => "application/octet-stream".to_string(),
        }
    }

    fn get_file_extension(&self, data: &[u8]) -> String {
        match Infer::new().get(data) {
            Some(infer) => infer.extension().to_string(),
            None => "bin".to_string(),
        }
    }

}