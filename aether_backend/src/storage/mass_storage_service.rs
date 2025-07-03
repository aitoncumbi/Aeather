use crate::config::Config;
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn, error};

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
}