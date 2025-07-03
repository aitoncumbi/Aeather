use std::env;
use serde::Deserialize;
use dotenv::dotenv;
use tracing::{error};


#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub api_key: String,
    pub storage_root: String,
    pub api_port: u16,
    pub web_console_port: u16,
}

pub fn init_config() -> Result<Config, Box<dyn std::error::Error>> {
    dotenv().ok();

    let api_key = match env::var("API_KEY") {
        Ok(val) => val.trim().to_string(),
        Err(_) => {
            error!("API_KEY must be set in the environment");
            return Err("API_KEY must be set in the environment".into());
        }
    };

    let storage_root = match env::var("STORAGE_ROOT") {
        Ok(val) => val.trim().to_string(),
        Err(_) => {
            error!("STORAGE_ROOT must be set in the environment");
            return Err("STORAGE_ROOT must be set in the environment".into());
        }
    };

    let api_port = match env::var("API_PORT") {
        Ok(val) => match val.trim().parse::<u16>() {
            Ok(port) => port,
            Err(_) => {
                error!("API_PORT must be a valid number");
                return Err("API_PORT must be a valid number".into());
            }
        },
        Err(_) => {
            error!("API_PORT must be set in the environment");
            return Err("API_PORT must be set in the environment".into());
        }
    };

    let web_console_port = match env::var("WEB_CONSOLE_PORT") {
        Ok(val) => match val.trim().parse::<u16>() {
            Ok(port) => port,
            Err(_) => {
                error!("WEB_CONSOLE_PORT must be a valid number");
                return Err("WEB_CONSOLE_PORT must be a valid number".into());
            }
        },
        Err(_) => {
            error!("WEB_CONSOLE_PORT must be set in the environment");
            return Err("WEB_CONSOLE_PORT must be set in the environment".into());
        }
    };

    Ok(Config {
        api_key,
        storage_root,
        api_port,
        web_console_port,
    })
}