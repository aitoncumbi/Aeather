use tracing_subscriber::EnvFilter;

use crate::config::{LogConfig, LogFormat};

/// Logs to stdout. `RUST_LOG`, when set, overrides the configured level.
pub fn init(config: &LogConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(true);
    let result = match config.format {
        LogFormat::Json => builder.json().try_init(),
        LogFormat::Text => builder.compact().try_init(),
    };
    if let Err(e) = result {
        eprintln!("logger already initialized: {e}");
    }
}
