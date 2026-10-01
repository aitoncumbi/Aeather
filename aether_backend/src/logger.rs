use tracing_subscriber::EnvFilter;

use crate::config::{LogConfig, LogFormat};

/// Logs to stdout. `RUST_LOG`, when set, overrides the configured level.
pub fn init(config: &LogConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(directives(&config.level)));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_target(true);
    let result = match config.format {
        LogFormat::Json => builder.json().try_init(),
        LogFormat::Text => builder.compact().try_init(),
    };
    if let Err(e) = result {
        eprintln!("logger already initialized: {e}");
    }
}

/// The S3 protocol library logs every failed request at ERROR, down to the
/// body of malformed XML. Client mistakes are not server errors (the access
/// log records them), so its logs are off unless the level names it, e.g.
/// `info,s3s=debug`.
fn directives(level: &str) -> String {
    if level.split(',').any(|d| d.trim().starts_with("s3s")) { level.to_owned() } else { format!("{level},s3s=off") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3s_logs_are_off_unless_asked_for() {
        assert_eq!(directives("info"), "info,s3s=off");
        assert_eq!(directives("warn,aether::access=info"), "warn,aether::access=info,s3s=off");
        assert_eq!(directives("info, s3s=debug"), "info, s3s=debug");
    }
}
