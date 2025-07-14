use tracing_subscriber::{EnvFilter};
use tracing_appender::rolling::daily;
use tracing_subscriber::fmt::writer::MakeWriterExt;

pub fn init_logger() {
     let file_appender = daily("logs", "app.log").with_max_level(tracing::Level::DEBUG);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let result = tracing_subscriber::fmt()
        .with_writer(std::io::stdout.and(file_appender))
        .with_env_filter(env_filter)
        .with_target(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        // .json()
        .compact()
        .try_init();

    if let Err(e) = result {
        // Use println! here since tracing might not be set up yet
        println!("Warning: Logger already initialized: {}", e);
    }
}