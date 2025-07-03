use tracing_subscriber::{EnvFilter};
use tracing_appender::rolling::daily;
use tracing_subscriber::fmt::writer::MakeWriterExt;

pub fn init_logger() {
     let file_appender = daily("logs", "app.log").with_max_level(tracing::Level::DEBUG);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_writer(std::io::stdout.and(file_appender))
        .with_env_filter(env_filter)
        .with_target(true)
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        // .json()
        .compact()
        .init();

}