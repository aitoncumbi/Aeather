use std::path::PathBuf;
use std::process::ExitCode;

use aether_backend::config::Config;
use aether_backend::{logger, server};
use clap::Parser;
use tracing::error;

/// S3-compatible object storage.
#[derive(Parser)]
#[command(name = "aether", version)]
struct Args {
    /// Config file. Defaults to ./aether.toml when it exists.
    #[arg(short, long, env = "AETHER_CONFIG")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let config = match Config::load(args.config.as_deref()) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("aether: invalid configuration: {e}");
            return ExitCode::FAILURE;
        }
    };
    logger::init(&config.log);

    match server::run(config, server::shutdown_signal()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "aether stopped");
            ExitCode::FAILURE
        }
    }
}
