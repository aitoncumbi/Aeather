use std::path::PathBuf;
use std::process::ExitCode;

use aether_backend::cli::{self, BucketCommand, ClientArgs, KeyCommand};
use aether_backend::config::Config;
use aether_backend::{logger, server};
use clap::{Parser, Subcommand};
use tracing::error;

/// S3-compatible object storage.
#[derive(Parser)]
#[command(name = "aether", version)]
struct Args {
    /// Config file. Defaults to ./aether.toml when it exists.
    #[arg(short, long, env = "AETHER_CONFIG", global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server (the default)
    Serve,
    /// Manage access keys and their permissions
    Key {
        #[command(flatten)]
        client: ClientArgs,
        #[command(subcommand)]
        command: KeyCommand,
    },
    /// Manage buckets
    Bucket {
        #[command(flatten)]
        client: ClientArgs,
        #[command(subcommand)]
        command: BucketCommand,
    },
    /// Show server status and usage
    Status {
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Verify every stored block now (normally runs on a schedule)
    Scrub {
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Exit successfully if the server is ready (for container health checks)
    Health {
        /// S3 endpoint [default: from s3.listen in the config file]
        #[arg(long)]
        url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let config_path = args.config.as_deref();
    let (request, client) = match args.command {
        None | Some(Command::Serve) => return serve(config_path).await,
        Some(Command::Key { client, command }) => (cli::Request::Key(command), client),
        Some(Command::Bucket { client, command }) => (cli::Request::Bucket(command), client),
        Some(Command::Status { client }) => (cli::Request::Status, client),
        Some(Command::Scrub { client }) => (cli::Request::Scrub, client),
        Some(Command::Health { url }) => {
            return match cli::health(url.as_deref(), config_path).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("aether: {e:#}");
                    ExitCode::FAILURE
                }
            };
        }
    };
    match cli::run(request, &client, config_path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("aether: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(config_path: Option<&std::path::Path>) -> ExitCode {
    let config = match Config::load(config_path) {
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
