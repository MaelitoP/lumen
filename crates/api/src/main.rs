use clap::Parser;
use lumen_api::{serve, serve_cluster, Cli, Command};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Standalone(config) => serve(config).await,
        Command::Cluster(config) => serve_cluster(config).await,
    }
}
