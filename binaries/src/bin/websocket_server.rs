#![allow(unused_crate_dependencies)]
use std::{env::home_dir, net::Ipv4Addr};

use clap::Parser;
use server::{Result, run_websocket_server, ProcessingMode};

#[derive(Debug, Clone, clap::ValueEnum)]
enum CliProcessingMode {
    /// Process events individually as they arrive (default)
    Single,
    /// Process events in batches by block  
    Batched,
}

impl From<CliProcessingMode> for ProcessingMode {
    fn from(cli_mode: CliProcessingMode) -> Self {
        match cli_mode {
            CliProcessingMode::Single => ProcessingMode::Single,
            CliProcessingMode::Batched => ProcessingMode::Batched,
        }
    }
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Directory path for node data files (defaults to home directory)
    #[arg(long)]
    directory: Option<std::path::PathBuf>,

    /// Processing mode: single (default) or batched
    #[arg(long, value_enum, default_value = "single")]
    mode: CliProcessingMode,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let directory = args.directory.unwrap_or_else(|| {
        home_dir().expect("Could not find home directory")
    });

    let processing_mode = ProcessingMode::from(args.mode);

    run_websocket_server(&full_address, directory, processing_mode, true).await?;

    Ok(())
}
