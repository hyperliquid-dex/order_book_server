#![allow(unused_crate_dependencies)]
use std::{env::home_dir, net::Ipv4Addr};

use clap::Parser;
use server::{Result, run_websocket_server};

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

    run_websocket_server(&full_address, directory, true).await?;

    Ok(())
}
