#![allow(unused_crate_dependencies)]
use std::{env::home_dir, net::Ipv4Addr, path::PathBuf};

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

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Base directory containing hl/data/ subdirectories (host path).
    /// Defaults to the user's home directory.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Container's equivalent of `data_dir`, used for API requests when the HL node
    /// runs in a container. If not specified, defaults to `data_dir`.
    /// Example: --data-dir /data --container-data-dir /home/hluser
    #[arg(long)]
    container_data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    let data_dir = args.data_dir.or_else(home_dir).ok_or("Could not determine data directory")?;
    let container_data_dir = args.container_data_dir.unwrap_or_else(|| data_dir.clone());
    run_websocket_server(&full_address, true, compression_level, data_dir, container_data_dir).await?;

    Ok(())
}
