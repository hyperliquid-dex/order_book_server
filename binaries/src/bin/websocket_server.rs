#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;

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

    /// Enable to include orders from spot markets. This is "unsafe" because order statuses for new
    /// orders from special addresses (e.g. the assistance fund and HIP-2) are not emitted so we
    /// **unsafely** assume that these orders are all "Alo" limit orders. Default is false, meaning
    /// that spot orders are ignored.
    #[arg(long)]
    include_spot_unsafe: Option<bool>, // Default is false

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
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let ignore_spot = !args.include_spot_unsafe.unwrap_or(false);

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    run_websocket_server(&full_address, ignore_spot, compression_level).await?;

    Ok(())
}
