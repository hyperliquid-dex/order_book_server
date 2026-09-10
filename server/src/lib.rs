//! # Order Book Server
//!
//! A high-performance WebSocket server for streaming Hyperliquid order book data.
//!
//! ## Features
//! - L2 book streaming with configurable price levels and precision
//! - L4 book streaming with full order granularity
//! - Real-time trade streaming
//! - WebSocket compression support
//! - Automatic snapshot validation and consistency checking
//!
//! ## Architecture
//! - `listeners` - File system monitoring for node data
//! - `order_book` - In-memory order book state management
//! - `servers` - WebSocket server implementation
//! - `types` - Data types and serialization
//!
//! ## Quick Start
//! ```no_run
//! use server::{run_websocket_server, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Run server on port 8000 with compression enabled
//!     run_websocket_server("0.0.0.0:8000", true, 1).await
//! }
//! ```
//!
//! ## Endpoints
//! - `l2Book` - Level 2 order book (aggregated by price)
//! - `l4Book` - Level 4 order book (individual orders)
//! - `trades` - Real-time trade stream
//!
//! ## Error Handling
//! All operations return `Result<T, OrderBookError>` where `OrderBookError` provides
//! specific error variants for different failure scenarios:
//! - `Io` - File system and network errors
//! - `Json` - JSON serialization/deserialization errors
//! - `PriceParse` / `SizeParse` - Price and size parsing errors
//! - `OrderNotFound` / `InsertBeforeNotFound` - Order lookup errors
//! - `BlockHeightMismatch` - Block consistency errors
//! - `SnapshotValidation` - Snapshot integrity errors
//! - `WebSocket` - WebSocket communication errors
//! - `Http` - HTTP request errors
//! - `FileWatch` - File system watcher errors
//! - `NodeCommunication` - Hyperliquid node communication errors
//! - `InvalidSubscription` - WebSocket subscription errors
//! - `NotReady` - Server state errors
//! - `Channel` - Internal channel communication errors
//! - `Generic` - Other errors with custom messages
//!
//! ## Error Handling Example
//! ```rust
//! use server::Result;
//! use server::OrderBookError;
//!
//! fn example_function() -> Result<(), OrderBookError> {
//!     // Operations that return Result<T, OrderBookError>
//!     let order_id = process_order()?;
//!     let price = parse_price("34.5")?;
//!     Ok(())
//! }
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod error;
mod listeners;
mod order_book;
mod prelude;
mod servers;
mod types;

pub use error::OrderBookError;
pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Environment variable or binary name for the Hyperliquid node
pub const HL_NODE: &str = "hl-node";
