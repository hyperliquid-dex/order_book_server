#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod config;
mod listeners;
mod order_book;
mod prelude;
mod servers;
pub mod slack_alerts;
mod types;

pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

pub const HL_NODE: &str = "hl-node";
