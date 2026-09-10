//! Custom error types for the order book server.
//!
//! This module provides strongly-typed errors with proper categorization,
//! enabling better error handling, debugging, and error messages throughout
//! the application.

use std::path::PathBuf;
use thiserror::Error;

/// The main error type for the order book server.
///
/// This enum provides specific error variants for different failure modes,
/// making it easier to handle errors appropriately and provide meaningful
/// error messages to users.
#[derive(Debug, Error)]
pub enum OrderBookError {
    /// I/O errors (file operations, network, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization errors
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    /// Float parsing errors
    #[error("Float parsing error: {0}")]
    FloatParse(#[from] std::num::ParseFloatError),

    /// WebSocket upgrade errors
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// HTTP request errors
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    /// File system watching errors
    #[error("File watching error: {0}")]
    FileWatch(String),

    /// Price parsing errors
    #[error("Price parsing error: invalid format '{0}'")]
    PriceParse(String),

    /// Size parsing errors
    #[error("Size parsing error: invalid format '{0}'")]
    SizeParse(String),

    /// Order not found in the book
    #[error("Order not found: oid {0}")]
    OrderNotFound(u64),

    /// Insert-before order not found when adding a priority order
    #[error("Insert-before order not found: oid {0}")]
    InsertBeforeNotFound(u64),

    /// Order status not found when processing diff
    #[error("Order status not found for diff: oid {0}")]
    OrderStatusNotFound(u64),

    /// Block height mismatch during state updates
    #[error("Block height mismatch: expected {expected}, got {actual}")]
    BlockHeightMismatch {
        /// Expected block height
        expected: u64,
        /// Actual block height received
        actual: u64,
    },

    /// Snapshot validation failed
    #[error("Snapshot validation failed: {0}")]
    SnapshotValidation(String),

    /// Node communication errors
    #[error("Node communication error: {0}")]
    NodeCommunication(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Invalid subscription request
    #[error("Invalid subscription: {0}")]
    InvalidSubscription(String),

    /// Order book not ready for streaming
    #[error("Order book not ready for streaming (waiting for snapshot)")]
    NotReady,

    /// Channel communication errors
    #[error("Channel communication error: {0}")]
    Channel(String),

    /// File path errors
    #[error("File path error: {0}")]
    FilePath(String),

    /// Home directory not found
    #[error("Could not find home directory")]
    HomeDirectoryNotFound,

    /// Stream has fallen behind
    #[error("Stream has fallen behind ({HL_NODE} failed?)")]
    StreamFallenBehind,

    /// Generic error with message
    #[error("{0}")]
    Generic(String),
}

/// Constant for node name in error messages
const HL_NODE: &str = "hl-node";

impl From<String> for OrderBookError {
    fn from(msg: String) -> Self {
        OrderBookError::Generic(msg)
    }
}

impl From<&str> for OrderBookError {
    fn from(msg: &str) -> Self {
        OrderBookError::Generic(msg.to_string())
    }
}

impl From<notify::Error> for OrderBookError {
    fn from(err: notify::Error) -> Self {
        OrderBookError::FileWatch(err.to_string())
    }
}

impl OrderBookError {
    /// Create an error for when a coin is missing from the universe
    pub fn coin_not_found(coin: &str) -> Self {
        OrderBookError::InvalidSubscription(format!("Coin '{}' not found in universe", coin))
    }

    /// Create an error for file path issues
    pub fn file_path(path: PathBuf, context: &str) -> Self {
        OrderBookError::FilePath(format!("{}: {}", context, path.display()))
    }

    /// Check if this error is recoverable (can retry)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            OrderBookError::WebSocket(_)
                | OrderBookError::NodeCommunication(_)
                | OrderBookError::Http(_)
        )
    }

    /// Check if this error indicates the server should shut down
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            OrderBookError::Io(_)
                | OrderBookError::Channel(_)
                | OrderBookError::StreamFallenBehind
        )
    }
}
