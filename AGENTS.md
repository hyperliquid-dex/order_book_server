# AGENTS.md

Comprehensive guidance for AI assistants and developers working in this repository.

## Quick Start

```bash
# Build and lint
cargo build --workspace
# Must pass before commits
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run all tests
cargo test --workspace
# Run deeper tests (requires nightly)
cargo test --workspace --all-features -- -Z unstable-options --shuffle

# Run specific tests
cargo test -p server              # Library crate only
cargo test test_trade_listener    # Single test by name

# Format code (max width 120)
cargo fmt --all
# Check only
cargo fmt --all -- --check

# Run the server
RUST_LOG=info cargo run --release --bin websocket_server -- \
  --address 0.0.0.0 \
  --port 8000 \
  --websocket-compression-level 1
```

## Architecture

**Purpose:** Local WebSocket server for Hyperliquid order book data. Ingests events from a non-validating Hyperliquid node and serves `l2book`, `trades`, and `l4book` subscriptions with ~100ms latency (batched by block).

### Data Flow

```
Node event files ($HOME/hl-node/events/)
  ├── order_statuses/hourly/YYYYMMDD/*.jsonl
  ├── fills/hourly/YYYYMMDD/*.jsonl
  └── order_diffs/hourly/YYYYMMDD/*.jsonl
            ↓
listeners/order_book/ (DirectoryListener trait)
  - File watchers (notify crate)
  - Event parsers (rmp + serde_json)
  - Batch synchronization by block number
            ↓
order_book/ (in-memory state)
  - OrderBook<O>: BTreeMap<Px, LinkedList<O>>
  - OrderBooks<O>: HashMap<Coin, OrderBook<O>>
  - Snapshot validation every 10s via reqwest
            ↓
servers/websocket_server.rs (axum + yawc)
  - tokio::broadcast channels
  - Per-client subscription filtering
            ↓
Client WebSocket subscriptions
  - l2book: Aggregated levels (up to 100)
  - trades: Fill events
  - l4book: Full order-level book + diffs
```

### Workspace Structure

```
order_book_server/
├── server/              # Core library crate
│   └── src/
│       ├── listeners/   # Event ingestion layer
│       │   ├── directory.rs        # DirectoryListener trait
│       │   └── order_book/         # OrderBookListener implementation
│       │       ├── mod.rs          # Main listener + file watching
│       │       ├── state.rs        # OrderBookState (update logic)
│       │       └── utils.rs        # RMP processing, validation
│       ├── order_book/  # In-memory data structures
│       │   ├── mod.rs              # OrderBook<O> generic book
│       │   ├── types.rs            # InnerOrder trait, Coin, Px, Sz
│       │   ├── multi_book.rs       # OrderBooks<O>, Snapshots<O>
│       │   ├── linked_list.rs      # Custom list for O(1) removal
│       │   └── levels.rs           # Price level aggregation
│       ├── servers/     # HTTP/WebSocket layer
│       │   └── websocket_server.rs # axum handlers + routing
│       ├── types/       # Public API types
│       │   ├── mod.rs              # Trade, L2Book, L4Book, Level
│       │   ├── subscription.rs     # SubscriptionManager, validation
│       │   ├── node_data.rs        # Node event schemas
│       │   └── inner.rs            # Serialization wrappers
│       └── prelude.rs   # Common imports + type aliases
└── binaries/            # Executable crates
    └── src/bin/
        ├── websocket_server.rs     # Main server binary
        └── example_client.rs       # Client example
```

### Key Abstractions

| Component | Location | Purpose |
|-----------|----------|---------|
| `InnerOrder` trait | `order_book/types.rs` | Generic contract for order types (oid, side, price, size, matching) |
| `OrderBook<O>` | `order_book/mod.rs` | Single-coin book: BTreeMap of LinkedLists for bid/ask |
| `OrderBooks<O>` | `order_book/multi_book.rs` | Multi-coin container with concurrent access |
| `OrderBookListener` | `listeners/order_book/mod.rs` | Event ingestion + file watching + state updates |
| `OrderBookState` | `listeners/order_book/state.rs` | Apply order diffs, compute snapshots, validate consistency |
| `SubscriptionManager` | `types/subscription.rs` | Client subscription routing and filtering |
| `DirectoryListener` | `listeners/directory.rs` | Generic trait for file system watchers |

### Concurrency Model

- **Shared state:** `Arc<Mutex<OrderBookListener>>` for order book access
- **Broadcasting:** `tokio::broadcast::channel` for snapshot/fill/L4 updates
- **Async runtime:** Tokio with separate tasks for:
  - File system watching (`notify` crate)
  - Periodic snapshot validation (every 10s)
  - WebSocket connection handlers
- **Parallelism:** `rayon` for parallel snapshot processing

### Subscription Types

1. **`trades`** - Fill events for a single coin
2. **`l2book`** - Aggregated price levels
   - Optional `n_levels` (1-100, default 20)
   - Optional `sig_figs` and `mantissa` for rounding
3. **`l4book`** - Full order-level book (new endpoint)
   - Initial snapshot of all orders
   - Block-by-block diffs (adds, cancels, size changes)

## Code Conventions

### Language & Formatting

- **Edition:** Rust 2024
- **Max line width:** 120 characters (`rustfmt.toml`)
- **Import style:** Crate-level granularity, grouped as StdExternalCrate
- **Naming:** Standard Rust conventions (`snake_case`, `CamelCase`, `SCREAMING_SNAKE_CASE`)

### Linting

Workspace-level clippy with **strict** configuration:
- All categories enabled at warn level: correctness, suspicious, complexity, perf, style, pedantic, nursery, cargo
- `unwrap_used` and `expect_used` warned (allowed only in `#[cfg(test)]`)
- `unsafe_code` warned (avoid unless necessary)
- See `Cargo.toml` for 20+ allowed exceptions (e.g., `cast_precision_loss`, `missing_panics_doc`)

### Error Handling

```rust
// Standard error type (see prelude.rs)
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

// Avoid unwrap/expect except in tests
#[cfg(test)]
fn test_something() {
    let val = maybe_value.unwrap(); // OK in tests
}
```

### Generic Design

The codebase uses trait-based generics for flexibility:
```rust
pub trait InnerOrder: Clone + Send + Sync {
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn price(&self) -> Px;
    fn size(&self) -> Sz;
    // ... matching logic
}

pub struct OrderBook<O: InnerOrder> {
    bids: BTreeMap<Px, LinkedList<O>>,
    asks: BTreeMap<Px, LinkedList<O>>,
}
```

## Testing

### Test Organization

- **Location:** Colocated in module files using `#[test]` blocks (no top-level `tests/` directory)
- **Async tests:** Use `#[tokio::test]` for async functions
- **Deterministic:** Fixed random seeds (`[42; 32]`), explicit wait durations (100ms)
- **Focus:** Order book operations, subscription validation, file system events

### Test Patterns

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trade_listener() {
        let temp_dir = tempfile::tempdir().unwrap();
        // ... setup with mock node data
    }

    #[test]
    fn test_order_book_add() {
        // ... deterministic unit test
    }
}
```

### Running Tests

```bash
cargo test --workspace                                                  # All tests
cargo test --workspace --all-features -- -Z unstable-options --shuffle  # Deeper tests (nightly)
cargo test -p server                                                    # Library only
cargo test test_order_book_add                                          # Single test
cargo test -- --nocapture                                               # Show println! output
RUST_LOG=debug cargo test                                               # With logging
```

## Development Workflow

### Making Changes

1. Read existing code to understand patterns before modifying
2. Maintain consistency with current architecture (generic traits, error types, async patterns)
3. Ensure clippy passes: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
4. Format code: `cargo fmt --all`
5. Run relevant tests as per [Running Tests](#running-tests)
6. Update documentation if public API changes

### Commit Guidelines

Assume the user will be handling any git interactions. If you are asked to help:

- Use conventional commit prefixes
- Use short, imperative messages: "fix panic in orderbook listener"
- Keep commits focused on single behavior change
- Example history:
  ```
  fix: panic in orderbook listener
  feat: use yawc as default WebSocket crate supporting compression
  docs: update readme
  ```

### Pull Request Guidelines

Include in PR description:
1. Concise summary of changes
2. Test commands executed
3. Notes on any API or protocol changes
4. Breaking changes (if any)

## Runtime Behavior

### Node Dependency

- Requires locally running non-validating Hyperliquid node from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)
- Node must be configured with:
  - Batching by block enabled
  - Recording: fills, order statuses, raw book diffs
- Default event directory: `$HOME/hl-node/events/`

### Exit Conditions

Server will automatically exit when:
1. **No events for 5 seconds** - Node stopped emitting data
2. **Snapshot validation fails** - Local state diverged from node (checked every 10s)
3. **Fatal errors** - `std::process::exit(1)` or `exit(2)`

### Performance Characteristics

- **Latency:** ~100ms behind live (batched by block, not streaming)
- **Update frequency:** Per-block updates (every ~1 second in practice)
- **Memory:** Full in-memory order books for all subscribed coins
- **Compression:** Tunable WebSocket compression (0-9, default 1)
- **Snapshot sync:** Periodic validation via HTTP to node (every 10s)

### Logging

```bash
RUST_LOG=info  cargo run --bin websocket_server  # Info level
RUST_LOG=debug cargo run --bin websocket_server  # Debug level
RUST_LOG=server::listeners=trace                 # Module-specific
```

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `axum` | HTTP routing and WebSocket upgrade |
| `yawc` | WebSocket with compression support (replaces tungstenite) |
| `tokio` | Async runtime with full features |
| `notify` | File system event watching |
| `serde` + `serde_json` | JSON serialization |
| `alloy` | Ethereum primitives (addresses, signatures) |
| `rayon` | Parallel iteration for snapshot processing |
| `reqwest` | HTTP client for snapshot fetching |
| `itertools` | Iterator extensions |
| `chrono` | Date/time handling |

## Caveats & Limitations

- Does **not** show untriggered trigger orders
- Does **not** support spot order books currently
- Batched updates (not streaming) introduce ~100ms latency
- Requires node with specific configuration (batching, event recording)
- No persistence - state rebuilt on restart from node snapshots

## Project Context

This is a standalone educational project, not maintained by Hyperliquid Labs. See `README.md` disclaimer for full context. Use at your own risk.
