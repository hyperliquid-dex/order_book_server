# Hyperliquid Orderbook WebSocket Server

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

## API

Custom adaptions not included in the [official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions):

- The `l2book` subscription includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

## Architecture Overview

For a detailed guide with diagrams aimed at developers new to Rust, see [NEW.md](./NEW.md).

- `server/` is the core library crate. It ingests node event files via `listeners/`, maintains order book state in `order_book/`, and exposes WebSocket/http handlers in `servers/`.
- `binaries/` contains runnable entry points such as `websocket_server.rs`, which wires configuration, logging, and the `server` crate together.
- Data flow: node event files -> listener parsing -> in-memory order books -> websocket subscriptions (`l2book`, `trades`, `l4book`).

## Setup

### Rust

This project uses nightly Rust for formatting and test shuffling. We recommend `rustup` as the toolchain manager.

```bash
# Install rustup (see rustup.rs for platform-specific instructions)
rustup toolchain install nightly
rustup component add rustfmt clippy --toolchain nightly

# Use nightly for this repo
rustup default nightly
```

### justfile

Optional convenience commands are provided via `just`. Install it once:

```bash
cargo install just
```

Usage examples:

```bash
just --list # --unsorted (optional)
just build
just test
just fmt
just lint
just clean
just asan
just tsan
```

### Git Hooks

This repo ships a [pre-commit](.githooks/pre-commit) hook for formatting and linting. To enable it:

```bash
git config core.hooksPath .githooks
```

### Running the Server

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Ensure batching by block is enabled and the node records fills, order statuses, and raw book diffs.

2. Then run this local server:

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
# With custom inactivity timeout (e.g., 30s):
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000 --inactivity-exit-secs 30
```

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (default 5 seconds; configurable via `--inactivity-exit-secs <secs>`). In addition, the local server periodically fetches order book snapshots from the node, and compares to its own internal state. If a difference is detected, it will exit.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Development

### Format and Lint

```bash
# Format
cargo fmt --all

# Lint
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Note: formatting line length set to max 120 columns in [rustfmt.toml](./rustfmt.toml).

### Build

```bash
# Build everything (debug)
cargo build --workspace
# Build everything (release)
cargo build --release
```

Build artifacts are written under `target/`:
- Debug binaries in `target/debug/<bin_name>`
- Release binaries in `target/release/<bin_name>`

### Tests

```bash
# Run all tests
cargo test --workspace
# Run deeper tests (requires nightly)
cargo test --workspace --all-features -- -Z unstable-options --shuffle

# Run specific tests
cargo test -p server              # Library crate only
cargo test test_trade_listener    # Single test by name
```

Runs all unit tests for the `server` and `binaries` crates.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- The current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation.
