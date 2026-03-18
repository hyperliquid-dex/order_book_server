# Newcomer's Guide to the Order Book Server

A practical guide for developers experienced in Python/Go who are new to Rust.

**NOTE**: this guide was created on 2026-01-19, commit `cbeb1b5`, the project may have changed since then.

---

## What This Application Does

This server streams real-time order book data from a [Hyperliquid](https://hyperliquid.xyz) node to clients via WebSocket. Think of it as a data pipeline:

```
┌──────────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Hyperliquid Node    │────▶│  This Server         │────▶│  Your Clients   │
│  (produces events)   │     │  (processes & serves)│     │  (consume data) │
└──────────────────────┘     └──────────────────────┘     └─────────────────┘
```

**Key features:**
- Maintains in-memory order books for all traded assets
- Serves three subscription types: `l2book`, `trades`, and `l4book`
- Validates state against node snapshots every 10 seconds
- Supports WebSocket compression for bandwidth efficiency

---

## API Overview

Clients connect to `ws://<host>:<port>/ws` and send JSON messages using a simple subscribe envelope:

```json
{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}
{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
{"method":"subscribe","subscription":{"type":"l4Book","coin":"ETH"}}
```

**Subscriptions:**
- `l2Book`: Aggregated levels for a coin.
  - Optional fields: `n_levels` (1-100, default 20), `sig_figs`, `mantissa`.
- `trades`: Fill events for a coin.
- `l4Book`: Full order-level book. Sends an initial snapshot, then block-by-block diffs.

This server follows Hyperliquid’s WebSocket subscription format for `l2book` and `trades`, and adds `l4book` as an extra endpoint.

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WORKSPACE LAYOUT                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   order_book_server/                                                        │
│   ├── server/                    # Core library (all the logic)             │
│   │   └── src/                                                              │
│   │       ├── listeners/         # File watching & event parsing            │
│   │       ├── order_book/        # In-memory data structures                │
│   │       ├── servers/           # WebSocket handler                        │
│   │       ├── types/             # API types & subscriptions                │
│   │       ├── lib.rs             # Library entry point                      │
│   │       └── prelude.rs         # Common imports                           │
│   │                                                                         │
│   └── binaries/                  # Executable crates                        │
│       └── src/bin/                                                          │
│           ├── websocket_server.rs   # Main binary                           │
│           └── example_client.rs     # Client example                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow: The Big Picture

The server processes three types of events from the node:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          NODE EVENT FILES                                   │
│                     ($HOME/hl-node/events/)                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   fills/hourly/YYYYMMDD/*.jsonl                                             │
│       └─▶ Trade executions (who bought/sold, price, size)                   │
│                                                                             │
│   order_statuses/hourly/YYYYMMDD/*.jsonl                                    │
│       └─▶ Order lifecycle events (opened, triggered, cancelled)             │
│                                                                             │
│   order_diffs/hourly/YYYYMMDD/*.jsonl                                       │
│       └─▶ Raw book changes (add order, update size, remove order)           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DIRECTORY LISTENERS                                  │
│                   (server/src/listeners/)                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Uses `notify` crate to watch for file changes                             │
│   Parses MessagePack (RMP) encoded batches                                  │
│   Synchronizes events by block number                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ORDER BOOK STATE                                     │
│                   (server/src/order_book/)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   OrderBook<O>       Single-coin book with bid/ask sides                    │
│   OrderBooks<O>      Multi-coin container                                   │
│   LinkedList<K,T>    O(1) order cancellation                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BROADCAST CHANNELS                                   │
│                   (tokio::broadcast)                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   InternalMessage::Snapshot      →  L2 book updates                         │
│   InternalMessage::Fills         →  Trade events                            │
│   InternalMessage::L4BookUpdates →  Full book diffs                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WEBSOCKET CLIENTS                                    │
│                   (filtered by subscription)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Client A: subscribed to l2book ETH                                        │
│   Client B: subscribed to trades BTC, l4book ETH                            │
│   Client C: subscribed to l2book BTC (5 sig figs)                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Components Explained

### 1. The Order Book (`server/src/order_book/`)

**Rust vs Python/Go context:**
In Python, you might use a `dict` with price as key and a list of orders as value.
In Go, you'd use a `map[float64][]Order`.
In Rust, we use `BTreeMap<Px, LinkedList<O>>` for sorted iteration by price.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          OrderBook<O>                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   bids: BTreeMap<Px, LinkedList<Oid, O>>    (sorted high→low)               │
│         ┌─────────┬──────────────────────────────────────┐                  │
│         │ $100.50 │ [Order1] ←→ [Order2] ←→ [Order3]     │                  │
│         │ $100.40 │ [Order4] ←→ [Order5]                 │                  │
│         │ $100.30 │ [Order6]                             │                  │
│         └─────────┴──────────────────────────────────────┘                  │
│                                                                             │
│   asks: BTreeMap<Px, LinkedList<Oid, O>>    (sorted low→high)               │
│         ┌─────────┬──────────────────────────────────────┐                  │
│         │ $100.60 │ [Order7] ←→ [Order8]                 │                  │
│         │ $100.70 │ [Order9]                             │                  │
│         └─────────┴──────────────────────────────────────┘                  │
│                                                                             │
│   oid_to_side_px: HashMap<Oid, (Side, Px)>  (for O(1) lookup)               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Why a custom LinkedList?**
Standard library linked lists don't support O(1) removal by key.
Our `LinkedList<K, T>` uses a `slab::Slab` (arena allocator) plus a `HashMap<K, usize>`
to enable instant order cancellation without traversing the list.

### 2. The Listener (`server/src/listeners/order_book/`)

The listener watches node event files and applies updates to the order book.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      OrderBookListener                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   order_book_state: Option<OrderBookState>                                  │
│       └─▶ None until first snapshot fetched                                 │
│                                                                             │
│   order_status_cache: BatchQueue<NodeDataOrderStatus>                       │
│   order_diff_cache:   BatchQueue<NodeDataOrderDiff>                         │
│       └─▶ Buffer events until paired by block number                        │
│                                                                             │
│   internal_message_tx: Sender<InternalMessage>                              │
│       └─▶ Broadcast channel to WebSocket clients                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
   ┌─────────────┐          ┌─────────────┐           ┌─────────────┐
   │  Fills      │          │  Statuses   │           │  Diffs      │
   │  Watcher    │          │  Watcher    │           │  Watcher    │
   └─────────────┘          └─────────────┘           └─────────────┘
        │                           │                           │
        │                           └─────────┬─────────────────┘
        │                                     │
        ▼                                     ▼
   Broadcast                           Pair by block,
   immediately                         then apply to
   as Trades                           OrderBookState
```

### 3. The WebSocket Server (`server/src/servers/websocket_server.rs`)

Each connected client gets its own async task that:
1. Subscribes to the internal broadcast channel
2. Filters messages based on client subscriptions
3. Serializes and sends matching data

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WebSocket Handler Flow                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   tokio::select! {                                                          │
│       msg = internal_rx.recv() => {                                         │
│           // Filter by client's SubscriptionManager                         │
│           // Send matching L2/L4/Trades to client                           │
│       }                                                                     │
│       msg = client_socket.recv() => {                                       │
│           // Parse subscribe/unsubscribe requests                           │
│           // Update SubscriptionManager                                     │
│       }                                                                     │
│   }                                                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Rust Patterns (for Python/Go developers)

### Generics with Traits

**Python equivalent:** Duck typing / Protocol classes
**Go equivalent:** Interfaces

This codebase uses a trait called `InnerOrder` to abstract over different order types:

```rust
// server/src/order_book/types.rs
pub trait InnerOrder: Clone + Send + Sync {
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn limit_px(&self) -> Px;
    fn sz(&self) -> Sz;
    fn fill(&mut self, maker_order: &mut Self) -> Sz;
    fn modify_sz(&mut self, sz: Sz);
}
```

Then `OrderBook<O: InnerOrder>` works with any type implementing this trait.
This is similar to Go's `interface{}` but with compile-time guarantees.

### Error Handling

**Python:** Exceptions with try/except
**Go:** Multiple returns with `(value, error)`
**Rust:** `Result<T, E>` enum

```rust
// server/src/prelude.rs
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

// Usage (with ? operator for propagation, like Go's if err != nil)
fn do_something() -> Result<Data> {
    let file = std::fs::read_to_string(path)?;  // Returns early on error
    Ok(parse(file)?)
}
```

### Ownership and Borrowing

**The key concept Python/Go developers need to understand:**

```rust
// In Python/Go, this would just copy a reference:
let book = OrderBook::new();
process(book);      // In Rust: book is MOVED, can't use it anymore!

// To keep using it, you borrow:
process(&book);     // Immutable borrow (like passing a const pointer)
process(&mut book); // Mutable borrow (exclusive access)

// Or clone (explicit copy, like Python's copy.deepcopy):
process(book.clone());
```

### Async/Await

**Python:** `async def` / `await`
**Go:** Goroutines / channels
**Rust:** `async fn` / `.await` with an executor (Tokio)

```rust
// Rust async looks like Python's
async fn fetch_data() -> Result<Data> {
    let response = client.get(url).await?;
    Ok(response.json().await?)
}

// But spawning is explicit (like Go's go keyword)
tokio::spawn(async move {
    loop {
        process().await;
    }
});
```

---

## The Subscription System

Clients send JSON messages to subscribe:

```json
{"method": "subscribe", "subscription": {"type": "l2Book", "coin": "ETH"}}
{"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}
{"method": "subscribe", "subscription": {"type": "l4Book", "coin": "ETH"}}
```

**L2 Book options:**
- `n_levels`: 1-100 (default 20) - depth of book
- `n_sig_figs`: 2-5 - price grouping by significant figures
- `mantissa`: 2 or 5 - rounding factor

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Subscription Data Flow                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐                                                           │
│   │ Client      │─── subscribe(l2book, ETH) ───▶ SubscriptionManager        │
│   │ WebSocket   │                                      │                    │
│   └─────────────┘                                      │                    │
│         ▲                                              ▼                    │
│         │                                   ┌──────────────────┐            │
│         │                                   │  HashSet<Sub>    │            │
│         │                                   │  - l2Book(ETH)   │            │
│         │                                   └──────────────────┘            │
│         │                                              │                    │
│         │                                              │ filter             │
│         │                                              ▼                    │
│         └──── L2Book { coin: "ETH", ... } ────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Running and Testing

### Quick Start

```bash
# Build everything (debug)
cargo build --workspace
# Build everything (release)
cargo build --release

# Run linter (required before commits)
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run tests
cargo test --workspace
# Run deeper tests (nightly, shuffled)
cargo test --workspace --all-features -- -Z unstable-options --shuffle

# Run server (requires node running)
RUST_LOG=info cargo run --release --bin websocket_server -- \
  --address 0.0.0.0 \
  --port 8000
```

### Testing a Subscription

**Using websocat (simple interactive client):**
```bash
websocat ws://localhost:8000/ws

# Then send:
{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}
```

**Using wsstat (performance testing and stats):**
```bash
# Install wsstat: https://github.com/jkbrsn/wsstat
# Test connection and measure latency
wsstat \
  -s -t '{"method":"subscribe","subscription":{"type":"l2Book","coin":"ETH"}}' \
  --summary-interval 10s \
  ws://localhost:8000/ws
```

---

## File-by-File Guide

| File | What it does | Read this if... |
|------|--------------|-----------------|
| `server/src/prelude.rs` | Type aliases (`Error`, `Result`) | You want to understand error handling |
| `server/src/order_book/types.rs` | Core types (`Px`, `Sz`, `Coin`, `InnerOrder`) | You want to understand the data model |
| `server/src/order_book/mod.rs` | `OrderBook<O>` implementation | You want to understand order matching |
| `server/src/order_book/linked_list.rs` | Custom linked list with O(1) removal | You want to understand the performance optimization |
| `server/src/listeners/order_book/mod.rs` | File watching and event processing | You want to understand data ingestion |
| `server/src/listeners/order_book/state.rs` | Order book state updates | You want to understand how updates are applied |
| `server/src/types/subscription.rs` | Subscription types and validation | You want to understand the client API |
| `server/src/servers/websocket_server.rs` | WebSocket routing and handlers | You want to understand the server layer |
| `binaries/src/bin/websocket_server.rs` | CLI entry point | You want to understand startup configuration |

---

## Common Tasks

### Adding a New Subscription Type

1. Add variant to `Subscription` enum in `types/subscription.rs`
2. Add variant to `InternalMessage` in `listeners/order_book/mod.rs`
3. Add handling in `handle_socket()` in `servers/websocket_server.rs`
4. Add serialization type in `types/mod.rs`

### Modifying Order Book Logic

1. Update `InnerOrder` trait if changing order properties
2. Modify `OrderBook::add_order()` for matching logic
3. Update `OrderBookState::apply_updates()` for state transitions
4. Add tests in the same file using `#[cfg(test)]` blocks

### Debugging

```bash
# Full debug logging
RUST_LOG=debug cargo run --bin websocket_server

# Module-specific logging
RUST_LOG=server::listeners=trace cargo run --bin websocket_server

# Run specific test with output
cargo test test_order_book_add -- --nocapture
```

---

## Concurrency Model Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Concurrency Architecture                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Main Thread                                                               │
│   └─▶ Spawns Tokio runtime                                                  │
│                                                                             │
│   Tokio Tasks:                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  hl_listen() task                                                   │   │
│   │  └─▶ File watchers (notify crate, 3 directories)                    │   │
│   │  └─▶ Snapshot fetch timer (every 10s)                               │   │
│   │  └─▶ Heartbeat checker (exit if no events for 5s)                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  Per-client WebSocket tasks (spawned on connection)                 │   │
│   │  └─▶ Receives from broadcast channel                                │   │
│   │  └─▶ Sends filtered messages to client                              │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   Shared State:                                                             │
│   └─▶ Arc<Mutex<OrderBookListener>>  (async mutex, short hold times)        │
│                                                                             │
│   Communication:                                                            │
│   └─▶ tokio::broadcast::channel (100 capacity, multi-consumer)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Go comparison:**
- `tokio::spawn` is like `go func()`
- `tokio::broadcast` is like a buffered channel with multiple receivers
- `Arc<Mutex<T>>` is like `sync.Mutex` protecting shared data

---

## Exit Conditions

The server will exit automatically when:

| Condition | Exit Code | Meaning |
|-----------|-----------|---------|
| No events for 5 seconds | 1 | Node stopped producing data |
| Snapshot validation fails | 2 | Local state diverged from node |
| Channel closed | 1 | Internal communication failure |

This is intentional: the server should be restarted by a supervisor (systemd, Docker, etc.) if it exits.

---

## Further Reading

- [The Rust Book](https://doc.rust-lang.org/book/) - Official Rust tutorial
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial) - Async Rust
- [AGENTS.md](./AGENTS.md) - Detailed technical reference for this codebase
- [README.md](./README.md) - Quick start and API reference
