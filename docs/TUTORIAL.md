# Getting Started with Order Book Server

## Prerequisites

1. **Running Hyperliquid Node**
   - Install a Hyperliquid node that batches by block
   - The node must record:
     - Fills (trades)
     - Order statuses
     - Raw book diffs (order deltas)
   - Handle info requests

2. **Rust Environment**
   - Rust 1.75+ installed
   - Cargo package manager

3. **Basic Understanding**
   - Familiarity with order books (bids/asks)
   - Understanding of Level 2 (aggregated) vs Level 4 (individual orders) data
   - Basic WebSocket concepts

## Setup Steps

### 1. Clone the Repository

```bash
git clone https://github.com/hyperliquid-dex/order_book_server.git
cd order_book_server
cargo build --release
```

### 2. Run the Hyperliquid Node

You need a Hyperliquid node that outputs data to the specified directories:

```bash
# The server expects data in these directories relative to its working directory:
# - hl/data/node_fills_by_block/
# - hl/data/node_order_statuses_by_block/
# - hl/data/node_raw_book_diffs_by_block/
```

This is typically handled by running the Hyperliquid node with appropriate configuration.

### 3. Start the WebSocket Server

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

**With logging enabled:**
```bash
RUST_LOG=info cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

**With compression tuned:**
```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000 --websocket-compression-level 6
```

### 4. Test with Example Client

The repository includes an example client:

```bash
cargo run --bin example_client -- --address 127.0.0.1 --port 8000 --subscription l2-book
```

## Understanding the API

### WebSocket Endpoint

The server listens on `ws://localhost:8000/ws` for WebSocket connections.

### Subscription Types

#### L2 Book (`l2Book`)
Level 2 order book with aggregated positions by price level.

**Subscription:**
```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "BTC",
    "nSigFigs": 5,
    "mantissa": 5,
    "nLevels": 20
  }
}
```

**Parameters:**
- `coin`: Trading pair (e.g., "BTC", "ETH", "SOL")
- `nSigFigs`: Number of significant figures (2-5)
- `mantissa`: Power of 10 for precision (2, 5, or null)
- `nLevels`: Number of price levels to return (1-100, defaults to 20)

#### L4 Book (`l4Book`)
Level 4 order book with individual order details.

**Subscription:**
```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "BTC"
  }
}
```

#### Trades (`trades`)
Real-time trade stream.

**Subscription:**
```json
{
  "method": "subscribe",
  "subscription": {
    "type": "trades",
    "coin": "BTC"
  }
}
```

### Data Formats

#### L2 Book Response
```json
{
  "channel": "l2Book",
  "data": {
    "coin": "BTC",
    "time": 1751427259657,
    "levels": [
      [
        {"px": "106217.0", "sz": "0.001", "n": 1},
        {"px": "106215.0", "sz": "0.001", "n": 1},
        // ... more levels
      ],
      [
        {"px": "106233.0", "sz": "0.267", "n": 3},
        // ... more levels
      ]
    ]
  }
}
```

#### L4 Book Response (Snapshot)
```json
{
  "channel": "l4Book",
  "data": {
    "coin": "BTC",
    "time": 1751427259657,
    "height": 100,
    "levels": [
      [
        {
          "user": "0x1234...",
          "coin": "BTC",
          "side": "B",
          "limitPx": "106217.0",
          "sz": "0.001",
          "oid": 105338503859,
          "timestamp": 1750660644034,
          "triggerCondition": "N/A",
          "isTrigger": false,
          "triggerPx": "0.0",
          "isPositionTpsl": false,
          "reduceOnly": false,
          "orderType": "Limit",
          "tif": "Gtc",
          "cloid": null
        }
        // ... more orders
      ],
      [
        // asks
      ]
    ]
  }
}
```

#### L4 Book Response (Updates)
```json
{
  "channel": "l4Book",
  "data": {
    "time": 1751427259657,
    "height": 101,
    "orderStatus": [...],
    "bookDiffs": [...]
  }
}
```

#### Trade Response
```json
{
  "channel": "trades",
  "data": [
    {
      "coin": "BTC",
      "side": "A",
      "px": "106296.0",
      "sz": "0.00017",
      "time": 1751430933565,
      "hash": "0xde93a8a0729ade63d8840417805ba9010b008818422ddedb1285744426b73503",
      "tid": 293353986402527,
      "users": ["0xcc0a3b6e3267c84361e91d8230868eea53431e4b", "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8"]
    }
    // ... more trades
  ]
}
```

## Common Use Cases

### 1. Real-time Price Monitoring

```rust
// Connect to L2 book and track best bid/ask
let bid = l2_book.levels[0][0].px;  // Best bid
let ask = l2_book.levels[1][0].px;  // Best ask
let spread = ask - bid;
```

### 2. Order Book Analysis

```rust
// Analyze order depth
fn calculate_spread_depth(l2_book: &L2Book) -> f64 {
    let mut depth = 0.0;
    // Sum volume at top 10 levels on both sides
    for i in 0..min(10, l2_book.levels[0].len()) {
        depth += l2_book.levels[0][i].sz;
    }
    for i in 0..min(10, l2_book.levels[1].len()) {
        depth += l2_book.levels[1][i].sz;
    }
    depth
}
```

### 3. Trading Bot Integration

```rust
// Subscribe to L4 book for detailed order information
let subscription = Subscription::L4Book { coin: "BTC".to_string() };
// Handle incoming orders
fn handle_order_update(order: L4Order) {
    if order.side == Side::Bid && order.is_position_tpsl {
        // Handle TP/SL order
        process_take_profit_stop_loss(order);
    }
}
```

### 4. Trade Analysis

```rust
// Analyze trade patterns
fn analyze_trades(trades: &[Trade]) -> TradeAnalysis {
    let mut volume_by_side = HashMap::new();
    let mut price_impact = Vec::new();
    
    for trade in trades {
        *volume_by_side.entry(trade.side).or_insert(0.0) += trade.sz;
        price_impact.push((trade.px, trade.sz));
    }
    
    TradeAnalysis { volume_by_side, price_impact }
}
```

## Advanced Topics

### Error Handling

The server returns specific error types for different failure scenarios:

```rust
use server::OrderBookError;

match operation() {
    Ok(data) => process_data(data),
    Err(OrderBookError::InvalidSubscription(msg)) => {
        println!("Invalid subscription: {}", msg);
    }
    Err(OrderBookError::WebSocket(msg)) => {
        println!("WebSocket error: {}", msg);
    }
    Err(OrderBookError::OrderNotFound(oid)) => {
        println!("Order {} not found", oid);
    }
    Err(e) => {
        println!("Unexpected error: {:?}", e);
    }
}
```

### Spot Markets

**Note**: As of current version, spot markets are not supported. The server filters out spot coins:

```rust
// Coin::is_spot() returns true for:
// - Coins starting with '@' (spot markets)
// - "PURR/USDC" (specific spot market)
// 
// When ignore_spot is true (default), these coins are filtered out
// and won't appear in subscriptions or snapshots.
```

### Performance Considerations

1. **Compression**: Enable compression for better network efficiency
2. **Level Selection**: Use lower `nLevels` for reduced bandwidth
3. **Subscription Management**: Unsubscribe when not needed
4. **Error Recovery**: Implement retry logic for transient errors

## Troubleshooting

### Server Won't Start

1. **Check node data**: Ensure the Hyperliquid node is writing to the expected directories
2. **Check ports**: Verify no other process is using port 8000
3. **Check permissions**: Ensure read/write permissions for data directories

### Connection Refused

1. **Server running**: Verify the server is running and listening on the specified address
2. **Firewall**: Check if firewall is blocking the connection
3. **Address**: Ensure you're using the correct IP address and port

### Empty Order Book

1. **No data**: The node may not be producing the expected data format
2. **Time sync**: Ensure system time is synchronized
3. **Configuration**: Check node configuration for data output

### High Latency

1. **Network**: Check network latency to the Hyperliquid node
2. **Batching**: The server batches data by block, causing slight delays
3. **Processing**: High volume of data may cause backpressure

## Next Steps

1. **Subscribe to multiple coins**: Modify example client to track multiple trading pairs
2. **Implement real-time analysis**: Add analysis logic to process incoming data
3. **Deploy to production**: Set up proper logging and monitoring
4. **Extend functionality**: Add support for additional Hyperliquid features

## Example: Enhanced Trading Bot

Here's a more complete example showing how to use the server for trading:

```rust
use tokio_tungstenite::{connect_async, tungstenite::Message};
use serde_json::json;

async fn trading_bot() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to server
    let url = "ws://localhost:8000/ws";
    let (mut ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();
    
    // Subscribe to multiple markets
    let btc_subscription = json!({
        "method": "subscribe",
        "subscription": {
            "type": "l2Book",
            "coin": "BTC",
            "nSigFigs": 5,
            "nLevels": 10
        }
    });
    
    let eth_subscription = json!({
        "method": "subscribe",
        "subscription": {
            "type": "l2Book", 
            "coin": "ETH",
            "nSigFigs": 5,
            "nLevels": 10
        }
    });
    
    // Send subscriptions
    write.send(Message::Text(btc_subscription.to_string())).await?;
    write.send(Message::Text(eth_subscription.to_string())).await?;
    write.flush().await?;
    
    // Process messages
    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                let l2_book: L2Book = serde_json::from_str(&text)?;
                match l2_book.coin.as_str() {
                    "BTC" => analyze_btc_spread(&l2_book),
                    "ETH" => analyze_eth_spread(&l2_book),
                    _ => (),
                }
            }
            Message::Close(_) => break,
            _ => (),
        }
    }
    
    Ok(())
}
```

## Resources

- [Hyperliquid Documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/)
- [Example Client Source](https://github.com/hyperliquid-dex/order_book_server/blob/main/binaries/src/bin/example_client.rs)
- [API Specifications](https://github.com/hyperliquid-dex/order_book_server/blob/main/IMPLEMENTATION_PLAN.md)

## Getting Help

1. **GitHub Issues**: Report bugs or request features
2. **Documentation**: Check the `docs/` directory for additional guides
3. **Community**: Join the Hyperliquid Discord for broader support

---

**Last Updated**: 2026-09-10
**Version**: 0.1.0
**Status**: Production-ready for educational and illustrative purposes
