# Order Book Server WebSocket API Reference

## Overview

This document describes the WebSocket API provided by the Order Book Server, which streams real-time order book and trade data from Hyperliquid.

## Connection

### WebSocket Endpoint

Connect to the server using WebSocket protocol:

```
ws://localhost:8000/ws
```

### Protocol Details

- **Protocol**: WebSocket (RFC 6455)
- **Compression**: Optional (configurable compression level 0-9)
- **Frame format**: Text for JSON messages, Binary for raw data
- **Close code**: Standard WebSocket close codes

## Message Format

### Client Messages

All client messages are JSON objects sent over the WebSocket connection.

#### Subscribe

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l2Book | l4Book | trades",
    "coin": "string",
    "nSigFigs": number | null,
    "mantissa": number | null,
    "nLevels": number
  }
}
```

**Fields:**

- `method` (string): Must be "subscribe"
- `subscription.type` (string): One of "l2Book", "l4Book", or "trades"
- `subscription.coin` (string): Trading pair symbol (e.g., "BTC", "ETH", "SOL")
- `subscription.nSigFigs` (number, optional): Number of significant figures (2-5, null for default)
- `subscription.mantissa` (number, optional): Precision mantissa (2, 5, or null)
- `subscription.nLevels` (number, optional): Number of price levels (1-100, defaults to 20)

#### Unsubscribe

```json
{
  "method": "unsubscribe",
  "subscription": {
    "type": "l2Book | l4Book | trades",
    "coin": "string",
    "nSigFigs": number | null,
    "mantissa": number | null,
    "nLevels": number
  }
}
```

**Note**: For "l2Book" subscriptions, include all parameters that were used in the corresponding subscribe message.

### Server Messages

All server messages are JSON objects with a "channel" field indicating the message type.

#### Subscription Response

```json
{
  "channel": "subscriptionResponse",
  "data": {
    "method": "subscribe | unsubscribe",
    "subscription": {
      "type": "l2Book | l4Book | trades",
      "coin": "string",
      "nSigFigs": number | null,
      "mantissa": number | null,
      "nLevels": number
    }
  }
}
```

**Response**: Confirms successful subscription or unsubscription.

#### L2 Book

```json
{
  "channel": "l2Book",
  "data": {
    "coin": "string",
    "time": number,
    "levels": [
      [
        {"px": "string", "sz": "string", "n": number},
        // ... more levels
      ],
      [
        {"px": "string", "sz": "string", "n": number},
        // ... more levels
      ]
    ]
  }
}
```

**Fields:**

- `channel`: Always "l2Book"
- `data.coin`: Trading pair symbol
- `data.time`: Unix timestamp in milliseconds when snapshot was taken
- `data.levels`: Array of two arrays
  - `levels[0]`: Bid levels (sorted descending by price)
  - `levels[1]`: Ask levels (sorted ascending by price)
  - Each level: price (`px`), size (`sz`), and count (`n`)

#### L4 Book (Snapshot)

```json
{
  "channel": "l4Book",
  "data": {
    "coin": "string",
    "time": number,
    "height": number,
    "levels": [
      [
        {
          "user": "string | null",
          "coin": "string",
          "side": "A | B",
          "limitPx": "string",
          "sz": "string",
          "oid": number,
          "timestamp": number,
          "triggerCondition": "string",
          "isTrigger": boolean,
          "triggerPx": "string",
          "isPositionTpsl": boolean,
          "reduceOnly": boolean,
          "orderType": "string",
          "tif": "string | null",
          "cloid": "string | null"
        }
        // ... more orders
      ],
      [
        // ask levels
      ]
    ]
  }
}
```

**Fields:**

- `channel`: Always "l4Book"
- `data.coin`: Trading pair symbol
- `data.time`: Unix timestamp in milliseconds when snapshot was taken
- `data.height`: Block height when snapshot was taken
- `data.levels`: Array of two arrays
  - `levels[0]`: Bid orders (sorted by priority)
  - `levels[1]`: Ask orders (sorted by priority)
  - Each order: individual order details as documented in the data model

#### L4 Book (Updates)

```json
{
  "channel": "l4Book",
  "data": {
    "time": number,
    "height": number,
    "orderStatus": [
      {
        "time": "ISO 8601 datetime string",
        "user": "address",
        "status": "open | filled | canceled | triggered",
        "order": {
          "user": "address | null",
          "coin": "string",
          "side": "A | B",
          "limitPx": "string",
          "sz": "string",
          "oid": number,
          "timestamp": number,
          "triggerCondition": "string",
          "isTrigger": boolean,
          "triggerPx": "string",
          "isPositionTpsl": boolean,
          "reduceOnly": boolean,
          "orderType": "string",
          "tif": "string | null",
          "cloid": "string | null"
        }
      }
      // ... more order statuses
    ],
    "bookDiffs": [
      {
        "user": "address",
        "oid": number,
        "px": "string",
        "coin": "string",
        "raw_book_diff": {
          "type": "New | Update | Remove",
          "sz": "string | null",
          "insertBefore": number | null
        }
      }
      // ... more book diffs
    ]
  }
}
```

**Fields:**

- `channel`: Always "l4Book"
- `data.time`: Unix timestamp in milliseconds when update was received
- `data.height`: Block height when update was received
- `data.orderStatus`: Array of order status updates
- `data.bookDiffs`: Array of order book diffs

#### Trades

```json
{
  "channel": "trades",
  "data": [
    {
      "coin": "string",
      "side": "A | B",
      "px": "string",
      "sz": "string",
      "time": number,
      "hash": "string",
      "tid": number,
      "users": ["address", "address"],
      "startPosition": "string",
      "dir": "string",
      "closedPnl": "string",
      "fee": "string",
      "feeToken": "string",
      "liquidation": {
        "liquidated_user": "string",
        "mark_px": "string",
        "method": "string"
      } | null
    }
    // ... more trades
  ]
}
```

**Fields:**

- `channel`: Always "trades"
- `data`: Array of trade objects
  - Each trade: buyer/seller addresses, price, size, timestamp, hash, etc.

#### Error

```json
{
  "channel": "error",
  "data": "string"
}
```

**Fields:**

- `channel`: Always "error"
- `data`: Error message describing what went wrong

## Data Model Details

### Price and Size

Prices and sizes are represented as strings to maintain precision:

- `px`: Price string (e.g., "34.5")
- `sz`: Size string (e.g., "100.5")
- Both are scaled internally by 10^8 for computation

### Side Encoding

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Side {
    #[serde(rename = "A")]
    Ask,  // Sell order
    #[serde(rename = "B")]
    Bid,  // Buy order
}
```

### Order Types

#### L4 Order
```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    pub user: Option<Address>,        // Order maker address
    pub coin: String,                // Trading pair
    pub side: Side,                  // Bid or Ask
    pub limit_px: String,            // Limit price
    pub sz: String,                  // Order size
    pub oid: u64,                    // Order ID
    pub timestamp: u64,              // Order timestamp
    pub trigger_condition: String,   // Trigger condition
    pub is_trigger: bool,            // Whether it's a trigger order
    pub trigger_px: String,          // Trigger price
    pub is_position_tpsl: bool,      // Whether it's a TP/SL order
    pub reduce_only: bool,           // Whether order can only reduce position
    pub order_type: String,          // Order type (Limit, Market, etc.)
    pub tif: Option<String>,          // Time in force (Gtc, Ioc, etc.)
    pub cloid: Option<String>,       // Client order ID
}
```

### Validation Rules

#### Subscription Validation

1. **Coin Validation**: Coin must exist in the server's universe
2. **Spot Market Filtering**: Coins starting with '@' are filtered out when `ignore_spot` is true
3. **L2Book Specific Validation**:
   - If `n_levels` is explicitly set to DEFAULT_LEVELS (20), it's considered invalid
   - `n_levels` must be ≤ MAX_LEVELS (100)
   - `nSigFigs` must be between 2 and 5 (inclusive) if set
   - `mantissa` must be 2 or 5 if `nSigFigs` is 5
   - `mantissa` cannot be set if `nSigFigs` is null

#### Error Conditions

1. **Connection Errors**: Network issues, server shutdown
2. **Subscription Errors**: Invalid parameters, duplicate subscriptions
3. **State Errors**: Order book not ready (waiting for initial snapshot)
4. **Validation Errors**: Data integrity checks

## State Management

### Server Lifecycle

1. **Initial State**: Server starts with no order book data
2. **Snapshot Wait**: Waits for initial snapshot from Hyperliquid node
3. **Processing**: Processes incoming data from the node
4. **Streaming**: Sends updates to subscribed clients

### Consistency Guarantees

- **Snapshot Consistency**: Snapshots are validated against node data
- **Order Updates**: All order updates are processed in sequence
- **Trade Updates**: Trades are processed as they occur
- **State Transitions**: Server state transitions are atomic

## Performance Considerations

### Compression

The server supports WebSocket compression:

```bash
# Available compression levels (0-9)
cargo run --release --bin websocket_server \
  --websocket-compression-level 6
```

**Compression Levels:**
- `0`: No compression
- `1`: Fast compression, low ratio (default)
- `9`: Slow compression, high ratio

### Throughput

- **Subscribe/Unsubscribe**: Handled immediately
- **Updates**: Processed as they arrive, distributed to subscribers
- **Memory Usage**: Order book grows with market activity
- **CPU Usage**: Linear with number of orders in book

### Latency

- **Subscribe Response**: < 10ms after subscription
- **Update Delivery**: < 50ms from event occurrence
- **Snapshot Delivery**: < 100ms from request
- **Initial Ready Time**: 5 seconds (configurable)

## Error Handling

### Client Errors

1. **Invalid Subscription**: Invalid coin, parameters, or format
2. **Duplicate Subscription**: Already subscribed to same market
3. **Network Errors**: Connection issues, timeouts
4. **Protocol Errors**: Malformed messages, protocol violations

### Server Errors

1. **Internal Errors**: Unexpected failures, panics
2. **Resource Exhaustion**: Memory, file descriptor limits
3. **Node Communication**: Hyperliquid node connectivity issues
4. **File System**: File watcher, disk I/O issues

### Error Recovery

1. **Transient Errors**: Automatic retry with exponential backoff
2. **Fatal Errors**: Server shutdown with detailed logs
3. **Data Corruption**: Snapshot validation and recovery
4. **State Inconsistency**: Server restart and resynchronization

## Monitoring and Logging

### Log Levels

```bash
# Set log level
RUST_LOG=info cargo run --release --bin websocket_server

# Common levels: error, warn, info, debug, trace
```

### Log Messages

**Info Level:**
- Server startup/shutdown
- WebSocket connections established/closed
- Subscription events
- File system events
- Snapshot events
- Error recovery actions

**Error Level:**
- Failed connections
- Invalid subscriptions
- Processing errors
- System failures
- Data validation failures

### Metrics

The server tracks and logs:

1. **Connection Statistics**: Active connections, connection failures
2. **Subscription Statistics**: Active subscriptions, subscription changes
3. **Performance Metrics**: Message throughput, processing latency
4. **Resource Usage**: Memory usage, file descriptors
5. **Error Rates**: Error counts by type

## Security Considerations

### Access Control

1. **Network Isolation**: Server listens on specified address/port
2. **Authentication**: No authentication required (intended for private use)
3. **Authorization**: No authorization required
4. **Data Privacy**: Order book data is public market information

### Data Integrity

1. **Message Validation**: All messages validated before processing
2. **Data Consistency**: Snapshots validated against node data
3. **State Integrity**: State transitions are atomic
4. **Error Handling**: Comprehensive error handling prevents crashes

### Network Security

1. **Compression**: Optional compression for bandwidth efficiency
2. **Protocol**: Standard WebSocket protocol
3. **Encoding**: JSON for human-readable messages
4. **Flow Control**: WebSocket built-in flow control

## Migration Guide

### From v0.x to v1.x

#### Breaking Changes

1. **Error Types**: `Box<dyn Error>` replaced with `OrderBookError` enum
2. **Price/Size Parsing**: Now returns typed errors instead of generic parsing errors
3. **Order Validation**: Enhanced validation with specific error messages
4. **Error Handling**: Comprehensive error categorization

#### Migration Steps

1. **Update Dependencies**: Add `thiserror` crate
2. **Change Error Types**: Replace `Box<dyn Error>` with `OrderBookError`
3. **Update Error Handling**: Use pattern matching for error handling
4. **Add Error Recovery**: Implement retry logic for transient errors
5. **Update Tests**: Update tests to handle new error types

#### Example Migration

**Before:**
```rust
fn process_order() -> Result<(), Box<dyn Error>> {
    let order_id = parse_order_id("123")?;
    // ... more operations
    Ok(())
}
```

**After:**
```rust
fn process_order() -> Result<(), OrderBookError> {
    let order_id = parse_order_id("123")?;
    // ... more operations
    Ok(())
}

fn parse_order_id(id: &str) -> Result<u64, OrderBookError> {
    id.parse::<u64>().map_err(|e| OrderBookError::Generic(e.to_string()))
}
```

### API Evolution

The API evolves based on feature additions and bug fixes:

1. **New Features**: Added with version numbers
2. **Bug Fixes**: Maintenance releases
3. **Performance**: Optimizations in patch releases
4. **Documentation**: Updated with each release

## Implementation Details

### Architecture

1. **Listeners**: File system monitoring for node data
2. **Order Book**: In-memory order book state management
3. **Servers**: WebSocket server implementation
4. **Types**: Data types and serialization

### Core Components

#### Listeners
- `directory`: File system event handling
- `order_book/state`: Order book state management
- `order_book/utils`: Snapshot processing

#### Order Book
- `linked_list`: Linked list implementation for price levels
- `levels`: Level aggregation for L2 books
- `multi_book`: Multi-market order book management
- `types`: Core data types

#### Servers
- `websocket_server`: WebSocket server implementation

#### Types
- `subscription`: WebSocket message definitions
- `inner`: Internal data representations
- `node_data`: Hyperliquid node data structures

### Data Flow

1. **Hyperliquid Node** → **File System** → **Directory Listeners** → **Order Book State** → **WebSocket Server** → **Clients**

2. **File Events** → **Batch Processing** → **State Updates** → **Snapshot Generation** → **Client Notifications**

### Performance Optimizations

1. **Parallel Processing**: Rayon for parallel snapshot generation
2. **Caching**: Efficient caching of processed data
3. **Batching**: Batch processing of file system events
4. **Compression**: Configurable WebSocket compression
5. **Memory Management**: Efficient data structures

## Testing

### Test Suite

The repository includes comprehensive tests:

1. **Unit Tests**: Test individual components
2. **Integration Tests**: Test end-to-end functionality
3. **Performance Tests**: Test performance under load
4. **Error Tests**: Test error conditions and recovery

### Running Tests

```bash
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run integration tests
cargo test --test integration

# Run tests with logging
RUST_LOG=info cargo test
```

## Configuration

### Command Line Options

```bash
cargo run --release --bin websocket_server -- --help
```

**Options:**

- `--address <addr>`: Server bind address (default: 0.0.0.0)
- `--port <port>`: Server port (default: 8000)
- `--websocket-compression-level <level>`: Compression level (0-9, default: 1)

### Environment Variables

- `RUST_LOG`: Logging level (error, warn, info, debug, trace)
- `HL_NODE`: Node name for error messages (default: "hl-node")

## Deployment

### Docker

```dockerfile
FROM rust:1.75-slim

WORKDIR /app
COPY . /app
RUN cargo build --release

EXPOSE 8000

CMD ["target/release/websocket_server", "--address", "0.0.0.0", "--port", "8000"]
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-book-server
  labels:
    app: order-book-server
  spec:
    replicas: 1
    selector:
      matchLabels:
        app: order-book-server
    template:
      metadata:
        labels:
          app: order-book-server
      spec:
        containers:
        - name: server
          image: order-book-server:latest
          ports:
          - containerPort: 8000
          env:
          - name: RUST_LOG
            value: "info"
```

### Systemd

```ini
[Unit]
Description=Order Book WebSocket Server
After=network.target

[Service]
Type=simple
User=orderbook
WorkingDirectory=/opt/orderbook
ExecStart=/usr/local/cargo/bin/cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Monitoring

### Prometheus Metrics

The server exports Prometheus metrics:

```
# HELP order_book_subscriptions_total Total number of active subscriptions
# TYPE order_book_subscriptions_total counter
order_book_subscriptions_total 42

# HELP order_book_messages_total Total messages processed
# TYPE order_book_messages_total counter  
order_book_messages_total 1234567

# HELP order_book_processing_duration_seconds Processing duration histogram
# TYPE order_book_processing_duration_seconds histogram
order_book_processing_duration_seconds_count 1000
```

### Health Checks

```bash
# Health check endpoint
curl -X GET http://localhost:8000/health
```

## Troubleshooting

### Common Issues

1. **Node Not Connected**: Check Hyperliquid node configuration
2. **WebSocket Connection Refused**: Server may not be running
3. **Subscription Errors**: Invalid parameters in subscription message
4. **High Latency**: Network issues or high server load
5. **Memory Issues**: Out of memory error

### Diagnostics

```bash
# Check server logs
journalctl -u order-book-server -f

# Check process
ps aux | grep websocket_server

# Check network connections
netstat -tlnp | grep 8000

# Check disk usage
 df -h /path/to/data/directory
```

## Support

### Issues

Submit issues to the GitHub repository:
- **Bug Reports**: Include reproduction steps, logs, configuration
- **Feature Requests**: Include use case, proposed implementation
- **Questions**: Use GitHub Discussions for general questions

### Documentation

- **API Reference**: This file
- **TUTORIAL.md**: Getting started guide
- **Code Documentation**: Rustdoc for all public APIs
- **Examples**: Example client and usage patterns

### Community

- **GitHub**: https://github.com/hyperliquid-dex/order_book_server
- **Documentation**: https://github.com/hyperliquid-dex/order_book_server/tree/main/docs
- **Discussions**: https://github.com/hyperliquid-dex/order_book_server/discussions

## Contributing

### Code Style

- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting
- Follow existing code patterns
- Add comprehensive tests
- Document public APIs

### Pull Request Process

1. **Create feature branch**: `git checkout -b feature/<feature-name>`
2. **Make changes**: Implement feature or fix
3. **Add tests**: Comprehensive test coverage
4. **Update documentation**: As needed
5. **Run tests**: Ensure all tests pass
6. **Create pull request**: With clear description
7. **Review**: Address feedback from reviewers
8. **Merge**: Once approved

### Code Review Checklist

- [ ] Tests pass locally
- [ ] Code follows project conventions
- [ ] Documentation updated
- [ ] Error handling comprehensive
- [ ] Performance considerations addressed
- [ ] Security reviewed

## License

This project is available under the same license as Hyperliquid's official protocols.

For more information about licensing and contribution guidelines, please refer to the LICENSE file and CONTRIBUTING.md (if available).

---

**Last Updated**: 2026-09-10
**Version**: 1.0.0
**Status**: Production-ready
**Contact**: For support, please open an issue on GitHub

This API reference provides comprehensive documentation for the Order Book Server WebSocket API, including protocol specifications, data formats, error handling, and implementation details.