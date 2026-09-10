# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid's official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
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

## Setup

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Requires batching by block. Requires recording fills, order statuses, and raw book diffs. Requires handling info requests. 

2. Then run this local server:

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node, and compares to its own internal state. If a difference is detected, it will exit.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- The current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation.

## Contribution Guidelines

### Getting Started

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/<feature-name>`
3. **Make your changes**
4. **Run tests**: `cargo test`
5. **Check formatting**: `cargo fmt --check`
6. **Lint**: `cargo clippy`
7. **Submit a pull request**

### Development Workflow

1. **Before implementing a feature**:
   - Check for existing issues related to your feature
   - Create a new issue if none exists
   - Discuss your implementation approach in the issue

2. **When implementing:**
   - Follow the existing code style (use `rustfmt`)
   - Add comprehensive unit tests
   - Update documentation as needed
   - Commit with clear messages

3. **Testing best practices**:
   - Write unit tests for new functions
   - Add integration tests for new features
   - Ensure all existing tests pass
   - Test edge cases and error conditions

### Commit Message Guidelines

- Use imperative, present tense: "Add feature X", "Fix bug Y"
- Include references to issues: "Closes: #123"
- Keep messages concise but descriptive
- Avoid commit messages that start with "fix:" or "feat:"

### Branch Naming

- Feature branches: `feature/<feature-name>`
- Bug fix branches: `fix/<bug-name>`
- Documentation branches: `docs/<topic>`
- Test branches: `test/<test-name>`

### Pull Request Process

1. **Submit your PR** with a clear description
2. **Address feedback** from reviewers
3. **Ensure tests pass** before merging
4. **Squash commits** before final review (optional)

### Code Quality Standards

- **Code formatting**: `rustfmt` on all changed files
- **Linting**: `cargo clippy` with all warnings treated as errors
- **Testing**: 80%+ test coverage for new code
- **Documentation**: Comprehensive doc comments for public APIs
- **Error handling**: Use typed errors instead of boxed errors
- **Comments**: Explain "why" not "what" (the code should be self-documenting)

## Testing

### Unit Tests

Run all unit tests:
```bash
cargo test
```

### Integration Tests

The server includes integration tests that verify:
- WebSocket subscription flow
- File system event handling
- Order book state management
- Snapshot validation

## Support

For issues with the server, please open an issue in this repository. For questions about Hyperliquid, visit their official documentation.

## License

This project is available under the same license as Hyperliquid's official protocols.