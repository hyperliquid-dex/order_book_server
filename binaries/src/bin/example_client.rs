#![allow(unused_crate_dependencies)]
use std::{fmt, str::FromStr, time::Duration};

use chrono::Utc;
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use server::Result;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Coin identifier for subscriptions.
/// - Perpetuals: uppercase letters (e.g., BTC, ETH, SOL)
/// - Spot: @{index} format (e.g., @107 for HYPE, @151 for wrapped ETH)
/// - Special: PURR/USDC
#[derive(Debug, Clone)]
struct Coin(String);

impl FromStr for Coin {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim();

        // Spot token: @{digits}
        if s.starts_with('@') {
            let rest = &s[1..];
            if rest.is_empty() {
                return Err("Spot token index cannot be empty (e.g., @107)".to_string());
            }
            if !rest.chars().all(|c| c.is_ascii_digit()) {
                return Err(format!("Spot token index must be numeric: {s}"));
            }
            return Ok(Self(s.to_string()));
        }

        // Special case: PURR/USDC
        if s == "PURR/USDC" {
            return Ok(Self(s.to_string()));
        }

        // Perpetual: uppercase letters (may contain numbers for some coins)
        if s.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) && !s.is_empty() {
            return Ok(Self(s.to_string()));
        }

        Err(format!("Invalid coin format: {s}. Expected: BTC, ETH (perpetuals) or @107, @151 (spot)"))
    }
}

impl fmt::Display for Coin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, ValueEnum)]
enum Subscription {
    L2Book,
    L4Book,
    Trades,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
/// Example WebSocket client for subscribing to order book and trades data.
///
/// # Examples
///
/// Subscribe to BTC perpetual trades:
///   example_client --address 127.0.0.1 --port 8000 --subscription trades --coin BTC
///
/// Subscribe to HYPE spot trades (token index @107):
///   example_client --address 127.0.0.1 --port 8000 --subscription trades --coin @107
///
/// To find spot token indices, query the Hyperliquid API:
///   curl -X POST https://api.hyperliquid.xyz/info -H "Content-Type: application/json" \
///        -d '{"type": "spotMeta"}' | jq '.universe[] | select(.tokens[0] == 150)'
///   # Returns: {"tokens":[150,0],"name":"@107",...} -> HYPE spot is @107
struct Args {
    /// WebSocket server address (e.g., 127.0.0.1)
    #[arg(long)]
    address: String,

    /// WebSocket server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Subscription type: l2-book, l4-book, trades
    #[arg(long, value_enum, default_value_t = Subscription::L2Book)]
    subscription: Subscription,

    /// Coin symbol to subscribe to.
    /// For perpetuals: BTC, ETH, SOL, etc.
    /// For spot: use @{index} format (e.g., @107 for HYPE, @151 for wrapped ETH)
    #[arg(long, default_value = "BTC")]
    coin: Coin,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let url = format!("ws://{}:{}/ws", args.address, args.port);
    println!("Connecting to {url}");

    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    // Build subscription message based on type and coin
    let sub_msg = match args.subscription {
        Subscription::L2Book => format!(
            r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{}","nSigFigs":5,"mantissa":5}}}}"#,
            args.coin
        ),
        Subscription::L4Book => {
            format!(r#"{{"method":"subscribe","subscription":{{"type":"l4Book","coin":"{}"}}}}"#, args.coin)
        }
        Subscription::Trades => {
            format!(r#"{{"method":"subscribe","subscription":{{"type":"trades","coin":"{}"}}}}"#, args.coin)
        }
    };

    write.send(Message::Text(sub_msg.into())).await?;

    let mut msg_cnt = 0;
    while let Some(msg) = read.next().await {
        let now_ms = Utc::now().timestamp_millis();
        match msg {
            Ok(Message::Text(txt)) => {
                if let Some(latency) = extract_latency(&txt, now_ms) {
                    println!("Received {msg_cnt} [latency: {latency:?}]: {txt}");
                } else {
                    println!("Received {msg_cnt}: {txt}");
                }
            }
            Ok(Message::Binary(bin)) => println!("Received binary: {bin:?}"),
            Ok(Message::Ping(_)) => println!("Received ping"),
            Ok(Message::Pong(_)) => println!("Received pong"),
            Ok(Message::Close(frame)) => {
                println!("Received close: {frame:?}");
                break;
            }
            Ok(other) => println!("Received other message: {other:?}"),
            Err(err) => {
                eprintln!("WebSocket error: {err}");
                break;
            }
        }
        msg_cnt += 1;
    }

    println!("Connection closed");
    Ok(())
}

fn extract_latency(json_str: &str, now_ms: i64) -> Option<Duration> {
    let parsed: serde_json::Value = serde_json::from_str(json_str).ok()?;

    if let Some(time) = parsed.get("data").and_then(|d| d.get("time")).and_then(serde_json::Value::as_i64) {
        return Some(Duration::from_millis((now_ms - time).cast_unsigned()));
    }

    if let Some(data) = parsed.get("data").and_then(|d| d.as_array())
        && let Some(first) = data.first()
        && let Some(time) = first.get("time").and_then(serde_json::Value::as_i64)
    {
        return Some(Duration::from_millis((now_ms - time).cast_unsigned()));
    }

    None
}
