#![allow(unused_crate_dependencies)]
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use server::Result;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, ValueEnum)]
enum Subscription {
    L2Book,
    L4Book,
    Trades,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let url = format!("ws://{}:{}/ws", args.address, args.port);
    println!("Connecting to {url}");

    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    // Define subscription messages
    let l2_book_sub =
        r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":5}}"#;
    let l4_book_sub = r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#;
    let trades_sub = r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#;

    // Choose subscription
    match args.subscription {
        Subscription::L2Book => write.send(Message::Text(l2_book_sub.into())).await?,
        Subscription::L4Book => write.send(Message::Text(l4_book_sub.into())).await?,
        Subscription::Trades => write.send(Message::Text(trades_sub.into())).await?,
    }

    let mut msg_cnt = 0;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(txt)) => println!("Received text {msg_cnt}: {txt}"),
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
