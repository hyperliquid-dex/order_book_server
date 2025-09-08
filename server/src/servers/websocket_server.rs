use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    sync::Arc,
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let internal_message_tx = internal_message_tx.clone();
            async move |ws_upgrade| {
                ws_handler(ws_upgrade, internal_message_tx.clone(), listener.clone(), ignore_spot, websocket_opts)
            }
        }),
    );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, ignore_spot).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                            },
                        }

                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone()).await;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

// derive it from l2_snapshots because thats convenient
fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let snapshot = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time);
            let msg = ServerResponse::L2Book(l2_book);
            send_socket_message(socket, msg).await;
        } else {
            error!("Coin {coin} not found");
        }
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut fills = batch.clone().events();
    let mut trades = HashMap::new();
    while fills.len() >= 2 {
        let f2 = fills.pop();
        let f1 = fills.pop();
        if let Some(f1) = f1 {
            if let Some(f2) = f2 {
                let mut fills = HashMap::new();
                fills.insert(f1.1.side, f1);
                fills.insert(f2.1.side, f2);
                let trade = Trade::from_fills(fills);
                let coin = trade.coin.clone();
                trades.entry(coin).or_insert_with(Vec::new).push(trade);
            }
        }
    }
    for list in trades.values_mut() {
        list.reverse();
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}
