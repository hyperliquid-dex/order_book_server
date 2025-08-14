use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, SingleEvent, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(
    listener: Arc<Mutex<OrderBookListener>>,
    dir: PathBuf,
    processing_mode: crate::types::node_data::ProcessingMode,
) -> Result<()> {
    // Get the appropriate event sources for this processing mode
    let event_sources = processing_mode.event_sources();

    // Get directory paths for the active processing mode
    let order_statuses_dir = event_sources.order_statuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = event_sources.fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = event_sources.order_diffs.event_source_dir(&dir).canonicalize()?;

    info!(
        "Running in {} mode",
        match processing_mode {
            crate::types::node_data::ProcessingMode::Single => "single",
            crate::types::node_data::ProcessingMode::Batched => "batched",
        }
    );
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    // Watch only the directories for the active processing mode
    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.is_file() {
                            // Determine event source based on path and processing mode
                            let event_source = if new_path.starts_with(&order_statuses_dir) {
                                event_sources.order_statuses // OrderStatuses variant for current mode
                            } else if new_path.starts_with(&fills_dir) {
                                event_sources.fills // Fills variant for current mode
                            } else if new_path.starts_with(&order_diffs_dir) {
                                event_sources.order_diffs // OrderDiffs variant for current mode
                            } else {
                                continue; // Skip unknown paths
                            };

                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, event_source)
                                .map_err(|err| format!("{event_source} processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                let listener = listener.clone();
                let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
            }
            () = sleep(Duration::from_secs(5)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let state = {
                    let mut listener = listener.lock().await;
                    listener.begin_caching();
                    listener.clone_state()
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    state.apply_updates(order_statuses, order_diffs)?;
                                } else {
                                    return Err::<(), Error>("Not enough cached updates".into());
                                }
                            }
                            if state.height() > height {
                                return Err("Fetched snapshot lagging stored state".into());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot");
                            validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot)
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    processing_mode: crate::types::node_data::ProcessingMode,
    // Non-batched file handles
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // Batched file handles
    fill_status_batched_file: Option<File>,
    order_status_batched_file: Option<File>,
    order_diff_batched_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
}

impl OrderBookListener {
    pub(crate) const fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        processing_mode: crate::types::node_data::ProcessingMode,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            processing_mode,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            fill_status_batched_file: None,
            order_status_batched_file: None,
            order_diff_batched_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    #[allow(dead_code)]
    // Legacy method - pops earliest pair of cached updates that have the same timestamp if possible
    // No longer used with immediate processing, kept for potential future use
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    // Legacy method - replaced by receive_batch_immediate for immediate processing
    #[allow(dead_code)]
    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn receive_event(&mut self, event: SingleEvent) -> Result<()> {
        use chrono::Utc;
        let current_time = Utc::now().naive_utc();

        match event {
            SingleEvent::OrderStatus(order_status) => {
                if self.is_ready() {
                    // Create a single-event batch for compatibility with existing order book logic
                    let batch = Batch::new(
                        order_status.time,
                        order_status.time, // Use same time for both local and block time
                        0,                 // Block number not relevant for individual events
                        vec![order_status],
                    );

                    let empty_diffs =
                        Batch::new(batch.local_time(), batch.block_time_raw(), batch.block_number(), Vec::new());

                    self.order_book_state
                        .as_mut()
                        .map(|book| book.apply_updates(batch.clone(), empty_diffs.clone()))
                        .transpose()?;

                    if let Some(cache) = &mut self.fetched_snapshot_cache {
                        cache.push_back((batch.clone(), empty_diffs.clone()));
                    }

                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let updates = Arc::new(InternalMessage::L4BookUpdates {
                                diff_batch: empty_diffs,
                                status_batch: batch,
                            });
                            let _unused = tx.send(updates);
                        });
                    }
                }
            }
            SingleEvent::BookDiff(book_diff) => {
                if self.is_ready() {
                    // Create a single-event batch for compatibility
                    // Use current time since NodeDataOrderDiff doesn't have a time field
                    let batch = Batch::new(
                        current_time,
                        current_time,
                        0, // Block number not relevant for individual events
                        vec![book_diff],
                    );

                    let empty_orders =
                        Batch::new(batch.local_time(), batch.block_time_raw(), batch.block_number(), Vec::new());

                    self.order_book_state
                        .as_mut()
                        .map(|book| book.apply_updates(empty_orders.clone(), batch.clone()))
                        .transpose()?;

                    if let Some(cache) = &mut self.fetched_snapshot_cache {
                        cache.push_back((empty_orders.clone(), batch.clone()));
                    }

                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let updates = Arc::new(InternalMessage::L4BookUpdates {
                                diff_batch: batch,
                                status_batch: empty_orders,
                            });
                            let _unused = tx.send(updates);
                        });
                    }
                }
            }
            SingleEvent::Fill(fill) => {
                // For fills, create a single-event batch and send immediately
                // Convert Fill time (u64 milliseconds) to NaiveDateTime
                let fill_time = chrono::DateTime::from_timestamp_millis(fill.1.time as i64)
                    .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap())
                    .naive_utc();

                let batch = Batch::new(
                    fill_time,
                    fill_time,
                    0, // Block number not relevant for individual events
                    vec![fill],
                );

                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let snapshot = Arc::new(InternalMessage::Fills { batch });
                        let _unused = tx.send(snapshot);
                    });
                }
            }
        }
        Ok(())
    }

    fn process_single_line(&mut self, line: &str, event_source: EventSource) -> Result<()> {
        if line.is_empty() {
            return Ok(());
        }

        let single_event = match event_source {
            EventSource::Fills => serde_json::from_str::<NodeDataFill>(line)
                .map(SingleEvent::Fill)
                .map_err(|e| format!("Fill parsing error: {e}"))?,
            EventSource::OrderStatuses => serde_json::from_str::<NodeDataOrderStatus>(line)
                .map(SingleEvent::OrderStatus)
                .map_err(|e| format!("OrderStatus parsing error: {e}"))?,
            EventSource::OrderDiffs => serde_json::from_str::<NodeDataOrderDiff>(line)
                .map(SingleEvent::BookDiff)
                .map_err(|e| format!("OrderDiff parsing error: {e}"))?,
            _ => return Err("process_single_line called with batched EventSource".into()),
        };

        self.receive_event(single_event)?;

        // Send L2 snapshot after each individual event
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }

        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                new_file.seek(SeekFrom::End(0))?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
            EventSource::FillsBatched => self.fill_status_batched_file.is_some(),
            EventSource::OrderStatusesBatched => self.order_status_batched_file.is_some(),
            EventSource::OrderDiffsBatched => self.order_diff_batched_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
            EventSource::FillsBatched => &mut self.fill_status_batched_file,
            EventSource::OrderStatusesBatched => &mut self.order_status_batched_file,
            EventSource::OrderDiffsBatched => &mut self.order_diff_batched_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        match event_source {
            // Non-batched processing: each line is a single event
            EventSource::Fills | EventSource::OrderStatuses | EventSource::OrderDiffs => {
                let lines = data.lines();
                for line in lines {
                    if let Err(err) = self.process_single_line(line, event_source) {
                        error!("Single line processing error for {event_source}: {err}");
                        // Continue processing other lines even if one fails
                        continue;
                    }
                }
                Ok(())
            }
            EventSource::FillsBatched | EventSource::OrderStatusesBatched | EventSource::OrderDiffsBatched => {
                let total_len = data.len();
                let lines = data.lines();
                for line in lines {
                    if line.is_empty() {
                        continue;
                    }
                    let res = match event_source {
                        EventSource::FillsBatched => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                            let height = batch.block_number();
                            (height, EventBatch::Fills(batch))
                        }),
                        EventSource::OrderStatusesBatched => serde_json::from_str(line)
                            .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                        EventSource::OrderDiffsBatched => {
                            serde_json::from_str(line).map(|batch: Batch<NodeDataOrderDiff>| {
                                (batch.block_number(), EventBatch::BookDiffs(batch))
                            })
                        }
                        _ => unreachable!("Non-batched sources handled above"),
                    };
                    let (height, event_batch) = match res {
                        Ok(data) => data,
                        Err(err) => {
                            // if we run into a serialization error (hitting EOF), just return to last line.
                            error!(
                                "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                                self.order_book_state.as_ref().map(OrderBookState::height),
                                &line[..100.min(line.len())],
                            );
                            #[allow(clippy::unwrap_used)]
                            let total_len: i64 = total_len.try_into().unwrap();
                            self.file_mut(event_source).as_mut().map(|f| f.seek_relative(-total_len));
                            break;
                        }
                    };
                    if height % 100 == 0 {
                        info!("{event_source} block: {height}");
                    }
                    self.receive_batch(event_batch)?;
                }
                if let Some(snapshot) = self.l2_snapshots(false) {
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot =
                                Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
                Ok(())
            }
        }
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
