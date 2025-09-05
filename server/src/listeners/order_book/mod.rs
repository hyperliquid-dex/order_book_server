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
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    fmt::Write as _,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(
    listener: Arc<Mutex<OrderBookListener>>,
    dir: PathBuf,
    secrets: Option<crate::config::Secrets>,
) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());
    
    // Get hostname for alerts
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());

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

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;

    // Check if we should skip initial snapshot
    let skip_initial_snapshot = std::env::var("SKIP_INITIAL_SNAPSHOT").is_ok();
    if skip_initial_snapshot {
        info!("SKIP_INITIAL_SNAPSHOT is set, starting without initial snapshot");
        listener.lock().await.mark_ready_without_snapshot();
    }

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));

    // -------- Option A watchdog: outer timeout envelope --------
    const WATCHDOG: Duration = Duration::from_secs(5);

    loop {
        let outcome = tokio::time::timeout(WATCHDOG, async {
            tokio::select! {
                // ---- filesystem events ----
                event = fs_event_rx.recv() =>  match event {
                    Some(Ok(event)) => {
                        {
                            listener.lock().await.note_fs_event();
                        }
                        if event.kind.is_create() || event.kind.is_modify() {
                            for new_path in &event.paths {
                                if !new_path.is_file() { continue; }
                                if new_path.starts_with(&order_statuses_dir) {
                                    listener
                                        .lock()
                                        .await
                                        .process_update(&event, new_path, EventSource::OrderStatuses)
                                        .map_err(|err| format!("Order status processing error: {err}"))?;
                                } else if new_path.starts_with(&fills_dir) {
                                    listener
                                        .lock()
                                        .await
                                        .process_update(&event, new_path, EventSource::Fills)
                                        .map_err(|err| format!("Fill update processing error: {err}"))?;
                                } else if new_path.starts_with(&order_diffs_dir) {
                                    listener
                                        .lock()
                                        .await
                                        .process_update(&event, new_path, EventSource::OrderDiffs)
                                        .map_err(|err| format!("Book diff processing error: {err}"))?;
                                }
                            }
                        }
                        Ok::<(), Error>(())
                    }
                    Some(Err(err)) => {
                        error!("Watcher error: {err}");
                        let error_message = format!("Watcher error: {err}");
                        let diag = listener.lock().await.diagnostics_json(Instant::now(), skip_initial_snapshot);
                        error!("Watcher error details | ctx={}", diag);
                        if let Some(ref sec) = secrets {
                            crate::slack_alerts::send_alert_before_exit_with_details(
                                Some(sec),
                                crate::slack_alerts::AlertType::FileWatcherError,
                                &error_message,
                                &hostname,
                                &diag
                            ).await;
                        }
                        Err(error_message.into())
                    }
                    None => {
                        error!("Channel closed. Listener exiting");
                        let error_message = "Channel closed.";
                        let diag = listener.lock().await.diagnostics_json(Instant::now(), skip_initial_snapshot);
                        error!("Channel closed details | ctx={}", diag);
                        if let Some(ref sec) = secrets {
                            crate::slack_alerts::send_alert_before_exit_with_details(
                                Some(sec),
                                crate::slack_alerts::AlertType::ChannelError,
                                error_message,
                                &hostname,
                                &diag
                            ).await;
                        }
                        Err(error_message.into())
                    }
                },

                // ---- snapshot fetch task results ----
                snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                    // Clear the in-flight marker regardless of success/failure
                    listener.lock().await.snapshot_in_flight_since = None;
                    
                    match snapshot_fetch_res {
                        None => {
                            let error_message = "Snapshot fetch task sender dropped";
                            let diag = listener.lock().await.diagnostics_json(Instant::now(), skip_initial_snapshot);
                            error!("Snapshot fetch task sender dropped | ctx={}", diag);
                            if let Some(ref sec) = secrets {
                                crate::slack_alerts::send_alert_before_exit_with_details(
                                    Some(sec),
                                    crate::slack_alerts::AlertType::ChannelError, // reuse existing type
                                    error_message,
                                    &hostname,
                                    &diag
                                ).await;
                            }
                            Err(error_message.into())
                        }
                        Some(Err(err)) => {
                            let error_message = format!("Abci state reading error: {err}");
                            let diag = listener.lock().await.diagnostics_json(Instant::now(), skip_initial_snapshot);
                            error!("ABCI state reading error | ctx={}", diag);
                            if let Some(ref sec) = secrets {
                                crate::slack_alerts::send_alert_before_exit_with_details(
                                    Some(sec),
                                    crate::slack_alerts::AlertType::SnapshotSyncError,
                                    &error_message,
                                    &hostname,
                                    &diag
                                ).await;
                            }
                            Err(error_message.into())
                        }
                        Some(Ok(())) => Ok(())
                    }
                }

                // ---- periodic snapshot ticker ----
                _ = ticker.tick() => {
                    if !skip_initial_snapshot || listener.lock().await.is_ready() {
                        // Mark snapshot as in-flight before spawning the task
                        listener.lock().await.snapshot_in_flight_since = Some(Instant::now());
                        
                        let listener = listener.clone();
                        let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                        fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                    }
                    Ok::<(), Error>(())
                }
            }
        }).await;

        // If timeout elapsed (no inner branch fired within WATCHDOG), and we are ready → liveness failure.
        if outcome.is_err() {
            let listener_guard = listener.lock().await;
            
            // Check if a snapshot is in flight
            if let Some(snapshot_start) = listener_guard.snapshot_in_flight_since {
                let snapshot_duration = Instant::now().duration_since(snapshot_start);
                
                // Give snapshots more time (30 seconds) before considering it a timeout
                const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);
                if snapshot_duration < SNAPSHOT_TIMEOUT {
                    // Snapshot still processing within reasonable time, continue
                    drop(listener_guard);
                    continue;
                }
                // If snapshot is taking too long, fall through to timeout handling
            }
            
            if listener_guard.is_ready() {
                // Build structured diagnostics for logs + Slack "details"
                let diag = listener_guard.diagnostics_json(Instant::now(), skip_initial_snapshot);
                let mut error_message = format!("Stream has fallen behind ({HL_NODE} failed?)");
                
                // Also include a terse inline summary in the log line itself
                let status_front = listener_guard.order_status_cache.front().map(|b| b.block_number());
                let diff_front = listener_guard.order_diff_cache.front().map(|b| b.block_number());
                let current_height = listener_guard.order_book_state.as_ref().map(|s| s.height());
                let last_prog_ms = listener_guard.last_progress_at
                    .map(|t| Instant::now().saturating_duration_since(t).as_millis());
                let last_fs_ms = listener_guard.last_fs_event_at
                    .map(|t| Instant::now().saturating_duration_since(t).as_millis());
                let _ = write!(
                    &mut error_message,
                    " | h={:?} status_front={:?} diff_front={:?} last_prog_ms={:?} last_fs_ms={:?} skip_init={}",
                    current_height, status_front, diff_front, last_prog_ms, last_fs_ms, skip_initial_snapshot
                );

                // Check if upstream files have advanced
                let upstream_moved = listener_guard.upstream_advanced_since_last_seen(&order_statuses_dir, &fills_dir, &order_diffs_dir);
                let alert_type = if upstream_moved {
                    crate::slack_alerts::AlertType::FileWatcherError
                } else {
                    crate::slack_alerts::AlertType::NodeExecutionBehind
                };
                
                // Log with structured context (easy to grep/parse)
                error!("{} | ctx={}", error_message, diag);
                
                if let Some(ref sec) = secrets {
                    crate::slack_alerts::send_alert_before_exit_with_details(
                        Some(sec),
                        alert_type,
                        &error_message,
                        "",
                        &diag
                    ).await;
                }
                return Err(error_message.into());
            }
            // Not ready yet — keep waiting.
            continue;
        }

        // Propagate inner errors immediately
        if let Err(e) = outcome.unwrap() {
            return Err(e);
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
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    
    // --- Liveness/diagnostics ---
    last_progress_at: Option<Instant>,
    last_fs_event_at: Option<Instant>,
    last_height: Option<u64>,
    snapshot_in_flight_since: Option<Instant>,
    
    // --- Upstream file metadata tracking ---
    last_status_meta: Option<(u64, SystemTime)>,
    last_fills_meta: Option<(u64, SystemTime)>,
    last_diffs_meta: Option<(u64, SystemTime)>,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            last_progress_at: None,
            last_fs_event_at: None,
            last_height: None,
            snapshot_in_flight_since: None,
            last_status_meta: None,
            last_fills_meta: None,
            last_diffs_meta: None,
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn mark_ready_without_snapshot(&mut self) {
        use crate::order_book::multi_book::Snapshots;
        use std::collections::HashMap;
        info!("Marking order book as ready without initial snapshot");
        // Initialize with empty snapshot at height 0
        let empty_snapshot = Snapshots::new(HashMap::new());
        self.order_book_state = Some(OrderBookState::from_snapshot(empty_snapshot, 0, 0, true, self.ignore_spot));
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    // --- Liveness helpers ---
    fn note_progress(&mut self, height: u64) {
        self.last_progress_at = Some(Instant::now());
        self.last_height = Some(self.last_height.map_or(height, |h| h.max(height)));
    }

    fn note_fs_event(&mut self) {
        self.last_fs_event_at = Some(Instant::now());
    }

    fn diagnostics_json(&self, now: Instant, skip_initial_snapshot: bool) -> String {
        #[derive(Serialize)]
        struct Diag<'a> {
            ready: bool,
            height: Option<u64>,
            status_front: Option<u64>,
            diff_front: Option<u64>,
            last_progress_ms_ago: Option<u128>,
            last_fs_event_ms_ago: Option<u128>,
            skip_initial_snapshot: bool,
            ignore_spot: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            last_height: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            node: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            snapshot_in_flight_ms: Option<u128>,
        }

        let status_front = self.order_status_cache.front().map(|b| b.block_number());
        let diff_front   = self.order_diff_cache.front().map(|b| b.block_number());
        let last_prog    = self.last_progress_at.map(|t| now.saturating_duration_since(t).as_millis());
        let last_fs      = self.last_fs_event_at.map(|t| now.saturating_duration_since(t).as_millis());
        let height       = self.order_book_state.as_ref().map(|s| s.height());
        let snapshot_ms  = self.snapshot_in_flight_since.map(|t| now.saturating_duration_since(t).as_millis());
        
        let diag = Diag {
            ready: self.is_ready(),
            height,
            status_front,
            diff_front,
            last_progress_ms_ago: last_prog,
            last_fs_event_ms_ago: last_fs,
            skip_initial_snapshot,
            ignore_spot: self.ignore_spot,
            last_height: self.last_height,
            node: Some(HL_NODE),
            snapshot_in_flight_ms: snapshot_ms,
        };
        serde_json::to_string(&diag).unwrap_or_else(|_| "{\"diag\":\"serialize_failed\"}".into())
    }

    fn upstream_advanced_since_last_seen(&self, status_dir: &PathBuf, fills_dir: &PathBuf, diffs_dir: &PathBuf) -> bool {
        fn dir_changed(dir: &PathBuf, last: Option<(u64, SystemTime)>) -> bool {
            // best effort: check newest file in the dir
            let Ok(mut rd) = fs::read_dir(dir) else { return false; };
            let mut newest: Option<(u64, SystemTime)> = None;
            while let Some(Ok(e)) = rd.next() {
                let Ok(md) = e.metadata() else { continue; };
                if !md.is_file() { continue; }
                let mt = md.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                if newest.map(|(_, m)| mt > m).unwrap_or(true) {
                    newest = Some((md.len(), mt));
                }
            }
            if let (Some((cur_len, cur_mt)), Some((last_len, last_mt))) = (newest, last) {
                cur_len > last_len || cur_mt > last_mt
            } else {
                false
            }
        }
        dir_changed(status_dir, self.last_status_meta)
            || dir_changed(fills_dir, self.last_fills_meta)
            || dir_changed(diffs_dir, self.last_diffs_meta)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
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

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                let h = batch.block_number();
                self.order_status_cache.push(batch);
                // mark progress on new data
                self.note_progress(h);
            }
            EventBatch::BookDiffs(batch) => {
                let h = batch.block_number();
                self.order_diff_cache.push(batch);
                self.note_progress(h);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    self.note_progress(batch.block_number());
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
                // Track the height from the updates
                let height = order_statuses.block_number().max(order_diffs.block_number());
                
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                    
                // Mark progress after successful update
                self.note_progress(height);
                
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

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // take the cached updates and stop collecting updates
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
            self.note_progress(height);
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting multiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        // Track file metadata for upstream change detection
        if let Ok(md) = fs::metadata(new_path) {
            let snap = (md.len(), md.modified().unwrap_or(SystemTime::UNIX_EPOCH));
            match event_source {
                EventSource::OrderStatuses => self.last_status_meta = Some(snap),
                EventSource::Fills => self.last_fills_meta = Some(snap),
                EventSource::OrderDiffs => self.last_diffs_meta = Some(snap),
            }
        }
        
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        } else {
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
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
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
        let total_len = data.len();
        let lines = data.lines();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    // if we run into a serialization error (hitting EOF), just return to last line.
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        &line[..100],
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
            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }
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
