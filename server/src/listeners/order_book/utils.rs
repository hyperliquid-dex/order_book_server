use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    use log::info;
    use tokio::fs;
    use tokio::time::{sleep, timeout};
    use std::env;
    use std::error::Error as StdError;
    
    // Choose an output path that the node (info server) can write to.
    // Default to "~/hl/data/out.json" to match node data ownership/permissions.
    let default_out = dir.join("hl").join("data").join("out.json");
    let output_path = env::var("INFO_OUT_PATH").map(PathBuf::from).unwrap_or(default_out);
    
    // Check if USE_CACHED_SNAPSHOT env var is set
    if env::var("USE_CACHED_SNAPSHOT").is_ok() {
        info!("USE_CACHED_SNAPSHOT is set, looking for cached snapshots...");
        
        // Look for most recent snapshot in cache directory
        let cache_dir = dir.join("l4_snapshots_cache");
        if cache_dir.exists() {
            let mut entries = fs::read_dir(&cache_dir).await?;
            let mut snapshots: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    if let Ok(metadata) = entry.metadata().await {
                        if let Ok(modified) = metadata.modified() {
                            snapshots.push((path, modified));
                        }
                    }
                }
            }
            
            // Sort by modification time, most recent first
            snapshots.sort_by(|a, b| b.1.cmp(&a.1));
            
            if let Some((snapshot_path, _)) = snapshots.first() {
                info!("Found cached snapshot: {:?}", snapshot_path);
                
                // Copy the snapshot to the expected output location
                fs::copy(&snapshot_path, &output_path).await?;
                info!("Copied cached snapshot to output path");
                return Ok(output_path);
            }
        }
        info!("No cached snapshots found, falling back to HTTP request");
    }
    
    // Remove existing file if present to ensure we detect when new one is written
    if output_path.exists() {
        fs::remove_file(&output_path).await?;
    }
    
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    info!("Requesting L4 snapshot from node info server...");
    
    // Allow overriding the info server URL; default to 127.0.0.1 to avoid IPv6 localhost issues
    let info_server_url = env::var("INFO_SERVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3001/info".to_string());
    
    // Log the request details for debugging
    info!("L4 snapshot request URL: {}", info_server_url);
    info!("L4 snapshot request payload: {:?}", payload);
    
    // Create client with extended timeout for large snapshot responses
    let client = Client::builder()
        .timeout(Duration::from_secs(600)) // 10 minutes timeout
        .connect_timeout(Duration::from_secs(30)) // 30 seconds connection timeout
        .build()?;
    
    // Log client configuration
    info!("HTTP client configured with timeout: 600s, connect_timeout: 30s");
    
    let start_time = std::time::Instant::now();
    let response = client
        .post(&info_server_url)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            let elapsed = start_time.elapsed();
            let error_msg = format!(
                "Failed to send L4 snapshot request to {}: {}. Request failed after {:?}. Error type: {:?}",
                info_server_url,
                e,
                elapsed,
                StdError::source(&e)
            );
            
            // Check for specific error types
            if e.is_timeout() {
                log::error!("Request timed out. Consider increasing timeout or checking server responsiveness.");
            } else if e.is_connect() {
                log::error!("Connection error. Check if info server is running at {}", info_server_url);
            } else if e.is_request() {
                log::error!("Request error. Check payload format and server expectations.");
            }
            
            error_msg
        })?;
    
    // Log response status before checking for errors
    info!("Received response with status: {}", response.status());
    
    response
        .error_for_status()
        .map_err(|e| {
            log::error!("Node info server returned error status. Status code: {}, URL: {}", e.status().unwrap_or_default(), e.url().map(|u| u.as_str()).unwrap_or("unknown"));
            format!("Node info server returned error: {}", e)
        })?;
    
    info!("L4 snapshot request completed successfully, waiting for file to be written...");
    
    // Wait for the file to be written (with timeout)
    let wait_result = timeout(Duration::from_secs(60), async {
        let mut attempts = 0;
        let mut last_size = 0u64;
        let mut stable_count = 0;
        
        loop {
            if output_path.exists() {
                // Check if file has non-zero size and is no longer growing
                if let Ok(metadata) = fs::metadata(&output_path).await {
                    let current_size = metadata.len();
                    if current_size > 0 {
                        // Check if file size is stable (not growing)
                        if current_size == last_size {
                            stable_count += 1;
                            if stable_count >= 5 { // File size stable for 0.5 seconds
                                info!("L4 snapshot file written successfully (size: {} bytes)", current_size);
                                return Ok(());
                            }
                        } else {
                            stable_count = 0;
                            last_size = current_size;
                            if attempts % 50 == 0 { // Log progress every 5 seconds
                                info!("Snapshot file still being written (current size: {} bytes)", current_size);
                            }
                        }
                    }
                }
            }
            attempts += 1;
            if attempts > 600 { // 60 seconds total
                return Err(format!("Timeout waiting for snapshot file to be written (last size: {} bytes)", last_size));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }).await;
    
    match wait_result {
        Ok(Ok(())) => Ok(output_path),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err("Timeout waiting for snapshot file to be written after 60 seconds".into()),
    }
}

pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}
