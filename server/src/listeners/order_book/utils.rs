use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;

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
use log::info;
use tokio::fs;

/// Fetches an L4 snapshot and writes it to `out.json`.
///
/// First attempts the `fileSnapshot` API. The output path for the API is set
/// inside `hl/` (the shared volume) so the node process can write it and the
/// order book server can read it — important when running in separate containers.
///
/// If the API fails, falls back to reading the latest periodic ABCI state file
/// and computing L4 snapshots via `hl-node compute-l4-snapshots`. The fallback
/// requires `hl-node` on PATH (or via `HL_NODE_PATH` env var).
pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    // API: write into the shared volume so the node and OBS can both access it
    let api_output_path = dir.join("hl/out.json");
    // CLI: write to local filesystem (OBS runs hl-node as a subprocess)
    let cli_output_path = dir.join("out.json");

    match process_via_api(&api_output_path).await {
        Ok(()) => return Ok(api_output_path),
        Err(err) => {
            info!("fileSnapshot API unavailable ({err}), trying periodic ABCI state fallback");
        }
    }

    process_via_cli(dir, &cli_output_path).await?;
    Ok(cli_output_path)
}

async fn process_via_api(output_path: &Path) -> Result<()> {
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

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn process_via_cli(dir: &Path, output_path: &Path) -> Result<()> {
    let abci_states_dir = dir.join("hl/data/periodic_abci_states");
    let (rmp_path, height) = find_latest_rmp(&abci_states_dir).await?;
    info!("Using periodic ABCI state at height {height}: {}", rmp_path.display());

    let hl_node = std::env::var("HL_NODE_PATH").unwrap_or_else(|_| "hl-node".to_string());
    let chain = std::env::var("HL_CHAIN").unwrap_or_else(|_| "Mainnet".to_string());
    let raw_output = dir.join("l4_raw.json");

    let result = tokio::process::Command::new(&hl_node)
        .args(["--chain", &chain, "compute-l4-snapshots", "--include-users"])
        .arg(&rmp_path)
        .arg(&raw_output)
        .output()
        .await
        .map_err(|e| format!("Failed to run '{hl_node}': {e}. Set HL_NODE_PATH to the hl-node binary location."))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(format!("hl-node compute-l4-snapshots failed: {stderr}").into());
    }

    // The CLI outputs [[coin, [bids, asks]], ...] without a height prefix.
    // Wrap it as [height, data] to match the fileSnapshot API format.
    let raw_content = fs::read_to_string(&raw_output).await?;
    let wrapped = format!("[{height},{raw_content}]");
    fs::write(output_path, wrapped).await?;

    // Clean up temporary file
    drop(fs::remove_file(&raw_output).await);

    Ok(())
}

async fn find_latest_rmp(abci_states_dir: &Path) -> Result<(PathBuf, u64)> {
    let mut latest_date: Option<String> = None;
    let mut entries = fs::read_dir(abci_states_dir)
        .await
        .map_err(|e| format!("Cannot read periodic_abci_states directory: {e}"))?;

    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            if latest_date.as_ref().is_none_or(|d| name > *d) {
                latest_date = Some(name);
            }
        }
    }

    let date_dir = latest_date.ok_or("No date directories found in periodic_abci_states")?;
    let date_path = abci_states_dir.join(&date_dir);

    let mut latest_height: Option<u64> = None;
    let mut latest_path: Option<PathBuf> = None;
    let mut entries = fs::read_dir(&date_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(height_str) = name.strip_suffix(".rmp") {
            if let Ok(height) = height_str.parse::<u64>() {
                if latest_height.is_none_or(|h| height > h) {
                    latest_height = Some(height);
                    latest_path = Some(entry.path());
                }
            }
        }
    }

    match (latest_path, latest_height) {
        (Some(path), Some(height)) => Ok((path, height)),
        _ => Err("No .rmp files found in periodic_abci_states".into()),
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
        if let Some(last_ts) = self.last_ts
            && last_ts >= block.block_number()
        {
            return false;
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
