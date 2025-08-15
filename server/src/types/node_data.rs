use std::path::{Path, PathBuf};

use alloy::{contract::Event, primitives::Address};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::{Coin, Oid},
    types::{Fill, L4Order, OrderDiff},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_inserted_into_book(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && (self.order.tif != Some("Ioc".to_string())))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Clone, Copy, strum_macros::Display, PartialEq, Eq)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
    // Batched versions for block-by-block processing
    FillsBatched,
    OrderStatusesBatched,
    OrderDiffsBatched,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessingMode {
    /// Process events individually as they arrive (default)
    Single,
    /// Process events in batches by block
    Batched,
}

impl Default for ProcessingMode {
    fn default() -> Self {
        Self::Single
    }
}

pub struct EventSources {
    pub fills: EventSource,
    pub order_statuses: EventSource,
    pub order_diffs: EventSource,
}

impl ProcessingMode {
    /// Get the appropriate EventSource variants for this processing mode
    pub(crate) fn event_sources(self) -> EventSources {
        match self {
            Self::Single => EventSources {
                fills: EventSource::Fills,
                order_statuses: EventSource::OrderStatuses,
                order_diffs: EventSource::OrderDiffs,
            },
            Self::Batched => EventSources {
                fills: EventSource::FillsBatched,
                order_statuses: EventSource::OrderStatusesBatched,
                order_diffs: EventSource::OrderDiffsBatched,
            },
        }
    }
}

impl EventSource {
    #[must_use]
    pub(crate) fn event_source_dir(self, dir: &Path) -> PathBuf {
        match self {
            Self::Fills => dir.join("hl/data/node_fills"),
            Self::OrderStatuses => dir.join("hl/data/node_order_statuses"),
            Self::OrderDiffs => dir.join("hl/data/node_raw_book_diffs"),
            Self::FillsBatched => dir.join("hl/data/node_fills_by_block"),
            Self::OrderStatusesBatched => dir.join("hl/data/node_order_statuses_by_block"),
            Self::OrderDiffsBatched => dir.join("hl/data/node_raw_book_diffs_by_block"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<E>,
}

impl<E> Batch<E> {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) const fn new(
        local_time: NaiveDateTime,
        block_time: NaiveDateTime,
        block_number: u64,
        events: Vec<E>,
    ) -> Self {
        Self { local_time, block_time, block_number, events }
    }

    pub(crate) const fn local_time(&self) -> NaiveDateTime {
        self.local_time
    }

    pub(crate) const fn block_time_raw(&self) -> NaiveDateTime {
        self.block_time
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }
}
