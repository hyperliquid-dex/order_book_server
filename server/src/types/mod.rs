use std::collections::HashMap;

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::types::Side,
    types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub(crate) mod inner;
pub(crate) mod node_data;
pub(crate) mod subscription;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [Address; 2],
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Level {
    px: String,
    sz: String,
    n: usize,
}

impl Level {
    pub(crate) const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct L2Book {
    coin: String,
    time: u64,
    levels: [Vec<Level>; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

impl L2Book {
    pub(crate) const fn from_l2_snapshot(coin: String, snapshot: [Vec<Level>; 2], time: u64) -> Self {
        Self { coin, time, levels: snapshot }
    }
}

impl Trade {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn from_fills(mut fills: HashMap<Side, NodeDataFill>) -> Self {
        let NodeDataFill(seller, ask_fill) = fills.remove(&Side::Ask).unwrap();
        let NodeDataFill(buyer, bid_fill) = fills.remove(&Side::Bid).unwrap();
        let ask_is_taker = ask_fill.crossed;
        let side = if ask_is_taker { Side::Ask } else { Side::Bid };
        let coin = ask_fill.coin.clone();
        assert_eq!(coin, bid_fill.coin);
        let tid = ask_fill.tid;
        assert_eq!(tid, bid_fill.tid);
        let px = ask_fill.px;
        let sz = ask_fill.sz;
        let hash = ask_fill.hash;
        let time = ask_fill.time;
        let users = [buyer, seller];
        Self { coin, side, px, sz, hash, time, tid, users }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    pub order_statuses: Vec<NodeDataOrderStatus>,
    pub book_diffs: Vec<NodeDataOrderDiff>,
}

impl L4BookUpdates {
    pub(crate) const fn new(time: u64, height: u64) -> Self {
        Self { time, height, order_statuses: Vec::new(), book_diffs: Vec::new() }
    }
}

// RawL4Order is the version of a L4Order we want to serialize and deserialize directly
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub liquidation: Option<Liquidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Liquidation {
    liquidated_user: String,
    mark_px: String,
    method: String,
}
