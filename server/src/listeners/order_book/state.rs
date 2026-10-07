use crate::{
    listeners::order_book::{L2Snapshots, utils::compute_l2_snapshots},
    order_book::{
        Coin, InnerOrder, Oid, Snapshot,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    snapped: bool,
    ignore_spot: bool,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            snapped: false,
        }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    // forcibly take snapshot of every book
    pub(super) fn compute_snapshot(&self) -> Snapshots<InnerL4Order> {
        self.order_book.to_snapshots_par()
    }

    // (time, height, snapshot) of one coin's book
    pub(super) fn coin_snapshot(&self, coin: &Coin) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book.as_ref().get(coin).map(|book| (self.time, self.height, book.to_snapshot()))
    }

    // (time, snapshot)
    pub(super) fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            Some((self.time, compute_l2_snapshots(&self.order_book)))
        }
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    pub(super) fn apply_updates(
        &mut self,
        order_statuses: &Batch<NodeDataOrderStatus>,
        order_diffs: &Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        let time = order_statuses.block_time();
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if height > self.height + 1 {
            return Err(format!("Expecting block {}, got block {}", self.height + 1, height).into());
        } else if height <= self.height {
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        let mut order_map = order_statuses
            .events_ref()
            .iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        for diff in order_diffs.events_ref() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz, insert_before } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.clone().try_into()?;
                        inner_order.modify_sz(sz);
                        // must replace time with time of entering book, which is the timestamp of the order status update
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        if !self.order_book.add_order_before(inner_order, insert_before) {
                            return Err(format!("Unable to find insertBefore order on the book {diff:?}").into());
                        }
                    } else {
                        return Err(format!("Unable to find order opening status {diff:?}").into());
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    if !self.order_book.modify_sz(oid, coin, new_sz) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
                InnerOrderDiff::Remove => {
                    if !self.order_book.cancel_order(oid, coin) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
            }
        }
        self.height += 1;
        self.time = time;
        self.snapped = false;
        Ok(())
    }
}
