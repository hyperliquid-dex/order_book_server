use crate::order_book::{InnerOrder, Oid, OrderBook, Px, Side, Snapshot, Sz, linked_list::LinkedList};
use crate::types::Level;
use crate::types::inner::InnerLevel;
use std::collections::BTreeMap;

#[must_use]
fn bucket(px: Px, side: Side, n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Px {
    let m = mantissa.unwrap_or(1);
    n_sig_figs.map_or(px, |n| {
        let digs = px.num_digits();
        let p = digs.saturating_sub(n);
        let inc = m * 10u64.pow(p);
        match side {
            Side::Ask => Px::new(px.value().div_ceil(inc) * inc),
            Side::Bid => Px::new((px.value() / inc) * inc),
        }
    })
}

impl<O: InnerOrder> OrderBook<O> {
    #[must_use]
    pub(crate) fn to_l2_snapshot(
        &self,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let bids = &self.bids;
        let asks = &self.asks;
        let bids = map_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = map_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }
}

impl Snapshot<InnerLevel> {
    #[must_use]
    pub(crate) fn to_l2_snapshot(
        &self,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Self {
        let [bids, asks] = &self.0;
        let bids = l2_levels_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = l2_levels_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Self([bids, asks])
    }

    pub(crate) fn export_inner_snapshot(self) -> [Vec<Level>; 2] {
        self.0.map(|b| b.into_iter().map(Level::from).collect())
    }
}

#[must_use]
fn l2_levels_to_l2_levels(
    levels: &[InnerLevel],
    side: Side,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
) -> Vec<InnerLevel> {
    let mut new_levels = Vec::new();
    if n_levels == Some(0) {
        return new_levels;
    }
    let mut cur_level: Option<InnerLevel> = None;
    for level in levels {
        if build_l2_level(&mut cur_level, &mut new_levels, n_levels, n_sig_figs, mantissa, side, level.clone()) {
            break;
        }
    }
    new_levels.extend(cur_level.take());
    new_levels
}

#[must_use]
fn map_to_l2_levels<O: InnerOrder>(
    orders: &BTreeMap<Px, LinkedList<Oid, O>>,
    side: Side,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
) -> Vec<InnerLevel> {
    let mut levels = Vec::new();
    if n_levels == Some(0) {
        return levels;
    }
    let mut cur_level: Option<InnerLevel> = None;
    let order_iter: Box<dyn Iterator<Item = (&Px, &LinkedList<Oid, O>)>> = match side {
        Side::Ask => Box::new(orders.iter()),
        Side::Bid => Box::new(orders.iter().rev()),
    };
    for (px, orders) in order_iter {
        // could be done a bit more efficiently using caching
        let sz = orders.fold(Sz::new(0), |sz, order| *sz = *sz + order.sz());
        let n = orders.fold(0, |n, _| *n += 1);
        if build_l2_level(
            &mut cur_level,
            &mut levels,
            n_levels,
            n_sig_figs,
            mantissa,
            side,
            InnerLevel { px: *px, sz, n },
        ) {
            break;
        }
    }
    levels.extend(cur_level.take());
    levels
}

pub(super) fn build_l2_level(
    cur_level: &mut Option<InnerLevel>,
    levels: &mut Vec<InnerLevel>,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
    side: Side,
    level: InnerLevel,
) -> bool {
    let new_bucket = cur_level.as_ref().is_none_or(|c| match side {
        Side::Ask => level.px.value() > c.px.value(),
        Side::Bid => level.px.value() < c.px.value(),
    });
    if new_bucket {
        let bucket = bucket(level.px, side, n_sig_figs, mantissa);
        levels.extend(cur_level.take());
        if n_levels == Some(levels.len()) {
            return true;
        }
        *cur_level = Some(InnerLevel { px: bucket, sz: level.sz, n: level.n });
    } else if let Some(c) = cur_level.as_mut() {
        c.sz = level.sz + c.sz;
        c.n += level.n;
    }
    false
}
