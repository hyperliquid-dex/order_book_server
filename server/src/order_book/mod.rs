use crate::prelude::*;
use itertools::Itertools;
use linked_list::LinkedList;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) mod levels;
mod linked_list;
pub(crate) mod multi_book;
pub(crate) mod types;

pub(crate) use types::{Coin, InnerOrder, Oid, Px, Side, Sz};

#[derive(Clone, Default)]
pub(crate) struct OrderBook<O> {
    oid_to_side_px: HashMap<Oid, (Side, Px)>,
    bids: BTreeMap<Px, LinkedList<Oid, O>>,
    asks: BTreeMap<Px, LinkedList<Oid, O>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Snapshot<O>([Vec<O>; 2]);

impl<O: Clone> Snapshot<O> {
    pub(crate) const fn as_ref(&self) -> &[Vec<O>; 2] {
        &self.0
    }

    pub(crate) fn truncate(&self, n: usize) -> Self {
        Self(self.0.clone().map(|orders| orders.into_iter().take(n).collect_vec()))
    }
}

impl<O: InnerOrder> Snapshot<O> {
    pub(crate) fn remove_triggers(&mut self) {
        #[allow(clippy::unwrap_used)]
        let [bid_oids, ask_oids] = &self
            .0
            .iter()
            .map(|orders| orders.iter().map(InnerOrder::oid).collect::<HashSet<Oid>>())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        for orders in &mut self.0 {
            while let Some(order) = orders.last() {
                let oid = order.oid();
                if bid_oids.contains(&oid) && ask_oids.contains(&oid) {
                    orders.pop();
                } else {
                    break;
                }
            }
        }
    }
}

impl<O: InnerOrder> OrderBook<O> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { oid_to_side_px: HashMap::new(), bids: BTreeMap::new(), asks: BTreeMap::new() }
    }

    pub(crate) fn add_order(&mut self, mut order: O) {
        let (maker_orders, resting_book) = match order.side() {
            Side::Ask => (&mut self.bids, &mut self.asks),
            Side::Bid => (&mut self.asks, &mut self.bids),
        };
        let oids = match_order(maker_orders, &mut order);
        for oid in oids {
            self.oid_to_side_px.remove(&oid);
        }
        if order.sz().is_positive() {
            self.oid_to_side_px.insert(order.oid(), (order.side(), order.limit_px()));
            add_order_to_book(resting_book, order);
        }
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid) -> bool {
        if let Some((side, px)) = self.oid_to_side_px.remove(&oid) {
            let map = match side {
                Side::Ask => &mut self.asks,
                Side::Bid => &mut self.bids,
            };
            let list = map.get_mut(&px);
            if let Some(list) = list {
                let success = list.remove_node(oid.clone());
                if list.is_empty() {
                    map.remove(&px);
                }
                return success;
            }
        }
        false
    }

    pub(crate) fn modify_sz(&mut self, oid: Oid, sz: Sz) -> bool {
        if let Some((side, px)) = self.oid_to_side_px.get(&oid) {
            let map = match side {
                Side::Ask => &mut self.asks,
                Side::Bid => &mut self.bids,
            };
            let list = map.get_mut(px);
            if let Some(list) = list {
                let old_order = list.node_value_mut(&oid);
                if let Some(old_order) = old_order {
                    old_order.modify_sz(sz);
                    return true;
                }
                return false;
            }
        }
        false
    }

    // we go by the convention that prioritized orders go first in the vector; this makes aggregation step later easier.
    pub(crate) fn to_snapshot(&self) -> Snapshot<O> {
        let bids = self.bids.iter().rev().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        let asks = self.asks.iter().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        Snapshot([bids, asks])
    }

    #[must_use]
    pub(crate) fn from_snapshot(mut snapshot: Snapshot<O>, ignore_triggers: bool) -> Self {
        let mut book = Self::new();
        if ignore_triggers {
            snapshot.remove_triggers();
        }
        snapshot.0.into_iter().for_each(|orders| {
            for order in orders {
                book.add_order(order);
            }
        });
        book
    }
}

fn add_order_to_book<O: InnerOrder>(map: &mut BTreeMap<Px, LinkedList<Oid, O>>, order: O) {
    let oid = order.oid();
    let limit_px = order.limit_px();
    map.entry(limit_px).or_insert_with(|| LinkedList::new()).push_back(oid, order);
}

fn match_order<O: InnerOrder>(maker_orders: &mut BTreeMap<Px, LinkedList<Oid, O>>, taker_order: &mut O) -> Vec<Oid> {
    let mut filled_oids = Vec::new();
    let mut keys_to_remove = Vec::new();
    let taker_side = taker_order.side();
    let limit_px = taker_order.limit_px();
    let order_iter: Box<dyn Iterator<Item = (&Px, &mut LinkedList<Oid, O>)>> = match taker_side {
        Side::Ask => Box::new(maker_orders.iter_mut().rev()),
        Side::Bid => Box::new(maker_orders.iter_mut()),
    };
    for (&px, list) in order_iter {
        let matches = match taker_side {
            Side::Ask => px >= limit_px,
            Side::Bid => px <= limit_px,
        };
        if !matches {
            break;
        }
        while let Some(match_order) = list.head_value_ref_mut_unsafe() {
            taker_order.fill(match_order);
            if match_order.sz().is_zero() {
                filled_oids.push(match_order.oid());
                let _unused = list.remove_front();
            }
            if taker_order.sz().is_zero() {
                break;
            }
        }
        if list.is_empty() {
            keys_to_remove.push(px);
        }
        if taker_order.sz().is_zero() {
            break;
        }
    }
    for key in keys_to_remove {
        maker_orders.remove(&key);
    }
    filled_oids
}

#[cfg(test)]
mod tests {
    use crate::order_book::types::{Coin, Sz};

    use super::*;
    use std::collections::BTreeSet;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct MinimalOrder {
        oid: u64,
        side: Side,
        sz: u64,
        limit_px: u64,
    }

    impl InnerOrder for MinimalOrder {
        fn oid(&self) -> Oid {
            Oid::new(self.oid)
        }

        fn side(&self) -> Side {
            self.side
        }

        fn limit_px(&self) -> Px {
            Px::new(self.limit_px)
        }

        fn sz(&self) -> Sz {
            Sz::new(self.sz)
        }

        fn decrement_sz(&mut self, dec: Sz) {
            self.sz = self.sz.saturating_sub(dec.value());
        }

        fn fill(&mut self, maker_order: &mut Self) -> Sz {
            let match_sz = self.sz().min(maker_order.sz());
            maker_order.decrement_sz(match_sz);
            self.decrement_sz(match_sz);
            match_sz
        }

        fn modify_sz(&mut self, sz: Sz) {
            self.sz = sz.value();
        }

        fn convert_trigger(&mut self, _: u64) {}

        fn coin(&self) -> Coin {
            Coin::new("")
        }
    }

    impl MinimalOrder {
        fn new(oid: u64, sz: u64, limit_px: u64, side: Side) -> Self {
            Self { oid, side, sz, limit_px }
        }
    }

    #[derive(Default)]
    struct OrderFactory {
        next_oid: u64,
    }

    impl OrderFactory {
        fn order(&mut self, sz: u64, limit_px: u64, side: Side) -> MinimalOrder {
            let order = MinimalOrder::new(self.next_oid, sz, limit_px, side);
            self.next_oid += 1;
            order
        }

        fn batch_order(&mut self, sz: u64, limit_px: u64, side: Side, n: u64) -> Vec<MinimalOrder> {
            (0..n).map(|_| self.order(sz, limit_px, side)).collect_vec()
        }
    }

    #[test]
    fn simple_book_test() {
        let mut factory = OrderFactory::default();
        let buy_orders1 = factory.batch_order(100, 5, Side::Bid, 3);
        let buy_orders2 = factory.batch_order(200, 4, Side::Bid, 4);
        let sell_orders1 = factory.batch_order(150, 5, Side::Ask, 2);
        let sell_orders2 = factory.batch_order(500, 6, Side::Ask, 2);
        let mut book = OrderBook::new();
        for order in buy_orders2.clone() {
            book.add_order(order);
        }
        for order in sell_orders2.clone() {
            book.add_order(order);
        }
        for order in buy_orders1.clone() {
            book.add_order(order);
        }
        book.add_order(sell_orders1[0].clone());
        let mut bids = [buy_orders2, buy_orders1].concat();
        let mut asks = [sell_orders1.clone(), sell_orders2].concat();
        // remove index 4 and alter index 5 (matched)
        bids[5].sz -= 50;
        bids.remove(4);
        // remove index 0 (matched) and 1 (not inserted)
        asks.remove(1);
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        assert!(book.cancel_order(Oid::new(3)));
        assert!(book.cancel_order(Oid::new(9)));
        book.add_order(sell_orders1[1].clone());

        // index 4 and 5 both get matched, index 0 is canceled (first out of buy_orders2)
        bids.remove(5);
        bids.remove(4);
        bids.remove(0);

        // only thing changing in asks is that index 0 is canceled
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        // test modify size
        book.modify_sz(Oid::new(10), Sz::new(450));
        asks[0].sz = 450;

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());
    }

    fn assert_same_book(s1: Snapshot<MinimalOrder>, s2: Snapshot<MinimalOrder>) {
        let [b1, a1] = s1.0.map(BTreeSet::from_iter);
        let [b2, a2] = s2.0.map(BTreeSet::from_iter);
        assert_eq!(b1, b2);
        assert_eq!(a1, a2);
    }
}
