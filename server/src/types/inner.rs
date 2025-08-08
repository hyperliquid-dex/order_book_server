use alloy::primitives::Address;

use super::Level;
use crate::{
    order_book::{
        Oid,
        types::{Coin, InnerOrder, Px, Side, Sz},
    },
    prelude::*,
    types::{L4Order, OrderDiff, node_data::NodeDataOrderStatus},
};

// L4Order: the struct we keep in the orderbook (computationally better)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InnerL4Order {
    pub user: Address,
    pub coin: Coin,
    pub side: Side,
    pub limit_px: Px,
    pub sz: Sz,
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

impl InnerOrder for InnerL4Order {
    fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    fn side(&self) -> Side {
        self.side
    }

    fn limit_px(&self) -> Px {
        self.limit_px
    }

    fn sz(&self) -> Sz {
        self.sz
    }

    fn decrement_sz(&mut self, dec: Sz) {
        self.sz.decrement_sz(dec.value());
    }

    fn modify_sz(&mut self, sz: Sz) {
        self.sz = sz;
    }

    fn fill(&mut self, maker_order: &mut Self) -> Sz {
        let match_sz = self.sz().min(maker_order.sz());
        self.decrement_sz(match_sz);
        maker_order.decrement_sz(match_sz);
        match_sz
    }

    fn convert_trigger(&mut self, ts: u64) {
        if self.is_trigger {
            self.trigger_px = "0.0".to_string();
            self.trigger_condition = "Triggered".to_string();
            self.is_trigger = false;
            self.timestamp = ts;
            self.tif = Some("Gtc".to_string());
        }
    }

    fn coin(&self) -> Coin {
        self.coin.clone()
    }
}

impl TryFrom<(Address, L4Order)> for InnerL4Order {
    type Error = Error;

    fn try_from(value: (Address, L4Order)) -> Result<Self> {
        let L4Order {
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
            ..
        } = value.1;
        let user = value.0;
        let limit_px = Px::parse_from_str(&limit_px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(Self {
            user,
            coin: Coin::new(&coin),
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
        })
    }
}

impl From<InnerL4Order> for L4Order {
    fn from(value: InnerL4Order) -> Self {
        let InnerL4Order {
            user,
            coin,
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
        } = value;
        let limit_px = limit_px.to_str();
        let sz = sz.to_str();
        Self {
            user: Some(user),
            coin: coin.value(),
            side,
            limit_px,
            sz,
            oid,
            timestamp,
            trigger_condition,
            is_trigger,
            trigger_px,
            is_position_tpsl,
            reduce_only,
            order_type,
            tif,
            cloid,
        }
    }
}

impl TryFrom<NodeDataOrderStatus> for InnerL4Order {
    type Error = Error;

    fn try_from(value: NodeDataOrderStatus) -> Result<Self> {
        (value.user, value.order).try_into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InnerLevel {
    pub px: Px,
    pub sz: Sz,
    pub n: usize,
}

impl From<InnerLevel> for Level {
    fn from(value: InnerLevel) -> Self {
        Self::new(value.px.to_str(), value.sz.to_str(), value.n)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum InnerOrderDiff {
    New {
        sz: Sz,
    },
    #[allow(dead_code)]
    Update {
        orig_sz: Sz,
        new_sz: Sz,
    },
    Remove,
}

impl TryFrom<OrderDiff> for InnerOrderDiff {
    type Error = Error;

    fn try_from(value: OrderDiff) -> Result<Self> {
        Ok(match value {
            OrderDiff::New { sz } => Self::New { sz: Sz::parse_from_str(&sz)? },
            OrderDiff::Update { orig_sz, new_sz } => {
                Self::Update { orig_sz: Sz::parse_from_str(&orig_sz)?, new_sz: Sz::parse_from_str(&new_sz)? }
            }
            OrderDiff::Remove => Self::Remove,
        })
    }
}
