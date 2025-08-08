use crate::types::{L2Book, L4Book, Trade};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MAX_LEVELS: usize = 100;
pub(crate) const DEFAULT_LEVELS: usize = 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[serde(rename_all = "camelCase")]
pub(crate) enum ClientMessage {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub(crate) enum Subscription {
    #[serde(rename_all = "camelCase")]
    Trades { coin: String },
    #[serde(rename_all = "camelCase")]
    L2Book { coin: String, n_sig_figs: Option<u32>, n_levels: Option<usize>, mantissa: Option<u64> },
    #[serde(rename_all = "camelCase")]
    L4Book { coin: String },
}

impl Subscription {
    pub(crate) fn validate(&self, universe: &HashSet<String>) -> bool {
        match self {
            Self::Trades { coin } => universe.contains(coin),
            Self::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
                if !universe.contains(coin) || coin.starts_with('@') {
                    info!("Invalid subscription: coin not found");
                    return false;
                }
                if *n_levels == Some(DEFAULT_LEVELS) {
                    info!("Invalid subscription: set n_levels to this by using null");
                    return false;
                }
                let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
                if n_levels > MAX_LEVELS {
                    info!("Invalid subscription: n_levels too high");
                    return false;
                }
                if let Some(n_sig_figs) = *n_sig_figs {
                    if !(2..=5).contains(&n_sig_figs) {
                        info!("Invalid subscription: sig figs aren't set correctly");
                        return false;
                    }
                    if let Some(m) = *mantissa {
                        if n_sig_figs < 5 || (m != 5 && m != 2) {
                            return false;
                        }
                    }
                } else if mantissa.is_some() {
                    info!("Invalid subscription: mantissa can not be some if sig figs are not set");
                    return false;
                }
                info!("Valid subscription");
                true
            }
            Self::L4Book { coin } => {
                if !universe.contains(coin) || coin.starts_with('@') {
                    info!("Invalid subscription: coin not found");
                    return false;
                }
                info!("Valid subscription");
                true
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "channel", content = "data")]
#[serde(rename_all = "camelCase")]
pub(crate) enum ServerResponse {
    SubscriptionResponse(ClientMessage),
    L2Book(L2Book),
    L4Book(L4Book),
    Trades(Vec<Trade>),
    Error(String),
}

#[derive(Default)]
pub(crate) struct SubscriptionManager {
    subscriptions: HashSet<Subscription>,
}

impl SubscriptionManager {
    pub(crate) fn subscribe(&mut self, sub: Subscription) -> bool {
        self.subscriptions.insert(sub)
    }

    pub(crate) fn unsubscribe(&mut self, sub: Subscription) -> bool {
        self.subscriptions.remove(&sub)
    }

    pub(crate) const fn subscriptions(&self) -> &HashSet<Subscription> {
        &self.subscriptions
    }
}

#[cfg(test)]
mod test {
    use crate::types::subscription::Subscription;

    use super::{ClientMessage, ServerResponse};

    #[test]
    fn test_message_deserialization_subscription_response() {
        let message = r#"
            {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null}}}
        "#;
        let msg = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::SubscriptionResponse(_)));
    }

    #[test]
    fn test_message_deserialization_l2book() {
        let message = r#"
            {"channel":"l2Book","data":{"coin":"BTC","time":1751427259657,"levels":[[{"px":"106217.0","sz":"0.001","n":1},{"px":"106215.0","sz":"0.001","n":1},{"px":"106213.0","sz":"0.27739","n":1},{"px":"106193.0","sz":"0.49943","n":1},{"px":"106190.0","sz":"0.52899","n":1},{"px":"106162.0","sz":"0.55931","n":1},{"px":"106160.0","sz":"0.55023","n":1},{"px":"106140.0","sz":"0.001","n":1},{"px":"106137.0","sz":"0.001","n":1},{"px":"106131.0","sz":"0.001","n":1},{"px":"106111.0","sz":"0.01094","n":1},{"px":"106085.0","sz":"1.02207","n":2},{"px":"105916.0","sz":"0.001","n":1},{"px":"105913.0","sz":"1.01927","n":2},{"px":"105822.0","sz":"0.00474","n":1},{"px":"105698.0","sz":"0.51012","n":1},{"px":"105696.0","sz":"0.001","n":1},{"px":"105604.0","sz":"0.55072","n":1},{"px":"105579.0","sz":"0.00217","n":1},{"px":"105543.0","sz":"0.0197","n":1}],[{"px":"106233.0","sz":"0.26739","n":3},{"px":"106258.0","sz":"0.001","n":1},{"px":"106270.0","sz":"0.49128","n":2},{"px":"106306.0","sz":"0.27263","n":1},{"px":"106311.0","sz":"0.23837","n":1},{"px":"106350.0","sz":"0.001","n":1},{"px":"106396.0","sz":"0.24733","n":1},{"px":"106414.0","sz":"0.27088","n":1},{"px":"106560.0","sz":"0.0001","n":1},{"px":"106597.0","sz":"0.56981","n":1},{"px":"106637.0","sz":"0.57002","n":1},{"px":"106932.0","sz":"0.001","n":1},{"px":"107012.0","sz":"1.06873","n":2},{"px":"107094.0","sz":"0.0041","n":1},{"px":"107360.0","sz":"0.001","n":1},{"px":"107535.0","sz":"0.002","n":1},{"px":"107638.0","sz":"0.001","n":1},{"px":"107639.0","sz":"0.0007","n":1},{"px":"107650.0","sz":"0.00074","n":1},{"px":"107675.0","sz":"0.00083","n":1}]]}}
        "#;
        let msg: ServerResponse = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::L2Book(_)));
    }

    #[test]
    fn test_message_deserialization_trade() {
        let message = r#"
            {"channel":"trades","data":[{"coin":"BTC","side":"A","px":"106296.0","sz":"0.00017","time":1751430933565,"hash":"0xde93a8a0729ade63d8840417805ba9010b008818422ddedb1285744426b73503","tid":293353986402527,"users":["0xcc0a3b6e3267c84361e91d8230868eea53431e4b","0xc64cc00b46101bd40aa1c3121195e85c0b0918d8"]}]}
        "#;
        let msg: ServerResponse = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::Trades(_)));
    }

    #[test]
    fn test_client_message_deserialization() {
        let message = r#"
            { "method": "subscribe", "subscription":{ "type": "l2Book", "coin": "BTC" }}
        "#;
        let msg: ClientMessage = serde_json::from_str(message).unwrap();
        assert!(matches!(
            msg,
            ClientMessage::Subscribe {
                subscription: Subscription::L2Book { n_sig_figs: None, n_levels: None, mantissa: None, .. },
            }
        ));
    }
}
