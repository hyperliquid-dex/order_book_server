use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Deserialize, Clone)]
pub struct Secrets {
    pub slack_webhook_url: String,
    pub snape_slack_id: String,
    pub frank_slack_id: String,
}

impl Secrets {
    pub fn load() -> Option<Self> {
        let path = Path::new("secrets.json");
        if !path.exists() {
            log::warn!("secrets.json not found - Slack alerts will be disabled");
            return None;
        }
        
        match fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(secrets) => Some(secrets),
                Err(e) => {
                    log::error!("Failed to parse secrets.json: {}", e);
                    None
                }
            },
            Err(e) => {
                log::error!("Failed to read secrets.json: {}", e);
                None
            }
        }
    }
}