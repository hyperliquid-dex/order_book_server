use crate::config::Secrets;
use chrono::Utc;
use serde_json::json;
use std::time::Duration;

#[derive(Debug)]
pub enum AlertType {
    ListenerFatalError,
    ServerFatalError,
    NodeExecutionBehind,
    FileWatcherError,
    ChannelError,
    SnapshotSyncError,
    StateProcessingError,
}

impl AlertType {
    fn to_string(&self) -> &'static str {
        match self {
            AlertType::ListenerFatalError => "Listener Fatal Error",
            AlertType::ServerFatalError => "Server Fatal Error",
            AlertType::NodeExecutionBehind => "Node Execution Behind",
            AlertType::FileWatcherError => "File Watcher Error",
            AlertType::ChannelError => "Channel Communication Error",
            AlertType::SnapshotSyncError => "Snapshot Sync Error",
            AlertType::StateProcessingError => "State Processing Error",
        }
    }
    
    fn exit_code(&self) -> i32 {
        match self {
            AlertType::ListenerFatalError => 1,
            AlertType::ServerFatalError => 2,
            AlertType::NodeExecutionBehind => 3,
            AlertType::FileWatcherError => 4,
            AlertType::ChannelError => 5,
            AlertType::SnapshotSyncError => 6,
            AlertType::StateProcessingError => 7,
        }
    }
}

pub async fn send_slack_alert(
    secrets: &Secrets,
    alert_type: AlertType,
    error_message: &str,
    server_address: &str,
) {
    let timestamp = Utc::now().to_rfc3339();
    let exit_code = alert_type.exit_code();
    let alert_type_str = alert_type.to_string();
    
    // Always tag Frank and Snape for all critical errors
    let text = format!("🚨 WebSocket Server Alert <@{}> <@{}>", secrets.frank_slack_id, secrets.snape_slack_id);
    
    let payload = json!({
        "text": text,
        "attachments": [{
            "color": "danger",
            "fields": [
                {
                    "title": "Error Type",
                    "value": alert_type_str,
                    "short": true
                },
                {
                    "title": "Exit Code",
                    "value": exit_code.to_string(),
                    "short": true
                },
                {
                    "title": "Timestamp",
                    "value": timestamp,
                    "short": true
                },
                {
                    "title": "Server",
                    "value": server_address,
                    "short": true
                },
                {
                    "title": "Error Message",
                    "value": error_message,
                    "short": false
                }
            ]
        }]
    });
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build();
    
    match client {
        Ok(client) => {
            match client
                .post(&secrets.slack_webhook_url)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        log::info!("Slack alert sent successfully");
                    } else {
                        log::error!("Failed to send Slack alert: HTTP {}", response.status());
                    }
                }
                Err(e) => {
                    log::error!("Failed to send Slack alert: {}", e);
                }
            }
        }
        Err(e) => {
            log::error!("Failed to create HTTP client for Slack alert: {}", e);
        }
    }
}

pub async fn send_alert_before_exit(
    secrets: Option<&Secrets>,
    alert_type: AlertType,
    error_message: &str,
    server_address: &str,
) {
    if let Some(secrets) = secrets {
        send_slack_alert(secrets, alert_type, error_message, server_address).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}