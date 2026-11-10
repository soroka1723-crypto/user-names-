use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use grammers_client::{
    session::storages::SqliteSession,
    sender::SenderPool,
    tl,
    Client,
};
use std::{env, path::Path, sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::generator::{AvailabilityChecker, Status};

pub struct TelegramMtprotoChecker {
    client: Arc<Client>,
}

impl TelegramMtprotoChecker {
    pub async fn connect_from_env() -> Result<Option<Self>, String> {
        let api_id = match env::var("TG_API_ID") {
            Ok(v) => v.parse::<i32>().map_err(|_| "TG_API_ID must be an integer".to_string())?,
            Err(_) => return Ok(None),
        };
        let api_hash = match env::var("TG_API_HASH") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => return Ok(None),
        };

        let session_path = env::var("TG_SESSION_PATH")
            .unwrap_or_else(|_| "telegram_user.session".to_string());
        let session_b64 = env::var("TG_SESSION_B64").ok();
        if !Path::new(&session_path).exists() {
            let encoded = session_b64.ok_or_else(|| {
                "Telegram MTProto user session is missing. Set TG_SESSION_B64 once or provide TG_SESSION_PATH."
                    .to_string()
            })?;
            let bytes = STANDARD.decode(encoded.trim())
                .map_err(|e| format!("TG_SESSION_B64 is not valid base64: {e}"))?;
            std::fs::write(&session_path, bytes)
                .map_err(|e| format!("cannot restore Telegram session file: {e}"))?;
        }

        let api_session = Arc::new(
            SqliteSession::open(&session_path).await
                .map_err(|e| format!("cannot open Telegram session: {e}"))?
        );
        let SenderPool { runner, handle, .. } = SenderPool::new(Arc::clone(&api_session), api_id);
        let client = Client::new(handle);
        tokio::spawn(runner.run());

        let authorized = client.is_authorized().await
            .map_err(|e| format!("cannot verify Telegram MTProto session: {e}"))?;
        if !authorized {
            return Err(
                "Telegram MTProto session is not authorized. A user-account session is required; BOT_TOKEN alone cannot run account.checkUsername."
                    .to_string()
            );
        }
        info!("Telegram MTProto user session connected");
        Ok(Some(Self { client: Arc::new(client) }))
    }

    async fn check_with_retry(&self, name: &str) -> Status {
        let mut backoff = 1u64;
        for attempt in 0..3 {
            let request = tl::functions::account::CheckUsername {
                username: name.to_string(),
            };
            match self.client.invoke(&request).await {
                Ok(result) => {
                    let value = format!("{result:?}");
                    if value == "True" {
                        return Status::Free;
                    }
                    if value == "False" {
                        return Status::Taken;
                    }
                    warn!(%name, response = %value, "unrecognized Telegram username-check response");
                    return Status::Unknown;
                }
                Err(error) => {
                    let message = format!("{error:?} {error}");
                    if message.contains("USERNAME_OCCUPIED")
                        || message.contains("USERNAME_PURCHASE_AVAILABLE")
                        || message.contains("USERNAME_INVALID")
                    {
                        return Status::Taken;
                    }
                    if let Some(wait) = flood_wait_seconds(&message) {
                        let wait = wait.min(600);
                        warn!(%name, wait_seconds = wait, "Telegram FLOOD_WAIT");
                        sleep(Duration::from_secs(wait)).await;
                        continue;
                    }
                    warn!(%name, attempt, error = %message, "Telegram username check failed");
                    if attempt < 2 {
                        sleep(Duration::from_secs(backoff)).await;
                        backoff = (backoff * 2).min(8);
                    }
                }
            }
        }
        Status::Unknown
    }
}

#[async_trait]
impl AvailabilityChecker for TelegramMtprotoChecker {
    async fn check(&self, name: &str) -> Status {
        if name.len() != 5 || !name.bytes().all(|b| b.is_ascii_lowercase()) {
            return Status::Unknown;
        }
        self.check_with_retry(name).await
    }
}

fn flood_wait_seconds(message: &str) -> Option<u64> {
    for marker in ["FLOOD_WAIT_", "FLOOD_PREMIUM_WAIT_"] {
        if let Some(index) = message.find(marker) {
            let digits = message[index + marker.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>();
            if !digits.is_empty() {
                return digits.parse::<u64>().ok();
            }
        }
    }
    None
}
