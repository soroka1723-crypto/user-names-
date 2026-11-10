use async_trait::async_trait;
use regex::Regex;
use reqwest::Client;
use std::{sync::OnceLock, time::Duration};
use tracing::{debug, warn};

use crate::generator::{AvailabilityChecker, Status};

pub struct PublicTelegramChecker { client: Client }

impl PublicTelegramChecker {
    pub fn new() -> Result<Self, String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(4))
            .timeout(Duration::from_secs(7))
            .pool_max_idle_per_host(8)
            .user_agent("Mozilla/5.0 (compatible; UsernameAvailabilityChecker/1.0)")
            .build().map_err(|e| format!("HTTP client: {e}"))?;
        Ok(Self { client })
    }

    async fn telegram_once(&self, name: &str) -> Status {
        let response = match self.client.get(format!("https://t.me/{name}")).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => { debug!(%name, status = %r.status(), "Telegram public page inconclusive"); return Status::Unknown; }
            Err(e) => { warn!(%name, error = %e, "Telegram public page request failed"); return Status::Unknown; }
        };
        let page = match response.text().await {
            Ok(v) => v,
            Err(e) => { warn!(%name, error = %e, "Telegram public page body failed"); return Status::Unknown; }
        };
        static META: OnceLock<Regex> = OnceLock::new();
        static OG: OnceLock<Regex> = OnceLock::new();
        static CONTENT: OnceLock<Regex> = OnceLock::new();
        let meta = META.get_or_init(|| Regex::new(r#"(?is)<meta\b[^>]*>"#).expect("static regex"));
        let og = OG.get_or_init(|| Regex::new(r#"(?i)property\s*=\s*["']og:title["']"#).expect("static regex"));
        let content = CONTENT.get_or_init(|| Regex::new(r#"(?i)content\s*=\s*["']([^"']*)["']"#).expect("static regex"));
        for tag in meta.find_iter(&page) {
            if og.is_match(tag.as_str()) {
                if let Some(capture) = content.captures(tag.as_str()) {
                    let title = capture.get(1).map(|m| m.as_str()).unwrap_or("").trim().to_ascii_lowercase();
                    if title == format!("telegram: contact @{name}") { return Status::Free; }
                    if title.starts_with("telegram: contact @") || title.is_empty() { return Status::Unknown; }
                    return Status::Taken;
                }
            }
        }
        Status::Unknown
    }

    async fn fragment_excludes(&self, name: &str) -> bool {
        let response = match self.client.get(format!("https://fragment.com/username/{name}")).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => return false,
        };
        let page = match response.text().await { Ok(v) => v, Err(_) => return false };
        static STATUS: OnceLock<Regex> = OnceLock::new();
        let re = STATUS.get_or_init(|| Regex::new(r#"class=["'][^"']*tm-section-header-status[^"']*tm-status-(\w+)"#).expect("static regex"));
        re.captures(&page).and_then(|c| c.get(1))
            .map(|m| matches!(m.as_str(), "avail" | "auction" | "sold")).unwrap_or(false)
    }
}

#[async_trait]
impl AvailabilityChecker for PublicTelegramChecker {
    async fn check(&self, name: &str) -> Status {
        if name.len() != 5 || !name.bytes().all(|b| b.is_ascii_lowercase()) { return Status::Unknown; }
        let first = self.telegram_once(name).await;
        if first != Status::Free { return first; }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let second = self.telegram_once(name).await;
        if second != Status::Free { return second; }
        if self.fragment_excludes(name).await { return Status::Taken; }
        Status::Free
    }
}
