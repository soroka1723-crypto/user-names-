use async_trait::async_trait;
use reqwest::blocking::Client;
use std::time::Duration;

use crate::generator::{AvailabilityChecker, Status};

/// Public-page checker: no Telegram account, MTProto session, phone, or login code.
pub struct PublicTelegramChecker {
    client: Client,
}

impl PublicTelegramChecker {
    pub fn new() -> Result<Self, String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(4))
            .timeout(Duration::from_secs(8))
            .user_agent("Mozilla/5.0 (compatible; UsernameAvailabilityBot/1.0)")
            .pool_max_idle_per_host(8)
            .build()
            .map_err(|e| format!("create public Telegram checker: {e}"))?;
        Ok(Self { client })
    }

    fn check_public_page(&self, name: &str) -> Status {
        if name.len() != 5 || !name.bytes().all(|b| b.is_ascii_lowercase()) {
            return Status::Unknown;
        }
        let response = match self.client.get(format!("https://t.me/{name}")).send() {
            Ok(response) => response,
            Err(_) => return Status::Unknown,
        };
        if !response.status().is_success() {
            return Status::Unknown;
        }
        let page = match response.text() {
            Ok(page) => page,
            Err(_) => return Status::Unknown,
        };
        let lower = page.to_ascii_lowercase();
        if lower.contains("tgme_page_extra") || lower.contains("tgme_page_photo") {
            return Status::Taken;
        }

        let expected = format!("telegram: contact @{name}");
        match extract_og_title(&page) {
            Some(title) if title.trim().eq_ignore_ascii_case(&expected) => Status::Free,
            Some(title) if title.to_ascii_lowercase().starts_with("telegram: contact @") => Status::Unknown,
            Some(title) if !title.trim().is_empty() => Status::Taken,
            _ => Status::Unknown,
        }
    }
}

fn extract_og_title(page: &str) -> Option<String> {
    let tags = regex::Regex::new(r#"(?is)<meta\b[^>]*>"#).ok()?;
    let content = regex::Regex::new(r#"(?i)content\s*=\s*["']([^"']*)["']"#).ok()?;
    for tag in tags.find_iter(page).map(|m| m.as_str()) {
        let lower = tag.to_ascii_lowercase();
        if !lower.contains(r#"property="og:title""#)
            && !lower.contains("property='og:title'")
        {
            continue;
        }
        if let Some(capture) = content.captures(tag) {
            return capture.get(1).map(|m| m.as_str().to_string());
        }
    }
    None
}

#[async_trait]
impl AvailabilityChecker for PublicTelegramChecker {
    async fn check(&self, name: &str) -> Status {
        let client = self.client.clone();
        let name = name.to_string();
        match tokio::task::spawn_blocking(move || {
            PublicTelegramChecker { client }.check_public_page(&name)
        }).await {
            Ok(status) => status,
            Err(_) => Status::Unknown,
        }
    }
}
