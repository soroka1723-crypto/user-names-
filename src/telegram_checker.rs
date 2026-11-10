use async_trait::async_trait;
use reqwest::blocking::Client;
use std::time::Duration;
use crate::generator::{AvailabilityChecker, Status};

/// Checks public Telegram web pages only. It never logs into a Telegram account.
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
        let url = format!("https://t.me/{name}");
        let response = match self.client.get(url).send() {
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

        // An actual profile/contact page is evidence that the name is occupied.
        if lower.contains("tgme_page_extra") || lower.contains("tgme_page_photo") {
            return Status::Taken;
        }

        // A public t.me page for an unassigned name exposes this exact OG title.
        let expected = format!("telegram: contact @{name}");
        if let Some(title) = extract_og_title(&page) {
            if title.trim().eq_ignore_ascii_case(&expected) {
                return Status::Free;
            }
            if title.to_ascii_lowercase().starts_with("telegram: contact @") {
                return Status::Unknown;
            }
            if !title.trim().is_empty() {
                return Status::Taken;
            }
        }
        Status::Unknown
    }
}

fn extract_og_title(page: &str) -> Option<String> {
    let tags = regex::Regex::new(r#"(?is)<meta\\b[^>]*>"#).ok()?;
    for tag in tags.find_iter(page).map(|m| m.as_str()) {
        let tag_lower = tag.to_ascii_lowercase();
        if !tag_lower.contains("property="og:title"") && !tag_lower.contains("property='og:title'") {
            continue;
        }
        let content = regex::Regex::new(r#"(?i)content\\s*=\\s*["']([^"']*)["']"#).ok()?;
        if let Some(capture) = content.captures(tag) {
            return capture.get(1).map(|m| m.as_str().to_string());
        }
    }
    None
}

#[async_trait]
impl AvailabilityChecker for PublicTelegramChecker {
    async fn check(&self, name: &str) -> Status {
        // reqwest's blocking client is isolated from the async executor.
        let client = self.client.clone();
        let name = name.to_string();
        match tokio::task::spawn_blocking(move || {
            let checker = PublicTelegramChecker { client };
            checker.check_public_page(&name)
        }).await {
            Ok(status) => status,
            Err(_) => Status::Unknown,
        }
    }
}
