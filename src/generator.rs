use async_trait::async_trait;
use rand::{seq::SliceRandom, Rng};
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
const VOWELS: &[u8] = b"aeiou";
const CONSONANTS: &[u8] = b"bcdfghjklmnpqrstvwxyz";
const CACHE_FREE_TTL_SECS: i64 = 600;
const CACHE_TAKEN_TTL_SECS: i64 = 21_600;
const SEEN_TTL_SECS: i64 = 86_400;
const DEFAULT_CONCURRENCY: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Free,
    Taken,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub name: String,
    pub len: u8,
    pub score: u8,
    pub status: Status,
    pub checked_at: i64,
}

#[async_trait]
pub trait AvailabilityChecker: Send + Sync {
    async fn check(&self, name: &str) -> Status;
}

#[derive(Debug, Clone)]
struct CachedStatus {
    status: Status,
    checked_at: i64,
}

pub struct GeneratorStore {
    db: Mutex<Connection>,
}

impl GeneratorStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = Connection::open(path).map_err(|e| format!("open generator database: {e}"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|e| format!("set database timeout: {e}"))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS generator_seen(
               user_id INTEGER NOT NULL,
               username TEXT NOT NULL,
               seen_at INTEGER NOT NULL,
               PRIMARY KEY(user_id, username)
             );
             CREATE TABLE IF NOT EXISTS generator_status_cache(
               username TEXT PRIMARY KEY,
               status TEXT NOT NULL,
               checked_at INTEGER NOT NULL
             );",
        )
        .map_err(|e| format!("create generator tables: {e}"))?;
        Ok(Self { db: Mutex::new(db) })
    }

    fn cached(&self, name: &str) -> Result<Option<CachedStatus>, String> {
        let db = self.db.lock().map_err(|_| "generator database lock poisoned".to_string())?;
        let row = db.query_row(
            "SELECT status, checked_at FROM generator_status_cache WHERE username=?1",
            [name],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        ).optional().map_err(|e| format!("read status cache: {e}"))?;
        Ok(row.and_then(|(status, checked_at)| {
            let status = match status.as_str() {
                "free" => Status::Free,
                "taken" => Status::Taken,
                _ => Status::Unknown,
            };
            Some(CachedStatus { status, checked_at })
        }))
    }

    fn cache(&self, name: &str, status: Status, checked_at: i64) -> Result<(), String> {
        let status = match status {
            Status::Free => "free",
            Status::Taken => "taken",
            Status::Unknown => "unknown",
        };
        let db = self.db.lock().map_err(|_| "generator database lock poisoned".to_string())?;
        db.execute(
            "INSERT INTO generator_status_cache(username,status,checked_at) VALUES(?1,?2,?3)
             ON CONFLICT(username) DO UPDATE SET status=excluded.status, checked_at=excluded.checked_at",
            params![name, status, checked_at],
        ).map_err(|e| format!("write status cache: {e}"))?;
        Ok(())
    }

    fn was_seen(&self, user_id: i64, name: &str) -> Result<bool, String> {
        let db = self.db.lock().map_err(|_| "generator database lock poisoned".to_string())?;
        db.query_row(
            "SELECT 1 FROM generator_seen WHERE user_id=?1 AND username=?2 AND seen_at>=?3",
            params![user_id, name, now_unix().saturating_sub(SEEN_TTL_SECS)],
            |_| Ok(()),
        ).optional().map(|v| v.is_some())
            .map_err(|e| format!("check per-user duplicate: {e}"))
    }

    fn mark_seen(&self, user_id: i64, name: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|_| "generator database lock poisoned".to_string())?;
        db.execute(
            "INSERT INTO generator_seen(user_id,username,seen_at) VALUES(?1,?2,?3)
             ON CONFLICT(user_id,username) DO UPDATE SET seen_at=excluded.seen_at",
            params![user_id, name, now_unix()],
        ).map_err(|e| format!("save per-user seen username: {e}"))?;
        Ok(())
    }
}

pub struct Generator {
    store: Arc<GeneratorStore>,
    checker: Arc<dyn AvailabilityChecker>,
    gate: Arc<Semaphore>,
    words: HashSet<String>,
    words4: HashSet<String>,
    blacklist: HashSet<String>,
}

impl Generator {
    pub fn new(
        store: Arc<GeneratorStore>,
        checker: Arc<dyn AvailabilityChecker>,
        concurrency: usize,
    ) -> Self {
        let words = load_words("words5.txt", DEFAULT_WORDS, 5);
        let words4 = load_words("words4.txt", DEFAULT_WORDS4, 4);
        let blacklist = load_words("blacklist.txt", DEFAULT_BLACKLIST, 5);
        Self {
            store,
            checker,
            gate: Arc::new(Semaphore::new(concurrency.max(1))),
            words,
            words4,
            blacklist,
        }
    }

    pub async fn generate_free(
        &self,
        count: usize,
        min_len: u8,
        max_len: u8,
        user_id: i64,
    ) -> Result<Vec<Candidate>, String> {
        if user_id <= 0 {
            return Err("user_id must be positive".into());
        }
        if min_len != 5 || max_len != 5 {
            return Err("this bot is configured for exactly 5 characters".into());
        }
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        let mut queued = HashSet::new();
        let mut attempts = 0usize;
        let max_attempts = count.saturating_mul(2500).max(5000);

        while candidates.len() < count && attempts < max_attempts {
            let mut batch = Vec::with_capacity(DEFAULT_CONCURRENCY);
            while batch.len() < DEFAULT_CONCURRENCY && attempts < max_attempts {
                attempts += 1;
                let name = self.make_candidate();
                if !validate_username(&name) {
                    debug!(%name, reason = "validator", "candidate discarded");
                    continue;
                }
                if self.blacklist.contains(&name) {
                    debug!(%name, reason = "blacklist", "candidate discarded");
                    continue;
                }
                if !queued.insert(name.clone()) {
                    debug!(%name, reason = "batch_duplicate", "candidate discarded");
                    continue;
                }
                if self.store.was_seen(user_id, &name)? {
                    debug!(%name, reason = "user_seen", "candidate discarded");
                    continue;
                }
                batch.push(name);
            }
            if batch.is_empty() {
                break;
            }

            let mut to_check = Vec::new();
            for name in batch {
                let now = now_unix();
                match self.store.cached(&name)? {
                    Some(cached) if cached.status != Status::Unknown
                        && now.saturating_sub(cached.checked_at) < (match cached.status {
                            Status::Free => CACHE_FREE_TTL_SECS,
                            Status::Taken => CACHE_TAKEN_TTL_SECS,
                            Status::Unknown => 0,
                        }) => {
                            if cached.status == Status::Free {
                                candidates.push(Candidate {
                                    name,
                                    len: 5,
                                    score: 0,
                                    status: Status::Free,
                                    checked_at: cached.checked_at,
                                });
                            } else {
                                debug!(%name, status = ?cached.status, "cached username not free");
                            }
                        }
                    _ => to_check.push(name),
                }
            }

            let statuses = futures_util::future::join_all(
                to_check.iter().map(|name| async move {
                    (name.clone(), self.check_twice(name).await)
                })
            ).await;

            for (name, status) in statuses {
                let checked_at = now_unix();
                self.store.cache(&name, status, checked_at)?;
                if status == Status::Free && candidates.len() < count {
                    candidates.push(Candidate {
                        score: score_name(&name, &self.words),
                        name,
                        len: 5,
                        status,
                        checked_at,
                    });
                } else {
                    info!(%name, ?status, "candidate not confirmed free");
                }
                if candidates.len() >= count {
                    break;
                }
            }
        }
        for candidate in &mut candidates {
            if candidate.score == 0 {
                candidate.score = score_name(&candidate.name, &self.words);
            }
        }
        candidates.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.name.cmp(&b.name)));
        for item in &candidates {
            self.store.mark_seen(user_id, &item.name)?;
        }
        info!(user_id, requested = count, returned = candidates.len(), attempts, "generation finished");
        Ok(candidates)
    }

    async fn check_twice(&self, name: &str) -> Status {
        let first = self.check_once(name).await;
        if first != Status::Free {
            return first;
        }
        tokio::time::sleep(Duration::from_millis(350)).await;
        self.check_once(name).await
    }

    async fn check_once(&self, name: &str) -> Status {
        let permit = match self.gate.acquire().await {
            Ok(p) => p,
            Err(_) => return Status::Unknown,
        };
        let status = self.checker.check(name).await;
        drop(permit);
        status
    }

    fn make_candidate(&self) -> String {
        let mut rng = rand::thread_rng();
        let word = self.words.iter().collect::<Vec<_>>();
        let roll = rng.gen_range(0..100);
        if roll < 42 && !word.is_empty() {
            if let Some(w) = word.choose(&mut rng) {
                return (*w).clone();
            }
        }
        if roll < 57 && !self.words4.is_empty() {
            if let Some(w) = self.words4.iter().collect::<Vec<_>>().choose(&mut rng) {
                let suffix = ALPHABET[rng.gen_range(0..ALPHABET.len())] as char;
                return format!("{w}{suffix}");
            }
        }

        let patterns: [&[u8]; 4] = [
            b"CVCVC", b"CVCCV", b"CCVCV", b"VCVCV",
        ];
        let pattern = patterns[rng.gen_range(0..patterns.len())];
        let mut name = String::with_capacity(5);
        for slot in pattern {
            let set = match slot {
                b'C' => CONSONANTS,
                b'V' => VOWELS,
                _ => ALPHABET,
            };
            let ch = set[rng.gen_range(0..set.len())] as char;
            name.push(ch);
        }
        name
    }
}

fn load_words(path: &str, fallback: &[&str], expected_len: usize) -> HashSet<String> {
    let raw = fs::read_to_string(path).unwrap_or_else(|_| fallback.join("\n"));
    raw.lines()
        .map(|line| line.trim().to_ascii_lowercase())
        .filter(|line| !line.is_empty() && line.len() == expected_len && line.bytes().all(|b| b.is_ascii_lowercase()))
        .collect()
}

pub fn validate_username(name: &str) -> bool {
    if name.len() != 5 || !name.is_ascii() {
        return false;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    if !bytes.iter().all(|b| b.is_ascii_lowercase()) {
        return false;
    }
    if bytes.iter().all(|b| *b == bytes[0]) {
        return false;
    }
    if bytes.windows(3).any(|w| w[0] == w[1] && w[1] == w[2]) {
        return false;
    }
    if bytes[1..].iter().all(u8::is_ascii_digit) {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    let garbage = ["qwert", "asdfg", "zxcvb", "12345", "54321", "xxxxx"];
    if garbage.iter().any(|p| lower.contains(p)) {
        return false;
    }
    true
}

pub fn score_name(name: &str, dictionary: &HashSet<String>) -> u8 {
    if !validate_username(name) {
        return 0;
    }
    let bytes = name.as_bytes();
    let vowels = bytes.iter().filter(|b| VOWELS.contains(b)).count();
    let digits = bytes.iter().filter(|b| b.is_ascii_digit()).count();
    let mut score: i32 = 42;
    if dictionary.contains(name) {
        score += 42;
    }
    if (2..=3).contains(&vowels) {
        score += 10;
    } else {
        score -= 8;
    }
    if digits == 0 {
        score += 8;
    } else {
        score -= (digits as i32) * 9;
    }
    if bytes.windows(2).all(|w| w[0] != w[1]) {
        score += 4;
    }
    if bytes.windows(2).all(|w| (w[0] as i16 - w[1] as i16).abs() <= 10) {
        score += 3;
    }
    score.clamp(0, 100) as u8
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64).unwrap_or(0)
}

const DEFAULT_WORDS: &[&str] = &[
    "apple","beach","black","brave","bread","brick","bring","brown","candy","charm",
    "chess","clean","clear","climb","cloud","dance","dream","drink","eagle","earth",
    "fairy","faith","flame","fleet","fresh","giant","globe","grace","grape","green",
    "happy","heart","honey","horse","house","light","lucky","magic","maple","match",
    "metal","money","mouse","music","night","ocean","olive","peace","pearl","piano",
    "plant","queen","quick","quiet","river","robot","rough","round","scale","smile",
    "snake","solar","sound","space","spice","spike","sport","stone","storm","sugar",
    "sweet","table","tiger","toast","touch","track","train","trust","unity","vivid",
    "water","whale","wheat","white","world","young","zebra",
];

const DEFAULT_WORDS4: &[&str] = &[
    "able","acid","aged","also","area","army","away","baby","back","ball","band","bank",
    "base","blue","body","book","born","calm","care","case","city","cold","come","cool",
    "dark","data","dawn","days","deep","door","down","draw","drop","each","easy","edge",
    "else","even","ever","face","fact","fair","fall","farm","fast","fear","feel","fire",
    "fish","five","food","foot","form","free","from","full","game","gate","gift","girl",
    "give","gold","good","gray","grow","half","hall","hand","hard","have","head","hear",
    "heat","help","here","hero","high","hold","home","hope","hour","idea","into","iron",
    "just","keep","kind","king","knee","know","lake","land","last","late","lead","left",
    "life","like","line","lion","list","live","long","look","love","made","main","make",
    "many","mark","mass","meal","mean","meet","mile","mind","mine","moon","more","most",
    "move","name","near","neck","need","news","next","nice","nine","none","note","once",
    "only","open","over","page","paid","pair","park","part","pass","past","path","peak",
    "pick","pink","play","plus","pool","poor","pull","pure","push","race","rain","rare",
    "rate","read","real","rest","rich","ride","ring","rise","road","rock","role","room",
    "root","rule","safe","same","sand","save","seat","seed","seem","self","sell","send",
    "ship","shop","show","shut","side","sign","sing","site","size","skin","slow","snow",
    "soft","some","song","soon","star","stay","step","stop","such","sure","take","talk",
    "task","team","tell","tend","term","test","text","than","that","them","then","they",
    "thin","this","time","tiny","told","took","tool","town","tree","true","turn","type",
    "unit","upon","used","user","vary","vast","very","view","vote","wait","walk","wall",
    "want","warm","wash","wave","ways","week","well","went","were","west","what","when",
    "wide","wife","wild","will","wind","wine","wing","wise","wish","with","wood","word",
    "work","yard","yeah","year","your","zero","zone",
];

const DEFAULT_BLACKLIST: &[&str] = &[
    "bitch","bastard","cunts","fucks","fucku","nigga","niger","penis","pussy",
    "sluts","whore","telegram","toncoin","wallet","fragment",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_five_character_usernames() {
        assert!(validate_username("apple"));
        assert!(!validate_username("a1b2c"));
        assert!(!validate_username("abcd"));
        assert!(!validate_username("abcdef"));
        assert!(!validate_username("_bcde"));
        assert!(!validate_username("abcde_"));
        assert!(!validate_username("12345"));
    }

    #[test]
    fn rejects_repeated_and_garbage_patterns() {
        assert!(!validate_username("aaaaa"));
        assert!(!validate_username("aaabc"));
        assert!(!validate_username("qwert"));
        assert!(!validate_username("a1234"));
        assert!(!validate_username("ab1cd"));

    }

    #[test]
    fn real_words_score_higher() {
        let words = ["apple".to_string()].into_iter().collect();
        assert!(score_name("apple", &words) > score_name("brxpt", &words));
    }

    #[test]
    fn score_is_bounded() {
        let words = HashSet::new();
        assert!(score_name("apple", &words) <= 100);
    }
}
