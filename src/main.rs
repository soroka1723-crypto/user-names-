use chrono::Local;
use rayon::prelude::*;
use reqwest::blocking::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

const ADMIN_ID: i64 = 7502792642;
const DAILY_REQUEST_LIMIT: i64 = 20;
const NAMES_PER_REQUEST: usize = 5;
const CHECK_THREADS: usize = 12;
const CHECK_TIME_BUDGET_SECS: u64 = 20;
const CHECK_TIMEOUT_SECS: u64 = 4;
const PREMIUM_CONTACT: &str = "@xldzo";

// Exact active generator from the latest Python bot (bot(5).py).
// Python's generate_names() always sets mode = "letters", and generate_one()
// returns random.choices(string.ascii_lowercase, k=5).
// The pseudo_word/combo_word/RARE_WORDS helpers are dormant in that runtime path.
const LETTERS: &[u8; 26] = b"abcdefghijklmnopqrstuvwxyz";

fn is_simple(name: &str) -> bool {
    if name.len() != 5 { return true; }
    let bytes = name.as_bytes();
    let mut counts = [0u8; 26];
    let mut unique = 0usize;
    for &b in bytes {
        if !b.is_ascii_lowercase() { return true; }
        let slot = (b - b'a') as usize;
        if counts[slot] == 0 { unique += 1; }
        counts[slot] += 1;
        if counts[slot] > 2 { return true; }
    }
    if unique < 4 { return true; }
    for i in 0..4 {
        if bytes[i] == bytes[i + 1] { return true; }
    }
    for i in 0..3 {
        let a = bytes[i] as i16;
        let b = bytes[i + 1] as i16;
        let c = bytes[i + 2] as i16;
        if (b == a + 1 && c == b + 1) || (b == a - 1 && c == b - 1) {
            return true;
        }
    }
    ["aaa", "bbb", "ccc"].iter().any(|p| name.contains(*p))
}

fn generate_one<R: rand::Rng + ?Sized>(rng: &mut R) -> String {
    (0..5)
        .map(|_| LETTERS[rng.gen_range(0..LETTERS.len())] as char)
        .collect()
}

#[derive(Clone, Default)]
struct UserState { step: String, target: i64 }

struct App {
    token: String,
    password: String,
    client: Client,
    pool: rayon::ThreadPool,
    db: Mutex<Connection>,
    states: Mutex<HashMap<i64, UserState>>,
}

impl App {
    fn new(token: String, password: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(CHECK_TIMEOUT_SECS + 2))
            .pool_max_idle_per_host(24)
            .user_agent("Mozilla/5.0 UsernameGeneratorBot/2.0")
            .build()?;
        let pool = rayon::ThreadPoolBuilder::new().num_threads(CHECK_THREADS).build()?;
        let db = Connection::open("username_data.db")?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA temp_store=MEMORY;
             CREATE TABLE IF NOT EXISTS users(
               user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
               blocked INTEGER DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS premium_users(
               user_id INTEGER PRIMARY KEY, activated_at TEXT, activated_by INTEGER
             );
             CREATE TABLE IF NOT EXISTS daily_requests(
               user_id INTEGER, usage_date TEXT, amount INTEGER DEFAULT 0,
               PRIMARY KEY(user_id,usage_date)
             );
             CREATE TABLE IF NOT EXISTS bonus_requests(
               user_id INTEGER PRIMARY KEY, amount INTEGER DEFAULT 0, updated_at TEXT
             );
             CREATE TABLE IF NOT EXISTS generated(
               name TEXT PRIMARY KEY, mode TEXT, created_at TEXT
             );
"
        )?;
        Ok(Self { token, password, client, pool, db: Mutex::new(db), states: Mutex::new(HashMap::new()) })
    }

    fn api(&self, method: &str, data: Value) -> Result<Value, String> {
        let url = format!("https://api.telegram.org/bot{}/{}", self.token, method);
        let response = self.client.post(url).json(&data).timeout(Duration::from_secs(35))
            .send().map_err(|e| e.to_string())?;
        let status = response.status();
        let body: Value = response.json().map_err(|e| e.to_string())?;
        if !status.is_success() || body["ok"] != true {
            let code = body["error_code"].as_i64().unwrap_or(status.as_u16() as i64);
            let retry = body["parameters"]["retry_after"].as_i64().unwrap_or(0);
            return Err(format!("{code}|{retry}|{}", body["description"].as_str().unwrap_or("Telegram API error")));
        }
        Ok(body["result"].clone())
    }

    fn send_raw(&self, chat: i64, text: &str, markup: Option<Value>, business: Option<&str>) -> bool {
        let mut data = json!({"chat_id":chat,"text":text});
        if let Some(kb) = markup { data["reply_markup"] = kb; }
        if let Some(id) = business { data["business_connection_id"] = json!(id); }
        self.api("sendMessage", data).is_ok()
    }

    fn keyboard(rows: &[&[&str]]) -> Value {
        json!({"keyboard":rows.iter().map(|r|r.iter().map(|s|s.to_string()).collect::<Vec<_>>()).collect::<Vec<_>>(),"resize_keyboard":true})
    }

    fn send(&self, chat: i64, text: &str, rows: Option<&[&[&str]]>) -> bool {
        self.send_raw(chat, text, rows.map(Self::keyboard), None)
    }

    fn state(&self, id: i64) -> UserState {
        self.states.lock().unwrap().get(&id).cloned().unwrap_or_default()
    }
    fn set_state(&self, id: i64, state: UserState) {
        self.states.lock().unwrap().insert(id, state);
    }
    fn today() -> String { Local::now().format("%Y-%m-%d").to_string() }

    fn save_user(&self, user: &Value) {
        let id = user["id"].as_i64().unwrap_or(0);
        if id <= 0 { return; }
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO users(user_id,first_name,username,blocked) VALUES(?1,?2,?3,0)
             ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name,username=excluded.username",
            params![id,user["first_name"].as_str().unwrap_or(""),user["username"].as_str().unwrap_or("")]
        );
    }

    fn blocked(&self, id: i64) -> bool {
        self.db.lock().unwrap().query_row("SELECT blocked FROM users WHERE user_id=?1", [id], |r| r.get::<_,i64>(0))
            .optional().ok().flatten().unwrap_or(0) != 0
    }
    fn premium(&self, id: i64) -> bool {
        id == ADMIN_ID || self.db.lock().unwrap().query_row(
            "SELECT 1 FROM premium_users WHERE user_id=?1", [id], |r| r.get::<_,i64>(0)
        ).optional().ok().flatten().is_some()
    }
    fn request_count(&self, id: i64) -> i64 {
        self.db.lock().unwrap().query_row(
            "SELECT amount FROM daily_requests WHERE user_id=?1 AND usage_date=?2",
            params![id,Self::today()],|r|r.get(0)
        ).optional().ok().flatten().unwrap_or(0)
    }
    fn bonus_count(&self, id: i64) -> i64 {
        self.db.lock().unwrap().query_row("SELECT amount FROM bonus_requests WHERE user_id=?1",[id],|r|r.get(0))
            .optional().ok().flatten().unwrap_or(0)
    }

    fn check_tme_once(&self, name: &str) -> &'static str {
        let url = format!("https://t.me/{name}");
        let page = match self.client.get(url).timeout(Duration::from_secs(CHECK_TIMEOUT_SECS)).send() {
            Ok(r) if r.status().is_success() => match r.text() { Ok(v) => v, Err(_) => return "unknown" },
            _ => return "unknown",
        };
        let lower = page.to_ascii_lowercase();
        if lower.contains("tgme_page_extra") || lower.contains("tgme_page_photo") {
            return "taken";
        }
        static META_TAG: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        static OG_PROPERTY: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        static CONTENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let tags = META_TAG.get_or_init(|| regex::Regex::new(r#"(?is)<meta\b[^>]*>"#).expect("valid meta regex"));
        let property = OG_PROPERTY.get_or_init(|| regex::Regex::new(r#"(?i)property\s*=\s*["']og:title["']"#).expect("valid property regex"));
        let content = CONTENT.get_or_init(|| regex::Regex::new(r#"(?i)content\s*=\s*["']([^"']*)["']"#).expect("valid content regex"));
        for tag in tags.find_iter(&page) {
            let tag = tag.as_str();
            if property.is_match(tag) {
                if let Some(capture) = content.captures(tag) {
                    let title = capture.get(1).map(|m|m.as_str()).unwrap_or("").trim().to_ascii_lowercase();
                    if title == format!("telegram: contact @{name}") { return "free"; }
                    if title.starts_with("telegram: contact @") { return "unknown"; }
                    if !title.is_empty() { return "taken"; }
                }
            }
        }
        "unknown"
    }

    // Same behavior as the original Python check_fragment:
    // Fragment is an additional blocklist; a failed request does not cancel
    // two successful Telegram availability checks.
    fn check_fragment(&self, name: &str) -> bool {
        let url = format!("https://fragment.com/username/{name}");
        let response = match self.client.get(url).timeout(Duration::from_secs(CHECK_TIMEOUT_SECS)).send() {
            Ok(r) if r.status().is_success() => r,
            _ => return false,
        };
        let page = match response.text() { Ok(v) => v, Err(_) => return false };
        static STATUS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let re = STATUS.get_or_init(|| regex::Regex::new(r#"class="[^"]*tm-section-header-status[^"]*tm-status-(\w+)"#).expect("valid Fragment regex"));
        re.captures(&page).and_then(|c|c.get(1))
            .map(|m|matches!(m.as_str(),"avail"|"auction"|"sold")).unwrap_or(false)
    }

    fn check_name(&self, name: &str) -> &'static str {
        if name.len() != 5 || !name.bytes().all(|b| b.is_ascii_lowercase()) { return "invalid"; }
        if self.check_tme_once(name) != "free" { return "unknown_or_taken"; }
        thread::sleep(Duration::from_millis(500));
        if self.check_tme_once(name) != "free" { return "unknown_or_taken"; }
        if self.check_fragment(name) { return "taken"; }
        "free"
    }

    fn generate(&self, id: i64) -> Result<Vec<String>, String> {
        let premium = self.premium(id);
        let used = self.request_count(id);
        let bonus = self.bonus_count(id);
        if !premium && used >= DAILY_REQUEST_LIMIT && bonus <= 0 {
            return Err("limit".into());
        }

        let known = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT name FROM generated").map_err(|_|"db")?;
            let rows = stmt.query_map([],|r|r.get::<_,String>(0)).map_err(|_|"db")?;
            rows.collect::<rusqlite::Result<std::collections::HashSet<_>>>().map_err(|_|"db")?
        };
        // Port of Python find_free_names(): generate candidates in batches
        // until five Telegram-confirmed free names are found or 20 seconds pass.
        let deadline = Instant::now() + Duration::from_secs(CHECK_TIME_BUDGET_SECS);
        let mut rng = rand::thread_rng();
        let mut seen = std::collections::HashSet::new();
        let mut free = Vec::with_capacity(NAMES_PER_REQUEST);
        while free.len() < NAMES_PER_REQUEST && Instant::now() < deadline {
            let mut batch = Vec::with_capacity(CHECK_THREADS * 2);
            let mut guard = 0usize;
            while batch.len() < CHECK_THREADS * 2 && guard < 500 {
                guard += 1;
                let name = generate_one(&mut rng);
                // Python initializes smart=True and filters simple patterns.
                if !is_simple(&name) && !known.contains(&name) && seen.insert(name.clone()) {
                    batch.push(name);
                }
            }
            if batch.is_empty() { break; }
            let statuses: Vec<(String,&'static str)> = self.pool.install(|| {
                batch.par_iter().map(|n|(n.clone(),self.check_name(n))).collect()
            });
            for (name,status) in statuses {
                if status == "free" && free.len() < NAMES_PER_REQUEST {
                    free.push(name);
                }
            }
        }
        if free.len() < NAMES_PER_REQUEST { return Err("none".into()); }

        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().map_err(|_|"db")?;
        let mut saved = Vec::new();
        for name in free {
            if tx.execute("INSERT OR IGNORE INTO generated(name,mode,created_at) VALUES(?1,'words',?2)",
                params![name,Local::now().to_rfc3339()]).map_err(|_|"db")? > 0 {
                saved.push(name);
            }
            if saved.len() == NAMES_PER_REQUEST { break; }
        }
        if saved.len() != NAMES_PER_REQUEST {
            tx.rollback().map_err(|_|"db")?;
            return Err("none".into());
        }
        if !premium && !saved.is_empty() {
            if used >= DAILY_REQUEST_LIMIT && bonus > 0 {
                tx.execute("UPDATE bonus_requests SET amount=amount-1,updated_at=?1 WHERE user_id=?2 AND amount>0",
                    params![Local::now().to_rfc3339(),id]).map_err(|_|"db")?;
            } else {
                tx.execute("INSERT INTO daily_requests(user_id,usage_date,amount) VALUES(?1,?2,1)
                    ON CONFLICT(user_id,usage_date) DO UPDATE SET amount=amount+1",
                    params![id,Self::today()]).map_err(|_|"db")?;
            }
        }
        tx.commit().map_err(|_|"db")?;
        if saved.is_empty() { Err("none".into()) } else { Ok(saved) }
    }

    fn limit_text(&self, id: i64) -> String {
        if self.premium(id) {
            return "👑 PREMIUM АКТИВЕН\n♾ Безлимитные запросы.".into();
        }
        let used = self.request_count(id);
        format!("📊 ТВОЙ ЛИМИТ\n\nЗапросов сегодня: {used}/{DAILY_REQUEST_LIMIT}\nОсталось: {}\n🎁 Бонусных запросов: {}",(DAILY_REQUEST_LIMIT-used).max(0),self.bonus_count(id))
    }

    fn admin_menu(&self, chat: i64) {
        self.send(chat,"🛠 АДМИН-ПАНЕЛЬ",Some(&[
            &["👑 Выдать Premium","❌ Снять Premium"],
            &["🎁 Подарить запросы","➖ Отнять запросы"],
            &["🚫 Заблокировать","🔓 Разблокировать"],
            &["✉️ Написать пользователю"],
            &["⬅️ Назад"]
        ]));
    }

    fn admin_input(&self, id: i64, chat: i64, text: &str, state: &mut UserState) -> bool {
        if state.step.is_empty() { return false; }
        if state.step == "admin_password" {
            state.step.clear();
            if text.as_bytes() == self.password.as_bytes() {
                self.admin_menu(chat);
            } else {
                self.send(chat,"❌ Неверный пароль.",Some(&[&["🔐 Админ-панель"]]));
            }
            self.set_state(id,state.clone());
            return true;
        }
        if state.step == "message_id" {
            if let Ok(target) = text.parse::<i64>() {
                if target > 0 {
                    state.target = target;
                    state.step = "message_text".into();
                    self.send(chat,"Теперь отправь текст сообщения:",None);
                } else {
                    self.send(chat,"Введи корректный Telegram ID.",None);
                }
            } else {
                self.send(chat,"Введи числовой Telegram ID.",None);
            }
            self.set_state(id,state.clone());
            return true;
        }
        if state.step == "message_text" {
            let target = state.target;
            state.step.clear(); state.target=0;
            let ok = self.send(target,text,None);
            self.send(chat,if ok{"✅ Сообщение отправлено."}else{"❌ Не удалось отправить сообщение."},Some(&[&["⬅️ Назад"]]));
            self.set_state(id,state.clone());
            return true;
        }
        let parts = text.split_whitespace().collect::<Vec<_>>();
        let target = parts.first().and_then(|x|x.parse::<i64>().ok()).unwrap_or(0);
        if target <= 0 {
            self.send(chat,"Нужен числовой Telegram ID.",None);
            return true;
        }
        let action = state.step.clone();
        state.step.clear();
        let mut db = self.db.lock().unwrap();
        let answer = match action.as_str() {
            "premium_on" => {
                db.execute("INSERT OR REPLACE INTO premium_users(user_id,activated_at,activated_by) VALUES(?1,?2,?3)",
                    params![target,Local::now().to_rfc3339(),id]).map(|_|"👑 Premium выдан.").unwrap_or("❌ Ошибка базы данных.")
            }
            "premium_off" => db.execute("DELETE FROM premium_users WHERE user_id=?1",[target])
                .map(|n|if n>0{"❌ Premium снят."}else{"У пользователя нет Premium."}).unwrap_or("❌ Ошибка базы данных."),
            "ban" => db.execute("UPDATE users SET blocked=1 WHERE user_id=?1",[target])
                .map(|n|if n>0{"🚫 Пользователь заблокирован."}else{"Пользователь не найден."}).unwrap_or("❌ Ошибка базы данных."),
            "unban" => db.execute("UPDATE users SET blocked=0 WHERE user_id=?1",[target])
                .map(|n|if n>0{"🔓 Пользователь разблокирован."}else{"Пользователь не найден."}).unwrap_or("❌ Ошибка базы данных."),
            "gift_requests" | "remove_requests" => {
                let amount = parts.get(1).and_then(|x|x.parse::<i64>().ok()).unwrap_or(0);
                if amount <= 0 { "Укажи количество после ID: ID количество." }
                else if action=="gift_requests" {
                    db.execute("INSERT INTO bonus_requests(user_id,amount,updated_at) VALUES(?1,?2,?3)
                        ON CONFLICT(user_id) DO UPDATE SET amount=amount+excluded.amount,updated_at=excluded.updated_at",
                        params![target,amount,Local::now().to_rfc3339()]).map(|_|"🎁 Запросы добавлены.").unwrap_or("❌ Ошибка базы данных.")
                } else {
                    db.execute("UPDATE bonus_requests SET amount=MAX(0,amount-?1),updated_at=?2 WHERE user_id=?3",
                        params![amount,Local::now().to_rfc3339(),target]).map(|_|"➖ Запросы сняты.").unwrap_or("❌ Ошибка базы данных.")
                }
            }
            _ => "Неизвестное действие."
        };
        drop(db);
        self.send(chat,answer,Some(&[&["⬅️ Назад"]]));
        self.set_state(id,state.clone());
        true
    }

    fn handle(&self, message: &Value) {
        let user = &message["from"];
        let chat = message["chat"]["id"].as_i64().unwrap_or(0);
        let id = user["id"].as_i64().unwrap_or(0);
        if id<=0 || chat<=0 || user["is_bot"]==true || message["chat"]["type"]!="private" { return; }
        self.save_user(user);
        if self.blocked(id) { return; }
        let text = message["text"].as_str().unwrap_or("").trim();
        let mut state = self.state(id);

        if self.admin_input(id,chat,text,&mut state) { return; }
        if text=="/start" {
            self.set_state(id,UserState::default());
            let menu = if id == ADMIN_ID {
                &[&["🎲 Генерировать"][..],&["📊 Мой лимит"][..],&["👑 Купить Premium"][..],&["💬 Поддержка"][..],&["🔐 Админ-панель"][..]][..]
            } else {
                &[&["🎲 Генерировать"][..],&["📊 Мой лимит"][..],&["👑 Купить Premium"][..],&["💬 Поддержка"][..]][..]
            };
            self.send(chat,"👋 Username Generator\n\nГенерация английских username из 5 букв.\n5 вариантов за запрос.\n20 запросов в день.\n\nНажми «Генерировать».",Some(menu));
            return;
        }
        if text=="/myid" {
            self.send(chat,&format!("Твой Telegram ID: {id}"),None); return;
        }
        if id==ADMIN_ID && text=="🔐 Админ-панель" {
            state.step="admin_password".into(); self.set_state(id,state);
            self.send(chat,"Введи пароль администратора:",None); return;
        }
        if id==ADMIN_ID && !state.step.is_empty() {
            self.admin_input(id,chat,text,&mut state); return;
        }
        match text {
            "🎲 Генерировать" => match self.generate(id) {
                Ok(names) => {
                    let body = format!("🎲 ГОТОВО!\n\n{}\n\nПроверка: каждый вариант проверен через t.me.",
                        names.iter().map(|n|format!("✅ @{n} — свободен")).collect::<Vec<_>>().join("\n"));
                    self.send(chat,&body,Some(&[&["🎲 Генерировать"],&["📊 Мой лимит"],&["👑 Купить Premium"],&["💬 Поддержка"]]));
                }
                Err(e) if e=="limit" => { self.send(chat,&format!("⛔ Дневной лимит исчерпан.\n\n👑 Premium: {PREMIUM_CONTACT}"),None); }
                Err(_) => { self.send(chat,"😕 Сейчас не удалось найти свободные варианты. Попробуй ещё раз чуть позже. Лимит не списан.",None); }
            },
            "📊 Мой лимит" => { let body=self.limit_text(id); self.send(chat,&body,None); }
            "👑 Купить Premium" => {
                let body=format!("👑 PREMIUM UNLIMITED\n\nДля покупки и активации напиши {PREMIUM_CONTACT}.\nPremium снимает дневной лимит.");
                self.send(chat,&body,None);
            }
            "💬 Поддержка" => { self.send(chat,&format!("💬 Поддержка: {PREMIUM_CONTACT}\nТвой ID: {id}"),None); }
            "👑 Выдать Premium"|"❌ Снять Premium"|"🎁 Подарить запросы"|"➖ Отнять запросы"|"🚫 Заблокировать"|"🔓 Разблокировать"|"✉️ Написать пользователю" if id==ADMIN_ID => {
                state.step=match text {
                    "👑 Выдать Premium"=>"premium_on","❌ Снять Premium"=>"premium_off",
                    "🎁 Подарить запросы"=>"gift_requests","➖ Отнять запросы"=>"remove_requests",
                    "🚫 Заблокировать"=>"ban","🔓 Разблокировать"=>"unban",_=>"message_id"
                }.into();
                self.set_state(id,state);
                let prompt=if text=="✉️ Написать пользователю"{"Введи Telegram ID пользователя:"}
                    else if text=="🎁 Подарить запросы"||text=="➖ Отнять запросы"{"Введи Telegram ID и количество через пробел:"}
                    else{"Введи Telegram ID пользователя:"};
                self.send(chat,prompt,None);
            }
            "⬅️ Назад" if id==ADMIN_ID => {
                self.send(chat,"Главное меню.",Some(&[&["🎲 Генерировать"],&["📊 Мой лимит"],&["👑 Купить Premium"],&["💬 Поддержка"],&["🔐 Админ-панель"]]));
            }
            _ => {}
        }
    }
}

fn main() {
    let token=env::var("BOT_TOKEN").unwrap_or_default().trim().to_string();
    let password=env::var("ADMIN_PASSWORD").unwrap_or_default().trim().to_string();
    if token.is_empty() || !token.contains(':') { eprintln!("BOT_TOKEN не задан."); return; }
    if password.is_empty() { eprintln!("ADMIN_PASSWORD не задан."); return; }
    let app=match App::new(token,password) { Ok(a)=>Arc::new(a),Err(e)=>{eprintln!("Startup error: {e}");return;} };
    match app.api("getMe",json!({})) {
        Ok(me)=>println!("Bot connected: @{}",me["username"].as_str().unwrap_or("?")),
        Err(e)=>{eprintln!("Telegram connection error: {e}");return;}
    }
    let mut offset=0i64;
    loop {
        match app.api("getUpdates",json!({"offset":offset,"timeout":25,"allowed_updates":["message"]})) {
            Ok(updates)=>{
                if let Some(items)=updates.as_array() {
                    for update in items {
                        offset=update["update_id"].as_i64().unwrap_or(offset)+1;
                        if !update["message"].is_null() {
                            app.handle(&update["message"]);
                        }
                    }
                }
            }
            Err(e)=>{
                if e.starts_with("401|") { eprintln!("BOT_TOKEN invalid."); break; }
                if e.starts_with("429|") {
                    let wait=e.split('|').nth(1).and_then(|x|x.parse::<u64>().ok()).unwrap_or(5).min(60);
                    thread::sleep(Duration::from_secs(wait));
                } else {
                    eprintln!("Polling error: {e}");
                    thread::sleep(Duration::from_secs(3));
                }
            }
        }
    }
}
