use chrono::Local;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use reqwest::blocking::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

const ADMIN_ID: i64 = 7502792642;
const DAILY_REQUEST_LIMIT: i64 = 20;
const NAMES_PER_REQUEST: usize = 3;
const CHECK_THREADS: usize = 12;
const CHECK_TIMEOUT_SECS: u64 = 4;
const PREMIUM_CONTACT: &str = "@xldzo";

const WORDS: &[&str] = &[
    "amber","angel","apple","arbor","argue","arise","aside","audio","awake","baker",
    "basic","beach","begin","below","berry","birth","black","blaze","bloom","board",
    "brave","bread","brick","bring","broad","brown","brush","cabin","cable","camel",
    "candy","cargo","cause","chain","chair","charm","chase","cheer","chess","chief",
    "child","chime","civic","claim","clean","clear","clerk","cliff","climb","clock",
    "cloud","coach","coast","color","coral","couch","cover","craft","crane","crash",
    "cream","crime","cross","crowd","crown","dance","delta","demon","depth","diary",
    "dream","dress","drift","eager","eagle","early","earth","eight","elite","empty",
    "enemy","enjoy","enter","entry","equal","error","event","every","faith","fancy",
    "feast","fence","fever","field","final","flame","flash","fleet","floor","focus",
    "force","frame","fresh","front","fruit","giant","given","glass","globe","grace",
    "grade","grain","grand","grant","grass","great","green","group","guard","guess",
    "guest","guide","habit","happy","heart","honey","honor","horse","hotel","house",
    "human","humor","ideal","image","index","inner","input","ivory","jelly","jewel",
    "joint","judge","juice","karma","knife","known","label","laser","later","laugh",
    "layer","learn","lemon","level","light","limit","linen","liver","local","logic",
    "magic","major","maple","march","match","maybe","medal","media","mercy","metal",
    "micro","might","minor","model","money","month","moral","motor","mount","mouse",
    "music","nerve","never","night","noble","noise","north","novel","ocean","offer",
    "often","olive","omega","onion","orbit","order","other","outer","owner","paint",
    "panel","paper","party","peace","pearl","phase","phone","photo","piano","piece",
    "pilot","pixel","place","plain","plane","plant","plate","point","power","press",
    "price","pride","prime","print","prior","prize","proof","proud","queen","quick",
    "quiet","radio","raise","range","rapid","ratio","reach","react","ready","realm",
    "rebel","refer","relax","reply","rider","river","robot","rocky","rough","round",
    "royal","ruler","rural","saint","scale","scene","scope","score","sense","serve",
    "seven","shade","shake","shall","shape","share","sharp","sheep","sheet","shelf",
    "shell","shift","shine","shirt","shock","shore","short","shown","sight","since",
    "skill","sleep","slice","slide","small","smart","smile","smoke","solid","solve",
    "sound","south","space","spare","speak","speed","spice","spike","spirit","split",
    "sport","staff","stage","stand","start","state","steam","steel","stick","still",
    "stone","store","storm","story","style","sugar","sunny","super","sweet","swift",
    "table","taste","teach","thank","theme","there","thick","thing","think","third",
    "thorn","three","throw","tight","timer","title","today","token","topic","total",
    "touch","tower","trace","track","trade","train","treat","trend","trial","trust",
    "truth","under","union","unity","until","upper","upset","urban","usage","usual",
    "valid","value","video","visit","vital","voice","waste","watch","water","wheel",
    "where","which","while","white","whole","woman","world","worry","worth","would",
    "wrist","write","wrong","young","youth","zebra"
];

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

    // One Telegram page request per candidate. No second pass and no Fragment request.
    fn check_name(&self, name: &str) -> &'static str {
        let url = format!("https://t.me/{name}");
        let page = match self.client.get(url).timeout(Duration::from_secs(CHECK_TIMEOUT_SECS)).send() {
            Ok(r) => match r.text() { Ok(v) => v, Err(_) => return "unknown" },
            Err(_) => return "unknown",
        };
        let lower = page.to_ascii_lowercase();
        if lower.contains("tgme_page_extra") || lower.contains("tgme_page_photo") {
            return "taken";
        }
        static META: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let re = META.get_or_init(|| regex::Regex::new(r#"(?i)<meta\s+property="og:title"\s+content="([^"]*)""#).expect("valid meta regex"));
        if let Some(capture) = re.captures(&page) {
            let title = capture.get(1).map(|m|m.as_str()).unwrap_or("").to_ascii_lowercase();
            if title == format!("telegram: contact @{name}") { return "free"; }
            if title.starts_with("telegram: contact @") { return "unknown"; }
            if !title.is_empty() { return "taken"; }
        }
        "unknown"
    }

    fn generate(&self, id: i64) -> Result<Vec<String>, String> {
        let premium = self.premium(id);
        let used = self.request_count(id);
        let bonus = self.bonus_count(id);
        if !premium && used >= DAILY_REQUEST_LIMIT && bonus <= 0 {
            return Err("limit".into());
        }

        let mut candidates = WORDS.iter().map(|s|s.to_string()).collect::<Vec<_>>();
        candidates.shuffle(&mut rand::thread_rng());
        let known = {
            let db = self.db.lock().unwrap();
            let mut stmt = db.prepare("SELECT name FROM generated").map_err(|_|"db")?;
            let rows = stmt.query_map([],|r|r.get::<_,String>(0)).map_err(|_|"db")?;
            let values = rows.collect::<rusqlite::Result<std::collections::HashSet<_>>>().map_err(|_|"db")?;
            values
        };
        candidates.retain(|n| n.len()==5 && n.bytes().all(|b|b.is_ascii_lowercase()) && !known.contains(n));
        let mut free = Vec::new();
        // Work in small parallel batches; stop as soon as 3 free names are found.
        for batch in candidates.chunks(CHECK_THREADS) {
            let statuses: Vec<(String,&'static str)> = self.pool.install(|| {
                batch.par_iter().map(|n|(n.clone(),self.check_name(n))).collect()
            });
            for (name,status) in statuses {
                if status == "free" {
                    free.push(name);
                    if free.len() >= NAMES_PER_REQUEST { break; }
                }
            }
            if free.len() >= NAMES_PER_REQUEST { break; }
        }
        if free.is_empty() { return Err("none".into()); }

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
            self.send(chat,"👋 Username Generator\n\nГенерация английских username из 5 букв.\n3 варианта за запрос.\n20 запросов в день.\n\nНажми «Генерировать».",Some(menu));
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
