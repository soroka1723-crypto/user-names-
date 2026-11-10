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
const MAX_CHECK_BATCHES: usize = 10;
const CHECK_TIMEOUT_SECS: u64 = 4;
const PREMIUM_CONTACT: &str = "@xldzo";

// Curated, recognizable English words only. No syllable-based pseudo-words.
const COMMON_WORDS: &[&str] = &[
    "about","above","abuse","actor","acute","admit","adopt","adult","after","again","agent","agree","ahead","alarm","album","alert","alike","alive","allow","alone","along","alter","among","angel","anger","angle","angry","ankle","apart","apple","apply","arena","argue","arise","armed","armor","aroma","arrow","aside","asset","audio","audit","avoid","awake","award","aware","awful","beach","begin","being","below","bench","berry","birth","black","blame","blind","block","blood","board","brain","brand","bread","break","brick","bride","brief","bring","broad","brown","build","built","buyer","cable","camel","candy","carry","catch","cause","chain","chair","charm","chart","chase","cheap","check","chest","chief","child","china","civil","claim","class","clean","clear","clock","close","cloud","coach","coast","color","could","count","court","cover","craft","crash","cream","crime","cross","crowd","crown","curve","daily","dance","death","delay","depth","dirty","doubt","dozen","draft","drama","dream","dress","drink","drive","earth","eight","elite","empty","enemy","enjoy","enter","entry","equal","error","event","every","exact","exist","extra","faith","false","fault","field","fifth","fifty","fight","final","first","flame","flash","floor","focus","force","forth","forty","found","frame","frank","fresh","front","fruit","funny","giant","given","glass","globe","going","grace","grade","grand","grant","grass","great","green","greet","group","guard","guess","guest","guide","habit","happy","heart","heavy","hello","hence","honey","horse","hotel","house","human","ideal","image","imply","index","inner","input","issue","joint","judge","known","label","labor","large","laser","later","laugh","layer","learn","least","leave","legal","level","light","limit","local","logic","loose","lower","lucky","lunch","magic","major","maker","march","match","maybe","mayor","meant","media","metal","might","minor","minus","model","money","month","moral","motor","mount","mouse","mouth","movie","music","needs","never","night","noise","north","noted","novel","nurse","occur","ocean","offer","often","order","other","ought","outer","owner","paint","panel","paper","party","peace","phase","phone","photo","piece","pilot","pitch","place","plain","plane","plant","plate","point","pound","power","press","price","pride","prime","print","prior","prize","proof","proud","prove","queen","quick","quiet","quite","radio","raise","range","rapid","ratio","reach","ready","refer","right","river","roast","rough","round","route","royal","rural","scale","scene","scope","score","sense","serve","seven","shall","shape","share","sharp","sheet","shelf","shell","shift","shine","shirt","shock","shoot","short","shown","sight","since","skill","sleep","slice","small","smart","smile","smoke","solid","solve","sorry","sound","south","space","spare","speak","speed","spend","spent","split","sport","staff","stage","stake","stand","start","state","steam","steel","stick","still","stock","stone","stood","store","storm","story","strip","study","style","sugar","suite","super","sweet","table","taken","taste","teach","teeth","thank","their","theme","there","these","thick","thing","think","third","those","three","threw","tight","tired","title","today","topic","total","touch","tough","tower","track","trade","train","treat","trend","trial","tried","truck","truly","trust","truth","twice","under","union","unity","until","upper","urban","usage","usual","valid","value","video","visit","vital","voice","waste","watch","water","wheel","where","which","while","white","whole","whose","woman","women","world","worry","worse","worst","worth","would","write","wrong","wrote","youth"
];
const WORD_BLOCKLIST: &[&str] = &[
    "fuck","shit","cunt","dick","cock","porn","nazi","rape","slut","whore",
    "nigg","fagg","bitch","piss","tits","penis","vagin"
];

fn word_ok(word: &str) -> bool {
    word.len() == 5
        && word.is_ascii()
        && word.bytes().all(|b| b.is_ascii_lowercase())
        && !WORD_BLOCKLIST.iter().any(|bad| word.contains(bad))
}

fn generate_word<R: rand::Rng + ?Sized>(rng: &mut R) -> String {
    COMMON_WORDS.choose(rng).copied().unwrap_or("apple").to_string()
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

    fn check_fragment(&self, name: &str) -> Option<bool> {
        let url = format!("https://fragment.com/username/{name}");
        let response = self.client.get(url).timeout(Duration::from_secs(CHECK_TIMEOUT_SECS)).send().ok()?;
        if !response.status().is_success() { return None; }
        let page = response.text().ok()?;
        static STATUS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let re = STATUS.get_or_init(|| regex::Regex::new(r#"class="[^"]*tm-section-header-status[^"]*tm-status-(\w+)"#).expect("valid Fragment regex"));
        Some(re.captures(&page).and_then(|c|c.get(1)).map(|m|matches!(m.as_str(),"avail"|"auction"|"sold")).unwrap_or(false))
    }

    fn check_name(&self, name: &str) -> &'static str {
        if !word_ok(name) { return "invalid"; }
        if self.check_tme_once(name) != "free" { return "unknown_or_taken"; }
        thread::sleep(Duration::from_millis(450));
        if self.check_tme_once(name) != "free" { return "unknown_or_taken"; }
        match self.check_fragment(name) {
            Some(true) => "taken",
            Some(false) => "free",
            None => "unknown",
        }
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
        let mut rng = rand::thread_rng();
        let mut candidates = std::collections::HashSet::new();
        for _ in 0..600 {
            let name = generate_word(&mut rng);
            if word_ok(&name) && !known.contains(&name) {
                candidates.insert(name);
            }
        }
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        candidates.shuffle(&mut rng);
        let mut free = Vec::new();
        // Work in small parallel batches; stop as soon as 3 free names are found.
        for batch in candidates.chunks(CHECK_THREADS).take(MAX_CHECK_BATCHES) {
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
