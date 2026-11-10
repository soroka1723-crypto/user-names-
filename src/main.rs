use chrono::{Local, NaiveDateTime, Duration as ChronoDuration, TimeZone};
use rand::{seq::SliceRandom, Rng};
use rayon::prelude::*;
use reqwest::blocking::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::{collections::{HashMap, HashSet}, env, fs::OpenOptions, io::Write, sync::{Arc, Mutex, OnceLock}, thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

const ADMIN_ID: i64 = 7502792642;
const DB_FILE: &str = "username_data.db";
const CHANNEL: &str = "@mynoretresnews";
const CHANNEL_URL: &str = "https://t.me/mynoretresnews";
const PREMIUM_CONTACT: &str = "@xldzo";
const GENERATION_SIZE: usize = 3;
const DAILY_REQUEST_LIMIT: i64 = 10;
const DAILY_LIMIT: i64 = (GENERATION_SIZE as i64) * DAILY_REQUEST_LIMIT;
const MESSAGE_LIMIT: usize = 3800;
const PREMIUM_GIFT_EMOJI: &str = "🧸";
const PREMIUM_GIFT_STARS: i64 = 15;
const FLOOD_LIMIT: usize = 5;
const FLOOD_WINDOW: f64 = 3.0;
const CHECK_THREADS: usize = 6;
const CHECK_TIMEOUT: u64 = 8;
const CHECK_TIME_BUDGET: u64 = 20;
const CHECK_RECHECK_DELAY_MS: u64 = 500;
const SUBSCRIPTION_CACHE_SECONDS: u64 = 300;
const USERS_PAGE_SIZE: i64 = 20;
const GIFT_REQUESTS_MAX: i64 = 100;
const CHECK_FAVORITES_LIMIT: usize = 30;
const LETTERS: &str = "abcdefghijklmnopqrstuvwxyz";
const ALL_CHARS: &str = "abcdefghijklmnopqrstuvwxyz0123456789";
const WORD_MIN_LENGTH: usize = 5;
const WORD_MAX_LENGTH: usize = 5;

#[derive(Clone)]
struct UserState {
    mode: String,
    smart: bool,
    step: Option<String>,
    admin: bool,
    target_id: Option<i64>,
    pending: Option<Value>,
    users_offset: i64,
    temp: Option<Value>,
    last_check: Option<Value>,
}
impl Default for UserState {
    fn default() -> Self { Self { mode: "words".into(), smart: true, step: None, admin: false, target_id: None, pending: None, users_offset: 0, temp: None, last_check: None } }
}

struct App {
    token: String,
    admin_password: String,
    client: Client,
    check_pool: rayon::ThreadPool,
    db: Mutex<Connection>,
    states: Mutex<HashMap<i64, UserState>>,
    last_lists: Mutex<HashMap<i64, Vec<String>>>,
    subscription_cache: Mutex<HashMap<i64, Instant>>,
    flood_events: Mutex<HashMap<i64, Vec<f64>>>,
    check_cooldown: Mutex<Option<Instant>>,
}

fn now_iso() -> String { Local::now().to_rfc3339() }
fn today() -> String { Local::now().format("%Y-%m-%d").to_string() }
fn fmt_dt(value:&str)->String{if value.is_empty(){return "—".into();}if let Ok(d)=chrono::DateTime::parse_from_rfc3339(value){return d.with_timezone(&Local).format("%d.%m.%Y %H:%M").to_string();}for pattern in ["%Y-%m-%dT%H:%M:%S%.f","%Y-%m-%d %H:%M:%S"]{if let Ok(d)=NaiveDateTime::parse_from_str(value,pattern){return d.format("%d.%m.%Y %H:%M").to_string();}}value.to_string()}
fn fmt_ts(value:f64)->String{if value<=0.0{return "—".into();}Local.timestamp_opt(value as i64,0).single().map(|d|d.format("%d.%m.%Y %H:%M").to_string()).unwrap_or_else(||"—".into())}
fn epoch() -> f64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64() }
fn log_line(level: &str, msg: &str) {
    let secret=env::var("BOT_TOKEN").unwrap_or_default();
    let safe=if secret.is_empty(){msg.to_string()}else{msg.replace(&secret,"***")};
    let line = format!("{} | {} | {}\n", now_iso(), level, safe);
    eprint!("{}", line);
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open("username_generator.log") { let _ = f.write_all(line.as_bytes()); }
}
fn db_init() -> rusqlite::Result<Connection> {
    let db = Connection::open(DB_FILE)?;
    db.busy_timeout(Duration::from_secs(30))?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=30000;
    CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT, blocked INTEGER DEFAULT 0, joined_at TEXT, blocked_until REAL DEFAULT 0, block_reason TEXT DEFAULT '', last_active TEXT, auto_banned INTEGER DEFAULT 0, banned_at TEXT);
    CREATE TABLE IF NOT EXISTS generated(name TEXT PRIMARY KEY, mode TEXT, created_at TEXT);
    CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,name TEXT,created_at TEXT);
    CREATE TABLE IF NOT EXISTS favorites(user_id INTEGER,name TEXT,added_at TEXT,PRIMARY KEY(user_id,name));
    CREATE TABLE IF NOT EXISTS daily_usage(user_id INTEGER,usage_date TEXT,amount INTEGER DEFAULT 0,PRIMARY KEY(user_id,usage_date));
    CREATE TABLE IF NOT EXISTS daily_requests(user_id INTEGER,usage_date TEXT,amount INTEGER DEFAULT 0,PRIMARY KEY(user_id,usage_date));
    CREATE TABLE IF NOT EXISTS premium_users(user_id INTEGER PRIMARY KEY,activated_at TEXT,activated_by INTEGER);
    CREATE TABLE IF NOT EXISTS ignore_list(user_id INTEGER PRIMARY KEY,added_at TEXT,added_by INTEGER,reason TEXT);
    CREATE TABLE IF NOT EXISTS spam_events(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,event_time REAL,message_hash TEXT);
    CREATE TABLE IF NOT EXISTS moderation_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,action TEXT,reason TEXT,created_at TEXT);
    CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY,role TEXT NOT NULL DEFAULT 'second',perms TEXT DEFAULT '',added_at TEXT,added_by INTEGER);
    CREATE TABLE IF NOT EXISTS activity_log(user_id INTEGER,day TEXT,PRIMARY KEY(user_id,day));
    CREATE TABLE IF NOT EXISTS generation_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,gen_date TEXT,amount INTEGER DEFAULT 0,created_at TEXT);
    CREATE TABLE IF NOT EXISTS gift_payments(gift_key TEXT PRIMARY KEY,user_id INTEGER,gift_id TEXT,stars INTEGER,created_at TEXT);
    CREATE TABLE IF NOT EXISTS bonus_requests(user_id INTEGER PRIMARY KEY,amount INTEGER DEFAULT 0,updated_at TEXT);
    ")?;
    // Keep migrations additive: never drop or clear user data.
    let migrations: &[(&str, &str, &str)] = &[
        ("users","first_name","TEXT"),("users","username","TEXT"),("users","blocked","INTEGER DEFAULT 0"),("users","joined_at","TEXT"),("users","blocked_until","REAL DEFAULT 0"),("users","block_reason","TEXT DEFAULT ''"),("users","last_active","TEXT"),("users","auto_banned","INTEGER DEFAULT 0"),("users","banned_at","TEXT"),
        ("ignore_list","added_at","TEXT"),("ignore_list","added_by","INTEGER"),("ignore_list","reason","TEXT"),
        ("moderation_log","user_id","INTEGER"),("moderation_log","action","TEXT"),("moderation_log","reason","TEXT"),("moderation_log","created_at","TEXT"),
        ("generated","mode","TEXT"),("generated","created_at","TEXT"),
        ("history","user_id","INTEGER"),("history","name","TEXT"),("history","created_at","TEXT"),
        ("favorites","user_id","INTEGER"),("favorites","name","TEXT"),("favorites","added_at","TEXT"),
        ("daily_usage","user_id","INTEGER"),("daily_usage","usage_date","TEXT"),("daily_usage","amount","INTEGER DEFAULT 0"),
        ("daily_requests","user_id","INTEGER"),("daily_requests","usage_date","TEXT"),("daily_requests","amount","INTEGER DEFAULT 0"),
        ("premium_users","activated_at","TEXT"),("premium_users","activated_by","INTEGER"),
        ("spam_events","user_id","INTEGER"),("spam_events","event_time","REAL"),("spam_events","message_hash","TEXT"),
        ("admins","role","TEXT NOT NULL DEFAULT 'second'"),("admins","perms","TEXT DEFAULT ''"),("admins","added_at","TEXT"),("admins","added_by","INTEGER"),
        ("activity_log","user_id","INTEGER"),("activity_log","day","TEXT"),
        ("generation_log","user_id","INTEGER"),("generation_log","gen_date","TEXT"),("generation_log","amount","INTEGER DEFAULT 0"),("generation_log","created_at","TEXT"),
        ("gift_payments","user_id","INTEGER"),("gift_payments","gift_id","TEXT"),("gift_payments","stars","INTEGER"),("gift_payments","created_at","TEXT"),
        ("bonus_requests","amount","INTEGER DEFAULT 0"),("bonus_requests","updated_at","TEXT")];
    for (table,col,def) in migrations.iter().copied() {
        let mut st=db.prepare(&format!("PRAGMA table_info({table})"))?;
        let cols=st.query_map([],|r|r.get::<_,String>(1))?.collect::<rusqlite::Result<Vec<_>>>()?;
        if !cols.iter().any(|c|c==col) { db.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {col} {def}"))?; }
    }
    db.execute_batch("CREATE INDEX IF NOT EXISTS idx_history_user ON history(user_id,id DESC); CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id,added_at DESC); CREATE INDEX IF NOT EXISTS idx_users_blocked ON users(blocked); CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_usage(usage_date); CREATE INDEX IF NOT EXISTS idx_requests_date ON daily_requests(usage_date); CREATE INDEX IF NOT EXISTS idx_spam_user_time ON spam_events(user_id,event_time); CREATE INDEX IF NOT EXISTS idx_generation_log_date ON generation_log(gen_date); CREATE INDEX IF NOT EXISTS idx_users_joined ON users(joined_at DESC,user_id DESC); CREATE INDEX IF NOT EXISTS idx_moderation_log_id ON moderation_log(id DESC); CREATE INDEX IF NOT EXISTS idx_activity_day ON activity_log(day,user_id); CREATE INDEX IF NOT EXISTS idx_generation_user_date ON generation_log(user_id,gen_date);")?;
    Ok(db)
}

impl App {
    fn new(token: String, password: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client=Client::builder().timeout(Duration::from_secs(45)).user_agent("UsernameGeneratorBot/1.0").build()?;
        let check_pool=rayon::ThreadPoolBuilder::new().num_threads(CHECK_THREADS).thread_name(|i|format!("username-check-{i}")).build()?;
        Ok(Self { token, admin_password: password, client, check_pool, db: Mutex::new(db_init()?), states: Mutex::new(HashMap::new()), last_lists: Mutex::new(HashMap::new()), subscription_cache: Mutex::new(HashMap::new()), flood_events: Mutex::new(HashMap::new()), check_cooldown: Mutex::new(None) })
    }
    fn api(&self, method: &str, data: Value) -> Result<Value, String> {
        let url=format!("https://api.telegram.org/bot{}/{}",self.token,method);
        let response=self.client.post(url).json(&data).timeout(Duration::from_secs(45)).send().map_err(|e|e.to_string().replace(&self.token,"***"))?;
        let status=response.status();
        let body:Value=response.json().map_err(|e|e.to_string().replace(&self.token,"***"))?;
        if !status.is_success() || body["ok"]!=true {
            let code=body["error_code"].as_i64().unwrap_or(status.as_u16() as i64);
            let desc=body["description"].as_str().unwrap_or("Telegram API error");
            let retry=body["parameters"]["retry_after"].as_i64().unwrap_or(0);
            return Err(format!("{code}|{retry}|{desc}"));
        }
        Ok(body["result"].clone())
    }
    fn send_raw(&self, chat_id:i64, text:&str, markup:Option<Value>, business:Option<&str>) -> bool {
        let chunks=split_message(text,MESSAGE_LIMIT);
        let mut ok=true;
        for (i,part) in chunks.iter().enumerate() {
            let mut data=json!({"chat_id":chat_id,"text":part});
            if i+1==chunks.len() { if let Some(m)=&markup { data["reply_markup"]=m.clone(); } }
            if let Some(c)=business { data["business_connection_id"]=json!(c); }
            let mut sent=false;
            for _ in 0..3 {
                match self.api("sendMessage",data.clone()) {
                    Ok(_)=>{sent=true;break;},
                    Err(e)=>{ let p=e.splitn(3,'|').collect::<Vec<_>>(); let code=*p.first().unwrap_or(&""); if code=="429" { let wait=p.get(1).and_then(|x|x.parse::<u64>().ok()).unwrap_or(1).min(30); thread::sleep(Duration::from_secs(wait)+Duration::from_millis(500)); continue; } if code.parse::<i64>().is_err(){log_line("WARN",&format!("Network error while sending to {chat_id}: {e}"));thread::sleep(Duration::from_millis(1500));continue;}log_line("WARN",&format!("Telegram sendMessage failed chat={chat_id}: {e}"));break; }
                }
            }
            if !sent { ok=false; }
        }
        ok
    }
    fn send(&self,chat:i64,text:&str,kb:Option<Vec<Vec<String>>>) -> bool {
        let markup=kb.map(|keyboard|json!({"keyboard":keyboard,"resize_keyboard":true}));
        self.send_raw(chat,text,markup,None)
    }
    fn state(&self,id:i64)->UserState { self.states.lock().unwrap().entry(id).or_default().clone() }
    fn put_state(&self,id:i64,s:UserState) { self.states.lock().unwrap().insert(id,s); }
    fn role_checked(&self,id:i64)->Result<Option<String>,()> {
        if id==ADMIN_ID {return Ok(Some("main".into()));}
        let db=self.db.lock().map_err(|_|())?;
        db.query_row("SELECT role FROM admins WHERE user_id=?1",[id],|r|r.get(0)).optional().map_err(|_|())
    }
    fn role(&self,id:i64)->Option<String>{self.role_checked(id).ok().flatten()}
    fn is_admin(&self,id:i64)->bool { id==ADMIN_ID }
    fn is_premium(&self,id:i64)->bool {
        if id==ADMIN_ID {return true;}
        self.db.lock().ok().and_then(|db|db.query_row("SELECT 1 FROM premium_users WHERE user_id=?1",[id],|r|r.get::<_,i64>(0)).optional().ok().flatten()).is_some()
    }
    fn moderation(&self,id:i64,action:&str,reason:&str) {
        if let Ok(db)=self.db.lock(){let _=db.execute("INSERT INTO moderation_log(user_id,action,reason,created_at) VALUES(?1,?2,?3,?4)",params![id,action,reason,now_iso()]);}
    }
    fn save_user(&self,user:&Value) {
        let id=user["id"].as_i64().unwrap_or(0); if id<=0{return;}
        let first=user["first_name"].as_str().unwrap_or(""); let uname=user["username"].as_str().unwrap_or(""); let now=now_iso();
                if let Ok(db)=self.db.lock(){ let _=db.execute("INSERT INTO users(user_id,first_name,username,joined_at,last_active) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name,username=excluded.username,last_active=excluded.last_active",params![id,first,uname,now,now]); let _=db.execute("INSERT OR IGNORE INTO activity_log(user_id,day) VALUES(?1,?2)",params![id,today()]); }
    }
    fn is_silenced(&self,id:i64)->bool {
        let Ok(db)=self.db.lock() else{return false};
        let row=db.query_row("SELECT blocked,blocked_until FROM users WHERE user_id=?1",[id],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,f64>(1)?))).optional().ok().flatten();
        if let Some((blocked,until))=row { if blocked!=0{return true;} if until>epoch(){return true;} if until>0.0 {let _=db.execute("UPDATE users SET blocked_until=0,block_reason='' WHERE user_id=?1",[id]);} }
        db.query_row("SELECT 1 FROM ignore_list WHERE user_id=?1",[id],|r|r.get::<_,i64>(0)).optional().ok().flatten().is_some()
    }
    fn autoban(&self,user:&Value,count:usize)->bool {
        let id=user["id"].as_i64().unwrap_or(0);
        match self.role_checked(id){Ok(Some(_))|Err(_)=>return false,Ok(None)=>{}}
        let now=now_iso(); let reason=format!("Флуд: {count} сообщений за {} сек. (лимит {})",FLOOD_WINDOW as i64,FLOOD_LIMIT);
        let committed=if let Ok(mut db)=self.db.lock(){
            let result=(||->rusqlite::Result<bool>{db.execute_batch("BEGIN IMMEDIATE")?;let changed=db.execute("UPDATE users SET blocked=1,auto_banned=1,blocked_until=0,block_reason=?1,banned_at=?2 WHERE user_id=?3",params![reason,now,id])?;if changed==0{db.execute_batch("ROLLBACK")?;return Ok(false);}db.execute("DELETE FROM spam_events WHERE user_id=?1",[id])?;db.execute("INSERT INTO moderation_log(user_id,action,reason,created_at) VALUES(?1,'AUTO_BAN_FLOOD',?2,?3)",params![id,reason,now_iso()])?;db.execute_batch("COMMIT")?;Ok(true)})();match result{Ok(v)=>v,Err(e)=>{let _=db.execute_batch("ROLLBACK");log_line("ERROR",&format!("autoban database transaction failed; no ban: {e}"));false}}
        }else{false};
        if !committed{return false;}
        self.flood_events.lock().unwrap().remove(&id);
        self.states.lock().unwrap().remove(&id);
        self.last_lists.lock().unwrap().remove(&id);
        let first=user["first_name"].as_str().unwrap_or("Без имени"); let uname=user["username"].as_str().unwrap_or("нет");
        self.send(ADMIN_ID,&format!("🚨 АВТОБАН\n━━━━━━━━━━━━━━━━━━\n🆔 Telegram ID: {id}\n👤 Имя: {first}\n🔗 Username: @{uname}\n📌 Причина: {reason}\n🗂 Тип нарушения: Флуд\n🕒 Дата и время: {}\n━━━━━━━━━━━━━━━━━━\n🔒 Бан постоянный. Бот молча игнорирует пользователя.\nСнять: Админ-панель → 🔓 Разблокировать.",now_iso()),None);
        log_line("WARN",&format!("AUTO BAN user={id} reason={reason}")); true
    }
    fn check_antispam(&self,user:&Value,msg_time:Option<f64>)->bool {
        let id=user["id"].as_i64().unwrap_or(0);
        match self.role_checked(id){Ok(Some(_))|Err(_)=>return true,Ok(None)=>{}}
        let t=msg_time.unwrap_or_else(epoch);
        let count={let mut map=self.flood_events.lock().unwrap();record_flood(&mut map,id,t)};
        if count>=FLOOD_LIMIT {return !self.autoban(user,count);} true
    }
    fn has_perm(&self,id:i64,perm:&str)->bool {if id==ADMIN_ID{return true;}if perm=="main"{return false;}self.db.lock().ok().and_then(|db|db.query_row("SELECT perms FROM admins WHERE user_id=?1 AND role='second'",[id],|r|r.get::<_,String>(0)).optional().ok().flatten()).map(|p|p.split(',').any(|x|x==perm)).unwrap_or(false)}
    fn premium_activate(&self,id:i64,by:i64) { if let Ok(db)=self.db.lock(){let _=db.execute("INSERT OR REPLACE INTO premium_users(user_id,activated_at,activated_by) VALUES(?1,?2,?3)",params![id,now_iso(),by]);} }
    fn is_subscribed(&self,id:i64,force:bool)->bool {
        if !force { if self.subscription_cache.lock().unwrap().get(&id).map(|t|t.elapsed()<Duration::from_secs(SUBSCRIPTION_CACHE_SECONDS)).unwrap_or(false){return true;} }
        match self.api("getChatMember",json!({"chat_id":CHANNEL,"user_id":id})) { Ok(m)=>{let s=m["status"].as_str().unwrap_or("");let yes=matches!(s,"creator"|"administrator"|"member")||(s=="restricted"&&m["is_member"]==true);if yes{self.subscription_cache.lock().unwrap().insert(id,Instant::now());}else{self.subscription_cache.lock().unwrap().remove(&id);}yes},Err(e)=>{log_line("WARN",&format!("subscription check failed user={id}: {e}"));false} }
    }
}

fn valid_username(name:&str)->bool{name.len()==5&&name.as_bytes()[0].is_ascii_lowercase()&&name.bytes().all(|c|c.is_ascii_lowercase()||c.is_ascii_digit())}
fn record_flood(events:&mut HashMap<i64,Vec<f64>>,id:i64,t:f64)->usize{let list=events.entry(id).or_default();list.push(t);list.retain(|x|*x>t-FLOOD_WINDOW);list.len()}
fn split_message(text:&str,limit:usize)->Vec<String>{
    let mut out=Vec::new(); let mut cur=String::new();
    for raw in text.split('\n') { let mut line=raw.to_string(); while line.chars().count()>limit { if !cur.is_empty(){out.push(std::mem::take(&mut cur));} let part:String=line.chars().take(limit).collect(); out.push(part); line=line.chars().skip(limit).collect(); } let cand=if cur.is_empty(){line.clone()}else{format!("{cur}\n{line}")}; if cand.chars().count()>limit {if !cur.is_empty(){out.push(std::mem::take(&mut cur));}cur=line;}else{cur=cand;} }
    if !cur.is_empty(){out.push(cur);} if out.is_empty(){out.push(String::new());} out
}

fn word_blocked(w:&str)->bool { ["fuck","shit","cunt","dick","cock","porn","nazi","rape","slut","whore","nigg","fagg","bitch","piss","tits","penis","vagin"].iter().any(|x|w.contains(x)) }
fn pseudo_word()->String {
    const ON:&[&str]= &["b","d","f","g","h","k","l","m","n","p","r","s","t","v","z","x","j","br","dr","fr","gr","kr","pr","tr","vr","zl","vl","kl","pl","sl","st","sk","sv","th","sh"];
    const VOW:&[&str]= &["a","e","i","o","u","y","a","e","i","o"];
    const MID:&[&str]= &["l","n","r","s","v","x","z","k","m"];
    const END:&[&str]= &["ix","ex","ox","yx","on","ora","ara","ina","elo","aro","eon","ith","ar","el","is","um","ia","ya","os","or","en","an","yr","ax","ul","ev"];
    let mut rng=rand::thread_rng();
    let mut w=format!("{}{}{}{}",ON.choose(&mut rng).unwrap(),VOW.choose(&mut rng).unwrap(),MID.choose(&mut rng).unwrap(),END.choose(&mut rng).unwrap()).to_lowercase();
    w.truncate(5); w
}
fn generate_word()->String {
    const RARE:&[&str]= &["azure","cipher","cobalt","ember","fathom","idyll","lumen","lucent","mirage","nebula","nimbus","onyx","quill","sable","serene","solace","sylph","umbra","vesper","vortex","zephyr","abyss","aurora","basalt","cinder","eclipse","fable","glacier","kismet","mystic","relic","saga","talon","tundra","utopia","vertex","vigil","willow","xenon","yonder","ash","bolt","cold","dark","dawn","dust","echo","fire","gold","grim","hawk","iron","jade","lone","moon","nova","pale","pine","raven","rift","rose","ruby","sage","shade","silk","snow","solar","star","steel","stone","storm","swift","thorn","void","wolf","wild","zero","flux","haze","mist","neon","opal","pulse","rain","rust","tide","vale","wisp","wren","zinc","crow","fox","owl","lynx"];
    const THREE:&[&str]= &["ash","fox","owl","ice","oak","elm","ivy","ray","fog","jet","gem","ore","orb","arc","bay","dew","fin","hex","ink","jay","key","lux","nix","pax","rue","sol","tor","vex","yew","zen","axe","bud","cog","eve"];
    let mut rng=rand::thread_rng();
    for _ in 0..300 {
        let roll=rng.gen::<f64>();
        let w=if roll<0.80{pseudo_word()}else if roll<0.90{let a=THREE.choose(&mut rng).unwrap();let b=THREE.choose(&mut rng).unwrap();if rng.gen_bool(0.5){format!("{}{}",&a[..2],b)}else{format!("{}{}",a,&b[..2])}}else{RARE.choose(&mut rng).unwrap().to_string()};
        if w.len()>=WORD_MIN_LENGTH && w.len()<=WORD_MAX_LENGTH && w.is_ascii() && w.bytes().all(|c|c.is_ascii_lowercase()) && w.chars().any(|c|"aeiouy".contains(c)) && !word_blocked(&w) && !(0..w.len().saturating_sub(2)).any(|i|w.as_bytes()[i]==w.as_bytes()[i+1]&&w.as_bytes()[i]==w.as_bytes()[i+2]) {return w;}
    }
    "zyrve".to_string()
}
fn generate_one(mode:&str)->String{if mode=="words"{return generate_word();}let chars=if mode=="letters"{LETTERS}else{ALL_CHARS};let mut rng=rand::thread_rng();let mut out=String::with_capacity(5);out.push(*LETTERS.as_bytes().choose(&mut rng).unwrap() as char);for _ in 0..4{out.push(*chars.as_bytes().choose(&mut rng).unwrap() as char);}out}
fn is_simple(name:&str)->bool {
    let unique=name.chars().collect::<HashSet<_>>().len(); if unique<4{return true;}
    for c in name.chars().collect::<HashSet<_>>() {if name.matches(c).count()>2{return true;}}
    let b=name.as_bytes();
    for i in 0..b.len().saturating_sub(1){if b[i]==b[i+1]{return true;}}
    for i in 0..b.len().saturating_sub(2){if (b[i+1]==b[i]+1&&b[i+2]==b[i+1]+1)||(b[i+1]+1==b[i]&&b[i+2]+1==b[i+1]){return true;}}
    false
}
fn decode_title(s:&str)->String { s.replace("&amp;","&").replace("&quot;","\"").replace("&#39;","'").replace("&lt;","<").replace("&gt;",">") }
impl App {
    fn fetch_page(&self,url:&str)->Result<String,String>{self.client.get(url).timeout(Duration::from_secs(CHECK_TIMEOUT)).header("User-Agent","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36").header("Accept-Language","en-US,en;q=0.9").send().map_err(|e|e.to_string())?.text().map_err(|e|e.to_string())}
    fn check_tme(&self,name:&str)->&'static str {
        let wait={let c=self.check_cooldown.lock().unwrap();c.as_ref().map(|t|t.saturating_duration_since(Instant::now())).unwrap_or_default()}; if !wait.is_zero(){thread::sleep(wait.min(Duration::from_secs(5)));}
        let page=match self.fetch_page(&format!("https://t.me/{name}")){Ok(x)=>x,Err(e)=>{if e.contains("429"){*self.check_cooldown.lock().unwrap()=Some(Instant::now()+Duration::from_secs(3));}return "unknown";}};
        let lower=page.to_lowercase(); if lower.contains("tgme_page_extra")||lower.contains("tgme_page_photo"){return "taken";}
        static META_RE:OnceLock<regex::Regex>=OnceLock::new();let re=META_RE.get_or_init(||regex::Regex::new(r#"(?i)<meta\s+property="og:title"\s+content="([^"]*)""#).expect("valid Telegram meta regex"));
        if let Some(c)=re.captures(&page){let title=decode_title(c.get(1).unwrap().as_str()).trim().to_lowercase();if title==format!("telegram: contact @{name}").to_lowercase(){return "free";}if title.starts_with("telegram: contact @"){return "unknown";}if !title.is_empty(){return "taken";}}
        "unknown"
    }
    fn check_fragment(&self,name:&str)->bool {
        let Ok(page)=self.fetch_page(&format!("https://fragment.com/username/{name}")) else{return false;};
        static FRAGMENT_RE:OnceLock<regex::Regex>=OnceLock::new();let re=FRAGMENT_RE.get_or_init(||regex::Regex::new(r#"class="[^"]*tm-section-header-status[^"]*tm-status-(\w+)"#).expect("valid Fragment status regex"));
                re.captures(&page).map(|c|matches!(c.get(1).map(|m|m.as_str()).unwrap_or(""),"avail"|"auction"|"sold")).unwrap_or(false)
    }
    fn check_name(&self,name:&str)->&'static str {
        let first=self.check_tme(name); if first!="free" {return first;}
        thread::sleep(Duration::from_millis(CHECK_RECHECK_DELAY_MS));
        let second=self.check_tme(name); if second!="free" {return second;}
        if self.check_fragment(name){return "taken";} "free"
    }
    fn find_free_names(&self,mode:&str,smart:bool,target:usize)->(Vec<String>,Value){
        let deadline=Instant::now()+Duration::from_secs(CHECK_TIME_BUDGET); let mut result=Vec::new(); let mut seen=HashSet::new(); let mut checked=0;let mut taken=0;let mut unknown=0;let mut bad_batches=0;let mut network_down=false;
        while result.len()<target && Instant::now()<deadline {
            let mut batch=Vec::new();let mut guard=0;
            while batch.len()<CHECK_THREADS*2&&guard<500 {guard+=1;let n=generate_one(mode);if !valid_username(&n)||(mode!="letters_digits"&&n.bytes().any(|c|c.is_ascii_digit()))||word_blocked(&n)||(smart&&mode!="words"&&is_simple(&n))||!seen.insert(n.clone()){continue;}let exists=self.db.lock().ok().and_then(|db|db.query_row("SELECT 1 FROM generated WHERE name=?1",[&n],|r|r.get::<_,i64>(0)).optional().ok().flatten()).is_some();if !exists{batch.push(n);}}
            if batch.is_empty(){break;}
            let outcomes:Vec<(&String,&'static str)>=self.check_pool.install(||batch.par_iter().map(|n|(n,self.check_name(n))).collect());let mut unknown_batch=0;
            for (n,status) in outcomes {checked+=1;match status{"free"=>{if result.len()<target{result.push(n.clone());}},"taken"=>taken+=1,_=>{unknown+=1;unknown_batch+=1;}}}
            if unknown_batch==batch.len(){bad_batches+=1;}else{bad_batches=0;}if bad_batches>=2{network_down=true;break;}
        }
        (result,json!({"checked":checked,"taken":taken,"unknown":unknown,"network_down":network_down}))
    }
    fn generate_names(&self,user_id:i64,mode:&str,smart:bool)->Result<(Vec<String>,Value),String>{
        let premium=self.is_premium(user_id);let date=today();let mut use_bonus=false;let mut wanted=GENERATION_SIZE;
        if !premium {
            let db=self.db.lock().map_err(|_|"error".to_string())?;
            let req:i64=db.query_row("SELECT amount FROM daily_requests WHERE user_id=?1 AND usage_date=?2",params![user_id,date],|r|r.get(0)).optional().map_err(|_|"error")?.unwrap_or(0);
            let used:i64=db.query_row("SELECT amount FROM daily_usage WHERE user_id=?1 AND usage_date=?2",params![user_id,date],|r|r.get(0)).optional().map_err(|_|"error")?.unwrap_or(0);
            let bonus:i64=db.query_row("SELECT amount FROM bonus_requests WHERE user_id=?1",[user_id],|r|r.get(0)).optional().map_err(|_|"error")?.unwrap_or(0);
            if req>=DAILY_REQUEST_LIMIT||used>=DAILY_LIMIT {if bonus>0{use_bonus=true;}else{return Err(if req>=DAILY_REQUEST_LIMIT{"requests"}else{"names"}.into());}}
        }
        let (verified,stats)=self.find_free_names(mode,smart,wanted);
        if verified.is_empty(){return Err(if stats["network_down"]==true{"network".into()}else{format!("nofree:{}",stats["checked"].as_i64().unwrap_or(0))});}
        let mut db=self.db.lock().map_err(|_|"error".to_string())?;
        db.execute_batch("BEGIN IMMEDIATE").map_err(|_|"error".to_string())?;
        let outcome=(||->rusqlite::Result<Vec<String>>{
            let mut target=wanted as i64;
            if !premium&&!use_bonus {let req:i64=db.query_row("SELECT amount FROM daily_requests WHERE user_id=?1 AND usage_date=?2",params![user_id,date],|r|r.get(0)).optional()?.unwrap_or(0);let used:i64=db.query_row("SELECT amount FROM daily_usage WHERE user_id=?1 AND usage_date=?2",params![user_id,date],|r|r.get(0)).optional()?.unwrap_or(0);if req>=DAILY_REQUEST_LIMIT{return Err(rusqlite::Error::QueryReturnedNoRows);}target=target.min(DAILY_LIMIT-used);if target<=0{return Err(rusqlite::Error::QueryReturnedNoRows);}}
            let mut out=Vec::new();for name in &verified {if out.len()>=target as usize{break;}let changed=db.execute("INSERT OR IGNORE INTO generated(name,mode,created_at) VALUES(?1,?2,?3)",params![name,mode,now_iso()])?;if changed==0{continue;}db.execute("INSERT INTO history(user_id,name,created_at) VALUES(?1,?2,?3)",params![user_id,name,now_iso()])?;out.push(name.clone());}
            if !out.is_empty(){db.execute("INSERT INTO generation_log(user_id,gen_date,amount,created_at) VALUES(?1,?2,?3,?4)",params![user_id,date,out.len() as i64,now_iso()])?;if use_bonus{db.execute("UPDATE bonus_requests SET amount=amount-1,updated_at=?1 WHERE user_id=?2 AND amount>0",params![now_iso(),user_id])?;}else if !premium{db.execute("INSERT INTO daily_usage(user_id,usage_date,amount) VALUES(?1,?2,?3) ON CONFLICT(user_id,usage_date) DO UPDATE SET amount=amount+excluded.amount",params![user_id,date,out.len() as i64])?;db.execute("INSERT INTO daily_requests(user_id,usage_date,amount) VALUES(?1,?2,1) ON CONFLICT(user_id,usage_date) DO UPDATE SET amount=amount+1",params![user_id,date])?;}}
            Ok(out)
        })();
        match outcome {Ok(names)=>{db.execute_batch("COMMIT").map_err(|_|"error".to_string())?;Ok((names,stats))},Err(_)=>{let _=db.execute_batch("ROLLBACK");Err("error".into())}}
    }
}

const MAIN_MENU:&[&[&str]]=&[&["🎲 Генерировать"],&["🔤 Режим"],&["🧠 Умная генерация"],&["⭐ Избранное","🔎 Поиск избранного"],&["📜 История"],&["✅ Проверить избранное"],&["💾 Сохранить список"],&["📊 Мой лимит"],&["👑 Купить Premium"]];
const ADMIN_MENU:&[&[&str]]=&[&["📊 Статистика","👥 Пользователи"],&["🪪 Карточка пользователя","📋 Журнал нарушений"],&["📈 Активность 7 дней","📈 Активность 30 дней"],&["🚫 Заблокировать","🔓 Разблокировать"],&["⏳ Временная блокировка","🚷 Игнор-лист"],&["➕ В игнор","➖ Из игнора"],&["✉️ Написать пользователю","📢 Массовая рассылка"],&["👑 Выдать Premium","❌ Снять Premium"],&["💾 Резервная копия","🛠 Права 2-го админа"],&["➕ Назначить второго администратора"],&["➖ Снять второго администратора"],&["🎁 Подарить запросы"],&["🤖 Разбан автобанов"],&["⬅️ Назад"]];
const MODE_MENU:&[&[&str]]=&[&["💎 Редкие слова"],&["🔤 Только буквы"],&["🔢 Буквы + цифры"],&["⬅️ Назад"]];
const SUB_MENU:&[&[&str]]=&[&["📢 Подписаться"],&["🔄 Проверить подписку"]];
fn keyboard(rows:&[&[&str]])->Vec<Vec<String>>{rows.iter().map(|r|r.iter().map(|s|s.to_string()).collect()).collect()}
impl App {
    fn main_kb(&self,id:i64)->Vec<Vec<String>>{let mut k=keyboard(MAIN_MENU);if self.role(id).is_some(){k.push(vec!["🔐 Админ-панель".into()]);}k}
    fn admin_kb(&self,id:i64)->Vec<Vec<String>>{if id==ADMIN_ID{return keyboard(ADMIN_MENU);}let perms=self.db.lock().ok().and_then(|db|db.query_row("SELECT perms FROM admins WHERE user_id=?1",[id],|r|r.get::<_,String>(0)).optional().ok().flatten()).unwrap_or_default();let mut items=Vec::new();for (p,label) in [("stats","📊 Статистика"),("users","👥 Пользователи"),("card","🪪 Карточка пользователя"),("activity","📈 Активность 7 дней"),("activity","📈 Активность 30 дней"),("violations","📋 Журнал нарушений"),("ignore_view","🚷 Игнор-лист")]{if perms.split(',').any(|x|x==p){items.push(label.to_string());}}let mut rows=items.chunks(2).map(|x|x.to_vec()).collect::<Vec<_>>();rows.push(vec!["⬅️ Назад".into()]);rows}
    fn daily_value(&self,table:&str,id:i64)->i64{self.db.lock().ok().and_then(|db|db.query_row(&format!("SELECT amount FROM {table} WHERE user_id=?1 AND {}=?2",if table=="daily_usage"{"usage_date"}else{"usage_date"}),params![id,today()],|r|r.get(0)).optional().ok().flatten()).unwrap_or(0)}
    fn bonus(&self,id:i64)->i64{self.db.lock().ok().and_then(|db|db.query_row("SELECT amount FROM bonus_requests WHERE user_id=?1",[id],|r|r.get(0)).optional().ok().flatten()).unwrap_or(0)}
    fn daily_limit_text(&self,id:i64)->String{if id==ADMIN_ID{return "👑 АДМИНИСТРАТОР\n━━━━━━━━━━━━━━━━━━\n\n♾ Полный безлимит.".into();}if self.is_premium(id){return "👑 PREMIUM UNLIMITED АКТИВЕН\n━━━━━━━━━━━━━━━━━━\n\n♾ Username: без ограничений.\n♾ Запросы: без ограничений.".into();}let u=self.daily_value("daily_usage",id);let r=self.daily_value("daily_requests",id);format!("📊 ТВОЙ ЛИМИТ\n━━━━━━━━━━━━━━━━━━\n\n🎲 Username: {u}/{DAILY_LIMIT}\nОсталось: {}\n\n📨 Запросы: {r}/{DAILY_REQUEST_LIMIT}\nОсталось: {}\n\n{}👑 Premium: {PREMIUM_CONTACT}", (DAILY_LIMIT-u).max(0),(DAILY_REQUEST_LIMIT-r).max(0),if self.bonus(id)>0{format!("🎁 Бонусных запросов: {}\n\n",self.bonus(id))}else{String::new()})}
    fn premium_text(&self,id:i64)->String{if self.is_premium(id){return "👑 У тебя уже активен PREMIUM UNLIMITED.\nПокупать ничего не нужно.".into();}format!("👑 КУПИТЬ PREMIUM UNLIMITED\n━━━━━━━━━━━━━━━━━━\n\nОтправь подарок {PREMIUM_GIFT_EMOJI} «Мишка» ({PREMIUM_GIFT_STARS} ⭐) — самый дешёвый подарок Telegram.\n\n1. Открой профиль {PREMIUM_CONTACT}.\n2. Нажми «⋮» → «Отправить подарок».\n3. Выбери {PREMIUM_GIFT_EMOJI} Мишка и отправь.\n4. Premium включится автоматически через несколько секунд.\n\n♾ Username и запросы без ограничений.\nЕсли Premium не включился — напиши {PREMIUM_CONTACT}.")}
    fn names_from(&self,table:&str,id:i64,limit:i64)->Vec<String>{let Ok(db)=self.db.lock()else{return vec![]};let q=if table=="favorites"{"SELECT name FROM favorites WHERE user_id=?1 ORDER BY added_at DESC LIMIT ?2"}else{"SELECT name FROM history WHERE user_id=?1 ORDER BY id DESC LIMIT ?2"};db.prepare(q).and_then(|mut s|s.query_map(params![id,limit],|r|r.get::<_,String>(0)).and_then(|it|it.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default()}
    fn add_favorites(&self,id:i64,names:&[String])->usize{let Ok(db)=self.db.lock()else{return 0};let mut count=0;for n in names{if db.execute("INSERT OR IGNORE INTO favorites(user_id,name,added_at) VALUES(?1,?2,?3)",params![id,n,now_iso()]).unwrap_or(0)>0{count+=1;}}count}
    fn add_favorite(&self,id:i64,name:&str)->bool{if !valid_username(name){return false;}let _=self.add_favorites(id,&[name.to_string()]);true}
    fn search_favorites(&self,id:i64,query:&str)->Vec<String>{let Ok(db)=self.db.lock()else{return vec![]};let escaped=query.replace('\\',"\\\\").replace('%',"\\%").replace('_',"\\_");db.prepare("SELECT name FROM favorites WHERE user_id=?1 AND name LIKE ?2 ESCAPE '\\' ORDER BY added_at DESC LIMIT 30").and_then(|mut st|st.query_map(params![id,format!("%{escaped}%")],|r|r.get::<_,String>(0)).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default()}
    fn unban_all_autobanned(&self)->Option<Vec<i64>>{let mut db=self.db.lock().ok()?;let result=(||->rusqlite::Result<Vec<i64>>{let ids=db.prepare("SELECT user_id FROM users WHERE blocked=1 AND (auto_banned=1 OR block_reason LIKE 'Флуд%' OR block_reason LIKE 'Одинаковое сообщение%')")?.query_map([],|r|r.get::<_,i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;db.execute_batch("BEGIN IMMEDIATE")?;for id in &ids{db.execute("UPDATE users SET blocked=0,auto_banned=0,blocked_until=0,block_reason='',banned_at=NULL WHERE user_id=?1",[id])?;db.execute("DELETE FROM spam_events WHERE user_id=?1",[id])?;db.execute("INSERT INTO moderation_log(user_id,action,reason,created_at) VALUES(?1,'UNBLOCK','Массовый разбан автобанов',?2)",params![id,now_iso()])?;}db.execute_batch("COMMIT")?;Ok(ids)})();match result{Ok(ids)=>{let mut flood=self.flood_events.lock().unwrap();for id in &ids{flood.remove(id);}Some(ids)},Err(e)=>{let _=db.execute_batch("ROLLBACK");log_line("ERROR",&format!("unban autobans failed: {e}"));None}}}
    fn show_names(&self,names:Vec<String>,title:&str)->String{if names.is_empty(){return format!("{title}\n\nПока пусто.");}format!("{title}\n\n{}",names.iter().map(|n|format!("@{n}")).collect::<Vec<_>>().join("\n"))}
    fn generate_message(&self,id:i64)->String{
        let state=self.state(id);
        let (names,stats)=match self.generate_names(id,&state.mode,state.smart){
            Ok(value)=>value,
            Err(error)=>{
                if let Some(checked)=error.strip_prefix("nofree:"){return format!("😕 За отведённое время свободных username не нашлось (проверено: {checked}).\nЛимит не списан. Попробуй ещё раз.");}
                return match error.as_str(){
                    "requests"=>format!("⛔ ЛИМИТ ЗАПРОСОВ ИСЧЕРПАН!\n\nДоступно {DAILY_REQUEST_LIMIT} запросов в день.\n💎 PREMIUM: {PREMIUM_CONTACT}"),
                    "names"=>format!("⛔ ДНЕВНОЙ ЛИМИТ USERNAME ИСЧЕРПАН!\n\nЛимит: {DAILY_LIMIT} вариантов.\nНовый лимит появится завтра."),
                    "network"=>"⚠️ Не удалось проверить username: нет связи с t.me.\nЛимит не списан. Попробуй через минуту.".into(),
                    _=>"❌ Не удалось создать варианты. Попробуй ещё раз.".into()
                };
            }
        };
        if names.is_empty(){return "❌ Не удалось создать варианты.".into();}
        self.last_lists.lock().unwrap().insert(id,names.clone());
        let mut out=vec!["🎲 USERNAME GENERATOR".to_string(),"━━━━━━━━━━━━━━━━━━".into(),format!("Режим: {}",if state.mode=="words"{"💎 редкие слова"}else{"🔤 только буквы"}),format!("Умная генерация: {}",if state.smart{"Включена"}else{"Выключена"}),format!("Создано: {}",names.len()),String::new()];
        for (i,name) in names.iter().enumerate(){out.push(format!("{}. @{} — ✅ СВОБОДЕН",i+1,name));}
        out.push(String::new());out.push("━━━━━━━━━━━━━━━━━━".into());
        if self.is_premium(id){out.push("👑 PREMIUM UNLIMITED".into());out.push("♾ Лимиты не действуют.".into());}
        else{out.push(format!("🎲 Username: {}/{}",self.daily_value("daily_usage",id),DAILY_LIMIT));out.push(format!("📨 Запросы: {}/{}",self.daily_value("daily_requests",id),DAILY_REQUEST_LIMIT));out.push(format!("🟢 Осталось запросов: {}",(DAILY_REQUEST_LIMIT-self.daily_value("daily_requests",id)).max(0)));if self.bonus(id)>0{out.push(format!("🎁 Бонусных запросов: {}",self.bonus(id)));}}
        out.push(String::new());out.push(format!("🔎 Проверено через t.me (двойная проверка). Просмотрено имён: {}, занято: {}.",stats["checked"],stats["taken"]));out.push("⚠️ Имя могут занять в любой момент — бери быстрее. Заблокированные Telegram имена проверкой не определяются.".into());out.join("\n")
    }
    fn check_favorites_text(&self,id:i64)->String{let names=self.names_from("favorites",id,CHECK_FAVORITES_LIMIT as i64);if names.is_empty(){return "⭐ Избранное пусто. Проверять нечего.".into();}let outcomes:Vec<_>=self.check_pool.install(||names.par_iter().map(|n|(n.clone(),self.check_name(n))).collect());let mut lines=vec!["✅ ПРОВЕРКА ИЗБРАННОГО".into(),"━━━━━━━━━━━━━━━━━━".into()];for (n,s) in outcomes{lines.push(format!("@{n} — {}",match s{"free"=>"✅ свободен","taken"=>"❌ занят",_=>"❓ не удалось проверить"}));}lines.join("\n")}
}
fn stats_placeholder()->usize{0}

impl App {
    fn business_message(&self,m:&Value){
        let gift=&m["gift"];if gift.is_null(){return;}let user=&m["from"];let chat=&m["chat"];let uid=user["id"].as_i64().unwrap_or(0);let cid=chat["id"].as_i64().unwrap_or(0);if uid<=0||uid!=cid||user["is_bot"]==true{return;}
        let info=gift["gift"].clone();let emoji=info["sticker"]["emoji"].as_str().unwrap_or("");let stars=info["star_count"].as_i64().unwrap_or(0);let key=gift["owned_gift_id"].as_str().map(str::to_string).unwrap_or_else(||format!("{}:{}",cid,m["message_id"].as_i64().unwrap_or(0)));let conn=m["business_connection_id"].as_str();
        if emoji!=PREMIUM_GIFT_EMOJI||stars<PREMIUM_GIFT_STARS{self.send_raw(cid,&format!("Для Premium нужен подарок {PREMIUM_GIFT_EMOJI} «Мишка» ({PREMIUM_GIFT_STARS} ⭐). Этот подарок не подходит."),None,conn);return;}
        if self.is_silenced(uid){self.send(ADMIN_ID,&format!("🧸 Подарок от заблокированного пользователя\n🆔 {uid}\nPremium автоматически НЕ выдан."),None);return;}
        let already=self.is_premium(uid);
        let payment_result=if let Ok(mut db)=self.db.lock(){let result=(||->rusqlite::Result<i64>{db.execute_batch("BEGIN IMMEDIATE")?;let inserted=db.execute("INSERT OR IGNORE INTO gift_payments(gift_key,user_id,gift_id,stars,created_at) VALUES(?1,?2,?3,?4,?5)",params![key,uid,info["id"].as_str().unwrap_or(""),stars,now_iso()])?;if inserted==0{db.execute_batch("ROLLBACK")?;return Ok(0);}if !already{db.execute("INSERT OR REPLACE INTO premium_users(user_id,activated_at,activated_by) VALUES(?1,?2,0)",params![uid,now_iso()])?;}db.execute_batch("COMMIT")?;Ok(inserted as i64)})();match result{Ok(v)=>Ok(v),Err(e)=>{let _=db.execute_batch("ROLLBACK");Err(e.to_string())}}}else{Err("database lock unavailable".into())};
        match payment_result{Ok(0)=>return,Ok(_)=>{},Err(e)=>{log_line("ERROR",&format!("gift premium transaction failed for {uid}: {e}"));self.send_raw(cid,&format!("❌ Не удалось включить Premium автоматически.\nНапиши {PREMIUM_CONTACT} — всё поправим."),None,conn);self.send(ADMIN_ID,&format!("⚠️ Ошибка выдачи Premium за подарок\n🆔 {uid}\nВыдай Premium вручную: Админ-панель → 👑 Выдать Premium."),None);return;}}
        self.save_user(user);
        if already{self.send_raw(cid,"👑 Спасибо за подарок! У тебя Premium уже был активен.",None,conn);}else{self.send_raw(cid,"👑 Спасибо за подарок! PREMIUM UNLIMITED активирован.\n\n♾ Username без ограничений.\n♾ Запросы без ограничений.",None,conn);self.send(uid,&format!("👑 PREMIUM UNLIMITED активирован за подарок {PREMIUM_GIFT_EMOJI}!"),None);}
        self.send(ADMIN_ID,&format!("🧸 ПОДАРОК ПОЛУЧЕН\n━━━━━━━━━━━━━━━━━━\n🆔 {uid}\n👤 {}\n🔗 @{}\n⭐ Стоимость: {stars}\n👑 Premium: {}\n🕒 {}",user["first_name"].as_str().unwrap_or("Без имени"),user["username"].as_str().unwrap_or("нет"),if already{"уже был"}else{"выдан"},now_iso()),None);
    }
    fn save_action(&self,user:&Value,action:&str){log_line("INFO",&format!("ID={} | Имя={} | Username=@{} | Действие={}",user["id"],user["first_name"].as_str().unwrap_or(""),user["username"].as_str().unwrap_or("нет"),action));}
    fn send_gate(&self,chat:i64){self.send(chat,"🔒 ДЛЯ ДОСТУПА К БОТУ НУЖНА ПОДПИСКА\n━━━━━━━━━━━━━━━━━━\n\n1. Нажми «📢 Подписаться».\n2. Подпишись на канал.\n3. Вернись сюда.\n4. Нажми «🔄 Проверить подписку».\n\nЕсли проверка не работает, убедись, что бот добавлен администратором канала.",Some(keyboard(SUB_MENU)));}
    fn handle(&self,m:&Value){
        let user=&m["from"];let chat=&m["chat"];if user.is_null()||chat["type"]!="private"||user["is_bot"]==true{return;}
        let id=user["id"].as_i64().unwrap_or(0);let cid=chat["id"].as_i64().unwrap_or(0);if id<=0||cid<=0{return;}
        let role=self.role(id);if id!=ADMIN_ID&&self.is_silenced(id){return;}
        let text=m["text"].as_str().unwrap_or("").trim().to_string();self.save_user(user);
        if role.is_none()&&!self.check_antispam(user,m["date"].as_f64()){return;}
        let mut s=self.state(id);
        if s.admin&&role.is_none(){s.admin=false;}
        if text=="/myid"{self.send(cid,&format!("Твой Telegram ID: {id}"),None);return;}
        if text=="/cancel"{s.step=None;s.pending=None;s.target_id=None;s.temp=None;self.put_state(id,s.clone());self.send(cid,"Действие отменено.",Some(if s.admin&&role.is_some(){self.admin_kb(id)}else{self.main_kb(id)}));return;}
        if text=="/start"{s=UserState::default();self.put_state(id,s.clone());self.save_action(user,"START");if role.is_some(){self.send(cid,if role.as_deref()==Some("main"){"👑 АДМИНИСТРАТОР\n\nДоступ открыт без подписки."}else{"🛡 ВТОРОЙ АДМИНИСТРАТОР\n\nДоступ открыт без подписки."},Some(self.main_kb(id)));return;}if !self.is_subscribed(id,true){self.send_gate(cid);return;}self.send(cid,&format!("👋 USERNAME GENERATOR\n\n💎 Редкие username-слова: читаемые и красивые.\n🔤 Только английские буквы.\n🧠 Умная генерация.\n⭐ Избранное и история.\n🛡 Защита от спама.\n\n🎲 Лимит: {DAILY_LIMIT} username в день.\n📨 Запросов: {DAILY_REQUEST_LIMIT} в день.\nВ каждой генерации — {GENERATION_SIZE} username.\n\n👑 PREMIUM UNLIMITED — без дневных ограничений.\nДля подключения: {PREMIUM_CONTACT}\n{PREMIUM_GIFT_EMOJI} Или отправь подарок «Мишка» — кнопка «👑 Купить Premium».\n\n✅ Каждый username проверяется на свободу."),Some(self.main_kb(id)));return;}
        if role.is_none(){if text=="📢 Подписаться"{self.send(cid,&format!("📢 Открой канал и подпишись:\n{CHANNEL_URL}"),Some(keyboard(SUB_MENU)));return;}if text=="🔄 Проверить подписку"{if self.is_subscribed(id,true){self.send(cid,"✅ Подписка подтверждена!",Some(self.main_kb(id)));}else{self.send_gate(cid);}return;}if !self.is_subscribed(id,false){self.send_gate(cid);return;}}
        // Password step: delete the message containing the password, and compare in constant time.
        if s.step.as_deref()==Some("admin_password") {s.step=None;if role.is_none(){self.send(cid,"⛔ Нет доступа.",Some(self.main_kb(id)));return;}let _=self.api("deleteMessage",json!({"chat_id":cid,"message_id":m["message_id"]}));let good=constant_time_eq(text.as_bytes(),self.admin_password.as_bytes());if good{s.admin=true;self.save_action(user,"ADMIN LOGIN");self.send(cid,"✅ Админ-панель открыта.",Some(self.admin_kb(id)));}else{self.send(cid,"❌ Неверный пароль.",Some(self.main_kb(id)));}self.put_state(id,s);return;}
        if s.step.as_deref()==Some("search"){s.step=None;let names=self.search_favorites(id,&text.to_lowercase());self.put_state(id,s);self.send(cid,&self.show_names(names,"🔎 РЕЗУЛЬТАТЫ ПОИСКА"),Some(self.main_kb(id)));return;}
        self.save_action(user,&format!("BUTTON: {}",text.chars().take(60).collect::<String>()));
        if s.admin&&role.is_some(){self.admin_action(user,cid,&text,&mut s);self.put_state(id,s);return;}
        let kb=self.main_kb(id);
        match text.as_str(){
            "🎲 Генерировать"=>{let out=self.generate_message(id);self.send(cid,&out,Some(kb));},
            "👑 Купить Premium"=>{self.send(cid,&self.premium_text(id),Some(kb));},
            "📊 Мой лимит"=>{self.send(cid,&self.daily_limit_text(id),Some(kb));},
            "🔤 Режим"=>{self.send(cid,"Выбери режим генерации.",Some(keyboard(MODE_MENU)));},
            "💎 Редкие слова"=>{s.mode="words".into();self.send(cid,"✅ Режим: редкие английские слова, максимум 5 букв.",Some(kb));},
            "🔤 Только буквы"=>{s.mode="letters".into();self.send(cid,"✅ Режим: только английские буквы, максимум 5.",Some(kb));},
            "🔢 Буквы + цифры"=>{s.mode="letters_digits".into();self.send(cid,"✅ Режим: первая буква английская, далее буквы и цифры.",Some(kb));},
            "🧠 Умная генерация"=>{s.smart=!s.smart;self.send(cid,&format!("🧠 Умная генерация {}.",if s.smart{"включена"}else{"выключена"}),Some(kb));},
                        "⭐ Избранное"=>{self.send(cid,&self.show_names(self.names_from("favorites",id,100),"⭐ ИЗБРАННОЕ"),Some(kb));},
            "📜 История"=>{self.send(cid,&self.show_names(self.names_from("history",id,50),"📜 ПОСЛЕДНИЕ ГЕНЕРАЦИИ"),Some(kb));},
            "💾 Сохранить список"=>{let names=self.last_lists.lock().unwrap().get(&id).cloned().unwrap_or_default();if names.is_empty(){self.send(cid,"Сначала создай список.",Some(kb));}else{let n=self.add_favorites(id,&names);self.send(cid,&format!("⭐ Новых избранных добавлено: {n}"),Some(kb));}},
            "🔎 Поиск избранного"=>{s.step=Some("search".into());self.put_state(id,s);self.send(cid,"Введи часть username для поиска:",None);return;},
            "✅ Проверить избранное"=>{self.send(cid,"⏳ Проверяю избранное, подожди...",None);let out=self.check_favorites_text(id);self.send(cid,&out,Some(kb));},
            "🔐 Админ-панель"=>{if role.is_none(){self.send(cid,"⛔ Нет доступа.",None);}else{s.step=Some("admin_password".into());self.put_state(id,s);self.send(cid,"🔐 Введи пароль администратора:",None);return;}},
            "⬅️ Назад"=>{self.send(cid,"Главное меню.",Some(kb));},
            _ if text.starts_with('@')=>{let n=text[1..].to_lowercase();if self.add_favorite(id,&n){self.send(cid,"⭐ Добавлено в избранное.\n⚠️ НЕ ПРОВЕРЕНО.",Some(kb));}else{self.send(cid,"Нужен username из пяти английских букв.",Some(kb));}},
            _=>{self.send(cid,"Используй кнопки меню.",Some(kb));}
        }
        self.put_state(id,s);
    }
}
fn constant_time_eq(a:&[u8],b:&[u8])->bool{let mut diff=a.len()^b.len();let n=a.len().max(b.len());for i in 0..n{diff|=(a.get(i).copied().unwrap_or(0)^b.get(i).copied().unwrap_or(0)) as usize;}diff==0}

impl App {
    fn create_backup(&self)->Result<(String,u64),String>{
        let stamp=Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();let mut path=format!("username_backup_{stamp}.db");let mut counter=2;while std::path::Path::new(&path).exists(){path=format!("username_backup_{stamp}_{counter}.db");counter+=1;}
        let db=self.db.lock().map_err(|e|e.to_string())?;let mut dest=Connection::open(&path).map_err(|e|e.to_string())?;rusqlite::backup::Backup::new(&*db,&mut dest).map_err(|e|e.to_string())?.run_to_completion(5,Duration::from_millis(250),None).map_err(|e|e.to_string())?;let integrity:String=dest.query_row("PRAGMA integrity_check",[],|r|r.get(0)).map_err(|e|e.to_string())?;if integrity!="ok"{drop(dest);let _=std::fs::remove_file(&path);return Err(format!("backup integrity check failed: {integrity}"));}drop(dest);let size=std::fs::metadata(&path).map_err(|e|e.to_string())?.len();Ok((path,size))
    }
    fn admin_stats(&self)->String {
        let Ok(db)=self.db.lock()else{return "❌ Ошибка базы данных.".into()};
        let q=|sql:&str|db.query_row(sql,[],|r|r.get::<_,i64>(0)).unwrap_or(0);
        let t=today();let users=q("SELECT COUNT(*) FROM users");let blocked=q("SELECT COUNT(*) FROM users WHERE blocked=1");let auto=q("SELECT COUNT(*) FROM users WHERE blocked=1 AND auto_banned=1");let temporary=db.query_row("SELECT COUNT(*) FROM users WHERE blocked_until>?1",[epoch()],|r|r.get::<_,i64>(0)).unwrap_or(0);let ignored=q("SELECT COUNT(*) FROM ignore_list");let generated=q("SELECT COUNT(*) FROM generated");let favorites=q("SELECT COUNT(*) FROM favorites");let history=q("SELECT COUNT(*) FROM history");let premium=q("SELECT COUNT(*) FROM premium_users");let violations=q("SELECT COUNT(*) FROM moderation_log");let admins2=q("SELECT COUNT(*) FROM admins WHERE role='second'");let gifts=q("SELECT COUNT(*) FROM gift_payments");let bonus=q("SELECT COALESCE(SUM(amount),0) FROM bonus_requests");let today_names=db.query_row("SELECT COUNT(*) FROM history WHERE substr(created_at,1,10)=?1",[&t],|r|r.get::<_,i64>(0)).unwrap_or(0);let req_a=db.query_row("SELECT COALESCE(SUM(amount),0) FROM daily_requests WHERE usage_date=?1",[&t],|r|r.get::<_,i64>(0)).unwrap_or(0);let req_b=db.query_row("SELECT COUNT(*) FROM generation_log WHERE gen_date=?1",[&t],|r|r.get::<_,i64>(0)).unwrap_or(0);let today_requests=req_a.max(req_b);let active=db.query_row("SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id FROM activity_log WHERE day=?1 UNION SELECT user_id FROM daily_requests WHERE usage_date=?1)",[&t],|r|r.get::<_,i64>(0)).unwrap_or(0);
        format!("📊 СТАТИСТИКА БОТА\n━━━━━━━━━━━━━━━━━━\n👥 Пользователи: {users}\n🟢 Активны сегодня: {active}\n🚫 Постоянно заблокированы: {blocked}\n🤖 Из них автобан: {auto}\n⏳ Временные блокировки: {temporary}\n🚷 Игнор-лист: {ignored}\n👑 Premium: {premium}\n🧸 Premium за подарки: {gifts}\n🎁 Подаренных запросов на руках: {bonus}\n🛡 Вторых администраторов: {admins2}\n🎲 Всего username: {generated}\n📅 Сегодня выдано: {today_names}\n📨 Сегодня запросов: {today_requests}\n⭐ Избранное: {favorites}\n📜 История: {history}\n⚠️ Записей в журнале: {violations}\n━━━━━━━━━━━━━━━━━━\nАнтифлуд: {FLOOD_LIMIT} сообщений за {} сек. = автобан\nАвтобан за одинаковые сообщения: отключён\nАдминистраторы: антиспам не применяется\nАвтобан: постоянный",FLOOD_WINDOW as i64)
    }
    fn users_page(&self,offset:i64)->String {
        let Ok(db)=self.db.lock()else{return "Ошибка базы.".into()};let total:i64=db.query_row("SELECT COUNT(*) FROM users",[],|r|r.get(0)).unwrap_or(0);if total==0{return "👥 Пользователей пока нет.".into();}
        let rows=db.prepare("SELECT user_id,first_name,username,blocked,blocked_until,auto_banned FROM users ORDER BY joined_at DESC,user_id DESC LIMIT ?1 OFFSET ?2").and_then(|mut st|st.query_map(params![USERS_PAGE_SIZE,offset],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,i64>(3)?,r.get::<_,Option<f64>>(4)?.unwrap_or(0.0),r.get::<_,i64>(5)?))).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default();if rows.is_empty(){return format!("👥 На этой странице пусто. Всего пользователей: {total}");}
        let ignored=db.prepare("SELECT user_id FROM ignore_list").and_then(|mut st|st.query_map([],|r|r.get::<_,i64>(0)).and_then(|x|x.collect::<rusqlite::Result<HashSet<_>>>())).unwrap_or_default();let now=epoch();let mut lines=vec![format!("👥 ПОЛЬЗОВАТЕЛИ ({}–{} из {total})",offset+1,offset+rows.len() as i64),"━━━━━━━━━━━━━━━━━━".into()];
        for (id,name,username,blocked,until,auto) in rows{let status=if blocked!=0{if auto!=0{"🤖 Автобан"}else{"🚫 Блок"}}else if until>now{"⏳ Врем. блок"}else if ignored.contains(&id){"🚷 Игнор"}else{"✅ Активен"};lines.push(format!("{status} | {id} | {} | @{}",name.as_deref().filter(|x|!x.is_empty()).unwrap_or("Без имени"),username.as_deref().filter(|x|!x.is_empty()).unwrap_or("нет")));}lines.join("\n")
    }
    fn user_card(&self,id:i64)->String {
        let Ok(db)=self.db.lock()else{return "Ошибка базы.".into()};
        let user=db.query_row("SELECT first_name,username,blocked,blocked_until,block_reason,auto_banned,joined_at,last_active FROM users WHERE user_id=?1",[id],|r|Ok((r.get::<_,Option<String>>(0)?.unwrap_or_default(),r.get::<_,Option<String>>(1)?.unwrap_or_default(),r.get::<_,i64>(2)?,r.get::<_,Option<f64>>(3)?.unwrap_or(0.0),r.get::<_,Option<String>>(4)?.unwrap_or_default(),r.get::<_,i64>(5)?,r.get::<_,Option<String>>(6)?.unwrap_or_default(),r.get::<_,Option<String>>(7)?.unwrap_or_default()))).optional().ok().flatten();
        let Some((name,username,blocked,until,reason,auto,joined,last))=user else{return format!("❌ Пользователь {id} не найден в базе.")};let t=today();let total:i64=db.query_row("SELECT COUNT(*) FROM history WHERE user_id=?1",[id],|r|r.get(0)).unwrap_or(0);let today_names:i64=db.query_row("SELECT COUNT(*) FROM history WHERE user_id=?1 AND substr(created_at,1,10)=?2",params![id,t],|r|r.get(0)).unwrap_or(0);let requests:i64=db.query_row("SELECT amount FROM daily_requests WHERE user_id=?1 AND usage_date=?2",params![id,t],|r|r.get(0)).optional().ok().flatten().unwrap_or(0);let log_requests:i64=db.query_row("SELECT COUNT(*) FROM generation_log WHERE user_id=?1 AND gen_date=?2",[id],|r|r.get(0)).unwrap_or(0);let favorites:i64=db.query_row("SELECT COUNT(*) FROM favorites WHERE user_id=?1",[id],|r|r.get(0)).unwrap_or(0);let premium=db.query_row("SELECT 1 FROM premium_users WHERE user_id=?1",[id],|r|r.get::<_,i64>(0)).optional().ok().flatten().is_some();let bonus:i64=db.query_row("SELECT amount FROM bonus_requests WHERE user_id=?1",[id],|r|r.get(0)).optional().ok().flatten().unwrap_or(0);let ignored=db.query_row("SELECT 1 FROM ignore_list WHERE user_id=?1",[id],|r|r.get::<_,i64>(0)).optional().ok().flatten().is_some();drop(db);let role=self.role(id);let permanent=if blocked!=0{format!("Да ({}, {})",if auto!=0{"автобан"}else{"ручная"},reason)}else{"Нет".into()};let temporary=if until>epoch(){format!("Да, до {}",fmt_ts(until))}else{"Нет".into()};let role_text=match role.as_deref(){Some("main")=>"\n🛡 Роль: главный администратор",Some("second")=>"\n🛡 Роль: второй администратор",_=>""};
        format!("🪪 КАРТОЧКА ПОЛЬЗОВАТЕЛЯ\n━━━━━━━━━━━━━━━━━━\n👤 Имя: {}\n🔗 Username: @{}\n🆔 Telegram ID: {id}\n📅 Дата регистрации: {}\n🕒 Последняя активность: {}\n🎲 Всего создано username: {total}\n📨 Запросов сегодня: {}\n🎲 Username сегодня: {today_names}\n⭐ Избранное: {favorites}\n📜 Записей истории: {total}\n👑 Premium: {}\n🎁 Бонусных запросов: {bonus}\n🚫 Постоянная блокировка: {permanent}\n⏳ Временная блокировка: {temporary}\n🚷 Игнор-лист: {}{}",if name.is_empty(){"Без имени"}else{&name},if username.is_empty(){"нет"}else{&username},fmt_dt(&joined),fmt_dt(&last),requests.max(log_requests),if premium{"Да"}else{"Нет"},if ignored{"Да"}else{"Нет"},role_text)
    }
    fn bar(value:i64,maximum:i64,width:usize)->String{let filled=if maximum<=0||value<=0{0}else{((value as f64*width as f64/maximum as f64).round() as usize).max(1).min(width)};format!("{}{}","█".repeat(filled),"░".repeat(width-filled))}
    fn activity_report(&self,days:i64)->String{
        let Ok(db)=self.db.lock()else{return "Ошибка базы данных.".into()};let today_date=Local::now().date_naive();let dates=(0..days).map(|i|today_date-ChronoDuration::days(i)).collect::<Vec<_>>();let keys=dates.iter().map(|d|d.format("%Y-%m-%d").to_string()).collect::<Vec<_>>();let start=keys.last().cloned().unwrap_or_else(today);
        let read_map=|sql:&str|->HashMap<String,i64>{db.prepare(sql).and_then(|mut st|st.query_map([&start],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).and_then(|x|x.collect::<rusqlite::Result<HashMap<_,_>>>())).unwrap_or_default()};
        let names=read_map("SELECT substr(created_at,1,10),COUNT(*) FROM history WHERE substr(created_at,1,10)>=?1 GROUP BY 1");let reqs=read_map("SELECT usage_date,SUM(amount) FROM daily_requests WHERE usage_date>=?1 GROUP BY usage_date");let genlog=read_map("SELECT gen_date,COUNT(*) FROM generation_log WHERE gen_date>=?1 GROUP BY gen_date");
        let active_rows=db.prepare("SELECT d,COUNT(DISTINCT user_id) FROM (SELECT day AS d,user_id FROM activity_log WHERE day>=?1 UNION SELECT substr(created_at,1,10),user_id FROM history WHERE substr(created_at,1,10)>=?1 UNION SELECT usage_date,user_id FROM daily_requests WHERE usage_date>=?1) GROUP BY d").and_then(|mut st|st.query_map([&start],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).and_then(|x|x.collect::<rusqlite::Result<HashMap<_,_>>>())).unwrap_or_default();
        let unique:i64=db.query_row("SELECT COUNT(DISTINCT user_id) FROM (SELECT day AS d,user_id FROM activity_log WHERE day>=?1 UNION SELECT substr(created_at,1,10),user_id FROM history WHERE substr(created_at,1,10)>=?1 UNION SELECT usage_date,user_id FROM daily_requests WHERE usage_date>=?1)",[&start],|r|r.get(0)).unwrap_or(0);
        let series_req=keys.iter().map(|k|reqs.get(k).copied().unwrap_or(0).max(genlog.get(k).copied().unwrap_or(0))).collect::<Vec<_>>();let series_names=keys.iter().map(|k|names.get(k).copied().unwrap_or(0)).collect::<Vec<_>>();let series_active=keys.iter().map(|k|active_rows.get(k).copied().unwrap_or(0)).collect::<Vec<_>>();let mut out=vec![format!("📈 АКТИВНОСТЬ ЗА {days} ДНЕЙ"),"━━━━━━━━━━━━━━━━━━".into(),format!("📨 Запросов: {}",series_req.iter().sum::<i64>()),format!("🎲 Создано username: {}",series_names.iter().sum::<i64>()),format!("👥 Активных пользователей: {unique}"),String::new()];
        for (title,series) in [("📨 ЗАПРОСЫ",series_req),("🎲 СОЗДАНО USERNAME",series_names),("👥 АКТИВНЫЕ ПОЛЬЗОВАТЕЛИ",series_active)]{let max=series.iter().copied().max().unwrap_or(0);out.push(title.into());for (i,v) in series.iter().enumerate(){out.push(format!("{} {} {}",dates[i].format("%d.%m"),Self::bar(*v,max,10),v));}out.push(String::new());}out.push("ℹ️ Данные взяты из базы. До установки этой версии учёт запросов и активности мог быть неполным.".into());out.join("\n")
    }
    fn autobanned_text(&self)->String{let Ok(db)=self.db.lock()else{return "Ошибка базы.".into()};let rows=db.prepare("SELECT user_id,first_name,username,banned_at FROM users WHERE blocked=1 AND (auto_banned=1 OR block_reason LIKE 'Флуд%' OR block_reason LIKE 'Одинаковое сообщение%') ORDER BY banned_at DESC,user_id DESC").and_then(|mut st|st.query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?))).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default();if rows.is_empty(){return "🤖 Забаненных автобаном нет.".into();}let mut out=vec!["🤖 РАЗБАН АВТОБАНОВ".into(),"━━━━━━━━━━━━━━━━━━".into(),format!("Забанено автобаном: {}",rows.len()),String::new()];for (id,name,username,date) in rows.iter().take(20){out.push(format!("{id} | {} | @{} | {}",name.as_deref().filter(|x|!x.is_empty()).unwrap_or("Без имени"),username.as_deref().filter(|x|!x.is_empty()).unwrap_or("нет"),fmt_dt(date.as_deref().unwrap_or(""))));}if rows.len()>20{out.push(format!("... и ещё {}",rows.len()-20));}out.push(String::new());out.push("Будут разбанены только пользователи, забаненные автобаном. Ручные блокировки и игнор-лист не затрагиваются.".into());out.join("\n")}
    fn moderation_history_text(&self)->String {let Ok(db)=self.db.lock()else{return "Ошибка базы.".into()};let rows=db.prepare("SELECT user_id,action,reason,created_at FROM moderation_log ORDER BY id DESC LIMIT 30").and_then(|mut st|st.query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?))).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default();if rows.is_empty(){return "📋 Журнал нарушений пуст.".into();}let mut out=vec!["📋 ЖУРНАЛ НАРУШЕНИЙ (последние 30)".into(),"━━━━━━━━━━━━━━━━━━".into()];for (id,action,reason,date) in rows{let label=match action.as_str(){"AUTO_BAN_FLOOD"=>"🤖 Автобан (флуд)","AUTO_BAN_REPEAT"=>"🤖 Автобан (повтор, старая версия)","AUTO_BLOCK"=>"⏳ Автоблокировка (старая версия)","BLOCK"=>"🚫 Блокировка","UNBLOCK"=>"🔓 Разблокировка","TEMP_BLOCK"=>"⏳ Временная блокировка","IGNORE_ADD"=>"🚷 В игнор-лист","IGNORE_REMOVE"=>"➖ Из игнора","ADMIN2_ADD"=>"➕ Назначен 2-й администратор","ADMIN2_REMOVE"=>"➖ Снят 2-й администратор","GIFT_REQUESTS"=>"🎁 Подарены запросы",_=>action.as_str()};out.push(format!("\n🆔 {id}\nДействие: {label}\nПричина: {}\nДата: {}",reason.as_deref().unwrap_or("—"),fmt_dt(date.as_deref().unwrap_or(""))));}out.join("\n")}
        fn action_log(&self,user:&Value,action:&str){self.save_action(user,action);}
    fn confirm(&self,chat:i64,s:&mut UserState,pending:Value,question:&str){s.pending=Some(pending);s.step=Some("confirm".into());self.send(chat,&format!("{question}\n\nПодтвердить?"),Some(vec![vec!["✅ Подтвердить".into(),"❌ Отмена".into()]]));}
    fn execute_pending(&self,user:&Value,chat:i64,s:&mut UserState){let Some(p)=s.pending.take()else{return;};s.step=None;let id=p["target"].as_i64().unwrap_or(0);let action=p["action"].as_str().unwrap_or("");let admin=user["id"].as_i64().unwrap_or(0);match action{
        "unban_auto"=>{match self.unban_all_autobanned(){Some(ids)=>{self.send(chat,&format!("✅ Разбанено пользователей: {}",ids.len()),Some(self.admin_kb(admin)));for uid in ids{self.send(uid,"✅ Доступ к боту восстановлен. Пожалуйста, не отправляй сообщения слишком часто.",None);thread::sleep(Duration::from_millis(50));}},None=>{self.send(chat,"❌ Ошибка базы данных. Разбан не выполнен.",Some(self.admin_kb(admin)));}}},
        "block"=>{if id==ADMIN_ID{self.send(chat,"Нельзя заблокировать главного администратора.",Some(self.admin_kb(admin)));return;}let changed=self.db.lock().ok().and_then(|db|db.execute("UPDATE users SET blocked=1,auto_banned=0,blocked_until=0,block_reason='Ручная блокировка',banned_at=?1 WHERE user_id=?2",params![now_iso(),id]).ok()).unwrap_or(0)>0;if changed{self.moderation(id,"BLOCK","Главный администратор");self.send(chat,&format!("🚫 Пользователь {id} заблокирован навсегда."),Some(self.admin_kb(admin)));}else{self.send(chat,"❌ Пользователь не найден.",Some(self.admin_kb(admin)));}},
        "temp_block"=>{let minutes=p["minutes"].as_i64().unwrap_or(0);if id==ADMIN_ID||minutes<1||minutes>10080{self.send(chat,"❌ Неверная временная блокировка.",Some(self.admin_kb(admin)));return;}let until=epoch()+minutes as f64*60.0;let changed=self.db.lock().ok().and_then(|db|db.execute("UPDATE users SET blocked_until=?1,block_reason='Временная блокировка администратором' WHERE user_id=?2",params![until,id]).ok()).unwrap_or(0)>0;if changed{self.moderation(id,"TEMP_BLOCK","Временная блокировка администратором");self.send(chat,&format!("⏳ ID {id} заблокирован на {minutes} минут.\nДо: {until}"),Some(self.admin_kb(admin)));self.send(id,&format!("⛔ Доступ временно ограничен на {minutes} минут."),None);}else{self.send(chat,"❌ Пользователь не найден.",Some(self.admin_kb(admin)));}},
        "unban"=>{if id==ADMIN_ID{self.send(chat,"Нельзя разблокировать главного администратора.",Some(self.admin_kb(admin)));return;}let changed=self.db.lock().ok().and_then(|db|db.execute("UPDATE users SET blocked=0,auto_banned=0,blocked_until=0,block_reason='',banned_at=NULL WHERE user_id=?1",[id]).ok()).unwrap_or(0)>0;if changed{if let Ok(db)=self.db.lock(){let _=db.execute("DELETE FROM spam_events WHERE user_id=?1",[id]);}self.flood_events.lock().unwrap().remove(&id);self.moderation(id,"UNBLOCK","Главный администратор");}self.send(chat,if changed{"🔓 Блокировка снята."}else{"❌ Пользователь не найден."},Some(self.admin_kb(admin)));},
        "premium_on"=>{self.premium_activate(id,admin);self.send(chat,&format!("👑 Premium активирован для ID {id}."),Some(self.admin_kb(admin)));self.send(id,"👑 Тебе активировали PREMIUM UNLIMITED!",None);self.action_log(user,&format!("PREMIUM ACTIVATED FOR {id}"));},
        "premium_off"=>{let changed=self.db.lock().ok().and_then(|db|db.execute("DELETE FROM premium_users WHERE user_id=?1",[id]).ok()).unwrap_or(0)>0;if changed{self.send(id,"Информация: Premium отключён. Теперь действуют обычные дневные лимиты.",None);self.action_log(user,&format!("PREMIUM REMOVED FROM {id}"));}self.send(chat,if changed{"✅ Premium снят."}else{"У пользователя нет Premium."},Some(self.admin_kb(admin)));},
        "ignore_add"=>{if id==ADMIN_ID{self.send(chat,"Нельзя добавить главного администратора в игнор.",Some(self.admin_kb(admin)));return;}let _=self.db.lock().map(|db|db.execute("INSERT OR REPLACE INTO ignore_list(user_id,added_at,added_by,reason) VALUES(?1,?2,?3,'Добавлен администратором')",params![id,now_iso(),admin]));self.moderation(id,"IGNORE_ADD","Добавлен администратором");self.send(chat,"🚷 Добавлен в игнор-лист. Бот будет молча игнорировать его.",Some(self.admin_kb(admin)));},
        "admin2_remove"=>{let _=self.db.lock().map(|db|db.execute("DELETE FROM admins WHERE user_id=?1",[id]));if let Some(mut state)=self.states.lock().ok().and_then(|mut map|map.remove(&id)){state.admin=false;state.step=None;state.pending=None;self.states.lock().unwrap().insert(id,state);}self.moderation(id,"ADMIN2_REMOVE","Второй администратор снят");self.send(chat,"➖ Второй администратор снят.",Some(self.admin_kb(admin)));self.send(id,"Роль второго администратора снята.",Some(self.main_kb(id)));},
        "admin2_add"=>{if id==ADMIN_ID{self.send(chat,"Это главный администратор.",Some(self.admin_kb(admin)));return;}let exists=self.db.lock().ok().and_then(|db|db.query_row("SELECT user_id FROM admins WHERE role='second' LIMIT 1",[],|r|r.get::<_,i64>(0)).optional().ok().flatten()).is_some();if exists{self.send(chat,"Второй администратор уже назначен. Сначала сними его.",Some(self.admin_kb(admin)));}else{let _=self.db.lock().map(|db|db.execute("INSERT OR REPLACE INTO admins(user_id,role,perms,added_at,added_by) VALUES(?1,'second','stats,users,card,activity,violations,ignore_view',?2,?3)",params![id,now_iso(),admin]));self.moderation(id,"ADMIN2_ADD","Назначен второй администратор");self.send(chat,&format!("➕ Второй администратор назначен: {id}"),Some(self.admin_kb(admin)));self.send(id,"🛡 Тебе назначена роль второго администратора. Открой «🔐 Админ-панель».",Some(self.main_kb(id)));}},
        "gift_requests"=>{let amount=p["amount"].as_i64().unwrap_or(0);if amount>0&&amount<=GIFT_REQUESTS_MAX{let _=self.db.lock().map(|db|db.execute("INSERT INTO bonus_requests(user_id,amount,updated_at) VALUES(?1,?2,?3) ON CONFLICT(user_id) DO UPDATE SET amount=amount+excluded.amount,updated_at=excluded.updated_at",params![id,amount,now_iso()]));self.moderation(id,"GIFT_REQUESTS",&format!("Подарено запросов: {amount}"));self.send(chat,&format!("🎁 Подарено {amount} запросов пользователю {id}."),Some(self.admin_kb(admin)));self.send(id,&format!("🎁 Тебе подарили запросы: {amount}. Каждый запрос — 3 username."),None);}else{self.send(chat,"❌ Неверное количество.",Some(self.admin_kb(admin)));}},
        _=>{self.send(chat,"Действие не поддерживается.",Some(self.admin_kb(admin)));}
    }}
    fn user_exists(&self,id:i64)->bool{self.db.lock().ok().and_then(|db|db.query_row("SELECT 1 FROM users WHERE user_id=?1",[id],|r|r.get::<_,i64>(0)).optional().ok().flatten()).is_some()}
    fn admin_step(&self,user:&Value,chat:i64,text:&str,s:&mut UserState)->bool {
        let admin=user["id"].as_i64().unwrap_or(0);let step=s.step.clone().unwrap_or_default();if step.is_empty(){return false;}
        let required=match step.as_str(){"users_view"=>"users","card_id"=>"card",_=>"main"};if !self.has_perm(admin,required){s.step=None;s.pending=None;self.send(chat,"⛔ Недостаточно прав.",Some(self.main_kb(admin)));return true;}
        if step=="perm_toggle"{let second=self.db.lock().ok().and_then(|db|db.query_row("SELECT user_id FROM admins WHERE role='second' LIMIT 1",[],|r|r.get::<_,i64>(0)).optional().ok().flatten());if second.is_none(){s.step=None;self.send(chat,"Второй администратор не назначен.",Some(self.admin_kb(admin)));return true;}let second=second.unwrap();if text=="Готово"||text=="✅ Готово"{s.step=None;self.send(chat,"✅ Права сохранены.",Some(self.admin_kb(admin)));return true;}if let Ok(n)=text.parse::<usize>(){let keys=["stats","users","card","activity","violations","ignore_view"];if (1..=6).contains(&n){let key=keys[n-1];let mut perms=self.db.lock().ok().and_then(|db|db.query_row("SELECT perms FROM admins WHERE user_id=?1",[second],|r|r.get::<_,String>(0)).optional().ok().flatten()).unwrap_or_default().split(',').filter(|x|!x.is_empty()).map(str::to_string).collect::<HashSet<_>>();if !perms.remove(key){perms.insert(key.into());}let joined=keys.iter().filter(|k|perms.contains(**k)).copied().collect::<Vec<_>>().join(",");let _=self.db.lock().map(|db|db.execute("UPDATE admins SET perms=?1 WHERE user_id=?2",params![joined,second]));}}let perms=self.db.lock().ok().and_then(|db|db.query_row("SELECT perms FROM admins WHERE user_id=?1",[second],|r|r.get::<_,String>(0)).optional().ok().flatten()).unwrap_or_default();let keys=["stats","users","card","activity","violations","ignore_view"];let labels=["Статистика","Пользователи","Карточки","Активность","Журнал нарушений","Игнор-лист"];let mut msg=String::from("🛠 ПРАВА ВТОРОГО АДМИНИСТРАТОРА\n");for i in 0..6{msg.push_str(&format!("{} {} — {}\n",i+1,labels[i],if perms.split(',').any(|x|x==keys[i]){"✅"}else{"❌"}));}msg.push_str("\nНажми номер, чтобы переключить. «Готово» — сохранить.");self.send(chat,&msg,Some(vec![vec!["1".into(),"2".into(),"3".into()],vec!["4".into(),"5".into(),"6".into()],vec!["✅ Готово".into()]]));return true;}
                if step=="confirm" {if text=="✅ Подтвердить"{self.execute_pending(user,chat,s);}else{s.pending=None;s.step=None;self.send(chat,"Действие отменено.",Some(self.admin_kb(admin)));}return true;}
        if step=="confirm_broadcast" {if text=="✅ Подтвердить" {let p=s.pending.take().unwrap_or(Value::Null);let body=p["text"].as_str().unwrap_or("").to_string();let ids=p["recipients"].as_array().cloned().unwrap_or_default();let total=p["total"].as_i64().unwrap_or(ids.len() as i64);let skipped=p["skipped"].as_i64().unwrap_or(0);self.send(chat,&format!("📢 Рассылка началась. Получателей: {}. Подожди.",ids.len()),None);let mut ok=0;let mut fail=0;for x in ids{if let Some(uid)=x.as_i64(){if self.send(uid,&body,None){ok+=1;}else{fail+=1;}thread::sleep(Duration::from_millis(50));}}s.step=None;self.send(chat,&format!("📢 РАССЫЛКА ЗАВЕРШЕНА\n👥 Всего пользователей: {total}\n✅ Успешно отправлено: {ok}\n❌ Ошибки отправки: {fail}\n🚫 Пропущено: {skipped}"),Some(self.admin_kb(admin)));}else{s.pending=None;s.step=None;self.send(chat,"Рассылка отменена.",Some(self.admin_kb(admin)));}return true;}
        if step=="users_view" {if text=="▶️ След. страница"{s.users_offset+=USERS_PAGE_SIZE;}else if text=="◀️ Пред. страница"{s.users_offset=(s.users_offset-USERS_PAGE_SIZE).max(0);}else{s.step=None;return false;}self.send(chat,&self.users_page(s.users_offset),Some(vec![vec!["◀️ Пред. страница".into(),"▶️ След. страница".into()]]));return true;}
        if !["card_id","block","unblock","ignore_add","ignore_remove","premium_on","premium_off","message_user_id","message_user_text","temp_block","gift_req","broadcast_text","admin2_add"].contains(&step.as_str()){return false;}
        if step=="broadcast_text" {if text.trim().is_empty(){self.send(chat,"Отправь текст сообщения или /cancel.",None);return true;}if text.chars().count()>MESSAGE_LIMIT{self.send(chat,&format!("Слишком длинный текст. Максимум {MESSAGE_LIMIT} символов."),None);return true;}let (total,recipients)=self.db.lock().ok().map(|db|{let total=db.query_row("SELECT COUNT(*) FROM users",[],|r|r.get::<_,i64>(0)).unwrap_or(0);let recipients=db.prepare("SELECT user_id FROM users WHERE blocked=0 AND (blocked_until IS NULL OR blocked_until<=?1) AND user_id NOT IN (SELECT user_id FROM ignore_list)").and_then(|mut st|st.query_map([epoch()],|r|r.get::<_,i64>(0)).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).unwrap_or_default();(total,recipients)}).unwrap_or((0,Vec::new()));let skipped=(total-recipients.len() as i64).max(0);s.step=None;s.pending=Some(json!({"action":"broadcast","text":text,"recipients":recipients,"total":total,"skipped":skipped}));self.send(chat,&format!("📢 Рассылка\nВсего пользователей: {total}\nПолучателей: {}\nБудет пропущено: {skipped}\n\nПодтвердить?",recipients.len()),Some(vec![vec!["✅ Подтвердить".into(),"❌ Отмена".into()]]));s.step=Some("confirm_broadcast".into());return true;}
        if step=="message_user_text"{if text.trim().is_empty(){self.send(chat,"❌ Пустое сообщение. Введи текст или /cancel.",None);return true;}let target=s.target_id.take().unwrap_or(0);s.step=None;let body=if target==8905801772{text.to_string()}else{format!("📩 СООБЩЕНИЕ ОТ АДМИНИСТРАТОРА\n━━━━━━━━━━━━━━━━━━\n{text}")};let ok=self.send(target,&body,None);self.send(chat,if ok{"✅ Сообщение отправлено."}else{"❌ Не удалось отправить сообщение."},Some(self.admin_kb(admin)));return true;}
        let parts=text.split_whitespace().collect::<Vec<_>>();let target=parts.first().and_then(|x|x.parse::<i64>().ok());if step=="temp_block"||step=="gift_req"{let value=parts.get(1).and_then(|x|x.parse::<i64>().ok()).unwrap_or(0);let Some(target)=target else{self.send(chat,"Формат: Telegram_ID число",None);return true;};if value<=0{self.send(chat,"Введи корректное положительное число.",None);return true;}s.step=None;if step=="temp_block"{if value>10080{self.send(chat,"Максимум 10080 минут.",Some(self.admin_kb(admin)));return true;}if !self.user_exists(target){self.send(chat,"❌ Пользователь не найден.",Some(self.admin_kb(admin)));return true;}self.confirm(chat,s,json!({"action":"temp_block","target":target,"minutes":value}),&format!("⏳ Заблокировать {target} на {value} минут?"));}else{if value>GIFT_REQUESTS_MAX{self.send(chat,&format!("Максимум {GIFT_REQUESTS_MAX} запросов."),Some(self.admin_kb(admin)));return true;}if !self.user_exists(target){self.send(chat,"❌ Пользователь не найден в базе. Он должен хотя бы раз написать боту.",Some(self.admin_kb(admin)));return true;}if target==ADMIN_ID||self.is_premium(target){self.send(chat,"У пользователя уже безлимит. Запросы не нужны.",Some(self.admin_kb(admin)));return true;}self.confirm(chat,s,json!({"action":"gift_requests","target":target,"amount":value}),&format!("🎁 Подарить {value} запросов пользователю {target}?"));}return true;}
        let Some(target)=target else{self.send(chat,"Введи числовой Telegram ID. Для отмены /cancel.",None);return true;};
        if (step=="message_user_id"||step=="admin2_add")&&!self.user_exists(target){self.send(chat,"❌ Пользователь не найден в базе. Он должен хотя бы раз написать боту.",None);return true;}
        if step=="admin2_add"&&target==ADMIN_ID{s.step=None;self.send(chat,"Это главный администратор.",Some(self.admin_kb(admin)));return true;}
        if (step=="premium_on"||step=="premium_off")&&target==ADMIN_ID{s.step=None;self.send(chat,"Администратору Premium не требуется.",Some(self.admin_kb(admin)));return true;}
        s.step=None;
        match step.as_str(){"card_id"=>{self.send(chat,&self.user_card(target),Some(self.admin_kb(admin)));},"message_user_id"=>{s.target_id=Some(target);s.step=Some("message_user_text".into());self.send(chat,"Теперь введи текст сообщения:",None);},"block"=>self.confirm(chat,s,json!({"action":"block","target":target}),&format!("🚫 Заблокировать навсегда {target}?")),"unblock"=>self.confirm(chat,s,json!({"action":"unban","target":target}),&format!("🔓 Разблокировать {target}?")),"ignore_add"=>self.confirm(chat,s,json!({"action":"ignore_add","target":target}),&format!("🚷 Добавить {target} в игнор?")),"ignore_remove"=>{let n=self.db.lock().ok().and_then(|db|db.execute("DELETE FROM ignore_list WHERE user_id=?1",[target]).ok()).unwrap_or(0);if n>0{self.moderation(target,"IGNORE_REMOVE","Удалён из игнор-листа");}self.send(chat,if n>0{"Удалён из игнор-листа."}else{"Пользователь не найден в игнор-листе."},Some(self.admin_kb(admin)));},"premium_on"=>self.confirm(chat,s,json!({"action":"premium_on","target":target}),&format!("👑 Выдать Premium пользователю {target}?")),"premium_off"=>self.confirm(chat,s,json!({"action":"premium_off","target":target}),&format!("❌ Снять Premium у {target}?")),"admin2_add"=>{let exists=self.db.lock().ok().and_then(|db|db.query_row("SELECT user_id FROM admins WHERE role='second' LIMIT 1",[],|r|r.get::<_,i64>(0)).optional().ok().flatten()).is_some();if exists{self.send(chat,"Второй администратор уже назначен. Сначала сними его.",Some(self.admin_kb(admin)));}else{self.confirm(chat,s,json!({"action":"admin2_add","target":target}),&format!("Назначить {target} вторым администратором?"));}},_=>{}}true
    }
    fn admin_action(&self,user:&Value,chat:i64,text:&str,s:&mut UserState){let id=user["id"].as_i64().unwrap_or(0);if self.admin_step(user,chat,text,s){return;}let perm=match text{"📊 Статистика"=>"stats","👥 Пользователи"=>"users","🪪 Карточка пользователя"=>"card","📈 Активность 7 дней"|"📈 Активность 30 дней"=>"activity","📋 Журнал нарушений"=>"violations","🚷 Игнор-лист"=>"ignore_view",_=>"main"};if text!="⬅️ Назад"&&!self.has_perm(id,perm){self.send(chat,"⛔ Недостаточно прав.",Some(self.main_kb(id)));return;}let kb=self.admin_kb(id);match text{
        "⬅️ Назад"=>{s.admin=false;s.step=None;self.send(chat,"Главное меню.",Some(self.main_kb(id)));},
        "📊 Статистика"=>{self.send(chat,&self.admin_stats(),Some(kb));},
        "👥 Пользователи"=>{s.step=Some("users_view".into());s.users_offset=0;self.send(chat,&self.users_page(0),Some(vec![vec!["◀️ Пред. страница".into(),"▶️ След. страница".into()]]));},
        "🪪 Карточка пользователя"=>{s.step=Some("card_id".into());self.send(chat,"Введи Telegram ID пользователя:",None);},
        "📋 Журнал нарушений"=>{self.send(chat,&self.moderation_history_text(),Some(kb));},
                "📈 Активность 7 дней"|"📈 Активность 30 дней"=>{let days=if text.contains("7"){7}else{30};let report=self.activity_report(days);self.send(chat,&report,Some(kb));},
        "🚫 Заблокировать"=>{s.step=Some("block".into());self.send(chat,"Введи ID пользователя для постоянной блокировки:",None);},
        "🔓 Разблокировать"=>{s.step=Some("unblock".into());self.send(chat,"Введи ID пользователя:",None);},
        "⏳ Временная блокировка"=>{s.step=Some("temp_block".into());self.send(chat,"Введи ID и минуты через пробел (максимум 10080):",None);},
        "🚷 Игнор-лист"=>{let rows=self.db.lock().ok().and_then(|db|db.prepare("SELECT user_id,added_at,reason FROM ignore_list ORDER BY added_at DESC LIMIT 100").and_then(|mut st|st.query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?))).and_then(|x|x.collect::<rusqlite::Result<Vec<_>>>())).ok()).unwrap_or_default();let body=if rows.is_empty(){"🚷 ИГНОР-ЛИСТ ПУСТ.".into()}else{format!("🚷 ИГНОР-ЛИСТ\n━━━━━━━━━━━━━━━━━━\n{}",rows.iter().map(|(u,d,r)|format!("ID: {u}\nПричина: {}\nДобавлен: {d}",if r.is_empty(){"Не указана"}else{r})).collect::<Vec<_>>().join("\n\n"))};self.send(chat,&body,Some(kb));},
        "➕ В игнор"=>{s.step=Some("ignore_add".into());self.send(chat,"Введи Telegram ID:",None);},"➖ Из игнора"=>{s.step=Some("ignore_remove".into());self.send(chat,"Введи Telegram ID:",None);},
        "👑 Выдать Premium"=>{s.step=Some("premium_on".into());self.send(chat,"Введи Telegram ID:",None);},"❌ Снять Premium"=>{s.step=Some("premium_off".into());self.send(chat,"Введи Telegram ID:",None);},
        "✉️ Написать пользователю"=>{s.step=Some("message_user_id".into());self.send(chat,"Введи Telegram ID:",None);},"📢 Массовая рассылка"=>{s.step=Some("broadcast_text".into());self.send(chat,"Введи текст рассылки:",None);},
        "💾 Резервная копия"=>{self.send(chat,"⏳ Создаю резервную копию...",None);match self.create_backup(){Ok((path,size))=>{self.send(chat,&format!("✅ РЕЗЕРВНАЯ КОПИЯ СОЗДАНА\n━━━━━━━━━━━━━━━━━━\n📁 Файл: {path}\n📦 Размер: {} КБ\n🕒 Время: {}\n\nОсновная база не изменена.",(size/1024).max(1),now_iso()),Some(kb));self.action_log(user,"BACKUP CREATED");},Err(e)=>{log_line("ERROR",&format!("backup failed: {e}"));self.send(chat,"❌ Не удалось создать резервную копию. Подробности записаны в лог.",Some(kb));}}},
        "🛠 Права 2-го админа"=>{s.step=Some("perm_toggle".into());self.admin_step(user,chat,"",s);},
        "➕ Назначить второго администратора"=>{s.step=Some("admin2_add".into());self.send(chat,"Введи Telegram ID второго администратора:",None);},
        "➖ Снять второго администратора"=>{let second=self.db.lock().ok().and_then(|db|db.query_row("SELECT user_id FROM admins WHERE role='second' LIMIT 1",[],|r|r.get::<_,i64>(0)).optional().ok().flatten());if let Some(target)=second{self.confirm(chat,s,json!({"action":"admin2_remove","target":target}),&format!("Снять второго администратора {target}?"));}else{self.send(chat,"Второй администратор не назначен.",Some(kb));}},
        "🎁 Подарить запросы"=>{s.step=Some("gift_req".into());self.send(chat,"Введи Telegram ID и количество через пробел:",None);},
        "🤖 Разбан автобанов"=>{let report=self.autobanned_text();if report=="🤖 Забаненных автобаном нет."{self.send(chat,&report,Some(kb));}else{self.confirm(chat,s,json!({"action":"unban_auto"}),&report);}},
        _=>{self.send(chat,"Используй кнопки админ-панели.",Some(kb));}
    }}
}

fn main(){
    let token=env::var("BOT_TOKEN").unwrap_or_default().trim().to_string();let password=env::var("ADMIN_PASSWORD").unwrap_or_default().trim().to_string();
    if token.is_empty()||!token.contains(':'){eprintln!("❌ Добавь секрет BOT_TOKEN в настройках GitHub репозитория.");return;}
    if password.is_empty(){eprintln!("❌ Добавь секрет ADMIN_PASSWORD в настройках GitHub репозитория.");return;}
    let app=match App::new(token,password){Ok(a)=>Arc::new(a),Err(e)=>{eprintln!("Database startup error: {e}");return;}};
    println!("USERNAME GENERATOR RUST\nКанал: {CHANNEL}\nБаза данных: {DB_FILE}\nUsername в день: {DAILY_LIMIT}\nЗапросов в день: {DAILY_REQUEST_LIMIT}\nМаксимум за запрос: {GENERATION_SIZE}\nДлина username: 5 английских букв\nАнтифлуд: {FLOOD_LIMIT} сообщений за {} сек. = автобан\nАвтобан за одинаковые сообщения: отключён\nАдминистраторы исключены из антиспама\nPremium: UNLIMITED\nPremium за подарок: {PREMIUM_GIFT_EMOJI} {PREMIUM_GIFT_STARS} ⭐",FLOOD_WINDOW as i64);
    let mut delay=3u64;
    loop {match app.api("getMe",json!({})){Ok(me)=>{println!("✅ Бот подключён: @{}",me["username"].as_str().unwrap_or("unknown"));break;},Err(e)=>{log_line("WARN",&format!("getMe: {e}"));if e.starts_with("401|"){eprintln!("❌ Токен недействителен.");return;}thread::sleep(Duration::from_secs(delay));delay=(delay*2).min(30);}}}
    println!("ВАЖНО: добавь бота администратором канала {CHANNEL}");
    let mut offset:i64=0;let mut failures=0u32;
    loop {
        match app.api("getUpdates",json!({"offset":offset,"timeout":25,"allowed_updates":["message","business_connection","business_message"]})) {
            Ok(updates)=>{failures=0;if let Some(list)=updates.as_array(){for update in list{offset=update["update_id"].as_i64().unwrap_or(offset)+1;if !update["business_message"].is_null(){let a=app.clone();let m=update["business_message"].clone();if std::panic::catch_unwind(std::panic::AssertUnwindSafe(||a.business_message(&m))).is_err(){log_line("ERROR","panic while processing business gift");}continue;}if !update["business_connection"].is_null(){log_line("INFO",&format!("Business connection enabled={}",update["business_connection"]["is_enabled"]));continue;}if !update["message"].is_null(){let a=app.clone();let m=update["message"].clone();if let Err(e)=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||a.handle(&m))){let _=e;log_line("ERROR","panic while processing message");}}}}},
            Err(e)=>{failures+=1;let parts=e.splitn(3,'|').collect::<Vec<_>>();if parts.first()==Some(&"401"){eprintln!("❌ Токен недействителен. Бот остановлен.");break;}if parts.first()==Some(&"429"){thread::sleep(Duration::from_secs(parts.get(1).and_then(|x|x.parse().ok()).unwrap_or(5).min(60)));}else{log_line("WARN",&format!("getUpdates error: {e}"));thread::sleep(Duration::from_secs((3*failures as u64).min(30)));}}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generation_modes_match_python_rules() {
        for _ in 0..500 {
            let word=generate_one("words"); assert_eq!(word.len(),5); assert!(word.bytes().all(|c|c.is_ascii_lowercase()));
            let letters=generate_one("letters"); assert_eq!(letters.len(),5); assert!(letters.bytes().all(|c|c.is_ascii_lowercase()));
            let mixed=generate_one("letters_digits"); assert_eq!(mixed.len(),5); assert!(mixed.as_bytes()[0].is_ascii_lowercase()); assert!(mixed.bytes().all(|c|c.is_ascii_lowercase()||c.is_ascii_digit()));
        }
    }
    #[test]
    fn split_message_respects_limit() {
        let chunks=split_message(&"x".repeat(9000),3800); assert!(chunks.iter().all(|x|x.chars().count()<=3800)); assert_eq!(chunks.iter().map(|x|x.len()).sum::<usize>(),9000);
    }
    #[test]
    fn manual_favorite_validation_matches_python_rules() {
        assert!(valid_username("abcde")); assert!(valid_username("abcd1")); assert!(valid_username("a1b2c")); assert!(!valid_username("1abcd")); assert!(!valid_username("abc$e")); assert!(!valid_username("abcdef")); assert!(!valid_username("abcd"));
    }
    #[test]
    fn anti_flood_threshold_is_five_messages_in_three_seconds() {
        let mut events=HashMap::new(); for i in 0..4 { assert_eq!(record_flood(&mut events,7,100.0+i as f64*0.5),i+1); }
        assert_eq!(record_flood(&mut events,7,102.0),5);
    }
    #[test]
    fn old_messages_outside_the_three_second_window_are_discarded() {
        let mut events=HashMap::new(); for i in 0..4 { record_flood(&mut events,7,10.0+i as f64*0.5); }
        assert_eq!(record_flood(&mut events,7,15.0),1);
    }
}