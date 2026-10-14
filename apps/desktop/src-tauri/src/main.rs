// Fling desktop backend (Tauri v2).
//
// The Rust side owns:
// - config file (server URL + API key/secret + device identity + download dir),
// - LiveKit token minting (HS256 JWT — the secret never enters the webview),
// - mDNS nearby discovery (service type `_fling._tcp.local.`),
// - clipboard image writes,
// - small OS helpers (folder picker, open folder, write file).
// Text clipboard goes through the official Tauri clipboard plugin.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc;
use std::sync::Mutex;
use tauri::{Emitter, Manager};

mod embedded_config;

// ================================================================ config ==

/// 16 random bytes as lowercase hex. Generated once on first run, never rotated.
fn new_device_id() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn default_device_name() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Fling-PC".to_owned())
}

fn default_download_dir() -> String {
    let base = dirs::download_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join("Fling");
    let _ = std::fs::create_dir_all(&dir);
    dir.to_string_lossy().into_owned()
}

#[derive(Serialize, Deserialize, Clone)]
struct FlingConfig {
    // 兼容手写配置：CLI/文档用驼峰，桌面端设置页用下划线，两边都接受
    #[serde(alias = "serverUrl")]
    server_url: String,
    #[serde(alias = "apiKey")]
    api_key: String,
    #[serde(alias = "apiSecret")]
    api_secret: String,
    #[serde(default = "new_device_id")]
    device_id: String,
    #[serde(default = "default_device_name")]
    device_name: String,
    #[serde(default = "default_download_dir")]
    download_dir: String,
    #[serde(default, alias = "turnUrl")]
    turn_url: Option<String>,
    #[serde(default, alias = "turnUsername")]
    turn_username: Option<String>,
    #[serde(default, alias = "turnPassword")]
    turn_password: Option<String>,
}

/// 默认配置：只有设备身份，没有服务器凭据（用户需在设置页填写或手写 config.json）
fn default_config() -> FlingConfig {
    FlingConfig {
        server_url: String::new(),
        api_key: String::new(),
        api_secret: String::new(),
        device_id: new_device_id(),
        device_name: default_device_name(),
        download_dir: default_download_dir(),
        turn_url: None,
        turn_username: None,
        turn_password: None,
    }
}

fn config_path(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("config dir: {e}"))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    Ok(dir.join("config.json"))
}

fn write_config_file(p: &std::path::Path, cfg: &FlingConfig) -> Result<(), String> {
    let s = serde_json::to_string_pretty(&cfg).map_err(|e| format!("encode: {e}"))?;
    std::fs::write(p, s).map_err(|e| format!("write: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Old config files may lack the v0.3 fields; fill defaults and write back.
fn needs_migration(raw: &str) -> bool {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(v) => ["device_id", "device_name", "download_dir"]
            .iter()
            .any(|k| v.get(k).is_none()),
        Err(_) => false,
    }
}

/// Serializes config-file writers so concurrent settings changes can't tear the file.
struct ConfigLock(Mutex<()>);

fn with_config_lock<T>(app: &tauri::AppHandle, f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    let state = app.state::<ConfigLock>();
    let _guard = state
        .0
        .lock()
        .map_err(|e| format!("lock: {e}"))?;
    f()
}

#[tauri::command]
fn get_config(app: tauri::AppHandle) -> Result<Option<FlingConfig>, String> {
    let cfg = effective_config(&app)?;
    // 没有服务器凭据（本地和嵌入都没有）视为"未配置"
    if cfg.server_url.trim().is_empty()
        || cfg.api_key.trim().is_empty()
        || cfg.api_secret.trim().is_empty()
    {
        return Ok(None);
    }
    Ok(Some(cfg))
}

/// Load the config, creating it (device identity only) on first run.
/// 注意：只读写本地文件，不合并嵌入凭据（避免解密后的凭据被写回磁盘明文落盘）。
fn ensure_config(app: &tauri::AppHandle) -> Result<(std::path::PathBuf, FlingConfig), String> {
    let p = config_path(app)?;
    if p.exists() {
        let s = std::fs::read_to_string(&p).map_err(|e| format!("read: {e}"))?;
        let migrate = needs_migration(&s);
        let cfg: FlingConfig = match serde_json::from_str(&s) {
            Ok(c) => c,
            Err(e) => {
                // 配置文件损坏：备份坏文件后重建默认配置，保证应用能启动
                //（用户可在 config.json.bak 里找回原来的值）
                let bak = p.with_extension("json.bak");
                let _ = std::fs::rename(&p, &bak);
                let cfg = default_config();
                write_config_file(&p, &cfg)?;
                eprintln!(
                    "config parse failed ({}), backed up to {:?} and recreated default",
                    e, bak
                );
                return Ok((p, cfg));
            }
        };
        if migrate {
            let _ = write_config_file(&p, &cfg);
        }
        return Ok((p, cfg));
    }
    let cfg = default_config();
    write_config_file(&p, &cfg)?;
    Ok((p, cfg))
}

/// 生效配置 = 本地文件配置 + 嵌入加密凭据（内存合并，不写盘）。
/// 本地已有凭据优先（用户手动覆盖）；本地缺失则用嵌入的补。
fn effective_config(app: &tauri::AppHandle) -> Result<FlingConfig, String> {
    let (_, mut cfg) = ensure_config(app)?;
    if let Ok(e) = decrypt_embedded_config() {
        if cfg.server_url.trim().is_empty() { cfg.server_url = e.server_url; }
        if cfg.api_key.trim().is_empty() { cfg.api_key = e.api_key; }
        if cfg.api_secret.trim().is_empty() { cfg.api_secret = e.api_secret; }
        if cfg.turn_url.is_none() { cfg.turn_url = e.turn_url; }
        if cfg.turn_username.is_none() { cfg.turn_username = e.turn_username; }
        if cfg.turn_password.is_none() { cfg.turn_password = e.turn_password; }
    }
    Ok(cfg)
}

/// 解密编译时嵌入的服务器凭据（AES-256-GCM）。
/// 返回 FlingConfig（只取其中的服务器字段，设备字段忽略）。
fn decrypt_embedded_config() -> Result<FlingConfig, String> {
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    use aes_gcm::aead::{Aead, KeyInit};
    if !embedded_config::HAS_EMBEDDED || embedded_config::CONFIG_CIPHERTEXT.is_empty() {
        return Err("no embedded config".into());
    }
    let key = Key::<Aes256Gcm>::from_slice(&embedded_config::CONFIG_KEY);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&embedded_config::CONFIG_NONCE);
    let pt = cipher.decrypt(nonce, embedded_config::CONFIG_CIPHERTEXT)
        .map_err(|e| format!("decrypt embedded config: {e}"))?;
    serde_json::from_slice::<FlingConfig>(&pt).map_err(|e| format!("parse embedded config: {e}"))
}

#[tauri::command]
fn save_config(app: tauri::AppHandle, cfg: FlingConfig) -> Result<(), String> {
    if cfg.server_url.trim().is_empty()
        || cfg.api_key.trim().is_empty()
        || cfg.api_secret.trim().is_empty()
    {
        return Err("server_url, api_key and api_secret are all required".into());
    }
    with_config_lock(&app, || {
        let p = config_path(&app)?;
        // device_id is immutable once created — never rotate the identity.
        // device_name / download_dir prefer the incoming non-empty value.
        let merged = match std::fs::read_to_string(&p) {
            Ok(s) => match serde_json::from_str::<FlingConfig>(&s) {
                Ok(old) => FlingConfig {
                    server_url: cfg.server_url,
                    api_key: cfg.api_key,
                    api_secret: cfg.api_secret,
                    device_id: old.device_id,
                    device_name: if cfg.device_name.trim().is_empty() {
                        old.device_name
                    } else {
                        cfg.device_name
                    },
                    download_dir: if cfg.download_dir.trim().is_empty() {
                        old.download_dir
                    } else {
                        cfg.download_dir
                    },
                    // TURN 字段：新值非空则采用，否则保留旧值（缺省为 None）
                    turn_url: cfg.turn_url.filter(|s| !s.trim().is_empty()).or(old.turn_url),
                    turn_username: cfg.turn_username.filter(|s| !s.trim().is_empty()).or(old.turn_username),
                    turn_password: cfg.turn_password.filter(|s| !s.trim().is_empty()).or(old.turn_password),
                },
                Err(_) => cfg,
            },
            Err(_) => cfg,
        };
        write_config_file(&p, &merged)
    })
}

/// Mint a LiveKit join token for `room`/`identity` using the stored secret.
#[tauri::command]
fn mint_token(app: tauri::AppHandle, room: String, identity: String) -> Result<String, String> {
    let cfg = effective_config(&app)?;
    if cfg.api_secret.trim().is_empty() {
        return Err("no config — run setup first".to_string());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs() as usize;

    // LiveKit access token: HS256, iss = API key, video grant object.
    let claims = serde_json::json!({
        "iss": cfg.api_key,
        "sub": identity,
        "name": identity,
        "nbf": now.saturating_sub(5),
        "exp": now + 6 * 3600,
        "video": {
            "roomJoin": true,
            "room": room,
            "canPublish": true,
            "canSubscribe": true,
            "canPublishData": true,
        },
    });

    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(cfg.api_secret.as_bytes()),
    )
    .map_err(|e| format!("sign: {e}"))
}

#[tauri::command]
fn server_url(app: tauri::AppHandle) -> Result<String, String> {
    let cfg = effective_config(&app)?;
    if cfg.server_url.trim().is_empty() {
        return Err("no config — run setup first".to_string());
    }
    Ok(cfg.server_url)
}

/// Open a save dialog and return the chosen path (None = cancelled).
#[tauri::command]
async fn pick_save_path(app: tauri::AppHandle, name: String) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;
    let path = app
        .dialog()
        .file()
        .set_file_name(&name)
        .blocking_save_file();
    Ok(path.map(|p| p.to_string()))
}

/// Write base64 data to `path` (used for received files).
#[tauri::command]
fn write_binary_file(path: String, data_b64: String) -> Result<(), String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&data_b64)
        .map_err(|e| format!("base64: {e}"))?;
    std::fs::write(&path, bytes).map_err(|e| format!("write: {e}"))?;
    Ok(())
}

/// Save a received file into `dir`: sanitizes the name, never overwrites
/// (appends " (1)", " (2)", … on collision), returns the final path.
/// `rel_path`: 文件夹发送时的相对路径（如 "docs/report.pdf"），按此重建子目录；
/// 为空时保持旧行为（直接存 dir 下）。
#[tauri::command]
fn save_received_file(
    dir: String,
    name: String,
    data_b64: String,
    rel_path: Option<String>,
) -> Result<String, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&data_b64)
        .map_err(|e| format!("base64: {e}"))?;

    // 取 basename，去掉非法字符
    let base = std::path::Path::new(&name)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let clean: String = base
        .chars()
        .filter(|c| !matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        .collect();
    let clean = clean.trim();
    let clean = if clean.is_empty() { "file".to_string() } else { clean.to_string() };

    let dir_path = std::path::PathBuf::from(&dir);
    // 相对路径：逐段清洗，拒绝 ".." 与绝对路径，防止目录穿越
    let target_dir = match rel_path.as_deref() {
        Some(rel) if !rel.trim().is_empty() => {
            let mut sub = std::path::PathBuf::new();
            for comp in rel.replace('\\', "/").split('/') {
                let c = comp.trim();
                if c.is_empty() || c == "." || c == ".." {
                    continue;
                }
                let safe: String = c
                    .chars()
                    .filter(|ch| !matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
                    .collect();
                let safe = safe.trim();
                if !safe.is_empty() {
                    sub.push(safe);
                }
            }
            sub.pop(); // 去掉最后一段（文件名用 clean），剩下的是子目录
            let d = dir_path.join(&sub);
            std::fs::create_dir_all(&d).map_err(|e| format!("创建目录失败: {e}"))?;
            d
        }
        _ => {
            std::fs::create_dir_all(&dir).map_err(|e| format!("创建目录失败: {e}"))?;
            dir_path
        }
    };

    let mut candidate = target_dir.join(&clean);
    if candidate.exists() {
        let (stem, ext) = match clean.rfind('.') {
            Some(i) if i > 0 => (clean[..i].to_string(), clean[i..].to_string()),
            _ => (clean.clone(), String::new()),
        };
        let mut i = 1u32;
        loop {
            candidate = target_dir.join(format!("{stem} ({i}){ext}"));
            if !candidate.exists() || i >= 9999 {
                break;
            }
            i += 1;
        }
        if candidate.exists() {
            return Err("无法生成不重名的文件名".to_string());
        }
    }
    std::fs::write(&candidate, bytes).map_err(|e| format!("写入失败: {e}"))?;
    Ok(candidate.to_string_lossy().into_owned())
}

/// 传输历史落盘：每条记录一行 JSON（JSONL），存在配置目录的 history.jsonl。
/// 前端 localStorage 仍保留一份做快速展示，硬盘文件是完整存档（含保存路径）。
#[tauri::command]
fn append_history(app: tauri::AppHandle, line: String) -> Result<(), String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("config dir: {e}"))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let p = dir.join("history.jsonl");
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("open history: {e}"))?;
    writeln!(f, "{line}").map_err(|e| format!("write history: {e}"))?;
    Ok(())
}

// ============================================ device identity & download ==

#[derive(Serialize, Clone)]
struct DeviceInfo {
    device_id: String,
    device_name: String,
}

#[tauri::command]
fn get_device_info(app: tauri::AppHandle) -> Result<DeviceInfo, String> {
    let (_, cfg) = ensure_config(&app)?;
    Ok(DeviceInfo {
        device_id: cfg.device_id,
        device_name: cfg.device_name,
    })
}

#[tauri::command]
fn set_device_name(app: tauri::AppHandle, name: String) -> Result<(), String> {
    let name = name.trim().to_string();
    if name.is_empty() {
        return Err("设备名称不能为空".into());
    }
    if name.chars().count() > 64 {
        return Err("设备名称太长（最多 64 个字符）".into());
    }
    let (p, mut cfg) = ensure_config(&app)?;
    cfg.device_name = name.clone();
    with_config_lock(&app, || write_config_file(&p, &cfg))?;
    // Re-announce on mDNS so peers see the new name immediately.
    app.state::<DiscoveryState>().send(DiscoveryCmd::UpdateIdentity {
        device_id: cfg.device_id,
        device_name: name,
    });
    Ok(())
}

#[tauri::command]
fn get_download_dir(app: tauri::AppHandle) -> Result<String, String> {
    Ok(ensure_config(&app)?.1.download_dir)
}

#[tauri::command]
fn set_download_dir(app: tauri::AppHandle, path: String) -> Result<(), String> {
    let path = path.trim().to_string();
    if path.is_empty() {
        return Err("目录不能为空".into());
    }
    let dir = std::path::PathBuf::from(&path);
    if dir.exists() && !dir.is_dir() {
        return Err("该路径不是目录".into());
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("创建目录失败: {e}"))?;
    let (p, mut cfg) = ensure_config(&app)?;
    cfg.download_dir = dir.to_string_lossy().into_owned();
    with_config_lock(&app, || write_config_file(&p, &cfg))
}

/// Open a folder picker and return the chosen directory (None = cancelled).
#[tauri::command]
async fn pick_download_dir(app: tauri::AppHandle) -> Result<Option<String>, String> {
    use tauri_plugin_dialog::DialogExt;
    let path = app.dialog().file().blocking_pick_folder();
    Ok(path.map(|p| p.to_string()))
}

// ======================================================== mDNS discovery ==

const MDNS_SERVICE_TYPE: &str = "_fling._tcp.local.";
const FLING_PROTO_VERSION: &str = "3";

#[derive(Serialize, Clone)]
struct PeerUpPayload {
    id: String,
    name: String,
}

#[derive(Serialize, Clone)]
struct PeerDownPayload {
    id: String,
}

enum DiscoveryCmd {
    SetBrowsing(bool),
    SetDiscoverable(bool),
    UpdateIdentity { device_id: String, device_name: String },
}

struct DiscoveryState {
    tx: mpsc::Sender<DiscoveryCmd>,
}

impl DiscoveryState {
    fn send(&self, cmd: DiscoveryCmd) {
        let _ = self.tx.send(cmd);
    }
}

/// Parse a resolved service's TXT records into (device_id, device_name).
/// Returns None for non-Fling services (wrong or absent protocol version).
fn peer_from_txt(v: Option<&str>, id: Option<&str>, name: Option<&str>) -> Option<(String, String)> {
    if v? != FLING_PROTO_VERSION {
        return None;
    }
    let id = id?;
    if id.is_empty() {
        return None;
    }
    let name = name.filter(|n| !n.is_empty()).unwrap_or(id);
    Some((id.to_string(), name.to_string()))
}

/// Tracks known peers for up/down dedup. Keyed by mDNS fullname (which may
/// gain a " (2)" suffix on instance-name conflicts); identity is device_id.
#[derive(Default)]
struct PeerSet {
    by_fullname: HashMap<String, String>, // fullname -> device_id
    known_ids: HashSet<String>,
}

impl PeerSet {
    /// Returns true when this is a newly seen peer (caller emits peer-up).
    fn insert(&mut self, fullname: &str, id: &str) -> bool {
        self.by_fullname.insert(fullname.to_string(), id.to_string());
        self.known_ids.insert(id.to_string())
    }
    /// Returns the device_id when the peer was known (caller emits peer-down).
    fn remove(&mut self, fullname: &str) -> Option<String> {
        let id = self.by_fullname.remove(fullname)?;
        if !self.by_fullname.values().any(|v| v == &id) {
            self.known_ids.remove(&id);
        }
        Some(id)
    }
    fn clear(&mut self) -> Vec<String> {
        let ids: Vec<String> = self.known_ids.drain().collect();
        self.by_fullname.clear();
        ids
    }
}

fn register_fling(
    daemon: &mdns_sd::ServiceDaemon,
    device_id: &str,
    device_name: &str,
) -> Option<String> {
    let short = device_id.get(..12).unwrap_or(device_id);
    let host = format!("fling-{short}.local.");
    // Port 0: we offer no TCP service; mDNS is discovery-only here.
    let props = [
        ("id", device_id),
        ("v", FLING_PROTO_VERSION),
        ("name", device_name),
    ];
    let info = mdns_sd::ServiceInfo::new(MDNS_SERVICE_TYPE, device_name, &host, "", 0, &props[..])
        .ok()?
        .enable_addr_auto();
    // register() re-announces in place; no need to unregister first.
    let _ = daemon.register(info);
    // Expected fullname; on instance-name conflict the daemon appends " (2)",
    // in which case unregister() with this name is a best-effort no-op.
    Some(format!("{device_name}.{MDNS_SERVICE_TYPE}"))
}

fn discovery_thread(
    app: tauri::AppHandle,
    rx: mpsc::Receiver<DiscoveryCmd>,
    initial_id: String,
    initial_name: String,
) {
    use mdns_sd::ServiceEvent;

    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[fling] mDNS daemon init failed: {e}");
            return;
        }
    };

    let mut device_id = initial_id;
    let mut device_name = initial_name;
    let mut browsing = false;
    let mut discoverable = true; // default on
    let mut browse_rx: Option<mdns_sd::Receiver<ServiceEvent>> = None;
    // Initial announce (discoverable defaults to on).
    let mut registered_name = register_fling(&daemon, &device_id, &device_name);
    let mut peers = PeerSet::default();

    loop {
        match rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(DiscoveryCmd::SetBrowsing(on)) => {
                if on && !browsing {
                    match daemon.browse(MDNS_SERVICE_TYPE) {
                        Ok(r) => {
                            browse_rx = Some(r);
                            browsing = true;
                        }
                        Err(e) => eprintln!("[fling] mDNS browse failed: {e}"),
                    }
                } else if !on && browsing {
                    let _ = daemon.stop_browse(MDNS_SERVICE_TYPE);
                    browse_rx = None;
                    browsing = false;
                    for id in peers.clear() {
                        let _ = app.emit("fling://peer-down", PeerDownPayload { id });
                    }
                }
            }
            Ok(DiscoveryCmd::SetDiscoverable(on)) => {
                discoverable = on;
                if on {
                    registered_name = register_fling(&daemon, &device_id, &device_name);
                } else if let Some(name) = registered_name.take() {
                    let _ = daemon.unregister(&name);
                }
            }
            Ok(DiscoveryCmd::UpdateIdentity {
                device_id: id,
                device_name: name,
            }) => {
                device_id = id;
                device_name = name;
                if discoverable {
                    registered_name = register_fling(&daemon, &device_id, &device_name);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if let Some(brx) = browse_rx.as_ref() {
            for ev in brx.try_iter() {
                match ev {
                    ServiceEvent::ServiceResolved(info) => {
                        match peer_from_txt(
                            info.get_property_val_str("v"),
                            info.get_property_val_str("id"),
                            info.get_property_val_str("name"),
                        ) {
                            Some((id, name)) if id != device_id => {
                                if peers.insert(info.get_fullname(), &id) {
                                    let _ =
                                        app.emit("fling://peer-up", PeerUpPayload { id, name });
                                }
                            }
                            _ => {}
                        }
                    }
                    ServiceEvent::ServiceRemoved(_ty, fullname) => {
                        if let Some(id) = peers.remove(&fullname) {
                            let _ = app.emit("fling://peer-down", PeerDownPayload { id });
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let _ = daemon.shutdown();
}

#[tauri::command]
fn discovery_set_browsing(app: tauri::AppHandle, on: bool) -> Result<(), String> {
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::SetBrowsing(on));
    Ok(())
}

#[tauri::command]
fn discovery_set_discoverable(app: tauri::AppHandle, on: bool) -> Result<(), String> {
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::SetDiscoverable(on));
    Ok(())
}

// ======================================================= clipboard image ==

/// Write a base64-encoded PNG to the system clipboard as an image
/// (used for received clipboard-images; they must NOT become files).
/// 读取拖放进来的文件内容（base64）。走 Rust 命令而不是 fs 插件，
/// 避免 fs scope 挡住用户文件目录的任意路径。
#[tauri::command]
fn read_file_bytes(path: String) -> Result<String, String> {
    use base64::Engine;
    let meta = std::fs::metadata(&path).map_err(|e| format!("读取文件失败: {e}"))?;
    if !meta.is_file() {
        return Err("仅支持拖入文件，暂不支持文件夹".into());
    }
    let bytes = std::fs::read(&path).map_err(|e| format!("读取文件失败: {e}"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&bytes))
}

/// 取文件大小（不读内容，供拖放时懒加载：只记路径+大小，发送时才读）。
#[tauri::command]
fn file_size(path: String) -> Result<u64, String> {
    let meta = std::fs::metadata(&path).map_err(|e| format!("读取文件失败: {e}"))?;
    if !meta.is_file() {
        return Err("不是文件".into());
    }
    Ok(meta.len())
}

/// 判断路径是否为目录（供拖放时区分文件/文件夹）。
#[tauri::command]
fn is_dir(path: String) -> bool {
    std::path::Path::new(&path).is_dir()
}

/// 递归列出目录下所有文件：返回 [{ rel_path, size }]，rel_path 用 "/" 分隔、
/// 相对于 dir。供"添加文件夹"发送用（发送时再按需读文件内容）。
#[tauri::command]
fn list_dir_recursive(dir: String) -> Result<Vec<serde_json::Value>, String> {
    fn walk(
        base: &std::path::Path,
        cur: &std::path::Path,
        out: &mut Vec<serde_json::Value>,
    ) -> Result<(), String> {
        let rd = std::fs::read_dir(cur).map_err(|e| format!("读取目录失败: {e}"))?;
        for ent in rd {
            let ent = ent.map_err(|e| format!("读取目录项失败: {e}"))?;
            let path = ent.path();
            if path.is_dir() {
                if path.is_symlink() {
                    continue; // 跳过符号链接目录，避免循环
                }
                walk(base, &path, out)?;
            } else if path.is_file() {
                let rel = path
                    .strip_prefix(base)
                    .map_err(|e| format!("路径处理失败: {e}"))?
                    .to_string_lossy()
                    .replace('\\', "/");
                let size = ent.metadata().map(|m| m.len()).unwrap_or(0);
                out.push(serde_json::json!({ "rel_path": rel, "size": size }));
            }
        }
        Ok(())
    }
    let base = std::path::PathBuf::from(&dir);
    if !base.is_dir() {
        return Err("不是有效目录".to_string());
    }
    let mut out = Vec::new();
    walk(&base, &base, &mut out)?;
    Ok(out)
}

#[tauri::command]
fn write_image_clipboard(data_b64: String) -> Result<(), String> {
    use base64::Engine;
    let png = base64::engine::general_purpose::STANDARD
        .decode(&data_b64)
        .map_err(|e| format!("base64 解码失败: {e}"))?;
    let img = image::load_from_memory(&png)
        .map_err(|e| format!("图片解码失败: {e}"))?
        .to_rgba8();
    let (w, h) = (img.width() as usize, img.height() as usize);
    if w == 0 || h == 0 {
        return Err("图片尺寸无效".into());
    }
    let mut clip = arboard::Clipboard::new().map_err(|e| format!("打开剪贴板失败: {e}"))?;
    clip.set_image(arboard::ImageData {
        width: w,
        height: h,
        bytes: img.into_raw().into(),
    })
    .map_err(|e| format!("写入剪贴板失败: {e}"))?;
    Ok(())
}

// ============================================================= OS helpers ==

/// Open a folder in the system file manager (Windows Explorer / xdg-open).
#[tauri::command]
fn open_folder(path: String) -> Result<(), String> {
    open::that(&path).map_err(|e| format!("打开文件夹失败: {e}"))?;
    Ok(())
}

// ============================================================= diagnostics ==

/// 应用版本号（供前端展示/日志）。
#[tauri::command]
fn app_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 日志目录：<app_log_dir>/fling
fn log_dir(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    use tauri::Manager;
    let base = app
        .path()
        .app_log_dir()
        .map_err(|e| format!("日志目录不可用: {e}"))?;
    let dir = base.join("fling");
    std::fs::create_dir_all(&dir).map_err(|e| format!("创建日志目录失败: {e}"))?;
    Ok(dir)
}

#[tauri::command]
fn get_log_dir(app: tauri::AppHandle) -> Result<String, String> {
    Ok(log_dir(&app)?.to_string_lossy().into_owned())
}

/// 追加日志行（前端批量调用）。文件名由前端按天生成，
/// 这里只做严格校验；每次写入后清理，只保留最近 7 天。
#[tauri::command]
fn append_log(app: tauri::AppHandle, file: String, lines: Vec<String>) -> Result<(), String> {
    if file.len() > 32
        || !file.starts_with("fling-")
        || !file.ends_with(".log")
        || !file
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
    {
        return Err("非法日志文件名".into());
    }
    let dir = log_dir(&app)?;
    let path = dir.join(&file);
    let mut content = String::with_capacity(lines.iter().map(|l| l.len() + 1).sum());
    for l in &lines {
        // 单行截断，防止异常大行撑爆文件
        let t = l.chars().take(2000).collect::<String>();
        content.push_str(&t);
        content.push('\n');
    }
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("写日志失败: {e}"))?;
    f.write_all(content.as_bytes())
        .map_err(|e| format!("写日志失败: {e}"))?;

    // 保留最近 7 个日志文件
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let mut logs: Vec<_> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("fling-") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .collect();
        logs.sort();
        if logs.len() > 7 {
            for old in logs.iter().take(logs.len() - 7) {
                let _ = std::fs::remove_file(old);
            }
        }
    }
    Ok(())
}

// ================================================================== main ==

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_notification::init())
        .setup(|app| {
            // Device identity (created on first run) + mDNS discovery thread.
            let (_, cfg) = ensure_config(app.handle())?;
            let (tx, rx) = mpsc::channel::<DiscoveryCmd>();
            app.manage(ConfigLock(Mutex::new(())));
            app.manage(DiscoveryState { tx });
            let handle = app.handle().clone();
            let (device_id, device_name) = (cfg.device_id, cfg.device_name);
            std::thread::Builder::new()
                .name("fling-mdns".into())
                .spawn(move || discovery_thread(handle, rx, device_id, device_name))
                .map_err(|e| format!("mdns thread: {e}"))?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            mint_token,
            server_url,
            pick_save_path,
            write_binary_file,
            save_received_file,
            get_device_info,
            set_device_name,
            get_download_dir,
            set_download_dir,
            pick_download_dir,
            discovery_set_browsing,
            discovery_set_discoverable,
            write_image_clipboard,
            read_file_bytes,
            file_size,
            list_dir_recursive,
            is_dir,
            open_folder,
            app_version,
            get_log_dir,
            append_log,
            append_history
        ])
        .run(tauri::generate_context!())
        .expect("error while running fling");
}

// ================================================================= tests ==

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_id_is_32_hex_and_unique() {
        let a = new_device_id();
        let b = new_device_id();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn migration_detects_old_config() {
        assert!(needs_migration(
            r#"{"server_url":"u","api_key":"k","api_secret":"s"}"#
        ));
        assert!(!needs_migration(
            r#"{"server_url":"u","api_key":"k","api_secret":"s","device_id":"x","device_name":"y","download_dir":"z"}"#
        ));
        assert!(!needs_migration("not json"));
    }

    #[test]
    fn old_config_parses_with_defaults() {
        let cfg: FlingConfig =
            serde_json::from_str(r#"{"server_url":"u","api_key":"k","api_secret":"s"}"#).unwrap();
        assert_eq!(cfg.device_id.len(), 32);
        assert!(!cfg.device_name.is_empty());
        assert!(cfg.download_dir.ends_with("Fling"));
    }

    #[test]
    fn camel_case_config_parses() {
        // 手写配置（CLI/文档格式）用驼峰：必须能解析，不能启动崩溃
        let cfg: FlingConfig = serde_json::from_str(
            r#"{"serverUrl":"wss://x","apiKey":"k","apiSecret":"s","turnUrl":"turn:1.2.3.4:3478","turnUsername":"u","turnPassword":"p"}"#,
        )
        .unwrap();
        assert_eq!(cfg.server_url, "wss://x");
        assert_eq!(cfg.api_key, "k");
        assert_eq!(cfg.api_secret, "s");
        assert_eq!(cfg.turn_url.as_deref(), Some("turn:1.2.3.4:3478"));
        assert_eq!(cfg.turn_username.as_deref(), Some("u"));
        assert_eq!(cfg.turn_password.as_deref(), Some("p"));
    }

    #[test]
    fn peer_txt_parsing() {
        assert_eq!(
            peer_from_txt(Some("3"), Some("abc123"), Some("PC")),
            Some(("abc123".to_string(), "PC".to_string()))
        );
        // wrong / missing version -> ignored
        assert_eq!(peer_from_txt(Some("2"), Some("abc123"), Some("PC")), None);
        assert_eq!(peer_from_txt(None, Some("abc123"), Some("PC")), None);
        // missing / empty id -> ignored
        assert_eq!(peer_from_txt(Some("3"), None, Some("PC")), None);
        assert_eq!(peer_from_txt(Some("3"), Some(""), Some("PC")), None);
        // missing name falls back to id
        assert_eq!(
            peer_from_txt(Some("3"), Some("abc123"), None),
            Some(("abc123".to_string(), "abc123".to_string()))
        );
    }

    #[test]
    fn peer_set_dedup() {
        let mut ps = PeerSet::default();
        assert!(ps.insert("PC._fling._tcp.local.", "id1")); // new -> emit up
        assert!(!ps.insert("PC._fling._tcp.local.", "id1")); // dup -> suppressed
        assert!(!ps.insert("PC (2)._fling._tcp.local.", "id1")); // same id -> suppressed
        assert_eq!(
            ps.remove("PC._fling._tcp.local."),
            Some("id1".to_string())
        );
        // id still referenced by the "(2)" name
        assert_eq!(
            ps.remove("PC (2)._fling._tcp.local."),
            Some("id1".to_string())
        );
        assert_eq!(ps.remove("PC (2)._fling._tcp.local."), None);
        // down then up may emit again
        assert!(ps.insert("PC._fling._tcp.local.", "id1"));
    }

    #[test]
    fn peer_set_clear() {
        let mut ps = PeerSet::default();
        ps.insert("a._fling._tcp.local.", "id1");
        ps.insert("b._fling._tcp.local.", "id2");
        let mut cleared = ps.clear();
        cleared.sort();
        assert_eq!(cleared, vec!["id1".to_string(), "id2".to_string()]);
        assert!(ps.insert("a._fling._tcp.local.", "id1"));
    }

    #[test]
    fn default_download_dir_exists() {
        let d = default_download_dir();
        assert!(d.ends_with("Fling"));
        assert!(std::path::Path::new(&d).is_dir());
    }

    #[test]
    fn default_device_name_nonempty() {
        assert!(!default_device_name().is_empty());
    }
}
