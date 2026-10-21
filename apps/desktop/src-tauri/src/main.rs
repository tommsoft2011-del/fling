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
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use tauri::{Emitter, Manager};
use tauri_plugin_dialog::DialogExt;

static HISTORY_IO_LOCK: Mutex<()> = Mutex::new(());

mod discovery_diagnostics;
mod discovery_round;
mod embedded_config;
mod pairing_diagnostics;
mod transport_diagnostics;

#[allow(dead_code)]
mod native_lan;
mod native_lan_runtime;
mod online_crypto;
mod online_diagnostics;
mod online_p2p;
mod online_proto;
mod online_room;
mod online_session;
mod online_setup;
mod online_transfer;
mod online_source;
mod ui_events;
mod native_drop;
mod ice_syntax;
mod online_native;
mod online_route_choice;
mod online_route_runtime;
mod online_direct;
#[cfg(all(target_os="linux",debug_assertions))]
mod online_probe;
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
    #[serde(default, alias = "turnUrl", skip_serializing_if = "Option::is_none")]
    turn_url: Option<String>,
    #[serde(
        default,
        alias = "turnUsername",
        skip_serializing_if = "Option::is_none"
    )]
    turn_username: Option<String>,
    #[serde(
        default,
        alias = "turnPassword",
        skip_serializing_if = "Option::is_none"
    )]
    turn_password: Option<String>,
    #[serde(
        default,
        alias = "remoteConfigUrl",
        skip_serializing_if = "Option::is_none"
    )]
    remote_config_url: Option<String>,
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
        remote_config_url: None,
    }
}

fn config_path(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    // 便携版：exe 文件名含 "portable" 时，配置放 exe 旁边，跟着 U 盘走
    if let Ok(exe) = std::env::current_exe() {
        if let Some(name) = exe.file_name().and_then(|n| n.to_str()) {
            if name.to_lowercase().contains("portable") {
                if let Some(dir) = exe.parent() {
                    return Ok(dir.join("config.json"));
                }
            }
        }
    }
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

fn with_config_lock<T>(
    app: &tauri::AppHandle,
    f: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let state = app.state::<ConfigLock>();
    let _guard = state.0.lock().map_err(|e| format!("lock: {e}"))?;
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
    Ok(Some(config_for_webview(cfg)))
}

/// TURN secrets are native-only, including when get_config is used by settings.
fn config_for_webview(mut cfg: FlingConfig) -> FlingConfig {
    cfg.turn_url = None;
    cfg.turn_username = None;
    cfg.turn_password = None;
    cfg
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
    // 远程配置（服务器下发）优先于嵌入配置，低于本地手动配置
    if let Ok(remote) = app.state::<RemoteConfig>().0.lock() {
        if let Some(r) = remote.as_ref() {
            if cfg.server_url.trim().is_empty() {
                cfg.server_url = r.server_url.clone();
            }
            if cfg.api_key.trim().is_empty() {
                cfg.api_key = r.api_key.clone();
            }
            if cfg.api_secret.trim().is_empty() {
                cfg.api_secret = r.api_secret.clone();
            }
            if cfg.turn_url.is_none() {
                cfg.turn_url = r.turn_url.clone();
            }
            if cfg.turn_username.is_none() {
                cfg.turn_username = r.turn_username.clone();
            }
            if cfg.turn_password.is_none() {
                cfg.turn_password = r.turn_password.clone();
            }
        }
    }
    if let Ok(e) = decrypt_embedded_config() {
        if cfg.server_url.trim().is_empty() {
            cfg.server_url = e.server_url;
        }
        if cfg.api_key.trim().is_empty() {
            cfg.api_key = e.api_key;
        }
        if cfg.api_secret.trim().is_empty() {
            cfg.api_secret = e.api_secret;
        }
        if cfg.turn_url.is_none() {
            cfg.turn_url = e.turn_url;
        }
        if cfg.turn_username.is_none() {
            cfg.turn_username = e.turn_username;
        }
        if cfg.turn_password.is_none() {
            cfg.turn_password = e.turn_password;
        }
    }
    Ok(cfg)
}

/// 远程下发的加密配置（内存缓存，不写盘）
struct RemoteConfig(Mutex<Option<FlingConfig>>);

/// 获取嵌入配置中的远程配置 URL（前端用它拉取服务器配置）
#[tauri::command]
fn get_remote_config_url(app: tauri::AppHandle) -> Result<Option<String>, String> {
    match decrypt_embedded_config() {
        Ok(cfg) => Ok(cfg.remote_config_url.filter(|u| !u.trim().is_empty())),
        Err(_) => Ok(None),
    }
}

/// 应用从服务器拉取的加密配置（前端 fetch 后传 nonce/data，Rust 解密并缓存）
#[tauri::command]
fn apply_remote_config(
    app: tauri::AppHandle,
    nonce_b64: String,
    data_b64: String,
) -> Result<(), String> {
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    if !embedded_config::HAS_EMBEDDED {
        return Err("no embedded key".into());
    }
    let nonce = B64
        .decode(nonce_b64.trim())
        .map_err(|e| format!("nonce: {e}"))?;
    let data = B64
        .decode(data_b64.trim())
        .map_err(|e| format!("data: {e}"))?;
    let key = Key::<Aes256Gcm>::from_slice(&embedded_config::CONFIG_KEY);
    let cipher = Aes256Gcm::new(key);
    let pt = cipher
        .decrypt(Nonce::from_slice(&nonce), data.as_slice())
        .map_err(|e| format!("decrypt remote config: {e}"))?;
    let cfg: FlingConfig = serde_json::from_slice(&pt).map_err(|e| format!("parse: {e}"))?;
    *app.state::<RemoteConfig>()
        .0
        .lock()
        .map_err(|e| format!("lock: {e}"))? = Some(cfg);
    Ok(())
}

/// 解密编译时嵌入的服务器凭据（AES-256-GCM）。
/// 返回 FlingConfig（只取其中的服务器字段，设备字段忽略）。
fn decrypt_embedded_config() -> Result<FlingConfig, String> {
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    if !embedded_config::HAS_EMBEDDED || embedded_config::CONFIG_CIPHERTEXT.is_empty() {
        return Err("no embedded config".into());
    }
    let key = Key::<Aes256Gcm>::from_slice(&embedded_config::CONFIG_KEY);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&embedded_config::CONFIG_NONCE);
    let pt = cipher
        .decrypt(nonce, embedded_config::CONFIG_CIPHERTEXT)
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
                    turn_url: cfg
                        .turn_url
                        .filter(|s| !s.trim().is_empty())
                        .or(old.turn_url),
                    turn_username: cfg
                        .turn_username
                        .filter(|s| !s.trim().is_empty())
                        .or(old.turn_username),
                    turn_password: cfg
                        .turn_password
                        .filter(|s| !s.trim().is_empty())
                        .or(old.turn_password),
                    remote_config_url: cfg
                        .remote_config_url
                        .filter(|s| !s.trim().is_empty())
                        .or(old.remote_config_url),
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

// ============================================================ online ===
// Online (LiveKit) sessions in Rust. The WebView only displays; signaling
// and the data plane both live here.

/// Connect to a LiveKit room for online transfers. Returns a session id.
/// The frontend listens on `fling://online/event` for messages, progress,
/// and peer events.
#[tauri::command]
async fn online_connect(
    app: tauri::AppHandle,
    window: tauri::WebviewWindow,
    native_task: Option<online_native::NativeTaskConfig>,
    client_session_id: Option<String>,
    registry: tauri::State<'_, online_session::OnlineSessionRegistry>,
    server_url: String,
    token: String,
    pairing_code: String,
    key_context: String,
) -> Result<String, String> {
    let download_dir = get_download_dir(app.clone())?;
    let id = client_session_id.unwrap_or_else(|| format!("{:032x}", rand::random::<u128>()));
    if id.len() != 32 || !id.bytes().all(|b| b.is_ascii_hexdigit()) { return Err("invalid session id".into()); }
    if registry.get(&id).await.is_some() { return Err("session id already exists".into()); }
    // Snapshot the effective local/remote/embedded TURN configuration in Rust.
    // Never pass these credentials through IPC or diagnostics.
    let cfg = effective_config(&app)?;
    let ice_servers = online_p2p::default_ice_servers(
        cfg.turn_url.as_deref(), cfg.turn_username.as_deref(), cfg.turn_password.as_deref(),
    );
    let session = online_session::OnlineSession::connect(
        app,
        id.clone(),
        &server_url,
        &token,
        &pairing_code,
        &key_context,
        std::path::PathBuf::from(download_dir),
        window.label().into(),
        native_task,
        ice_servers,
    )
    .await?;
    registry.insert(id.clone(), session).await;
    Ok(id)
}

/// The only entrance to the Rust-owned disk-file path. No frontend path input.
#[tauri::command]
async fn online_pick_source(app: tauri::AppHandle, window: tauri::WebviewWindow) -> Result<Option<online_source::SourceMetadata>, String> {
    use tauri_plugin_dialog::DialogExt;
    let owner = window.label().to_string();
    tauri::async_runtime::spawn_blocking(move || {
        let chosen = app.dialog().file().set_title("选择一个文件（Rust 原生传输）").blocking_pick_file();
        let Some(chosen) = chosen else { return Ok(None); };
        let path = chosen.into_path().map_err(|_| "仅支持普通磁盘文件")?;
        app.state::<online_source::OnlineSources>().register(&owner, &path).map(Some)
    }).await.map_err(|e| format!("picker worker: {e}"))?
}

#[tauri::command]
async fn online_pick_sources(app: tauri::AppHandle, window: tauri::WebviewWindow, folders: bool) -> Result<Vec<online_source::SourceMetadata>, String> {
    let owner = window.label().to_string();
    tauri::async_runtime::spawn_blocking(move || {
        let dialog = app.dialog().file().set_title(if folders { "选择文件夹（Rust 原生文件源）" } else { "选择多个文件（Rust 原生文件源）" });
        let chosen = if folders { dialog.blocking_pick_folders() } else { dialog.blocking_pick_files() };
        let paths = chosen.map(|items| items.into_iter().map(|item| item.into_path().map_err(|_| "仅支持本地磁盘项目".to_string())).collect::<Result<Vec<_>, _>>()).transpose()?;
        if app.get_webview_window(&owner).is_none() { return Err("选择窗口已关闭".into()); }
        let registry = app.state::<online_source::OnlineSources>();
        let selected = registry.register_selection(&owner, paths)?;
        if app.get_webview_window(&owner).is_none() { registry.remove_owner(&owner); return Err("选择窗口已关闭".into()); }
        Ok(selected)
    }).await.map_err(|_| "文件选择工作线程失败")?
}
#[tauri::command]
async fn online_send_registered_source(window: tauri::WebviewWindow, sources: tauri::State<'_, online_source::OnlineSources>, registry: tauri::State<'_, online_session::OnlineSessionRegistry>, session_id: String, source_id: String) -> Result<String, String> {
    let session = registry.get(&session_id).await.ok_or("unknown online session")?;
    session.authorize(window.label())?;
    if session.native.is_some() { return Err("single-file task must use online_send_source".into()); }
    let lease = sources.acquire(window.label(), &source_id)?;
    let _busy = sources.busy_guard(window.label(), &source_id);
    session.send_registered_source(lease).await
}
#[tauri::command]
fn online_release_source(window: tauri::WebviewWindow, sources: tauri::State<'_, online_source::OnlineSources>, source_id: String) -> Result<(), String> {
    sources.remove(window.label(), &source_id)
}

#[tauri::command]
async fn online_send_source(window: tauri::WebviewWindow, sources: tauri::State<'_, online_source::OnlineSources>, registry: tauri::State<'_, online_session::OnlineSessionRegistry>, session_id: String, source_id: String) -> Result<online_native::NativeTaskStatus, String> {
    let session = registry.get(&session_id).await.ok_or("unknown online session")?;
    session.authorize(window.label())?;
    let lease = sources.acquire(window.label(), &source_id)?;
    let _busy = sources.busy_guard(window.label(), &source_id);
    session.send_source(lease).await
}

#[tauri::command]
async fn online_cancel_native(window: tauri::WebviewWindow, registry: tauri::State<'_, online_session::OnlineSessionRegistry>, session_id: String) -> Result<(), String> {
    let session = registry.get(&session_id).await.ok_or("unknown online session")?;
    session.authorize(window.label())?;
    session.cancel_native(None, true).await
}

/// Send a text message in an online session.
#[tauri::command]
async fn online_send_text(
    window: tauri::WebviewWindow,
    registry: tauri::State<'_, online_session::OnlineSessionRegistry>,
    session_id: String,
    text: String,
) -> Result<(), String> {
    let session = registry
        .get(&session_id)
        .await
        .ok_or_else(|| "unknown online session".to_string())?;
    session.authorize(window.label())?;
    if session.native.is_some() { return Err("native task accepts high-level actions only".into()); }
    session.send_text(text).await
}

/// Send a file in an online session. Returns the transfer id.
/// Progress arrives via `fling://online/event`.
#[tauri::command]
async fn online_send_file(
    window: tauri::WebviewWindow,
    registry: tauri::State<'_, online_session::OnlineSessionRegistry>,
    session_id: String,
    path: String,
    name: String,
    kind: String,
) -> Result<String, String> {
    let session = registry
        .get(&session_id)
        .await
        .ok_or_else(|| "unknown online session".to_string())?;
    let file_kind = match kind.as_str() {
        "clipboard-image" => online_proto::FileKind::ClipboardImage,
        _ => online_proto::FileKind::File,
    };
    session.authorize(window.label())?;
    if session.native.is_some() { return Err("native task accepts high-level actions only".into()); }
    let spill = std::path::Path::new(&path).canonicalize().map_err(|_| "兼容临时文件不可用")?;
    let temp = fling_temp_dir()?.canonicalize().map_err(|_| "兼容临时目录不可用")?;
    if spill.parent() != Some(temp.as_path()) { return Err("ordinary files require sourceId".into()); }
    session
        .send_file(&spill, name, file_kind)
        .await
}

/// Send an arbitrary Fling protocol message (offer, task-ack, etc.).
/// The message is a JSON value matching the `FlingMessage` schema.
#[tauri::command]
async fn online_send_message(
    window: tauri::WebviewWindow,
    registry: tauri::State<'_, online_session::OnlineSessionRegistry>,
    session_id: String,
    message: serde_json::Value,
) -> Result<(), String> {
    let session = registry
        .get(&session_id)
        .await
        .ok_or_else(|| "unknown online session".to_string())?;
    session.authorize(window.label())?;
    if session.native.is_some() { return Err("native task accepts high-level actions only".into()); }
    let msg: online_proto::FlingMessage =
        serde_json::from_value(message).map_err(|e| format!("bad Fling message: {e}"))?;
    session.send_raw(msg).await
}

/// Legacy memory-Blob compatibility only, NOT a Rust-owned ordinary file source.
/// Write in-memory bytes (clipboard image) to a temp file so
/// the Rust transfer engine can stream it. Returns the temp path.
#[tauri::command]
fn write_temp_file(_app: tauri::AppHandle, name: String, data: Vec<u8>) -> Result<String, String> {
    let dir = std::env::temp_dir().join("fling-online");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create temp dir: {e}"))?;
    // Sanitize the name to avoid path traversal.
    let safe_name: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '.' || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let path = dir.join(format!(
        "{:x}-{safe_name}",
        rand::random::<u64>()
    ));
    std::fs::write(&path, &data).map_err(|e| format!("write temp file: {e}"))?;
    Ok(path.to_string_lossy().to_string())
}

/// Directory that holds spilled in-memory files for Rust-side sending.
fn fling_temp_dir() -> Result<std::path::PathBuf, String> {
    let dir = std::env::temp_dir().join("fling-online");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create temp dir: {e}"))?;
    Ok(dir)
}

/// Begin a chunked spill: create an empty temp file and return its path.
/// Chunked append (below) replaces the old one-shot `write_temp_file`
/// IPC, whose whole-file JSON payload made large files impossible.
#[tauri::command]
fn online_temp_begin(name: String) -> Result<String, String> {
    let dir = fling_temp_dir()?;
    let safe_name: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '.' || c == '-' || c == '_' { c } else { '_' })
        .collect();
    let path = dir.join(format!("{:x}-{safe_name}", rand::random::<u64>()));
    std::fs::write(&path, b"").map_err(|e| format!("create temp file: {e}"))?;
    Ok(path.to_string_lossy().to_string())
}

/// Append one raw-bytes chunk to a temp file created by `online_temp_begin`.
/// The invoke payload is a raw octet-stream frame (Tauri 2 ships a bare
/// Uint8Array arg as application/octet-stream, no JSON/base64 in the path):
/// `[u32 LE path_len][path UTF-8 bytes][chunk bytes]`.
/// Only paths inside the spill directory are allowed.
#[tauri::command]
fn online_temp_append(request: tauri::ipc::Request<'_>) -> Result<(), String> {
    let body = match request.body() {
        tauri::ipc::InvokeBody::Raw(b) => b,
        tauri::ipc::InvokeBody::Json(_) => {
            return Err("temp append expects a raw bytes payload".to_string())
        }
    };
    if body.len() < 4 {
        return Err("temp append frame too short".to_string());
    }
    let path_len =
        u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if path_len > 4096 || body.len() < 4 + path_len {
        return Err("temp append frame malformed".to_string());
    }
    let path = std::str::from_utf8(&body[4..4 + path_len])
        .map_err(|_| "temp append path not utf8".to_string())?;
    let dir = fling_temp_dir()?;
    let target = std::path::PathBuf::from(path);
    let canonical_dir = dir.canonicalize().map_err(|e| format!("temp dir: {e}"))?;
    let parent = target
        .parent()
        .ok_or_else(|| "temp path has no parent".to_string())?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|e| format!("temp path parent: {e}"))?;
    if canonical_parent != canonical_dir {
        return Err("temp append path outside spill dir".to_string());
    }
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&target)
        .map_err(|e| format!("open temp for append: {e}"))?;
    f.write_all(&body[4 + path_len..])
        .map_err(|e| format!("append temp: {e}"))?;
    Ok(())
}

/// Disconnect an online session and release its resources.
#[tauri::command]
async fn online_disconnect(
    window: tauri::WebviewWindow,
    registry: tauri::State<'_, online_session::OnlineSessionRegistry>,
    session_id: String,
) -> Result<(), String> {
    if let Some(session) = registry.get(&session_id).await {
        session.authorize(window.label())?;
        registry.remove(&session_id).await;
        session.disconnect().await?;
    }
    Ok(())
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
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SaveReceivedFileResult {
    path: String,
    write_all_ms: f64,
    sync_all_ms: f64,
}

#[tauri::command]
fn save_received_file(
    dir: String,
    name: String,
    data: Vec<u8>,
    rel_path: Option<String>,
) -> Result<SaveReceivedFileResult, String> {
    let bytes = data;

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
    let clean = if clean.is_empty() {
        "file".to_string()
    } else {
        clean.to_string()
    };

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

    // First write and fsync a unique temporary file in the destination
    // directory. Then hard-link it to the first unused name: link creation is
    // atomic and fails rather than overwriting a file created concurrently.
    let (temp_path, mut temp_file) = loop {
        let temp = target_dir.join(format!(".fling-{}.tmp", new_device_id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
        {
            Ok(file) => break (temp, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("创建临时文件失败: {e}")),
        }
    };
    use std::io::Write;
    let write_started = std::time::Instant::now();
    if let Err(e) = temp_file.write_all(&bytes) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(format!("写入失败: {e}"));
    }
    let write_all_ms = write_started.elapsed().as_secs_f64() * 1000.0;
    let sync_started = std::time::Instant::now();
    if let Err(e) = temp_file.sync_all() {
        let _ = std::fs::remove_file(&temp_path);
        return Err(format!("写入失败: {e}"));
    }
    let sync_all_ms = sync_started.elapsed().as_secs_f64() * 1000.0;
    drop(temp_file);

    let (stem, ext) = match clean.rfind('.') {
        Some(i) if i > 0 => (clean[..i].to_string(), clean[i..].to_string()),
        _ => (clean.clone(), String::new()),
    };
    for i in 0..10_000u32 {
        let filename = if i == 0 {
            clean.clone()
        } else {
            format!("{stem} ({i}){ext}")
        };
        let candidate = target_dir.join(filename);
        match std::fs::hard_link(&temp_path, &candidate) {
            Ok(()) => {
                let _ = std::fs::remove_file(&temp_path);
                return Ok(SaveReceivedFileResult {
                    path: candidate.to_string_lossy().into_owned(),
                    write_all_ms,
                    sync_all_ms,
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Unsupported | std::io::ErrorKind::InvalidInput
                ) =>
            {
                // Some removable/network filesystems do not implement hard
                // links. Reserve the final path with create_new before copy;
                // this fallback still never overwrites a concurrent file.
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&candidate)
                {
                    Ok(mut destination) => {
                        let mut source = std::fs::File::open(&temp_path)
                            .map_err(|err| format!("重开临时文件失败: {err}"))?;
                        if let Err(err) = std::io::copy(&mut source, &mut destination)
                            .and_then(|_| destination.sync_all())
                        {
                            let _ = std::fs::remove_file(&candidate);
                            let _ = std::fs::remove_file(&temp_path);
                            return Err(format!("写入失败: {err}"));
                        }
                        let _ = std::fs::remove_file(&temp_path);
                        return Ok(SaveReceivedFileResult {
                            path: candidate.to_string_lossy().into_owned(),
                            write_all_ms,
                            sync_all_ms,
                        });
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => {
                        let _ = std::fs::remove_file(&temp_path);
                        return Err(format!("创建目标文件失败: {err}"));
                    }
                }
            }
            Err(e) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(format!("原子保存失败: {e}"));
            }
        }
    }
    let _ = std::fs::remove_file(&temp_path);
    Err("无法生成不重名的文件名".to_string())
}

fn received_file_target(
    dir: &str,
    name: &str,
    rel_path: Option<&str>,
) -> Result<(std::path::PathBuf, String), String> {
    let base = std::path::Path::new(name)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let clean: String = base
        .chars()
        .filter(|c| !matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
        .collect();
    let clean = clean.trim();
    let clean = if clean.is_empty() {
        "file".to_string()
    } else {
        clean.to_string()
    };
    let dir_path = std::path::PathBuf::from(dir);
    let target_dir = match rel_path {
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
            sub.pop();
            let target = dir_path.join(sub);
            std::fs::create_dir_all(&target).map_err(|e| format!("创建目录失败: {e}"))?;
            target
        }
        _ => {
            std::fs::create_dir_all(&dir_path).map_err(|e| format!("创建目录失败: {e}"))?;
            dir_path
        }
    };
    Ok((target_dir, clean))
}

fn create_received_temp_file(
    target_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, std::fs::File), String> {
    loop {
        let temp = target_dir.join(format!(".fling-{}.tmp", new_device_id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
        {
            Ok(file) => return Ok((temp, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("创建临时文件失败: {e}")),
        }
    }
}

fn commit_received_temp_file(
    temp_path: &std::path::Path,
    target_dir: &std::path::Path,
    clean: &str,
    write_all_ms: f64,
    sync_all_ms: f64,
) -> Result<SaveReceivedFileResult, String> {
    let (stem, ext) = match clean.rfind('.') {
        Some(i) if i > 0 => (clean[..i].to_string(), clean[i..].to_string()),
        _ => (clean.to_string(), String::new()),
    };
    for i in 0..10_000u32 {
        let filename = if i == 0 {
            clean.to_string()
        } else {
            format!("{stem} ({i}){ext}")
        };
        let candidate = target_dir.join(filename);
        match std::fs::hard_link(temp_path, &candidate) {
            Ok(()) => {
                let _ = std::fs::remove_file(temp_path);
                return Ok(SaveReceivedFileResult {
                    path: candidate.to_string_lossy().into_owned(),
                    write_all_ms,
                    sync_all_ms,
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Unsupported | std::io::ErrorKind::InvalidInput
                ) =>
            {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&candidate)
                {
                    Ok(mut destination) => {
                        let copied = (|| -> Result<(), String> {
                            let mut source = std::fs::File::open(temp_path)
                                .map_err(|err| format!("重开临时文件失败: {err}"))?;
                            std::io::copy(&mut source, &mut destination)
                                .map_err(|err| format!("写入失败: {err}"))?;
                            destination
                                .sync_all()
                                .map_err(|err| format!("写入失败: {err}"))?;
                            Ok(())
                        })();
                        if let Err(err) = copied {
                            let _ = std::fs::remove_file(&candidate);
                            let _ = std::fs::remove_file(temp_path);
                            return Err(err);
                        }
                        let _ = std::fs::remove_file(temp_path);
                        return Ok(SaveReceivedFileResult {
                            path: candidate.to_string_lossy().into_owned(),
                            write_all_ms,
                            sync_all_ms,
                        });
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => {
                        let _ = std::fs::remove_file(temp_path);
                        return Err(format!("创建目标文件失败: {err}"));
                    }
                }
            }
            Err(e) => {
                let _ = std::fs::remove_file(temp_path);
                return Err(format!("原子保存失败: {e}"));
            }
        }
    }
    let _ = std::fs::remove_file(temp_path);
    Err("无法生成不重名的文件名".to_string())
}

struct StreamingReceivedFile {
    temp_path: std::path::PathBuf,
    file: Option<std::fs::File>,
    target_dir: std::path::PathBuf,
    clean_name: String,
    expected_size: u64,
    expected_sha256: String,
    written: u64,
    hasher: Sha256,
    write_all_ms: f64,
}

const MAX_RECEIVED_FILE_BATCH_BYTES: usize = 16 * 1024 * 1024;

fn raw_append_request_parts(
    body: &tauri::ipc::InvokeBody,
    session_id: Option<&str>,
) -> Result<(String, Vec<u8>), String> {
    let session_id = session_id.ok_or_else(|| "missing x-session-id header".to_string())?;
    if session_id.len() != 32 || !session_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid receive file session id".to_string());
    }
    let data = match body {
        tauri::ipc::InvokeBody::Raw(data) => data,
        tauri::ipc::InvokeBody::Json(_) => {
            return Err("append payload must use raw binary IPC".to_string());
        }
    };
    if data.is_empty() || data.len() > MAX_RECEIVED_FILE_BATCH_BYTES {
        return Err(format!(
            "invalid receive batch size (got {}, max {})",
            data.len(),
            MAX_RECEIVED_FILE_BATCH_BYTES
        ));
    }
    Ok((session_id.to_string(), data.clone()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AppendReceivedFileResult {
    bytes: usize,
    cumulative_bytes: u64,
    write_elapsed_ms: f64,
}

impl Drop for StreamingReceivedFile {
    fn drop(&mut self) {
        self.file.take();
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

#[derive(Clone, Default)]
struct ReceivedFileStore {
    sessions: Arc<Mutex<HashMap<String, StreamingReceivedFile>>>,
}

impl ReceivedFileStore {
    fn begin(
        &self,
        dir: String,
        name: String,
        rel_path: Option<String>,
        expected_size: u64,
        expected_sha256: String,
    ) -> Result<String, String> {
        if expected_sha256.len() != 64 || !expected_sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid expected sha256".to_string());
        }
        let (target_dir, clean_name) = received_file_target(&dir, &name, rel_path.as_deref())?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("receive file lock: {e}"))?;
        let (temp_path, file) = create_received_temp_file(&target_dir)?;
        let session_id = loop {
            let id = new_device_id();
            if !sessions.contains_key(&id) {
                break id;
            }
        };
        sessions.insert(
            session_id.clone(),
            StreamingReceivedFile {
                temp_path,
                file: Some(file),
                target_dir,
                clean_name,
                expected_size,
                expected_sha256,
                written: 0,
                hasher: Sha256::new(),
                write_all_ms: 0.0,
            },
        );
        Ok(session_id)
    }

    fn append(&self, session_id: &str, data: &[u8]) -> Result<AppendReceivedFileResult, String> {
        let result = (|| -> Result<AppendReceivedFileResult, String> {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|e| format!("receive file lock: {e}"))?;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| "unknown receive file session".to_string())?;
            let next_size = session
                .written
                .checked_add(data.len() as u64)
                .ok_or_else(|| "received file size overflow".to_string())?;
            if next_size > session.expected_size {
                return Err("received more bytes than declared".to_string());
            }
            let started = std::time::Instant::now();
            session
                .file
                .as_mut()
                .ok_or_else(|| "receive file is already closed".to_string())?
                .write_all(data)
                .map_err(|e| format!("写入失败: {e}"))?;
            let write_elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            session.write_all_ms += write_elapsed_ms;
            session.hasher.update(data);
            session.written = next_size;
            Ok(AppendReceivedFileResult {
                bytes: data.len(),
                cumulative_bytes: next_size,
                write_elapsed_ms,
            })
        })();
        if result.is_err() {
            let _ = self.abort(session_id);
        }
        result
    }

    fn finish(&self, session_id: &str) -> Result<SaveReceivedFileResult, String> {
        let mut session = self
            .sessions
            .lock()
            .map_err(|e| format!("receive file lock: {e}"))?
            .remove(session_id)
            .ok_or_else(|| "unknown receive file session".to_string())?;
        if session.written != session.expected_size {
            return Err(format!(
                "received file size mismatch (got {}, want {})",
                session.written, session.expected_size
            ));
        }
        let digest = format!("{:x}", session.hasher.clone().finalize());
        if !digest.eq_ignore_ascii_case(&session.expected_sha256) {
            return Err("sha256 mismatch while saving received file".to_string());
        }
        let file = session
            .file
            .take()
            .ok_or_else(|| "receive file is already closed".to_string())?;
        let sync_started = std::time::Instant::now();
        file.sync_all().map_err(|e| format!("写入失败: {e}"))?;
        let sync_all_ms = sync_started.elapsed().as_secs_f64() * 1000.0;
        drop(file);
        commit_received_temp_file(
            &session.temp_path,
            &session.target_dir,
            &session.clean_name,
            session.write_all_ms,
            sync_all_ms,
        )
    }

    fn abort(&self, session_id: &str) -> Result<(), String> {
        let session = self
            .sessions
            .lock()
            .map_err(|e| format!("receive file lock: {e}"))?
            .remove(session_id);
        drop(session);
        Ok(())
    }
}

#[tauri::command]
async fn begin_received_file(
    store: tauri::State<'_, ReceivedFileStore>,
    dir: String,
    name: String,
    rel_path: Option<String>,
    expected_size: u64,
    expected_sha256: String,
) -> Result<String, String> {
    let store = store.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        store.begin(dir, name, rel_path, expected_size, expected_sha256)
    })
    .await
    .map_err(|e| format!("开始接收文件失败: {e}"))?
}

#[tauri::command]
async fn append_received_file_chunk(
    store: tauri::State<'_, ReceivedFileStore>,
    request: tauri::ipc::Request<'_>,
) -> Result<AppendReceivedFileResult, String> {
    let session_header = request
        .headers()
        .get("x-session-id")
        .and_then(|value| value.to_str().ok());
    let (session_id, data) = raw_append_request_parts(request.body(), session_header)?;
    let store = store.inner().clone();
    tauri::async_runtime::spawn_blocking(move || store.append(&session_id, &data))
        .await
        .map_err(|e| format!("写入接收文件失败: {e}"))?
}

#[tauri::command]
async fn finish_received_file(
    store: tauri::State<'_, ReceivedFileStore>,
    session_id: String,
) -> Result<SaveReceivedFileResult, String> {
    let store = store.inner().clone();
    tauri::async_runtime::spawn_blocking(move || store.finish(&session_id))
        .await
        .map_err(|e| format!("完成接收文件失败: {e}"))?
}

#[tauri::command]
async fn abort_received_file(
    store: tauri::State<'_, ReceivedFileStore>,
    session_id: String,
) -> Result<(), String> {
    let store = store.inner().clone();
    tauri::async_runtime::spawn_blocking(move || store.abort(&session_id))
        .await
        .map_err(|e| format!("清理接收文件失败: {e}"))?
}

fn history_path(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("config dir: {e}"))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    Ok(dir.join("history.jsonl"))
}

/// Load the durable transfer archive; localStorage is only a cache.
#[tauri::command]
fn read_history(app: tauri::AppHandle) -> Result<Vec<String>, String> {
    use std::io::BufRead;
    let _guard = HISTORY_IO_LOCK
        .lock()
        .map_err(|e| format!("history lock: {e}"))?;
    let path = history_path(&app)?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(&path).map_err(|e| format!("open history: {e}"))?;
    std::io::BufReader::new(file)
        .lines()
        .filter_map(|line| match line {
            Ok(value) if !value.trim().is_empty() => Some(Ok(value)),
            Ok(_) => None,
            Err(e) => Some(Err(format!("read history: {e}"))),
        })
        .collect()
}

/// Append one validated JSON record to the durable history archive.
#[tauri::command]
fn append_history(app: tauri::AppHandle, line: String) -> Result<(), String> {
    use std::io::Write;
    let _guard = HISTORY_IO_LOCK
        .lock()
        .map_err(|e| format!("history lock: {e}"))?;
    let value: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| format!("invalid history JSON: {e}"))?;
    if !value.is_object() {
        return Err("history record must be a JSON object".to_string());
    }
    let line = serde_json::to_string(&value).map_err(|e| format!("encode history: {e}"))?;
    let path = history_path(&app)?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("open history: {e}"))?;
    writeln!(f, "{line}").map_err(|e| format!("write history: {e}"))?;
    f.sync_data().map_err(|e| format!("sync history: {e}"))?;
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
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::UpdateIdentity {
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
    Refresh {
        ticket: discovery_round::RefreshTicket,
        reply: mpsc::SyncSender<Result<discovery_round::RoundCounts, String>>,
    },
    SetDiscoverable(bool),
    UpdateIdentity {
        device_id: String,
        device_name: String,
    },
}

/// Stop the old queryer before browsing again. mdns-sd serializes these daemon
/// commands, and a new browse replays matching records from its cache.
fn refresh_browse<T>(
    browsing: &mut bool,
    mut stop: impl FnMut() -> Result<(), String>,
    browse: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    if *browsing {
        stop().map_err(|e| format!("停止现有 mDNS browse 失败: {e}"))?;
        *browsing = false;
    }
    let receiver = browse().map_err(|e| format!("重新启动 mDNS browse 失败: {e}"))?;
    *browsing = true;
    Ok(receiver)
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
fn peer_from_txt(
    v: Option<&str>,
    id: Option<&str>,
    name: Option<&str>,
) -> Option<(String, String)> {
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

fn diagnostics_counts(
    c: &discovery_round::RoundCounts,
    resolved: u32,
) -> discovery_diagnostics::PhaseCounts {
    discovery_diagnostics::PhaseCounts {
        resolved_services: resolved,
        valid_services: c.valid_services,
        lan_eligible_peers: c.lan_eligible_peers,
        ui_emitted_peers: c.ui_emitted_peers,
    }
}

fn finish_refresh(
    ticket: &discovery_round::RefreshTicket,
    outcome: discovery_diagnostics::Outcome,
    reason: discovery_diagnostics::Reason,
) -> bool {
    if !ticket.finish_once() {
        return false;
    }
    discovery_diagnostics::record_for_round(
        ticket.round_id(),
        ticket.started().elapsed().as_millis() as u64,
        discovery_diagnostics::Event::RefreshOutcome {
            outcome,
            reason,
            counts: ticket.counts(),
        },
    );
    true
}

/// Tracks known peers for up/down dedup. Keyed by mDNS fullname (which may
/// gain a " (2)" suffix on instance-name conflicts); identity is device_id.
#[derive(Default)]
struct LanPublishedSet(HashSet<String>);
impl LanPublishedSet {
    fn should_emit(&self, id: &str, enabled: bool, eligible: bool) -> bool {
        enabled && eligible && !self.0.contains(id)
    }
    fn mark_emitted(&mut self, id: &str, success: bool) {
        if success {
            self.0.insert(id.to_string());
        }
    }
    fn remove(&mut self, id: &str) {
        self.0.remove(id);
    }
    fn clear(&mut self) {
        self.0.clear();
    }
}

#[derive(Default)]
struct PeerSet {
    by_fullname: HashMap<String, String>, // fullname -> device_id
    known_ids: HashSet<String>,
}

impl PeerSet {
    /// Returns true when this is a newly seen peer (caller emits peer-up).
    fn insert(&mut self, fullname: &str, id: &str) -> bool {
        self.by_fullname
            .insert(fullname.to_string(), id.to_string());
        self.known_ids.insert(id.to_string())
    }
    /// Returns the device_id when the peer was known (caller emits peer-down).
    fn remove(&mut self, fullname: &str) -> Option<String> {
        let id = self.by_fullname.remove(fullname)?;
        if self.by_fullname.values().any(|v| v == &id) {
            return None;
        }
        self.known_ids.remove(&id);
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
    native_port: u16,
) -> Option<String> {
    let short = device_id.get(..12).unwrap_or(device_id);
    let instance = format!("fling-{short}");
    let host = format!("fling-{short}.local.");
    // The TCP listener is Rust-owned and only carries pairing/signaling/control.
    let props = [
        ("id", device_id),
        ("v", FLING_PROTO_VERSION),
        ("name", device_name),
    ];
    let info = mdns_sd::ServiceInfo::new(
        MDNS_SERVICE_TYPE,
        &instance,
        &host,
        "",
        native_port,
        &props[..],
    )
    .ok()?
    .enable_addr_auto();
    // register() re-announces in place; no need to unregister first.
    let _ = daemon.register(info);
    Some(format!("{instance}.{MDNS_SERVICE_TYPE}"))
}

fn discovery_thread(
    app: tauri::AppHandle,
    rx: mpsc::Receiver<DiscoveryCmd>,
    initial_id: String,
    initial_name: String,
    native_port: u16,
) {
    use mdns_sd::ServiceEvent;

    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            discovery_diagnostics::record(discovery_diagnostics::Event::DaemonInitFailed);
            eprintln!("[fling] mDNS daemon init failed: {e}");
            return;
        }
    };
    discovery_diagnostics::record(discovery_diagnostics::Event::DaemonInitSucceeded);
    let local_name = default_device_name();
    discovery_diagnostics::record(discovery_diagnostics::Event::ComputerName {
        local: true,
        name: local_name.clone(),
    });

    let mut device_id = initial_id;
    let mut device_name = initial_name;
    let mut browsing = false;
    let mut discoverable = true; // default on
    let mut browse_rx: Option<mdns_sd::Receiver<ServiceEvent>> = None;
    // Initial announce (discoverable defaults to on).
    let mut registered_name = register_fling(&daemon, &device_id, &device_name, native_port);
    let mut peers = PeerSet::default();
    let mut lan_published_ids = LanPublishedSet::default();
    let mut refresh_round: Option<(
        u64,
        std::time::Instant,
        discovery_round::DiscoveryRound,
        mpsc::SyncSender<Result<discovery_round::RoundCounts, String>>,
        discovery_round::RefreshTicket,
    )> = None;

    loop {
        // A caller timeout ends observation only, not automatic discovery or transfers.
        if refresh_round
            .as_ref()
            .is_some_and(|(_, _, _, _, ticket)| ticket.is_terminal())
        {
            refresh_round.take();
        }
        match rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(DiscoveryCmd::SetBrowsing(on)) => {
                if on && !browsing {
                    match daemon.browse(MDNS_SERVICE_TYPE) {
                        Ok(r) => {
                            browse_rx = Some(r);
                            browsing = true;
                            discovery_diagnostics::record(
                                discovery_diagnostics::Event::BrowsingStarted,
                            );
                        }
                        Err(e) => eprintln!("[fling] mDNS browse failed: {e}"),
                    }
                } else if !on && browsing {
                    let _ = daemon.stop_browse(MDNS_SERVICE_TYPE);
                    discovery_diagnostics::record(discovery_diagnostics::Event::BrowseStopped);
                    browse_rx = None;
                    browsing = false;
                    let lan_enabled = app
                        .state::<native_lan_runtime::NativeLanRuntime>()
                        .is_enabled();
                    app.state::<native_lan_runtime::NativeLanRuntime>()
                        .clear_peers();
                    lan_published_ids.clear();
                    if let Some((_, _, tracker, reply, ticket)) = refresh_round.take() {
                        ticket.update_counts(diagnostics_counts(
                            &tracker.counts(),
                            tracker.resolved_services(),
                        ));
                        finish_refresh(
                            &ticket,
                            discovery_diagnostics::Outcome::Error,
                            discovery_diagnostics::Reason::ModeStopped,
                        );
                        let _ = reply.send(Err("mDNS browsing stopped during refresh".to_string()));
                    }
                    for id in peers.clear() {
                        let payload = PeerDownPayload { id };
                        let _ = app.emit("fling://peer-down", payload.clone());
                        if lan_enabled {
                            let runtime = app.state::<native_lan_runtime::NativeLanRuntime>();
                            runtime.count_ipc(
                                "event:fling://lan/peer-down",
                                serde_json::to_vec(&payload)
                                    .map(|value| value.len())
                                    .unwrap_or_default(),
                            );
                            let success = app.emit("fling://lan/peer-down", payload).is_ok();
                            discovery_diagnostics::record(
                                discovery_diagnostics::Event::LanUiEvent { up: false, success },
                            );
                        }
                    }
                }
            }
            Ok(DiscoveryCmd::Refresh { ticket, reply }) => {
                let round_id = ticket.round_id();
                if ticket.is_terminal() {
                    continue;
                }
                if refresh_round.is_some() {
                    finish_refresh(
                        &ticket,
                        discovery_diagnostics::Outcome::Error,
                        discovery_diagnostics::Reason::Overlap,
                    );
                    let _ = reply.send(Err("LAN discovery refresh is already running".to_string()));
                    continue;
                }
                discovery_diagnostics::activate_round_at(round_id, ticket.started());
                let refreshed = refresh_browse(
                    &mut browsing,
                    || {
                        daemon
                            .stop_browse(MDNS_SERVICE_TYPE)
                            .map_err(|_| "stop failed".to_string())
                    },
                    || {
                        daemon
                            .browse(MDNS_SERVICE_TYPE)
                            .map_err(|_| "browse failed".to_string())
                    },
                );
                match refreshed {
                    Ok(receiver) => {
                        browse_rx = Some(receiver);
                        let now = ticket.started();
                        refresh_round = Some((
                            round_id,
                            now,
                            discovery_round::DiscoveryRound::new(now),
                            reply,
                            ticket,
                        ));
                    }
                    Err(error) => {
                        let reason = if browsing {
                            discovery_diagnostics::Reason::StopFailed
                        } else {
                            discovery_diagnostics::Reason::BrowseFailed
                        };
                        finish_refresh(&ticket, discovery_diagnostics::Outcome::Error, reason);
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Ok(DiscoveryCmd::SetDiscoverable(on)) => {
                discoverable = on;
                if on {
                    registered_name =
                        register_fling(&daemon, &device_id, &device_name, native_port);
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
                    registered_name =
                        register_fling(&daemon, &device_id, &device_name, native_port);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if let Some(brx) = browse_rx.as_ref() {
            loop {
                let ev = match brx.try_recv() {
                    Ok(ev) => ev,
                    Err(_) if !brx.is_disconnected() => break,
                    Err(_) => {
                        if let Some((_, _, tracker, reply, ticket)) = refresh_round.take() {
                            ticket.update_counts(diagnostics_counts(
                                &tracker.counts(),
                                tracker.resolved_services(),
                            ));
                            finish_refresh(
                                &ticket,
                                discovery_diagnostics::Outcome::Error,
                                discovery_diagnostics::Reason::BrowseDisconnected,
                            );
                            let _ = reply.send(Err("mDNS browse receiver disconnected".into()));
                        }
                        browse_rx = None;
                        browsing = false;
                        break;
                    }
                };
                match ev {
                    ServiceEvent::SearchStarted(_) => {
                        discovery_diagnostics::record(
                            discovery_diagnostics::Event::BrowseConfirmed,
                        );
                        if let Some((_, _, tracker, _, _)) = refresh_round.as_mut() {
                            tracker.confirm(std::time::Instant::now());
                        }
                    }
                    ServiceEvent::ServiceResolved(info) => {
                        discovery_diagnostics::record(discovery_diagnostics::Event::Resolved);
                        if let Some((_, _, tracker, _, _)) = refresh_round.as_mut() {
                            tracker.resolved();
                        }
                        let protocol_valid = peer_from_txt(
                            info.get_property_val_str("v"),
                            info.get_property_val_str("id"),
                            info.get_property_val_str("name"),
                        )
                        .is_some();
                        let self_service =
                            info.get_property_val_str("id") == Some(device_id.as_str());
                        discovery_diagnostics::record(discovery_diagnostics::Event::Protocol {
                            valid: protocol_valid,
                            reason: if self_service {
                                discovery_diagnostics::Reason::SelfService
                            } else if protocol_valid {
                                discovery_diagnostics::Reason::None
                            } else if info.get_property_val_str("v") != Some(FLING_PROTO_VERSION) {
                                discovery_diagnostics::Reason::ProtocolMismatch
                            } else {
                                discovery_diagnostics::Reason::MissingIdentity
                            },
                        });

                        match peer_from_txt(
                            info.get_property_val_str("v"),
                            info.get_property_val_str("id"),
                            info.get_property_val_str("name"),
                        ) {
                            Some((id, name)) if id != device_id => {
                                if let Some((_, _, round, _, _)) = refresh_round.as_mut() {
                                    round.valid_service(info.get_fullname());
                                }
                                if info
                                    .get_property_val_str("name")
                                    .is_some_and(|actual| actual == name && actual != id)
                                {
                                    discovery_diagnostics::record(
                                        discovery_diagnostics::Event::ComputerName {
                                            local: false,
                                            name: name.clone(),
                                        },
                                    );
                                }
                                let ipv6_ready = app
                                    .state::<native_lan_runtime::NativeLanRuntime>()
                                    .ipv6_listener_ready();
                                let endpoints = native_lan_runtime::eligible_lan_endpoints(
                                    info.get_addresses(),
                                    info.get_port(),
                                    ipv6_ready,
                                );
                                let mut private_ipv4 = 0u32;
                                let mut link_local_ipv4 = 0u32;
                                let mut ula_ipv6 = 0u32;
                                for endpoint in &endpoints {
                                    match endpoint.ip() {
                                        std::net::IpAddr::V4(ip) if ip.is_private() => {
                                            private_ipv4 += 1
                                        }
                                        std::net::IpAddr::V4(ip)
                                            if ip.octets()[0] == 169 && ip.octets()[1] == 254 =>
                                        {
                                            link_local_ipv4 += 1
                                        }
                                        std::net::IpAddr::V6(ip)
                                            if ipv6_ready && (ip.octets()[0] & 0xfe) == 0xfc =>
                                        {
                                            ula_ipv6 += 1
                                        }
                                        _ => {}
                                    }
                                }
                                discovery_diagnostics::record(
                                    discovery_diagnostics::Event::AddressEligibility {
                                        private_ipv4,
                                        link_local_ipv4,
                                        ula_ipv6,
                                        rejected: info
                                            .get_addresses()
                                            .len()
                                            .saturating_sub(endpoints.len())
                                            as u32,
                                        reason: if info.get_port() == 0 {
                                            discovery_diagnostics::Reason::ZeroPort
                                        } else if endpoints.is_empty() && !ipv6_ready && info.get_addresses().iter().any(|ip| matches!(ip, std::net::IpAddr::V6(ip) if (ip.octets()[0] & 0xfe) == 0xfc)) {
                                            discovery_diagnostics::Reason::Ipv6ListenerUnavailable
                                        } else if endpoints.is_empty() {
                                            discovery_diagnostics::Reason::NoEligible
                                        } else {
                                            discovery_diagnostics::Reason::None
                                        },
                                    },
                                );
                                app.state::<native_lan_runtime::NativeLanRuntime>()
                                    .upsert_peer(
                                        info.get_fullname(),
                                        &id,
                                        &name,
                                        endpoints.clone(),
                                    );
                                if !endpoints.is_empty() {
                                    if let Some((_, _, round, _, _)) = refresh_round.as_mut() {
                                        round.eligible_peer(&id);
                                    }
                                }
                                let first_seen = peers.insert(info.get_fullname(), &id);
                                if first_seen {
                                    discovery_diagnostics::record(
                                        discovery_diagnostics::Event::PeerUpCount(
                                            peers.known_ids.len().min(u32::MAX as usize) as u32,
                                        ),
                                    );
                                    let payload = PeerUpPayload {
                                        id: id.clone(),
                                        name: name.clone(),
                                    };
                                    let _ = app.emit("fling://peer-up", payload.clone());
                                }
                                // LAN publication must not be gated by the legacy online PeerSet:
                                // an earlier resolve can be valid but have no usable LAN address.
                                if app
                                    .state::<native_lan_runtime::NativeLanRuntime>()
                                    .is_enabled()
                                    && !endpoints.is_empty()
                                    && lan_published_ids.should_emit(&id, true, true)
                                {
                                    let runtime =
                                        app.state::<native_lan_runtime::NativeLanRuntime>();
                                    runtime.count_ipc(
                                        "event:fling://lan/peer-up",
                                        serde_json::to_vec(&PeerUpPayload {
                                            id: id.clone(),
                                            name: name.clone(),
                                        })
                                        .map(|value| value.len())
                                        .unwrap_or_default(),
                                    );
                                    let emitted = app
                                        .emit(
                                            "fling://lan/peer-up",
                                            PeerUpPayload {
                                                id: id.clone(),
                                                name: name.clone(),
                                            },
                                        )
                                        .is_ok();
                                    lan_published_ids.mark_emitted(&id, emitted);
                                    discovery_diagnostics::record(
                                        discovery_diagnostics::Event::LanUiEvent {
                                            up: true,
                                            success: emitted,
                                        },
                                    );
                                    if let Some((_, _, round, _, _)) = refresh_round.as_mut() {
                                        round.emitted_peer(&id, emitted);
                                    }
                                } else {
                                    let reason = if !app
                                        .state::<native_lan_runtime::NativeLanRuntime>()
                                        .is_enabled()
                                    {
                                        discovery_diagnostics::Reason::LanDisabled
                                    } else if endpoints.is_empty() {
                                        discovery_diagnostics::Reason::NoEligible
                                    } else {
                                        discovery_diagnostics::Reason::AlreadyPublished
                                    };
                                    discovery_diagnostics::record(
                                        discovery_diagnostics::Event::LanUiSuppressed(reason),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                    ServiceEvent::ServiceRemoved(_ty, fullname) => {
                        app.state::<native_lan_runtime::NativeLanRuntime>()
                            .remove_peer(&fullname);
                        if let Some(id) = peers.remove(&fullname) {
                            if !peers.known_ids.contains(&id) {
                                lan_published_ids.remove(&id);
                            }
                            discovery_diagnostics::record(
                                discovery_diagnostics::Event::PeerDownCount(
                                    peers.known_ids.len().min(u32::MAX as usize) as u32,
                                ),
                            );
                            let payload = PeerDownPayload { id };
                            let _ = app.emit("fling://peer-down", payload.clone());
                            if app
                                .state::<native_lan_runtime::NativeLanRuntime>()
                                .is_enabled()
                            {
                                let runtime = app.state::<native_lan_runtime::NativeLanRuntime>();
                                runtime.count_ipc(
                                    "event:fling://lan/peer-down",
                                    serde_json::to_vec(&payload)
                                        .map(|value| value.len())
                                        .unwrap_or_default(),
                                );
                                let success = app.emit("fling://lan/peer-down", payload).is_ok();
                                discovery_diagnostics::record(
                                    discovery_diagnostics::Event::LanUiEvent { up: false, success },
                                );
                            }
                        }
                    }
                    _ => {}
                }
                if let Some((_, _, tracker, _, ticket)) = refresh_round.as_ref() {
                    ticket.update_counts(diagnostics_counts(
                        &tracker.counts(),
                        tracker.resolved_services(),
                    ));
                }
            }
        }
        let end = refresh_round.as_ref().and_then(|(_, _, tracker, _, _)| {
            let now = std::time::Instant::now();
            if tracker.confirmation_expired(now) {
                Some(false)
            } else if tracker.complete(now) {
                Some(true)
            } else {
                None
            }
        });
        if let Some(completed) = end {
            if let Some((_, _, tracker, reply, ticket)) = refresh_round.take() {
                ticket.update_counts(diagnostics_counts(
                    &tracker.counts(),
                    tracker.resolved_services(),
                ));
                let (outcome, reason) = if completed {
                    (
                        discovery_diagnostics::Outcome::Completed,
                        discovery_diagnostics::Reason::None,
                    )
                } else {
                    (
                        discovery_diagnostics::Outcome::Timeout,
                        discovery_diagnostics::Reason::StartTimeout,
                    )
                };
                if finish_refresh(&ticket, outcome, reason) {
                    let _ = reply.send(if completed {
                        Ok(tracker.counts())
                    } else {
                        Err("mDNS browse start confirmation timed out".to_string())
                    });
                }
            }
        }
    }
    if let Some((_, _, tracker, reply, ticket)) = refresh_round.take() {
        ticket.update_counts(diagnostics_counts(
            &tracker.counts(),
            tracker.resolved_services(),
        ));
        if finish_refresh(
            &ticket,
            discovery_diagnostics::Outcome::Error,
            discovery_diagnostics::Reason::WorkerDisconnected,
        ) {
            let _ = reply.send(Err("mDNS discovery worker stopped".to_string()));
        }
    }
    let _ = daemon.shutdown();
}

#[tauri::command]
async fn discovery_report_ui(
    received_up: u32,
    received_down: u32,
    rendered_peers: u32,
    reason: String,
) -> Result<(), String> {
    use discovery_diagnostics::UiReason;
    let reason = match reason.as_str() {
        "initialized" => UiReason::Initialized,
        "peer-up" => UiReason::PeerUp,
        "peer-down" => UiReason::PeerDown,
        "refresh-completed" => UiReason::RefreshCompleted,
        _ => return Err("无效的诊断状态类别".to_string()),
    };
    tauri::async_runtime::spawn_blocking(move || {
        discovery_diagnostics::record(discovery_diagnostics::Event::UiSnapshot {
            received_up,
            received_down,
            rendered_peers,
            reason,
        })
    })
    .await
    .map_err(|_| "UI diagnostics task failed".to_string())?;
    Ok(())
}

#[tauri::command]
async fn pairing_report_ui(
    phase: String,
    ui_attempt: Option<u32>,
    attempt: Option<u64>,
) -> Result<(), String> {
    let (button, prompt, funnel_event) = match phase.as_str() {
        "button-clicked" if ui_attempt.is_some() && attempt.is_none() => (true, false, None),
        "prompt-received" if attempt.is_some() && ui_attempt.is_none() => (false, true, None),
        "modal-mounted" if attempt.is_some() && ui_attempt.is_none() => (false, false, Some(pairing_diagnostics::FunnelEvent::UiModalMounted)),
        "modal-visible" if attempt.is_some() && ui_attempt.is_none() => (false, false, Some(pairing_diagnostics::FunnelEvent::UiModalVisible)),
        "decision-confirmed" if attempt.is_some() && ui_attempt.is_none() => (false, false, Some(pairing_diagnostics::FunnelEvent::UiDecisionConfirmed)),
        "decision-rejected" if attempt.is_some() && ui_attempt.is_none() => (false, false, Some(pairing_diagnostics::FunnelEvent::UiDecisionRejected)),
        "decision-cancelled" if attempt.is_some() && ui_attempt.is_none() => (false, false, Some(pairing_diagnostics::FunnelEvent::UiDecisionCancelled)),
        _ => return Err("无效的配对界面诊断类别".to_string()),
    };
    tauri::async_runtime::spawn_blocking(move || {
        if button {
            pairing_diagnostics::record_ui_button(ui_attempt.unwrap());
        } else if prompt && !pairing_diagnostics::record_ui_prompt(attempt.unwrap()) {
            return Err("配对诊断尝试记录已过期".to_string());
        } else if let Some(event) = funnel_event {
            if !pairing_diagnostics::record_funnel_ui(attempt.unwrap(), event) {
                return Err("配对诊断尝试记录已过期".to_string());
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| "配对界面诊断任务失败".to_string())?
}

#[tauri::command]
fn discovery_set_browsing(app: tauri::AppHandle, on: bool) -> Result<(), String> {
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::SetBrowsing(on));
    Ok(())
}

#[tauri::command]
async fn discovery_refresh(app: tauri::AppHandle) -> Result<discovery_round::RoundCounts, String> {
    let tx = app.state::<DiscoveryState>().tx.clone();
    let ticket = discovery_round::RefreshTicket::new(discovery_diagnostics::next_round());
    discovery_diagnostics::record_for_round(
        ticket.round_id(),
        0,
        discovery_diagnostics::Event::RefreshRequested,
    );
    let worker_ticket = ticket.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        let ticket = worker_ticket;
        let (reply, result) = mpsc::sync_channel(1);
        if tx
            .send(DiscoveryCmd::Refresh {
                ticket: ticket.clone(),
                reply,
            })
            .is_err()
        {
            finish_refresh(
                &ticket,
                discovery_diagnostics::Outcome::Error,
                discovery_diagnostics::Reason::WorkerDisconnected,
            );
            return Err("mDNS discovery 线程已停止，无法刷新".to_string());
        }
        match result.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                finish_refresh(
                    &ticket,
                    discovery_diagnostics::Outcome::Timeout,
                    discovery_diagnostics::Reason::ReplyTimeout,
                );
                Err("等待 mDNS 刷新结果超时".to_string())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                finish_refresh(
                    &ticket,
                    discovery_diagnostics::Outcome::Error,
                    discovery_diagnostics::Reason::WorkerDisconnected,
                );
                Err("mDNS discovery 线程未返回刷新结果".to_string())
            }
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            finish_refresh(
                &ticket,
                discovery_diagnostics::Outcome::Error,
                discovery_diagnostics::Reason::WorkerDisconnected,
            );
            Err("mDNS 刷新工作线程失败".to_string())
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveryExportResult {
    saved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    saved_path: Option<String>,
}

#[tauri::command]
async fn discovery_export_diagnostics(
    app: tauri::AppHandle,
) -> Result<DiscoveryExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let app_data = app
            .path()
            .app_data_dir()
            .map_err(|_| "无法访问本地诊断目录")?;
        let Some(file) = app
            .dialog()
            .file()
            .set_file_name("fling-lan-discovery.log")
            .blocking_save_file()
        else {
            return Ok(DiscoveryExportResult {
                saved: false,
                saved_path: None,
            });
        };
        let path = file.as_path().ok_or("不支持此保存位置")?;
        let content =
            discovery_diagnostics::export_text(&app_data).map_err(|_| "无法读取本地诊断日志")?;
        std::fs::write(path, content).map_err(|_| "无法保存诊断日志")?;
        Ok(DiscoveryExportResult {
            saved: true,
            saved_path: Some(path.to_string_lossy().into_owned()),
        })
    })
    .await
    .map_err(|_| "诊断导出线程失败".to_string())?
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

#[tauri::command]
fn app_build_identity() -> String {
    env!("FLING_BUILD_IDENTITY").to_string()
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
    #[cfg(all(target_os="linux",debug_assertions))]
    if let Some(path)=std::env::args().nth(1).filter(|s|s=="--internal-native-acceptance").and_then(|_|std::env::args().nth(2)) {
        online_probe::run(&path); return;
    }
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_notification::init())
        .on_window_event(|window, event| {
            #[cfg(windows)]
            if let tauri::WindowEvent::DragDrop(event) = event { native_drop::handle(window.app_handle(), window.label(), event); }
            if matches!(event, tauri::WindowEvent::Destroyed) {
                window.state::<online_source::OnlineSources>().remove_owner(window.label());
                ui_events::cleanup(window);
            }
        })
        .on_webview_event(|webview, event| {
            #[cfg(not(windows))]
            if let tauri::WebviewEvent::DragDrop(event) = event { native_drop::handle(webview.app_handle(), webview.label(), event); }
        })
        .setup(|app| {
            // Device identity (created on first run) + mDNS discovery thread.
            let (_, cfg) = ensure_config(app.handle())?;
            if let Ok(data_dir) = app.path().app_data_dir() {
                discovery_diagnostics::initialize(&data_dir);
            }
            let (listeners, mut native_port, ipv6_ready) =
                match native_lan_runtime::bind_lan_listeners() {
                    Ok(bound) => bound,
                    Err(error) => {
                        eprintln!("[fling] offline LAN mode unavailable: {error}");
                        (Vec::new(), 0, false)
                    }
                };
            let (tx, rx) = mpsc::channel::<DiscoveryCmd>();
            discovery_diagnostics::record(discovery_diagnostics::Event::ListenerInit {
                ipv4_ready: native_port != 0,
                ipv6_ready,
            });
            if native_port == 0 {
                discovery_diagnostics::record(discovery_diagnostics::Event::ListenerFailed);
            }
            discovery_diagnostics::record(discovery_diagnostics::Event::InterfacePolicy);
            app.manage(ConfigLock(Mutex::new(())));
            app.manage(ReceivedFileStore::default());
            app.manage(RemoteConfig(Mutex::new(None)));
            app.manage(native_lan_runtime::NativeLanRuntime::new());
            app.manage(DiscoveryState { tx });
            app.manage(online_session::OnlineSessionRegistry::new());
            app.manage(online_source::OnlineSources::default());
            app.manage(ui_events::UiListeners::default());
            app.manage(native_drop::DropState::default());
            let source_app = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    source_app.state::<online_source::OnlineSources>().purge_expired();
                }
            });
            if native_port != 0 {
                if let Err(error) =
                    native_lan_runtime::start_listener(app.handle().clone(), listeners, ipv6_ready)
                {
                    eprintln!("[fling] offline LAN mode unavailable: {error}");
                    native_port = 0;
                }
            }
            let handle = app.handle().clone();
            let (device_id, device_name) = (cfg.device_id, cfg.device_name);
            std::thread::Builder::new()
                .name("fling-mdns".into())
                .spawn(move || discovery_thread(handle, rx, device_id, device_name, native_port))
                .map_err(|e| format!("mdns thread: {e}"))?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            mint_token,
            online_connect,
            online_pick_source,
            online_pick_sources,
            online_send_registered_source,
            ui_events::ui_listen,
            ui_events::ui_unlisten,
            online_release_source,
            online_send_source,
            online_cancel_native,
            online_send_text,
            online_send_file,
            online_send_message,
            online_disconnect,
            write_temp_file,
            online_temp_begin,
            online_temp_append,
            server_url,
            pick_save_path,
            write_binary_file,
            save_received_file,
            begin_received_file,
            append_received_file_chunk,
            finish_received_file,
            abort_received_file,
            get_device_info,
            set_device_name,
            get_download_dir,
            set_download_dir,
            pick_download_dir,
            discovery_set_browsing,
            discovery_refresh,
            discovery_report_ui,
            pairing_report_ui,
            discovery_export_diagnostics,
            discovery_set_discoverable,
            native_lan_runtime::native_lan_start_mode,
            native_lan_runtime::native_lan_stop_mode,
            native_lan_runtime::native_lan_pick_source,
            native_lan_runtime::native_lan_start_send,
            native_lan_runtime::native_lan_pair_decision,
            native_lan_runtime::native_lan_manifest_decision,
            native_lan_runtime::native_lan_cancel,
            native_lan_runtime::native_lan_ipc_counters,
            native_lan_runtime::native_lan_try_direct_offer,
            write_image_clipboard,
            open_folder,
            app_version,
            app_build_identity,
            get_log_dir,
            append_log,
            read_history,
            append_history,
            get_remote_config_url,
            apply_remote_config
        ])
        .run(tauri::generate_context!())
        .expect("error while running fling");
}

// ================================================================= tests ==

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webview_config_never_serializes_turn_credentials() {
        let mut cfg = default_config();
        cfg.turn_url = Some("turn:private.example:3478".into());
        cfg.turn_username = Some("secret-turn-user".into());
        cfg.turn_password = Some("secret-turn-pass".into());
        let value = serde_json::to_string(&config_for_webview(cfg)).unwrap();
        for forbidden in ["turn_url", "turn_username", "turn_password", "private.example", "secret-turn-user", "secret-turn-pass"] {
            assert!(!value.contains(forbidden));
        }
    }

    #[test]
    fn raw_append_ipc_accepts_binary_and_rejects_json_missing_headers_and_oversize() {
        let id = "a1b2c3d4e5f60718293a4b5c6d7e8f90";
        let raw = tauri::ipc::InvokeBody::Raw(vec![0, 1, 127, 255]);
        let (session_id, bytes) = raw_append_request_parts(&raw, Some(id)).unwrap();
        assert_eq!(session_id, id);
        assert_eq!(bytes, vec![0, 1, 127, 255]);

        let json = tauri::ipc::InvokeBody::Json(serde_json::json!({ "data": [0, 1, 127, 255] }));
        assert!(raw_append_request_parts(&json, Some(id))
            .unwrap_err()
            .contains("raw binary"));
        assert!(raw_append_request_parts(&raw, None)
            .unwrap_err()
            .contains("missing x-session-id"));
        assert!(raw_append_request_parts(&raw, Some("bad-session"))
            .unwrap_err()
            .contains("invalid receive file session id"));
        assert!(
            raw_append_request_parts(&tauri::ipc::InvokeBody::Raw(Vec::new()), Some(id)).is_err()
        );
        assert!(raw_append_request_parts(
            &tauri::ipc::InvokeBody::Raw(vec![0; MAX_RECEIVED_FILE_BATCH_BYTES + 1]),
            Some(id),
        )
        .unwrap_err()
        .contains("invalid receive batch size"));
    }

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
        assert_eq!(ps.remove("PC._fling._tcp.local."), None);
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
    fn lan_publication_recovers_cached_peer_after_ineligible_resolution_and_refresh() {
        let mut peers = PeerSet::default();
        let mut lan = LanPublishedSet::default();
        let fullname = "peer._fling._tcp.local.";
        assert!(peers.insert(fullname, "device")); // valid TXT, no eligible endpoint: legacy gate remembers it
        assert!(!lan.should_emit("device", true, false));
        let mut browsing = true;
        refresh_browse(&mut browsing, || Ok(()), || Ok(())).unwrap();
        assert!(!peers.insert(fullname, "device")); // old outer gate would suppress this cached resolve
        assert!(lan.should_emit("device", true, true)); // later resolve / refresh cached replay
        lan.mark_emitted("device", true);
        assert!(!lan.should_emit("device", true, true)); // repeated resolve is suppressed
        lan.remove("device"); // removal allows future discovery again
        assert!(lan.should_emit("device", true, true));
        assert!(peers.known_ids.contains("device")); // no global peer clearing required
    }

    #[test]
    fn refresh_outcome_guard_preserves_other_round_and_observed_counts() {
        use discovery_diagnostics::{Outcome, Reason};
        let active = discovery_round::RefreshTicket::new(900);
        let rejected = discovery_round::RefreshTicket::new(901);
        assert!(finish_refresh(&rejected, Outcome::Error, Reason::Overlap));
        assert!(!rejected.finish_once());
        assert!(!active.is_terminal());
        let counts = discovery_diagnostics::PhaseCounts {
            resolved_services: 2,
            valid_services: 1,
            lan_eligible_peers: 0,
            ui_emitted_peers: 0,
        };
        active.update_counts(counts);
        assert!(finish_refresh(
            &active,
            Outcome::Timeout,
            Reason::ReplyTimeout
        ));
        assert!(!finish_refresh(&active, Outcome::Completed, Reason::None));
        assert!(!finish_refresh(
            &active,
            Outcome::Error,
            Reason::WorkerDisconnected
        ));
        assert_eq!(active.counts(), counts);
    }

    #[test]
    fn failed_lan_emit_is_not_marked_published() {
        let mut lan = LanPublishedSet::default();
        assert!(lan.should_emit("device", true, true));
        lan.mark_emitted("device", false);
        assert!(lan.should_emit("device", true, true));
    }

    #[test]
    fn discovery_refresh_restarts_repeatedly_without_touching_peers_or_announcing() {
        let mut browsing = true;
        let mut peers = PeerSet::default();
        peers.insert("peer._fling._tcp.local.", "peer-id");
        let mut stops = 0;
        let mut starts = 0;
        // This workflow owns browse operations only; no register/unregister path
        // is called, and peer/runtime state is deliberately outside its scope.
        for _ in 0..2 {
            refresh_browse(
                &mut browsing,
                || {
                    stops += 1;
                    Ok(())
                },
                || {
                    starts += 1;
                    Ok(())
                },
            )
            .unwrap();
        }
        assert_eq!((stops, starts), (2, 2));
        assert!(browsing);
        assert!(peers.known_ids.contains("peer-id"));
        assert_eq!(peers.by_fullname.len(), 1);
    }

    #[test]
    fn discovery_refresh_reports_stop_and_browse_errors_and_tracks_state() {
        let mut browsing = true;
        let mut browse_called = false;
        let error = refresh_browse(
            &mut browsing,
            || Err("stop denied".to_string()),
            || {
                browse_called = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.contains("停止现有 mDNS browse 失败"));
        assert!(browsing);
        assert!(!browse_called);

        let error = refresh_browse(
            &mut browsing,
            || Ok(()),
            || Err::<(), _>("browse denied".to_string()),
        )
        .unwrap_err();
        assert!(error.contains("重新启动 mDNS browse 失败"));
        assert!(!browsing);

        refresh_browse(
            &mut browsing,
            || panic!("not browsing: stop must not be called"),
            || Ok(()),
        )
        .unwrap();
        assert!(browsing);
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

    #[test]
    fn save_received_file_never_overwrites_existing_data() {
        let dir = std::env::temp_dir().join(format!("fling-save-test-{}", new_device_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_string_lossy().into_owned();

        let first_result =
            save_received_file(dir_str.clone(), "../sample.txt".into(), vec![1, 2, 3], None)
                .unwrap();
        assert!(first_result.write_all_ms.is_finite() && first_result.write_all_ms >= 0.0);
        assert!(first_result.sync_all_ms.is_finite() && first_result.sync_all_ms >= 0.0);
        let ipc_result = serde_json::to_value(&first_result).unwrap();
        assert!(ipc_result.get("writeAllMs").is_some());
        assert!(ipc_result.get("syncAllMs").is_some());
        let first = first_result.path;
        let second = save_received_file(dir_str.clone(), "sample.txt".into(), vec![9, 8], None)
            .unwrap()
            .path;
        assert_ne!(first, second);
        assert_eq!(std::fs::read(&first).unwrap(), vec![1, 2, 3]);
        assert_eq!(std::fs::read(&second).unwrap(), vec![9, 8]);
        assert!(std::path::Path::new(&first).starts_with(&dir));

        let nested = save_received_file(
            dir_str.clone(),
            "evil.txt".into(),
            vec![4],
            Some("../../nested/evil.txt".into()),
        )
        .unwrap()
        .path;
        assert!(std::path::Path::new(&nested).starts_with(&dir));
        assert_eq!(std::fs::read(nested).unwrap(), vec![4]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_save_handles_123mb_hash_and_repeated_name_without_overwrite() {
        use std::io::Read;
        let benchmark_started = std::time::Instant::now();
        let dir = std::env::temp_dir().join(format!("fling-stream-large-{}", new_device_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_string_lossy().into_owned();
        let store = ReceivedFileStore::default();
        let size = 122_891_016u64;
        let mut expected_hasher = Sha256::new();
        let mut offset = 0u64;
        let mut batch = vec![0u8; 4 * 1024 * 1024];
        while offset < size {
            let length = std::cmp::min(batch.len() as u64, size - offset) as usize;
            for (index, byte) in batch[..length].iter_mut().enumerate() {
                *byte = ((offset + index as u64) % 251) as u8;
            }
            expected_hasher.update(&batch[..length]);
            offset += length as u64;
        }
        let expected_hash = format!("{:x}", expected_hasher.finalize());
        let session = store
            .begin(
                dir_str.clone(),
                "../large.bin".into(),
                None,
                size,
                expected_hash.clone(),
            )
            .unwrap();
        offset = 0;
        let mut last_append = None;
        let mut rust_append_elapsed_ms = 0.0;
        while offset < size {
            let length = std::cmp::min(batch.len() as u64, size - offset) as usize;
            for (index, byte) in batch[..length].iter_mut().enumerate() {
                *byte = ((offset + index as u64) % 251) as u8;
            }
            let append = store.append(&session, &batch[..length]).unwrap();
            rust_append_elapsed_ms += append.write_elapsed_ms;
            last_append = Some(append);
            offset += length as u64;
        }
        let final_append = last_append.unwrap();
        assert_eq!(final_append.cumulative_bytes, size);
        assert_eq!(final_append.bytes as u64, size % (4 * 1024 * 1024));
        assert!(final_append.write_elapsed_ms.is_finite() && final_append.write_elapsed_ms >= 0.0);
        let append_ipc = serde_json::to_value(&final_append).unwrap();
        assert_eq!(
            append_ipc.get("bytes").and_then(|v| v.as_u64()),
            Some(final_append.bytes as u64)
        );
        assert_eq!(
            append_ipc.get("cumulativeBytes").and_then(|v| v.as_u64()),
            Some(size)
        );
        assert!(append_ipc.get("writeElapsedMs").is_some());
        let first = store.finish(&session).unwrap();
        println!(
            "BENCH receive-save bytes={} batches={} total_elapsed_ms={} rust_write_all_ms={:.3} sync_all_ms={:.3}",
            size,
            (size + (4 * 1024 * 1024 - 1)) / (4 * 1024 * 1024),
            benchmark_started.elapsed().as_millis(),
            rust_append_elapsed_ms,
            first.sync_all_ms,
        );
        assert_eq!(std::fs::metadata(&first.path).unwrap().len(), size);
        assert!(first.write_all_ms.is_finite() && first.write_all_ms >= 0.0);
        assert!(first.sync_all_ms.is_finite() && first.sync_all_ms >= 0.0);
        let mut file = std::fs::File::open(&first.path).unwrap();
        let mut actual_hasher = Sha256::new();
        let mut read_buffer = vec![0u8; 64 * 1024];
        loop {
            let read = file.read(&mut read_buffer).unwrap();
            if read == 0 {
                break;
            }
            actual_hasher.update(&read_buffer[..read]);
        }
        assert_eq!(format!("{:x}", actual_hasher.finalize()), expected_hash);

        let second_bytes = b"second file";
        let second_hash = format!("{:x}", Sha256::digest(second_bytes));
        let second_session = store
            .begin(
                dir_str,
                "large.bin".into(),
                None,
                second_bytes.len() as u64,
                second_hash,
            )
            .unwrap();
        store.append(&second_session, second_bytes).unwrap();
        let second = store.finish(&second_session).unwrap();
        assert_ne!(first.path, second.path);
        assert_eq!(std::fs::read(&first.path).unwrap().len() as u64, size);
        assert_eq!(std::fs::read(&second.path).unwrap(), second_bytes);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_save_benchmarks_60mb_receive_size_and_verifies_hash() {
        use std::io::Read;
        let size = 63_116_252u64;
        let benchmark_started = std::time::Instant::now();
        let dir = std::env::temp_dir().join(format!("fling-stream-60m-{}", new_device_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = ReceivedFileStore::default();
        let mut batch = vec![0u8; 4 * 1024 * 1024];
        let fill_batch = |buffer: &mut [u8], offset: u64| {
            for (index, byte) in buffer.iter_mut().enumerate() {
                *byte = ((offset + index as u64) % 251) as u8;
            }
        };
        let mut expected_hasher = Sha256::new();
        let mut offset = 0u64;
        while offset < size {
            let length = std::cmp::min(batch.len() as u64, size - offset) as usize;
            fill_batch(&mut batch[..length], offset);
            expected_hasher.update(&batch[..length]);
            offset += length as u64;
        }
        let expected_hash = format!("{:x}", expected_hasher.finalize());
        let session = store
            .begin(
                dir.to_string_lossy().into_owned(),
                "60mib.bin".into(),
                None,
                size,
                expected_hash.clone(),
            )
            .unwrap();
        offset = 0;
        let mut rust_append_elapsed_ms = 0.0;
        let mut batches = 0u64;
        while offset < size {
            let length = std::cmp::min(batch.len() as u64, size - offset) as usize;
            fill_batch(&mut batch[..length], offset);
            let result = store.append(&session, &batch[..length]).unwrap();
            rust_append_elapsed_ms += result.write_elapsed_ms;
            offset += length as u64;
            batches += 1;
        }
        let saved = store.finish(&session).unwrap();
        assert_eq!(std::fs::metadata(&saved.path).unwrap().len(), size);
        let mut file = std::fs::File::open(&saved.path).unwrap();
        let mut actual_hasher = Sha256::new();
        let mut read_buffer = vec![0u8; 64 * 1024];
        loop {
            let read = file.read(&mut read_buffer).unwrap();
            if read == 0 {
                break;
            }
            actual_hasher.update(&read_buffer[..read]);
        }
        assert_eq!(format!("{:x}", actual_hasher.finalize()), expected_hash);
        println!(
            "BENCH receive-save bytes={} batches={} total_elapsed_ms={} rust_write_all_ms={:.3} sync_all_ms={:.3}",
            size,
            batches,
            benchmark_started.elapsed().as_millis(),
            rust_append_elapsed_ms,
            saved.sync_all_ms,
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_save_benchmarks_bounded_4_8_16_mib_batches() {
        use std::io::Read;
        let size = 63_116_252u64;
        let dir = std::env::temp_dir().join(format!("fling-stream-batch-ab-{}", new_device_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_string_lossy().into_owned();
        let store = ReceivedFileStore::default();
        let fill_batch = |buffer: &mut [u8], offset: u64| {
            for (index, byte) in buffer.iter_mut().enumerate() {
                *byte = ((offset + index as u64) % 251) as u8;
            }
        };
        let mut expected_hasher = Sha256::new();
        let mut hash_buffer = vec![0u8; 64 * 1024];
        let mut offset = 0u64;
        while offset < size {
            let length = std::cmp::min(hash_buffer.len() as u64, size - offset) as usize;
            fill_batch(&mut hash_buffer[..length], offset);
            expected_hasher.update(&hash_buffer[..length]);
            offset += length as u64;
        }
        let expected_hash = format!("{:x}", expected_hasher.finalize());

        for batch_size in [4usize, 8, 16].map(|mib| mib * 1024 * 1024) {
            assert!(batch_size <= MAX_RECEIVED_FILE_BATCH_BYTES);
            let mut batch = vec![0u8; batch_size];
            let started = std::time::Instant::now();
            let session = store
                .begin(
                    dir_str.clone(),
                    format!("batch-{batch_size}.bin"),
                    None,
                    size,
                    expected_hash.clone(),
                )
                .unwrap();
            offset = 0;
            let mut batches = 0u64;
            let mut write_all_ms = 0.0;
            let mut largest_batch = 0usize;
            while offset < size {
                let length = std::cmp::min(batch.len() as u64, size - offset) as usize;
                fill_batch(&mut batch[..length], offset);
                let appended = store.append(&session, &batch[..length]).unwrap();
                write_all_ms += appended.write_elapsed_ms;
                largest_batch = largest_batch.max(appended.bytes);
                offset += length as u64;
                batches += 1;
            }
            let saved = store.finish(&session).unwrap();
            assert_eq!(std::fs::metadata(&saved.path).unwrap().len(), size);
            let mut file = std::fs::File::open(&saved.path).unwrap();
            let mut actual_hasher = Sha256::new();
            let mut read_buffer = vec![0u8; 64 * 1024];
            loop {
                let read = file.read(&mut read_buffer).unwrap();
                if read == 0 {
                    break;
                }
                actual_hasher.update(&read_buffer[..read]);
            }
            assert_eq!(format!("{:x}", actual_hasher.finalize()), expected_hash);
            assert!(largest_batch <= MAX_RECEIVED_FILE_BATCH_BYTES);
            println!(
                "BENCH receive-save batch_bytes={} batches={} total_elapsed_ms={} rust_write_all_ms={:.3} sync_all_ms={:.3} largest_batch_bytes={} strict_sequential=true",
                batch_size,
                batches,
                started.elapsed().as_millis(),
                write_all_ms,
                saved.sync_all_ms,
                largest_batch,
            );
            std::fs::remove_file(saved.path).unwrap();
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streaming_save_supports_multiple_sessions_and_cleans_interrupted_or_invalid_files() {
        let dir = std::env::temp_dir().join(format!("fling-stream-cleanup-{}", new_device_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_string_lossy().into_owned();
        let store = ReceivedFileStore::default();
        let a = b"alpha";
        let b = b"bravo-two";
        let a_session = store
            .begin(
                dir_str.clone(),
                "a.bin".into(),
                Some("batch/a.bin".into()),
                a.len() as u64,
                format!("{:x}", Sha256::digest(a)),
            )
            .unwrap();
        let b_session = store
            .begin(
                dir_str.clone(),
                "b.bin".into(),
                Some("batch/b.bin".into()),
                b.len() as u64,
                format!("{:x}", Sha256::digest(b)),
            )
            .unwrap();
        store.append(&a_session, &a[..2]).unwrap();
        store.append(&b_session, &b[..4]).unwrap();
        store.append(&a_session, &a[2..]).unwrap();
        store.append(&b_session, &b[4..]).unwrap();
        let saved_a = store.finish(&a_session).unwrap();
        let saved_b = store.finish(&b_session).unwrap();
        assert_eq!(std::fs::read(saved_a.path).unwrap(), a);
        assert_eq!(std::fs::read(saved_b.path).unwrap(), b);

        let aborted = store
            .begin(
                dir_str.clone(),
                "interrupted.bin".into(),
                None,
                100,
                format!("{:x}", Sha256::digest(b"x")),
            )
            .unwrap();
        store.append(&aborted, b"partial").unwrap();
        store.abort(&aborted).unwrap();
        let invalid = store
            .begin(
                dir_str.clone(),
                "bad-hash.bin".into(),
                None,
                3,
                "0".repeat(64),
            )
            .unwrap();
        store.append(&invalid, b"bad").unwrap();
        assert!(store
            .finish(&invalid)
            .err()
            .unwrap()
            .contains("sha256 mismatch"));
        let overflow = store
            .begin(
                dir_str.clone(),
                "overflow.bin".into(),
                None,
                3,
                format!("{:x}", Sha256::digest(b"ok")),
            )
            .unwrap();
        assert!(store.append(&overflow, b"four").is_err());
        assert!(store.finish(&overflow).is_err());
        assert!(std::fs::read_dir(&dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".fling-")
        }));
        assert!(!dir.join("interrupted.bin").exists());
        assert!(!dir.join("bad-hash.bin").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
