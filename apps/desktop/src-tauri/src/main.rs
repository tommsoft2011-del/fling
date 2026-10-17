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

static HISTORY_IO_LOCK: Mutex<()> = Mutex::new(());

mod embedded_config;

#[allow(dead_code)]
mod native_lan;
mod native_lan_runtime;
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
    SetDiscoverable(bool),
    UpdateIdentity {
        device_id: String,
        device_name: String,
    },
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
    let mut registered_name = register_fling(&daemon, &device_id, &device_name, native_port);
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
                    let lan_enabled = app
                        .state::<native_lan_runtime::NativeLanRuntime>()
                        .is_enabled();
                    app.state::<native_lan_runtime::NativeLanRuntime>()
                        .clear_peers();
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
                            let _ = app.emit("fling://lan/peer-down", payload);
                        }
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
            for ev in brx.try_iter() {
                match ev {
                    ServiceEvent::ServiceResolved(info) => {
                        match peer_from_txt(
                            info.get_property_val_str("v"),
                            info.get_property_val_str("id"),
                            info.get_property_val_str("name"),
                        ) {
                            Some((id, name)) if id != device_id => {
                                let endpoints = native_lan_runtime::eligible_lan_endpoints(
                                    info.get_addresses(),
                                    info.get_port(),
                                    app.state::<native_lan_runtime::NativeLanRuntime>()
                                        .ipv6_listener_ready(),
                                );
                                app.state::<native_lan_runtime::NativeLanRuntime>()
                                    .upsert_peer(
                                        info.get_fullname(),
                                        &id,
                                        &name,
                                        endpoints.clone(),
                                    );
                                if peers.insert(info.get_fullname(), &id) {
                                    let payload = PeerUpPayload { id, name };
                                    let _ = app.emit("fling://peer-up", payload.clone());
                                    if app
                                        .state::<native_lan_runtime::NativeLanRuntime>()
                                        .is_enabled()
                                        && !endpoints.is_empty()
                                    {
                                        let runtime =
                                            app.state::<native_lan_runtime::NativeLanRuntime>();
                                        runtime.count_ipc(
                                            "event:fling://lan/peer-up",
                                            serde_json::to_vec(&payload)
                                                .map(|value| value.len())
                                                .unwrap_or_default(),
                                        );
                                        let _ = app.emit("fling://lan/peer-up", payload);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    ServiceEvent::ServiceRemoved(_ty, fullname) => {
                        app.state::<native_lan_runtime::NativeLanRuntime>()
                            .remove_peer(&fullname);
                        if let Some(id) = peers.remove(&fullname) {
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
                                let _ = app.emit("fling://lan/peer-down", payload);
                            }
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
fn emit_native_diagnostic(app: &tauri::AppHandle, payload: serde_json::Value) {
    let _ = app.emit("fling:diagnostic", payload);
}

#[tauri::command]
fn read_file_bytes(app: tauri::AppHandle, path: String) -> Result<Vec<u8>, String> {
    let metadata_started = std::time::Instant::now();
    let meta = match std::fs::metadata(&path) {
        Ok(meta) => meta,
        Err(error) => {
            emit_native_diagnostic(
                &app,
                serde_json::json!({
                    "stage": "source_file_metadata", "status": "error",
                    "errorType": format!("{:?}", error.kind()),
                    "elapsedMs": metadata_started.elapsed().as_secs_f64() * 1000.0,
                }),
            );
            return Err(format!("读取文件失败: {error}"));
        }
    };
    let metadata_ms = metadata_started.elapsed().as_secs_f64() * 1000.0;
    if !meta.is_file() {
        emit_native_diagnostic(
            &app,
            serde_json::json!({
                "stage": "source_file_metadata", "status": "not_file",
                "metadataMs": metadata_ms,
            }),
        );
        return Err("仅支持拖入文件，暂不支持文件夹".into());
    }

    let open_started = std::time::Instant::now();
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) => {
            emit_native_diagnostic(
                &app,
                serde_json::json!({
                    "stage": "source_file_open", "status": "error",
                    "expectedBytes": meta.len(), "metadataMs": metadata_ms,
                    "errorType": format!("{:?}", error.kind()),
                    "elapsedMs": open_started.elapsed().as_secs_f64() * 1000.0,
                }),
            );
            return Err(format!("读取文件失败: {error}"));
        }
    };
    let open_ms = open_started.elapsed().as_secs_f64() * 1000.0;
    let read_started = std::time::Instant::now();
    let mut bytes = Vec::with_capacity(meta.len().min(usize::MAX as u64) as usize);
    if let Err(error) = file.read_to_end(&mut bytes) {
        emit_native_diagnostic(
            &app,
            serde_json::json!({
                "stage": "source_file_read", "status": "error",
                "expectedBytes": meta.len(), "readBytes": bytes.len(),
                "metadataMs": metadata_ms, "openMs": open_ms,
                "errorType": format!("{:?}", error.kind()),
                "elapsedMs": read_started.elapsed().as_secs_f64() * 1000.0,
            }),
        );
        return Err(format!("读取文件失败: {error}"));
    }
    emit_native_diagnostic(
        &app,
        serde_json::json!({
            "stage": "source_file_read", "status": "ok",
            "expectedBytes": meta.len(), "readBytes": bytes.len(),
            "metadataMs": metadata_ms, "openMs": open_ms,
            "readMs": read_started.elapsed().as_secs_f64() * 1000.0,
        }),
    );
    Ok(bytes)
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
            let (listeners, mut native_port, ipv6_ready) =
                match native_lan_runtime::bind_lan_listeners() {
                    Ok(bound) => bound,
                    Err(error) => {
                        eprintln!("[fling] offline LAN mode unavailable: {error}");
                        (Vec::new(), 0, false)
                    }
                };
            let (tx, rx) = mpsc::channel::<DiscoveryCmd>();
            app.manage(ConfigLock(Mutex::new(())));
            app.manage(ReceivedFileStore::default());
            app.manage(RemoteConfig(Mutex::new(None)));
            app.manage(native_lan_runtime::NativeLanRuntime::new());
            app.manage(DiscoveryState { tx });
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
            discovery_set_discoverable,
            native_lan_runtime::native_lan_start_mode,
            native_lan_runtime::native_lan_stop_mode,
            native_lan_runtime::native_lan_pick_source,
            native_lan_runtime::native_lan_start_send,
            native_lan_runtime::native_lan_pair_decision,
            native_lan_runtime::native_lan_manifest_decision,
            native_lan_runtime::native_lan_cancel,
            native_lan_runtime::native_lan_ipc_counters,
            write_image_clipboard,
            read_file_bytes,
            file_size,
            list_dir_recursive,
            is_dir,
            open_folder,
            app_version,
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
