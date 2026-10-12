// Fling desktop backend (Tauri v2).
// The Rust side owns: config file (server URL + API key/secret),
// LiveKit token minting (HS256 JWT — the secret never enters the webview),
// and nothing else. Files/clipboard go through official Tauri plugins.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use tauri::Manager;

#[derive(Serialize, Deserialize, Clone)]
struct FlingConfig {
    server_url: String,
    api_key: String,
    api_secret: String,
}

fn config_path(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("config dir: {e}"))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    Ok(dir.join("config.json"))
}

#[tauri::command]
fn get_config(app: tauri::AppHandle) -> Result<Option<FlingConfig>, String> {
    let p = config_path(&app)?;
    if !p.exists() {
        return Ok(None);
    }
    let s = std::fs::read_to_string(&p).map_err(|e| format!("read: {e}"))?;
    serde_json::from_str::<FlingConfig>(&s)
        .map(Some)
        .map_err(|e| format!("parse: {e}"))
}

#[tauri::command]
fn save_config(app: tauri::AppHandle, cfg: FlingConfig) -> Result<(), String> {
    let p = config_path(&app)?;
    if cfg.server_url.trim().is_empty() || cfg.api_key.trim().is_empty() || cfg.api_secret.trim().is_empty() {
        return Err("server_url, api_key and api_secret are all required".into());
    }
    let s = serde_json::to_string_pretty(&cfg).map_err(|e| format!("encode: {e}"))?;
    std::fs::write(&p, s).map_err(|e| format!("write: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Mint a LiveKit join token for `room`/`identity` using the stored secret.
#[tauri::command]
fn mint_token(app: tauri::AppHandle, room: String, identity: String) -> Result<String, String> {
    let p = config_path(&app)?;
    let s = std::fs::read_to_string(&p).map_err(|_| "no config — run setup first".to_string())?;
    let cfg: FlingConfig = serde_json::from_str(&s).map_err(|e| format!("parse: {e}"))?;

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
    let p = config_path(&app)?;
    let s = std::fs::read_to_string(&p).map_err(|_| "no config — run setup first".to_string())?;
    let cfg: FlingConfig = serde_json::from_str(&s).map_err(|e| format!("parse: {e}"))?;
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

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            mint_token,
            server_url,
            pick_save_path,
            write_binary_file
        ])
        .run(tauri::generate_context!())
        .expect("error while running fling");
}
