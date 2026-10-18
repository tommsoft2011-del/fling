use crate::native_lan::{
    self, AuthenticatedLocalSession, EncryptedControlFrame, NativeControlMessage, PairingDecision,
    PairingRole, PendingLocalPair, SessionReplayGuard,
};
use crate::{ensure_config, DiscoveryCmd, DiscoveryState};
#[path = "native_lan_transfer.rs"]
mod transfer;
use serde::Serialize;
use sha2::{Digest, Sha256};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager, State};
use tauri_plugin_dialog::DialogExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc as async_mpsc, oneshot, watch, Mutex as AsyncMutex, Notify};
use transfer::TransferState;

const NATIVE_CHUNK_SIZE: u64 = 32 * 1024;
const MAX_SOURCE_CHUNKS: u64 = u32::MAX as u64;
const CONTROL_QUEUE_WAIT_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) async fn enqueue_ordered_control<T, R, F>(
    sender: &async_mpsc::Sender<T>,
    send_lock: &AsyncMutex<()>,
    build: F,
) -> Result<R, String>
where
    T: Send,
    R: Send,
    F: FnOnce() -> Result<(T, R), String> + Send,
{
    let _send_guard = send_lock.lock().await;
    let permit = tokio::time::timeout(CONTROL_QUEUE_WAIT_TIMEOUT, sender.reserve())
        .await
        .map_err(|_| "LAN control queue remained full for 15 seconds".to_string())?
        .map_err(|_| "LAN control connection is closed".to_string())?;
    let (frame, result) = build()?;
    permit.send(frame);
    Ok(result)
}

pub(super) fn try_enqueue_ordered_control<T, R, F>(
    sender: &async_mpsc::Sender<T>,
    send_lock: &AsyncMutex<()>,
    build: F,
) -> Result<R, String>
where
    F: FnOnce() -> Result<(T, R), String>,
{
    let _send_guard = send_lock
        .try_lock()
        .map_err(|_| "LAN control sender is busy".to_string())?;
    let permit = sender
        .try_reserve()
        .map_err(|_| "bounded LAN control queue is full or closed".to_string())?;
    let (frame, result) = build()?;
    permit.send(frame);
    Ok(result)
}

#[derive(Clone)]
pub(crate) struct NativeLanRuntime {
    inner: Arc<NativeLanInner>,
}

struct NativeLanInner {
    enabled: AtomicBool,
    listener_ready: AtomicBool,
    ipv6_listener_ready: AtomicBool,
    peers_by_fullname: Mutex<HashMap<String, DiscoveredPeer>>,
    sources: Mutex<HashMap<String, NativeSource>>,
    sessions: Mutex<HashMap<String, Arc<NativeLanSession>>>,
    replay_guard: Mutex<SessionReplayGuard>,
    ipc: Mutex<BTreeMap<String, IpcCounterEntry>>,
}

#[derive(Clone)]
struct DiscoveredPeer {
    device_id: String,
    device_name: String,
    endpoints: Vec<SocketAddr>,
}

/// Rust-only source record. Neither the path nor the file handle is serializable.
struct NativeSource {
    _path: PathBuf,
    file: File,
    name: String,
    size_bytes: u64,
    sha256: String,
    total_chunks: u32,
}

enum LocalAuthState {
    Pending(PendingLocalPair),
    Authenticated(AuthenticatedLocalSession),
    Closed,
}

struct NativeLanSession {
    session_id: String,
    task_id: String,
    role: PairingRole,
    peer_device_id: String,
    peer_device_label: String,
    auth: Mutex<LocalAuthState>,
    source: Mutex<Option<NativeSource>>,
    webrtc_started: AtomicBool,
    manifest_sent: AtomicBool,
    ack_next_seq: AtomicU32,
    ack_notify: Notify,
    durable_complete: AtomicBool,
    completed: AtomicBool,
    terminal_claimed: AtomicBool,
    data_channel_tx: async_mpsc::UnboundedSender<livekit::webrtc::data_channel::DataChannel>,
    transfer: Mutex<TransferState>,
    control_tx: async_mpsc::Sender<EncryptedControlFrame>,
    control_send_lock: AsyncMutex<()>,
    cancel_tx: watch::Sender<bool>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PairingEvent {
    session_id: String,
    task_id: String,
    peer_device_id: String,
    peer_device_label: String,
    verification_code: String,
    role: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum LanFailureKind {
    Close,
    Error,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct LanTaskEvent {
    task_id: String,
    phase: String,
    route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    received_bytes: u64,
    saved_bytes: u64,
    total_bytes: u64,
    speed_bps: u64,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_kind: Option<LanFailureKind>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeSourceMetadata {
    source_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeLanIdentity {
    device_id: String,
    device_name: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct IpcCounterEntry {
    calls: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeLanIpcCounters {
    by_type: BTreeMap<String, IpcCounterEntry>,
    total_calls: u64,
    total_bytes: u64,
}

impl NativeLanSession {
    fn claim_terminal(&self) -> bool {
        self.terminal_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl NativeLanRuntime {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(NativeLanInner {
                enabled: AtomicBool::new(false),
                listener_ready: AtomicBool::new(false),
                ipv6_listener_ready: AtomicBool::new(false),
                peers_by_fullname: Mutex::new(HashMap::new()),
                sources: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                replay_guard: Mutex::new(SessionReplayGuard::new(4096)),
                ipc: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.enabled.load(Ordering::Acquire)
    }

    pub(crate) fn ipv6_listener_ready(&self) -> bool {
        self.inner.ipv6_listener_ready.load(Ordering::Acquire)
    }

    fn set_listener_ready(&self, ipv6_ready: bool) {
        self.inner
            .ipv6_listener_ready
            .store(ipv6_ready, Ordering::Release);
        self.inner.listener_ready.store(true, Ordering::Release);
    }

    fn set_enabled(&self, enabled: bool) {
        self.inner.enabled.store(enabled, Ordering::Release);
    }

    pub(crate) fn upsert_peer(
        &self,
        fullname: &str,
        device_id: &str,
        device_name: &str,
        endpoints: Vec<SocketAddr>,
    ) {
        if let Ok(mut peers) = self.inner.peers_by_fullname.lock() {
            peers.insert(
                fullname.to_string(),
                DiscoveredPeer {
                    device_id: device_id.to_string(),
                    device_name: device_name.to_string(),
                    endpoints,
                },
            );
        }
    }

    fn peer_snapshot(&self) -> Vec<crate::PeerUpPayload> {
        let mut unique = BTreeMap::new();
        if let Ok(peers) = self.inner.peers_by_fullname.lock() {
            for peer in peers.values().filter(|peer| !peer.endpoints.is_empty()) {
                unique.insert(peer.device_id.clone(), peer.device_name.clone());
            }
        }
        unique
            .into_iter()
            .map(|(id, name)| crate::PeerUpPayload { id, name })
            .collect()
    }

    pub(crate) fn remove_peer(&self, fullname: &str) {
        if let Ok(mut peers) = self.inner.peers_by_fullname.lock() {
            peers.remove(fullname);
        }
    }

    pub(crate) fn clear_peers(&self) {
        if let Ok(mut peers) = self.inner.peers_by_fullname.lock() {
            peers.clear();
        }
    }

    fn endpoints_for_peer(&self, device_id: &str) -> Result<Vec<SocketAddr>, String> {
        let peers = self
            .inner
            .peers_by_fullname
            .lock()
            .map_err(|e| format!("peer directory lock: {e}"))?;
        let mut endpoints = peers
            .values()
            .filter(|peer| peer.device_id == device_id)
            .flat_map(|peer| peer.endpoints.iter().copied())
            .collect::<Vec<_>>();
        endpoints.sort_unstable();
        endpoints.dedup();
        if endpoints.is_empty() {
            return Err("设备当前没有可连接的局域网地址".to_string());
        }
        Ok(endpoints)
    }

    fn peer_matches_endpoint(&self, device_id: &str, remote: IpAddr) -> bool {
        self.inner
            .peers_by_fullname
            .lock()
            .map(|peers| {
                peers.values().any(|peer| {
                    peer.device_id == device_id
                        && peer.endpoints.iter().any(|endpoint| {
                            endpoint.ip() == remote
                                || match (endpoint.ip(), remote) {
                                    (IpAddr::V4(v4), IpAddr::V6(v6)) => {
                                        v6.to_ipv4_mapped() == Some(v4)
                                    }
                                    (IpAddr::V6(v6), IpAddr::V4(v4)) => {
                                        v6.to_ipv4_mapped() == Some(v4)
                                    }
                                    _ => false,
                                }
                        })
                })
            })
            .unwrap_or(false)
    }

    fn reserve_session_id(&self, session_id: &str) -> Result<(), String> {
        self.inner
            .replay_guard
            .lock()
            .map_err(|e| format!("session replay guard lock: {e}"))?
            .reserve(session_id)
    }

    fn insert_session(&self, session: Arc<NativeLanSession>) -> Result<(), String> {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .map_err(|e| format!("LAN session registry lock: {e}"))?;
        if sessions.contains_key(&session.session_id) {
            return Err("duplicate active LAN session".to_string());
        }
        sessions.insert(session.session_id.clone(), session);
        Ok(())
    }

    fn session(&self, session_id: &str) -> Result<Arc<NativeLanSession>, String> {
        self.inner
            .sessions
            .lock()
            .map_err(|e| format!("LAN session registry lock: {e}"))?
            .get(session_id)
            .cloned()
            .ok_or_else(|| "LAN session is no longer active".to_string())
    }

    fn remove_session(&self, session_id: &str) {
        if let Ok(mut sessions) = self.inner.sessions.lock() {
            if let Some(session) = sessions.remove(session_id) {
                let _ = session.cancel_tx.send(true);
                transfer::close(&session);
                if let Ok(mut auth) = session.auth.lock() {
                    *auth = LocalAuthState::Closed;
                }
                if let Ok(mut source) = session.source.lock() {
                    source.take();
                }
            }
        }
    }

    fn clear_sessions(&self) {
        let sessions = self
            .inner
            .sessions
            .lock()
            .map(|mut sessions| std::mem::take(&mut *sessions))
            .unwrap_or_default();
        for session in sessions.into_values() {
            let _ = session.cancel_tx.send(true);
            transfer::close(&session);
            if let Ok(mut auth) = session.auth.lock() {
                *auth = LocalAuthState::Closed;
            }
            if let Ok(mut source) = session.source.lock() {
                source.take();
            }
        }
    }

    fn take_source(&self, source_id: &str) -> Result<NativeSource, String> {
        self.inner
            .sources
            .lock()
            .map_err(|e| format!("source registry lock: {e}"))?
            .remove(source_id)
            .ok_or_else(|| "源文件已失效，请重新选择".to_string())
    }

    fn insert_source(&self, source_id: String, source: NativeSource) -> Result<(), String> {
        let mut sources = self
            .inner
            .sources
            .lock()
            .map_err(|e| format!("source registry lock: {e}"))?;
        if sources.insert(source_id, source).is_some() {
            return Err("源文件标识冲突".to_string());
        }
        Ok(())
    }

    fn clear_sources(&self) {
        if let Ok(mut sources) = self.inner.sources.lock() {
            sources.clear();
        }
    }

    pub(crate) fn count_ipc(&self, kind: &str, bytes: usize) {
        if let Ok(mut counters) = self.inner.ipc.lock() {
            let entry = counters.entry(kind.to_string()).or_default();
            entry.calls = entry.calls.saturating_add(1);
            entry.bytes = entry.bytes.saturating_add(bytes as u64);
        }
    }

    fn counters(&self) -> NativeLanIpcCounters {
        let by_type = self
            .inner
            .ipc
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default();
        let total_calls = by_type.values().map(|entry| entry.calls).sum();
        let total_bytes = by_type.values().map(|entry| entry.bytes).sum();
        NativeLanIpcCounters {
            by_type,
            total_calls,
            total_bytes,
        }
    }
}

pub(crate) fn eligible_lan_endpoints(
    addresses: &HashSet<IpAddr>,
    port: u16,
    ipv6_listener_ready: bool,
) -> Vec<SocketAddr> {
    if port == 0 {
        return Vec::new();
    }
    let mut endpoints = addresses
        .iter()
        .copied()
        .filter(|ip| is_local_network_address(*ip, ipv6_listener_ready))
        .map(|ip| SocketAddr::new(ip, port))
        .collect::<Vec<_>>();
    endpoints.sort_unstable();
    endpoints.dedup();
    endpoints
}

fn is_local_network_address(ip: IpAddr, ipv6_listener_ready: bool) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            ip.is_private() || (octets[0] == 169 && octets[1] == 254)
        }
        IpAddr::V6(ip) => ipv6_listener_ready && is_unique_local_v6(ip),
    }
}

fn is_unique_local_v6(ip: Ipv6Addr) -> bool {
    (ip.octets()[0] & 0xfe) == 0xfc
}

#[tauri::command]
pub(crate) fn native_lan_start_mode(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
) -> Result<NativeLanIdentity, String> {
    let runtime = runtime.inner().clone();
    if !runtime.inner.listener_ready.load(Ordering::Acquire) {
        return Err("本机 LAN listener 未能启动；可在线运行，但不能使用局域网测试".to_string());
    }
    runtime.set_enabled(true);
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::SetBrowsing(true));
    let cfg = ensure_config(&app)?.1;
    let identity = NativeLanIdentity {
        device_id: cfg.device_id,
        device_name: cfg.device_name,
    };
    runtime.count_ipc(
        "native_lan_start_mode",
        serde_json::to_vec(&identity).unwrap_or_default().len(),
    );
    for peer in runtime.peer_snapshot() {
        runtime.count_ipc(
            "event:fling://lan/peer-up",
            serde_json::to_vec(&peer)
                .map(|value| value.len())
                .unwrap_or_default(),
        );
        let success = app.emit("fling://lan/peer-up", peer).is_ok();
        crate::discovery_diagnostics::record(crate::discovery_diagnostics::Event::LanUiEvent {
            up: true,
            success,
        });
    }
    Ok(identity)
}

#[tauri::command]
pub(crate) fn native_lan_stop_mode(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
) -> Result<(), String> {
    let runtime = runtime.inner().clone();
    runtime.set_enabled(false);
    runtime.clear_sources();
    runtime.clear_sessions();
    app.state::<DiscoveryState>()
        .send(DiscoveryCmd::SetBrowsing(false));
    runtime.count_ipc("native_lan_stop_mode", 4);
    Ok(())
}

#[tauri::command]
pub(crate) async fn native_lan_start_send(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
    peer_id: String,
    source_id: String,
) -> Result<String, String> {
    let runtime = runtime.inner().clone();
    if !runtime.is_enabled() {
        return Err("请先进入仅局域网测试模式".to_string());
    }
    if !runtime
        .inner
        .sources
        .lock()
        .map_err(|error| format!("source registry lock: {error}"))?
        .contains_key(&source_id)
    {
        return Err("源文件已失效，请重新选择".to_string());
    }
    let endpoints = runtime.endpoints_for_peer(&peer_id)?;
    let cfg = ensure_config(&app)?.1;
    let task_id = random_hex_id();
    let mut last_error = "附近设备当前不可连接".to_string();
    for endpoint in endpoints {
        if !is_local_network_address(endpoint.ip(), runtime.ipv6_listener_ready()) {
            continue;
        }
        let (ephemeral, hello) =
            native_lan::start_pairing(&task_id, &cfg.device_id, &cfg.device_name, &peer_id)?;
        let connected =
            tokio::time::timeout(Duration::from_secs(4), TcpStream::connect(endpoint)).await;
        let mut stream = match connected {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                last_error = format!("无法连接附近设备：{error}");
                continue;
            }
            Err(_) => {
                last_error = "连接附近设备超时".to_string();
                continue;
            }
        };
        stream
            .set_nodelay(true)
            .map_err(|error| format!("configure LAN control socket: {error}"))?;
        tokio::time::timeout(
            Duration::from_secs(10),
            native_lan::write_pairing_hello(&mut stream, &hello),
        )
        .await
        .map_err(|_| "发送配对请求超时".to_string())?
        .map_err(|error| format!("发送配对请求失败：{error}"))?;
        let peer_hello = tokio::time::timeout(
            Duration::from_secs(10),
            native_lan::read_pairing_hello(&mut stream),
        )
        .await
        .map_err(|_| "等待对端配对响应超时".to_string())?
        .map_err(|error| format!("读取对端配对响应失败：{error}"))?;
        if peer_hello.from_device_id != peer_id {
            return Err("配对响应的设备身份与所选设备不一致".to_string());
        }
        runtime.reserve_session_id(&hello.session_id)?;
        let secure = native_lan::establish_local_session(ephemeral, &hello, &peer_hello)?;
        let source = runtime.take_source(&source_id)?;
        install_control_session(
            app.clone(),
            runtime.clone(),
            stream,
            secure,
            PairingRole::Initiator,
            peer_id.clone(),
            Some(source),
        )
        .await?;
        let request_bytes = serde_json::to_vec(&serde_json::json!({
            "peerId": peer_id,
            "sourceId": source_id,
        }))
        .map(|value| value.len())
        .unwrap_or_default();
        let response_bytes = serde_json::to_vec(&task_id)
            .map(|value| value.len())
            .unwrap_or_default();
        runtime.count_ipc(
            "native_lan_start_send",
            request_bytes.saturating_add(response_bytes),
        );
        return Ok(task_id);
    }
    Err(last_error)
}

#[tauri::command]
pub(crate) async fn native_lan_pair_decision(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
    session_id: String,
    approved: bool,
    peer_code: String,
) -> Result<(), String> {
    let runtime = runtime.inner().clone();
    let session = runtime.session(&session_id)?;
    let request_bytes = serde_json::to_vec(&serde_json::json!({
        "sessionId": &session_id,
        "approved": approved,
        "peerCode": &peer_code,
    }))
    .map(|value| value.len())
    .unwrap_or_default();
    runtime.count_ipc("native_lan_pair_decision", request_bytes.saturating_add(4));
    let decision = enqueue_ordered_control(&session.control_tx, &session.control_send_lock, || {
        let mut auth = session
            .auth
            .lock()
            .map_err(|error| format!("LAN auth state lock: {error}"))?;
        let LocalAuthState::Pending(pending) = &mut *auth else {
            return Err("pairing decision is no longer available".to_string());
        };
        let frame = pending.decide_locally(approved, &peer_code)?;
        Ok((frame, pending.decision()))
    })
    .await;
    let decision = match decision {
        Ok(decision) => decision,
        Err(error) => {
            if error.contains("code") || error.contains("verification") {
                let _ = session.claim_terminal();
            }
            return Err(error);
        }
    };
    if decision == PairingDecision::Rejected {
        if !session.claim_terminal() {
            runtime.remove_session(&session_id);
            return Ok(());
        }
        // Allow the encrypted rejection to leave the bounded queue before teardown.
        tokio::time::sleep(Duration::from_millis(80)).await;
        emit_lan_event(
            &app,
            &runtime,
            "fling://lan/progress",
            task_event(&session, "cancelled", 0, 0, 0, None),
        );
        runtime.remove_session(&session_id);
        return Ok(());
    }
    if decision == PairingDecision::Confirmed {
        authenticate_local_session(&session)?;
        emit_lan_event(
            &app,
            &runtime,
            "fling://lan/progress",
            task_event(&session, "paired", 0, 0, 0, None),
        );
        transfer::on_pair_confirmed(&app, &runtime, &session).await?;
    }
    Ok(())
}

fn authenticate_local_session(session: &NativeLanSession) -> Result<(), String> {
    let mut auth = session
        .auth
        .lock()
        .map_err(|error| format!("LAN auth state lock: {error}"))?;
    let pending = match std::mem::replace(&mut *auth, LocalAuthState::Closed) {
        LocalAuthState::Pending(pending) => pending,
        other => {
            *auth = other;
            return Err("pairing state changed unexpectedly".to_string());
        }
    };
    *auth = LocalAuthState::Authenticated(pending.into_authenticated()?);
    Ok(())
}

#[tauri::command]
pub(crate) async fn native_lan_cancel(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
    task_id: String,
) -> Result<(), String> {
    let runtime = runtime.inner().clone();
    let request_bytes = serde_json::to_vec(&serde_json::json!({ "taskId": &task_id }))
        .map(|value| value.len())
        .unwrap_or_default();
    runtime.count_ipc("native_lan_cancel", request_bytes.saturating_add(4));
    let session_id = runtime
        .inner
        .sessions
        .lock()
        .map_err(|error| format!("LAN session registry lock: {error}"))?
        .values()
        .find(|session| session.task_id == task_id)
        .map(|session| session.session_id.clone());
    if let Some(session_id) = session_id {
        if let Ok(session) = runtime.session(&session_id) {
            let _ = try_enqueue_ordered_control(
                &session.control_tx,
                &session.control_send_lock,
                || {
                    let mut auth = session
                        .auth
                        .lock()
                        .map_err(|error| format!("LAN auth state lock: {error}"))?;
                    let LocalAuthState::Authenticated(authenticated) = &mut *auth else {
                        return Err("LAN session is not authenticated".to_string());
                    };
                    let frame =
                        authenticated.encrypt_control(&NativeControlMessage::TaskCancel {
                            task_id: task_id.clone(),
                        })?;
                    Ok((frame, ()))
                },
            );
            if session.claim_terminal() {
                emit_lan_event(
                    &app,
                    &runtime,
                    "fling://lan/progress",
                    task_event(&session, "cancelled", 0, 0, 0, None),
                );
            }
        }
        runtime.remove_session(&session_id);
    }
    Ok(())
}

#[tauri::command]
pub(crate) async fn native_lan_manifest_decision(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
    session_id: String,
    accepted: bool,
) -> Result<(), String> {
    let runtime = runtime.inner().clone();
    let session = runtime.session(&session_id)?;
    let request_bytes = serde_json::to_vec(&serde_json::json!({
        "sessionId": &session_id,
        "accepted": accepted,
    }))
    .map(|value| value.len())
    .unwrap_or_default();
    runtime.count_ipc(
        "native_lan_manifest_decision",
        request_bytes.saturating_add(4),
    );
    transfer::decide_manifest(&app, &runtime, &session, accepted).await
}

#[tauri::command]
pub(crate) async fn native_lan_pick_source(
    app: AppHandle,
    runtime: State<'_, NativeLanRuntime>,
) -> Result<Option<NativeSourceMetadata>, String> {
    let runtime = runtime.inner().clone();
    if !runtime.is_enabled() {
        return Err("请先进入仅局域网测试模式".to_string());
    }
    let (picked_tx, picked_rx) = oneshot::channel();
    app.dialog()
        .file()
        .set_title("选择要发送的文件")
        .pick_file(move |picked| {
            let _ = picked_tx.send(picked);
        });
    let picked = picked_rx
        .await
        .map_err(|_| "文件选择器未能返回结果，请重试".to_string())?;
    let Some(picked) = picked else {
        runtime.count_ipc("native_lan_pick_source", 4);
        return Ok(None);
    };
    let path = picked
        .into_path()
        .map_err(|_| "无法访问所选文件".to_string())?;
    let canonical_path = path
        .canonicalize()
        .map_err(|_| "无法访问所选文件".to_string())?;
    let mut file = File::open(&canonical_path).map_err(|_| "无法打开所选文件".to_string())?;
    let metadata = file
        .metadata()
        .map_err(|_| "无法读取所选文件信息".to_string())?;
    if !metadata.is_file() {
        return Err("请选择单个普通文件".to_string());
    }
    let size_bytes = metadata.len();
    let total_chunks_u64 = if size_bytes == 0 {
        0
    } else {
        size_bytes
            .checked_add(NATIVE_CHUNK_SIZE - 1)
            .ok_or_else(|| "文件大小超出当前测试版本范围".to_string())?
            / NATIVE_CHUNK_SIZE
    };
    if total_chunks_u64 > MAX_SOURCE_CHUNKS {
        return Err("文件大小超出当前测试版本范围".to_string());
    }
    let name = canonical_path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "无法读取所选文件名".to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 256 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| "读取所选文件失败".to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|_| "无法重置所选文件读取位置".to_string())?;
    let source = NativeSource {
        _path: canonical_path,
        file,
        name: name.clone(),
        size_bytes,
        sha256: format!("{:x}", hasher.finalize()),
        total_chunks: total_chunks_u64 as u32,
    };
    let source_id = random_hex_id();
    runtime.insert_source(source_id.clone(), source)?;
    let result = NativeSourceMetadata { source_id };
    runtime.count_ipc(
        "native_lan_pick_source",
        serde_json::to_vec(&result)
            .map(|value| value.len())
            .unwrap_or_default(),
    );
    Ok(Some(result))
}

#[tauri::command]
pub(crate) fn native_lan_ipc_counters(
    runtime: State<'_, NativeLanRuntime>,
) -> NativeLanIpcCounters {
    let runtime = runtime.inner().clone();
    let counters = runtime.counters();
    let response_bytes = serde_json::to_vec(&counters)
        .map(|value| value.len())
        .unwrap_or_default();
    runtime.count_ipc("native_lan_ipc_counters", response_bytes);
    counters
}

fn random_hex_id() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn diagnostic_error_category(reason: &str) -> &'static str {
    if reason.contains("manifest transfer id mismatch")
        || reason.contains("manifest transfer nonce")
        || reason.contains("ACK transfer binding mismatch")
    {
        "manifest-binding-rejected"
    } else if reason.contains("DataChannel") && reason.contains("closed") {
        "channel-closed"
    } else if reason.contains("control write") {
        "control-write-failed"
    } else if reason.contains("control read") {
        "control-read-failed"
    } else if reason.contains("ICE") {
        "local-connectivity-failed"
    } else if reason.contains("cancel") || reason.contains("reject") {
        "cancelled-or-rejected"
    } else if reason.contains("SHA") || reason.contains("hash") {
        "integrity-check-failed"
    } else if reason.contains("save")
        || reason.contains("store")
        || reason.contains("commit")
        || reason.contains("sync")
    {
        "storage-failed"
    } else {
        "protocol-or-operation-failed"
    }
}

fn safe_failure_log(stage: &'static str, reason: &str) -> String {
    format!("[fling] LAN {stage}: {}", diagnostic_error_category(reason))
}

fn public_lan_error(reason: &str) -> String {
    match diagnostic_error_category(reason) {
        "manifest-binding-rejected" => "无法开始传输，请重新配对并重试",
        "channel-closed"
        | "control-write-failed"
        | "control-read-failed"
        | "local-connectivity-failed" => "连接中断，可重试",
        "storage-failed" => "无法保存文件，请检查接收目录与可用空间后重试",
        "integrity-check-failed" => "文件校验未通过，请重新发送",
        "cancelled-or-rejected" => "本次传输已取消",
        _ => "无法完成传输，请重新配对并重试",
    }
    .to_string()
}

fn transfer_id_for_session(session_id: &str, task_id: &str, transfer_nonce: &str) -> String {
    // Session/task are transcript-bound; the per-transfer nonce travels only in
    // authenticated control. Distinct files in one session must use fresh nonces.
    let mut digest = Sha256::new();
    digest.update(b"fling/native-lan-transfer-id/v1\0");
    digest.update((session_id.len() as u64).to_be_bytes());
    digest.update(session_id.as_bytes());
    digest.update((task_id.len() as u64).to_be_bytes());
    digest.update(task_id.as_bytes());
    digest.update((transfer_nonce.len() as u64).to_be_bytes());
    digest.update(transfer_nonce.as_bytes());
    digest.finalize()[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_claim_suppresses_cleanup_after_pairing_failure_but_allows_disconnect() {
        let (tx, _rx) = watch::channel(false);
        let session = NativeLanSession {
            session_id: "session".into(),
            task_id: "task".into(),
            role: PairingRole::Initiator,
            peer_device_id: "peer".into(),
            peer_device_label: "Peer".into(),
            auth: Mutex::new(LocalAuthState::Closed),
            source: Mutex::new(None),
            webrtc_started: AtomicBool::new(false),
            manifest_sent: AtomicBool::new(false),
            ack_next_seq: AtomicU32::new(0),
            ack_notify: Notify::new(),
            durable_complete: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            terminal_claimed: AtomicBool::new(false),
            data_channel_tx: async_mpsc::unbounded_channel().0,
            transfer: Mutex::new(TransferState::default()),
            control_tx: async_mpsc::channel(1).0,
            control_send_lock: AsyncMutex::new(()),
            cancel_tx: tx,
        };
        assert!(session.claim_terminal()); // wrong-code/rejection is primary
        assert!(!session.claim_terminal()); // cleanup close/EOF cannot replace or append

        let (tx, _rx) = watch::channel(false);
        let disconnected = NativeLanSession {
            session_id: "session-2".into(),
            task_id: "task-2".into(),
            role: PairingRole::Responder,
            peer_device_id: "peer".into(),
            peer_device_label: "Peer".into(),
            auth: Mutex::new(LocalAuthState::Closed),
            source: Mutex::new(None),
            webrtc_started: AtomicBool::new(false),
            manifest_sent: AtomicBool::new(false),
            ack_next_seq: AtomicU32::new(0),
            ack_notify: Notify::new(),
            durable_complete: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            terminal_claimed: AtomicBool::new(false),
            data_channel_tx: async_mpsc::unbounded_channel().0,
            transfer: Mutex::new(TransferState::default()),
            control_tx: async_mpsc::channel(1).0,
            control_send_lock: AsyncMutex::new(()),
            cancel_tx: tx,
        };
        assert!(disconnected.claim_terminal()); // genuine disconnect remains claimable
    }

    #[test]
    fn authenticated_pair_sides_derive_the_same_bound_transfer_id() {
        let task = "0123456789abcdef0123456789abcdef";
        let (a_key, a_hello) =
            native_lan::start_pairing(task, "device-a", "A", "device-b").unwrap();
        let (b_key, b_hello) =
            native_lan::respond_to_pairing(&a_hello, "device-b", "B", "device-a").unwrap();
        let a = PendingLocalPair::new(
            native_lan::establish_local_session(a_key, &a_hello, &b_hello).unwrap(),
        );
        let b = PendingLocalPair::new(
            native_lan::establish_local_session(b_key, &b_hello, &a_hello).unwrap(),
        );
        let nonce = "ffeeddccbbaa99887766554433221100";
        let a_id = transfer_id_for_session(a.session_id().unwrap(), a.task_id().unwrap(), nonce);
        let b_id = transfer_id_for_session(b.session_id().unwrap(), b.task_id().unwrap(), nonce);
        assert_eq!(a_id, b_id);
        assert_eq!(a_id.len(), 24);
        assert!(a_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(
            a_id,
            transfer_id_for_session("another-session", task, nonce)
        );
        assert_ne!(
            a_id,
            transfer_id_for_session(a.session_id().unwrap(), "another-task", nonce)
        );
        assert_ne!(
            a_id,
            transfer_id_for_session(a.session_id().unwrap(), task, "another-transfer")
        );
    }

    #[test]
    fn diagnostic_and_public_errors_never_echo_sensitive_input() {
        let sentinels = [
            "raw-session-sentinel",
            "raw-transfer-id-sentinel",
            "key-material-sentinel",
            "salt-marker-sentinel",
        ];
        let reason = sentinels.join(" ");
        let log = safe_failure_log("test-stage", &reason);
        let public = public_lan_error(&reason);
        for sentinel in sentinels {
            assert!(!log.contains(sentinel));
            assert!(!public.contains(sentinel));
        }
        assert_eq!(log, "[fling] LAN test-stage: protocol-or-operation-failed");
        assert_eq!(public, "无法完成传输，请重新配对并重试");
    }

    #[test]
    fn peer_snapshot_replays_display_metadata_without_network_endpoints() {
        let runtime = NativeLanRuntime::new();
        let endpoint = "192.168.4.20:4444".parse().unwrap();
        runtime.upsert_peer("one", "device-b", "B", vec![endpoint]);
        runtime.upsert_peer("duplicate", "device-b", "B", vec![endpoint]);
        runtime.upsert_peer("empty", "device-c", "C", vec![]);
        let snapshot = runtime.peer_snapshot();
        assert_eq!(snapshot.len(), 1);
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(
            value,
            serde_json::json!([{ "id": "device-b", "name": "B" }])
        );
        runtime.remove_peer("one");
        runtime.remove_peer("duplicate");
        assert!(runtime.peer_snapshot().is_empty());
    }

    #[test]
    fn endpoint_filter_rejects_global_addresses_and_keeps_private_lan_addresses() {
        let addresses = [
            IpAddr::V4(Ipv4Addr::new(192, 168, 4, 20)),
            IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 1, 2)),
            IpAddr::V6("fd12::1".parse().unwrap()),
            IpAddr::V6("2001:4860:4860::8888".parse().unwrap()),
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        let endpoints = eligible_lan_endpoints(&addresses, 4444, true);
        assert_eq!(endpoints.len(), 4);
        assert!(endpoints.iter().all(|endpoint| endpoint.port() == 4444));
        assert!(!eligible_lan_endpoints(&addresses, 0, true)
            .iter()
            .any(|_| true));
        assert_eq!(eligible_lan_endpoints(&addresses, 4444, false).len(), 3);
    }

    #[test]
    fn source_metadata_contains_no_filesystem_path_or_handle() {
        let metadata = NativeSourceMetadata {
            source_id: "0123456789abcdef0123456789abcdef".to_string(),
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("sourceId"));
        assert!(!json.contains("name"));
        assert!(!json.contains("sizeBytes"));
        assert!(!json.contains("path"));
        assert!(!json.contains("sha256"));
        assert!(!json.contains("fileHandle"));
    }

    #[test]
    fn full_control_queue_does_not_build_or_consume_a_sequence_number() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (sender, mut receiver) = async_mpsc::channel(1);
            sender.send("already queued").await.unwrap();
            let send_lock = Arc::new(AsyncMutex::new(()));
            let builds = Arc::new(AtomicU32::new(0));
            let queued_sender = sender.clone();
            let queued_lock = send_lock.clone();
            let queued_builds = builds.clone();
            let pending = tokio::spawn(async move {
                enqueue_ordered_control(&queued_sender, &queued_lock, || {
                    queued_builds.fetch_add(1, Ordering::AcqRel);
                    Ok(("next frame", ()))
                })
                .await
            });

            tokio::task::yield_now().await;
            assert_eq!(builds.load(Ordering::Acquire), 0);
            assert_eq!(receiver.recv().await, Some("already queued"));
            pending.await.unwrap().unwrap();
            assert_eq!(builds.load(Ordering::Acquire), 1);
            assert_eq!(receiver.recv().await, Some("next frame"));
        });
    }

    #[test]
    fn concurrent_control_builders_enqueue_in_sequence_order() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (sender, mut receiver) = async_mpsc::channel(2);
            let send_lock = Arc::new(AsyncMutex::new(()));
            let sequence = Arc::new(AtomicU32::new(0));
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let queued_sender = sender.clone();
                let queued_lock = send_lock.clone();
                let queued_sequence = sequence.clone();
                tasks.push(tokio::spawn(async move {
                    enqueue_ordered_control(&queued_sender, &queued_lock, || {
                        let assigned = queued_sequence.fetch_add(1, Ordering::AcqRel);
                        Ok((assigned, ()))
                    })
                    .await
                }));
            }
            for task in tasks {
                task.await.unwrap().unwrap();
            }
            assert_eq!(receiver.recv().await, Some(0));
            assert_eq!(receiver.recv().await, Some(1));
        });
    }
}

/// Start the native TCP control listener. File frames are never handled here;
/// this socket is reserved for the bounded pairing/signaling/ACK control lane.
pub(crate) fn bind_lan_listeners() -> Result<(Vec<std::net::TcpListener>, u16, bool), String> {
    let ipv4 = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
        .map_err(|e| format!("bind IPv4 LAN listener: {e}"))?;
    let port = ipv4
        .local_addr()
        .map_err(|e| format!("read IPv4 LAN listener address: {e}"))?
        .port();
    let mut listeners = vec![ipv4];
    let ipv6_ready = (|| -> Result<(), String> {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
            .map_err(|e| format!("create IPv6 LAN socket: {e}"))?;
        socket
            .set_only_v6(true)
            .map_err(|e| format!("configure IPv6-only LAN socket: {e}"))?;
        socket
            .set_reuse_address(true)
            .map_err(|e| format!("configure IPv6 LAN socket: {e}"))?;
        let address = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        socket
            .bind(&address.into())
            .map_err(|e| format!("bind IPv6 LAN socket: {e}"))?;
        socket
            .listen(64)
            .map_err(|e| format!("listen on IPv6 LAN socket: {e}"))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| format!("configure IPv6 LAN socket: {e}"))?;
        let listener: std::net::TcpListener = socket.into();
        listeners.push(listener);
        Ok(())
    })()
    .is_ok();
    Ok((listeners, port, ipv6_ready))
}

pub(crate) fn start_listener(
    app: AppHandle,
    listeners: Vec<std::net::TcpListener>,
    ipv6_ready: bool,
) -> Result<(), String> {
    if listeners.is_empty() {
        return Err("no LAN TCP listeners were bound".to_string());
    }
    for listener in &listeners {
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("configure LAN listener: {e}"))?;
    }
    let setup_app = app.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    tauri::async_runtime::spawn(async move {
        let mut tcp_listeners = Vec::with_capacity(listeners.len());
        for listener in listeners {
            match tokio::net::TcpListener::from_std(listener) {
                Ok(listener) => tcp_listeners.push(listener),
                Err(error) => {
                    let _ = ready_tx.send(Err(format!("start LAN listener: {error}")));
                    return;
                }
            }
        }
        setup_app
            .state::<NativeLanRuntime>()
            .set_listener_ready(ipv6_ready);
        let _ = ready_tx.send(Ok(()));
        for listener in tcp_listeners {
            let app = setup_app.clone();
            tauri::async_runtime::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, remote)) => {
                            let runtime = app.state::<NativeLanRuntime>().inner().clone();
                            if !runtime.is_enabled() || !is_local_network_address(remote.ip(), true)
                            {
                                drop(stream);
                                continue;
                            }
                            let app = app.clone();
                            tauri::async_runtime::spawn(async move {
                                if let Err(error) =
                                    handle_incoming_pairing(app, runtime, stream, remote.ip()).await
                                {
                                    eprintln!("{}", safe_failure_log("pairing-rejected", &error));
                                }
                            });
                        }
                        Err(error) => {
                            eprintln!(
                                "{}",
                                safe_failure_log("listener-accept", &error.to_string())
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            });
        }
    });
    ready_rx
        .recv()
        .map_err(|_| "LAN listener initialization task stopped before ready".to_string())?
}

async fn handle_incoming_pairing(
    app: AppHandle,
    runtime: NativeLanRuntime,
    mut stream: TcpStream,
    remote_ip: IpAddr,
) -> Result<(), String> {
    let hello = tokio::time::timeout(
        Duration::from_secs(10),
        native_lan::read_pairing_hello(&mut stream),
    )
    .await
    .map_err(|_| "LAN pairing hello timed out".to_string())?
    .map_err(|error| format!("read LAN pairing hello: {error}"))?;
    if !runtime.peer_matches_endpoint(&hello.from_device_id, remote_ip) {
        return Err("LAN pairing peer was not discovered at this address".to_string());
    }
    runtime.reserve_session_id(&hello.session_id)?;
    let cfg = ensure_config(&app)?.1;
    let (ephemeral, local_hello) = native_lan::respond_to_pairing(
        &hello,
        &cfg.device_id,
        &cfg.device_name,
        &hello.from_device_id,
    )?;
    tokio::time::timeout(
        Duration::from_secs(10),
        native_lan::write_pairing_hello(&mut stream, &local_hello),
    )
    .await
    .map_err(|_| "LAN pairing response timed out".to_string())?
    .map_err(|error| format!("write LAN pairing response: {error}"))?;
    let secure = native_lan::establish_local_session(ephemeral, &local_hello, &hello)?;
    install_control_session(
        app,
        runtime,
        stream,
        secure,
        PairingRole::Responder,
        hello.from_device_id,
        None,
    )
    .await
}

async fn install_control_session(
    app: AppHandle,
    runtime: NativeLanRuntime,
    stream: TcpStream,
    secure: native_lan::SecureLocalSession,
    role: PairingRole,
    peer_device_id: String,
    source: Option<NativeSource>,
) -> Result<(), String> {
    let pending = PendingLocalPair::new(secure);
    let session_id = pending
        .session_id()
        .ok_or_else(|| "pairing session metadata missing".to_string())?
        .to_string();
    let task_id = pending
        .task_id()
        .ok_or_else(|| "pairing task metadata missing".to_string())?
        .to_string();
    let peer_device_label = pending
        .peer_device_label()
        .ok_or_else(|| "pairing peer label missing".to_string())?
        .to_string();
    let verification_code = pending
        .verification_code()
        .ok_or_else(|| "pairing verification code missing".to_string())?
        .to_string();
    let (control_tx, control_rx) = async_mpsc::channel(32);
    let (data_channel_tx, data_channel_rx) = async_mpsc::unbounded_channel();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let session = Arc::new(NativeLanSession {
        session_id: session_id.clone(),
        task_id: task_id.clone(),
        role,
        peer_device_id: peer_device_id.clone(),
        peer_device_label: peer_device_label.clone(),
        auth: Mutex::new(LocalAuthState::Pending(pending)),
        source: Mutex::new(source),
        webrtc_started: AtomicBool::new(false),
        manifest_sent: AtomicBool::new(false),
        ack_next_seq: AtomicU32::new(0),
        ack_notify: Notify::new(),
        durable_complete: AtomicBool::new(false),
        completed: AtomicBool::new(false),
        terminal_claimed: AtomicBool::new(false),
        data_channel_tx,
        transfer: Mutex::new(TransferState::default()),
        control_tx,
        control_send_lock: AsyncMutex::new(()),
        cancel_tx,
    });
    runtime.insert_session(session.clone())?;
    let role_label = match role {
        PairingRole::Initiator => "initiator",
        PairingRole::Responder => "responder",
    };
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/pairing",
        PairingEvent {
            session_id,
            task_id,
            peer_device_id,
            peer_device_label,
            verification_code,
            role: role_label,
        },
    );
    transfer::start_data_channel_router(
        app.clone(),
        runtime.clone(),
        session.clone(),
        data_channel_rx,
    );
    let (reader, writer) = stream.into_split();
    let writer_runtime = runtime.clone();
    let writer_session = session.clone();
    let writer_cancel = cancel_rx.clone();
    let writer_app = app.clone();
    tauri::async_runtime::spawn(async move {
        run_control_writer(
            writer_app,
            writer_runtime,
            writer_session,
            writer,
            control_rx,
            writer_cancel,
        )
        .await;
    });
    tauri::async_runtime::spawn(async move {
        run_control_reader(app, runtime, session, reader, cancel_rx).await;
    });
    Ok(())
}

async fn run_control_writer(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut control_rx: async_mpsc::Receiver<EncryptedControlFrame>,
    mut cancel_rx: watch::Receiver<bool>,
) {
    let reason = loop {
        tokio::select! {
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() { break "session cancelled".to_string(); }
            }
            frame = control_rx.recv() => {
                let Some(frame) = frame else { break "control queue closed".to_string(); };
                let result = tokio::select! {
                    changed = cancel_rx.changed() => {
                        if changed.is_err() || *cancel_rx.borrow() { break "session cancelled during write".to_string(); }
                        continue;
                    }
                    result = tokio::time::timeout(Duration::from_secs(15), native_lan::write_control_frame(&mut writer, &frame)) => result,
                };
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break format!("control write failed: {error}"),
                    Err(_) => break "control write timed out".to_string(),
                }
            }
        }
    };
    eprintln!("{}", safe_failure_log("control-writer-stopped", &reason));
    if !*session.cancel_tx.borrow() && !session.completed.load(Ordering::Acquire) {
        interrupt_session(&app, &runtime, &session, reason, LanFailureKind::Error);
    }
    runtime.remove_session(&session.session_id);
}

async fn run_control_reader(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut cancel_rx: watch::Receiver<bool>,
) {
    let reason = loop {
        let frame = tokio::select! {
            changed = cancel_rx.changed() => {
                if changed.is_err() || *cancel_rx.borrow() { break "LAN session cancelled".to_string(); }
                continue;
            }
            frame = native_lan::read_control_frame(&mut reader) => {
                match frame {
                    Ok(frame) => frame,
                    Err(error) => break format!("LAN control read failed: {error}"),
                }
            }
        };
        let outcome = match process_control_frame(&session, &frame) {
            Ok(outcome) => outcome,
            Err(error) => break error,
        };
        match outcome {
            ControlOutcome::PairingPending => {}
            ControlOutcome::PairingConfirmed => {
                emit_lan_event(
                    &app,
                    &runtime,
                    "fling://lan/progress",
                    task_event(&session, "paired", 0, 0, 0, None),
                );
                if let Err(error) = transfer::on_pair_confirmed(&app, &runtime, &session).await {
                    break error;
                }
            }
            ControlOutcome::Message(message) => {
                if let Err(error) =
                    transfer::handle_authenticated_control(&app, &runtime, &session, message).await
                {
                    break error;
                }
            }
            ControlOutcome::PeerRejected => break "peer rejected LAN pairing".to_string(),
        }
    };
    eprintln!("{}", safe_failure_log("control-reader-stopped", &reason));
    if session.completed.load(Ordering::Acquire) || *session.cancel_tx.borrow() {
        runtime.remove_session(&session.session_id);
        return;
    }
    if reason.starts_with("LAN control read failed:") {
        interrupt_session(&app, &runtime, &session, reason, LanFailureKind::Error);
        runtime.remove_session(&session.session_id);
        return;
    }
    if !session.claim_terminal() {
        runtime.remove_session(&session.session_id);
        return;
    }
    let phase = if reason.contains("manifest transfer id mismatch")
        || reason.contains("manifest transfer nonce")
    {
        "start-rejected"
    } else if reason.contains("cancel") || reason.contains("reject") {
        "cancelled"
    } else {
        "failed"
    };
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/progress",
        task_event(&session, phase, 0, 0, 0, Some(reason)),
    );
    runtime.remove_session(&session.session_id);
}

enum ControlOutcome {
    PairingPending,
    PairingConfirmed,
    PeerRejected,
    Message(NativeControlMessage),
}

fn process_control_frame(
    session: &NativeLanSession,
    frame: &EncryptedControlFrame,
) -> Result<ControlOutcome, String> {
    let mut auth = session
        .auth
        .lock()
        .map_err(|error| format!("LAN auth state lock: {error}"))?;
    match &mut *auth {
        LocalAuthState::Pending(pending) => {
            let decision = pending.accept_peer_frame(frame)?;
            match decision {
                PairingDecision::Confirmed => {
                    let pending = match std::mem::replace(&mut *auth, LocalAuthState::Closed) {
                        LocalAuthState::Pending(pending) => pending,
                        _ => unreachable!(),
                    };
                    *auth = LocalAuthState::Authenticated(pending.into_authenticated()?);
                    Ok(ControlOutcome::PairingConfirmed)
                }
                PairingDecision::Rejected => Ok(ControlOutcome::PeerRejected),
                PairingDecision::AwaitingLocalApproval | PairingDecision::AwaitingPeerApproval => {
                    Ok(ControlOutcome::PairingPending)
                }
            }
        }
        LocalAuthState::Authenticated(authenticated) => {
            let message = authenticated.decrypt_control(frame)?;
            Ok(ControlOutcome::Message(message))
        }
        LocalAuthState::Closed => Err("LAN session is closed".to_string()),
    }
}

fn task_event(
    session: &NativeLanSession,
    phase: &str,
    received_bytes: u64,
    saved_bytes: u64,
    total_bytes: u64,
    error: Option<String>,
) -> LanTaskEvent {
    task_event_with_metadata(
        session,
        phase,
        received_bytes,
        saved_bytes,
        total_bytes,
        error,
        None,
    )
}

fn task_event_with_metadata(
    session: &NativeLanSession,
    phase: &str,
    received_bytes: u64,
    saved_bytes: u64,
    total_bytes: u64,
    error: Option<String>,
    name: Option<String>,
) -> LanTaskEvent {
    eprintln!("[fling] LAN stage: {phase}");
    LanTaskEvent {
        task_id: session.task_id.clone(),
        phase: phase.to_string(),
        route: "LAN host-candidate DataChannel".to_string(),
        name,
        received_bytes,
        saved_bytes,
        total_bytes,
        speed_bps: 0,
        error: error.as_deref().map(public_lan_error),
        failure_kind: None,
    }
}

fn interrupt_session(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    reason: String,
    kind: LanFailureKind,
) {
    if session.completed.load(Ordering::Acquire)
        || *session.cancel_tx.borrow()
        || !session.claim_terminal()
    {
        return;
    }
    let mut event = task_event(session, "interrupted", 0, 0, 0, Some(reason));
    event.failure_kind = Some(kind);
    eprintln!("[fling] LAN interruption: {kind:?}");
    emit_lan_event(app, runtime, "fling://lan/progress", event);
    runtime.remove_session(&session.session_id);
}

fn emit_lan_event<T: Serialize + Clone>(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    event: &str,
    payload: T,
) {
    let bytes = serde_json::to_vec(&payload)
        .map(|value| value.len())
        .unwrap_or_default();
    runtime.count_ipc(&format!("event:{event}"), bytes);
    let _ = app.emit(event, payload);
}
