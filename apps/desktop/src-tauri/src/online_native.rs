//! One ordinary disk file per task. Task state is authoritative in Rust.
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{Mutex, Notify};
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTaskConfig {
    pub task_id: String,
    pub direction: String,
    pub name: String,
    pub size: u64,
}
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTaskStatus {
    pub task_id: String,
    pub direction: String,
    pub stage: String,
    pub name: String,
    pub total_bytes: u64,
    pub local_bytes: u64,
    pub peer_bytes: u64,
    pub saved_bytes: u64,
    pub sha256: Option<String>,
    pub saved_path: Option<String>,
    pub error: Option<String>,
}
impl NativeTaskStatus {
    pub fn terminal(&self) -> bool {
        matches!(self.stage.as_str(), "done" | "error" | "cancelled")
    }
}
pub struct NativeTask {
    pub status: Mutex<NativeTaskStatus>,
    pub claimed: AtomicBool,
    pub task_acked: AtomicBool,
    pub ack: Notify,
    pub source_cancelled: Arc<AtomicBool>,
}
impl NativeTask {
    pub fn new(config: NativeTaskConfig) -> Result<Self, String> {
        if !matches!(config.direction.as_str(), "sending" | "receiving")
            || config.task_id.is_empty()
        {
            return Err("invalid native task configuration".into());
        }
        Ok(Self {
            status: Mutex::new(NativeTaskStatus {
                task_id: config.task_id,
                direction: config.direction,
                stage: "waiting".into(),
                name: config.name,
                total_bytes: config.size,
                local_bytes: 0,
                peer_bytes: 0,
                saved_bytes: 0,
                sha256: None,
                saved_path: None,
                error: None,
            }),
            claimed: AtomicBool::new(false),
            task_acked: AtomicBool::new(false),
            ack: Notify::new(),
            source_cancelled: Arc::new(AtomicBool::new(false)),
        })
    }
    pub fn cancel_source(&self) {
        self.source_cancelled.store(true, Ordering::Release);
    }
}
