//! OS drop ingress stays in Rust. Default Tauri JS event subscriptions are denied.
use serde::Serialize;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tauri::{Emitter, Manager};
#[derive(Default)]
pub struct DropState {
    busy: Arc<Mutex<HashSet<String>>>,
}
struct DropBusy {
    busy: Arc<Mutex<HashSet<String>>>,
    owner: String,
}
impl Drop for DropBusy {
    fn drop(&mut self) {
        if let Ok(mut busy) = self.busy.lock() {
            busy.remove(&self.owner);
        }
    }
}
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DropResult {
    sources: Vec<crate::online_source::SourceMetadata>,
    x: f64,
    y: f64,
    error: Option<String>,
}
pub fn handle(app: &tauri::AppHandle, owner: &str, event: &tauri::DragDropEvent) {
    match event {
        tauri::DragDropEvent::Enter { paths, .. } => {
            let _ = app.emit_to(owner, "fling://sources/enter", paths.len());
        }
        tauri::DragDropEvent::Leave => {
            let _ = app.emit_to(owner, "fling://sources/leave", ());
        }
        tauri::DragDropEvent::Drop { paths, position } => {
            let state = app.state::<DropState>();
            let x = f64::from(position.x);
            let y = f64::from(position.y);
            let Ok(mut busy) = state.busy.lock() else {
                return;
            };
            if !busy.insert(owner.into()) {
                let _ = app.emit_to(
                    owner,
                    "fling://sources/drop",
                    DropResult {
                        sources: Vec::new(),
                        x,
                        y,
                        error: Some("上一次拖放仍在处理中，请稍后重试".into()),
                    },
                );
                return;
            }
            drop(busy);
            let guard = DropBusy {
                busy: state.busy.clone(),
                owner: owner.into(),
            };
            let app = app.clone();
            let owner = owner.to_string();
            let paths = paths.clone();
            tauri::async_runtime::spawn_blocking(move || {
                let _guard = guard;
                let registry = app.state::<crate::online_source::OnlineSources>();
                let (sources, error) = match registry.register_selection(&owner, Some(paths)) {
                    Ok(sources) => (sources, None),
                    Err(error) => (Vec::new(), Some(error)),
                };
                if app.get_webview_window(&owner).is_none() {
                    registry.remove_owner(&owner);
                    return;
                }
                let result = DropResult {
                    sources,
                    x,
                    y,
                    error,
                };
                if app
                    .emit_to(&owner, "fling://sources/drop", &result)
                    .is_err()
                {
                    for source in result.sources {
                        let _ = registry.remove(&owner, &source.source_id);
                    }
                }
            });
        }
        _ => {}
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn drop_busy_is_window_scoped_and_released_on_error() {
        let state = DropState::default();
        assert!(state.busy.lock().unwrap().insert("main".into()));
        assert!(!state.busy.lock().unwrap().insert("main".into()));
        assert!(state.busy.lock().unwrap().insert("other".into()));
        {
            let _guard = DropBusy {
                busy: state.busy.clone(),
                owner: "main".into(),
            };
        }
        assert!(state.busy.lock().unwrap().contains("other"));
        assert!(!state.busy.lock().unwrap().contains("main"));
    }
}
