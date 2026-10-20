//! Allowlisted UI event bridge. Never subscribe JS to Tauri's path-bearing drag events.
use serde::Serialize;
use std::{collections::HashMap, sync::Mutex};
use tauri::{ipc::Channel, Listener, Manager};
#[derive(Default)]
pub struct UiListeners(Mutex<HashMap<tauri::EventId, String>>);
#[derive(Clone, Serialize)]
pub struct UiEvent {
    event: String,
    id: tauri::EventId,
    payload: serde_json::Value,
}
fn allowed(event: &str) -> bool {
    matches!(
        event,
        "fling:diagnostic"
            | "fling://online/event"
            | "fling://peer-up"
            | "fling://peer-down"
            | "fling://lan/peer-up"
            | "fling://lan/peer-down"
            | "fling://lan/pairing"
            | "fling://lan/manifest"
            | "fling://lan/progress"
            | "fling://sources/enter"
            | "fling://sources/leave"
            | "fling://sources/drop"
    )
}
#[tauri::command]
pub fn ui_listen(
    window: tauri::WebviewWindow,
    listeners: tauri::State<'_, UiListeners>,
    event: String,
    channel: Channel<UiEvent>,
) -> Result<tauri::EventId, String> {
    if !allowed(&event) {
        return Err("UI event not allowed".into());
    }
    let mut active = listeners
        .0
        .lock()
        .map_err(|_| "UI listener registry unavailable")?;
    if active.len() >= 256
        || active
            .values()
            .filter(|owner| owner.as_str() == window.label())
            .count()
            >= 64
    {
        return Err("UI listener limit reached".into());
    }
    let event_name = event.clone();
    let id = window.listen(event, move |e| {
        if let Ok(payload) = serde_json::from_str(e.payload()) {
            let _ = channel.send(UiEvent {
                event: event_name.clone(),
                id: e.id(),
                payload,
            });
        }
    });
    active.insert(id, window.label().into());
    Ok(id)
}
#[tauri::command]
pub fn ui_unlisten(
    window: tauri::WebviewWindow,
    listeners: tauri::State<'_, UiListeners>,
    id: tauri::EventId,
) -> Result<(), String> {
    let mut active = listeners
        .0
        .lock()
        .map_err(|_| "UI listener registry unavailable")?;
    if let Some(owner) = active.get(&id) {
        if owner != window.label() {
            return Err("UI listener not authorized".into());
        }
        active.remove(&id);
        window.unlisten(id);
    }
    Ok(())
}
pub fn cleanup(window: &tauri::Window) {
    if let Ok(mut active) = window.state::<UiListeners>().0.lock() {
        active.retain(|id, owner| {
            if owner == window.label() {
                window.unlisten(*id);
                false
            } else {
                true
            }
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn path_events_and_forged_events_cannot_be_subscribed() {
        for event in [
            "tauri://drag-enter",
            "tauri://drag-drop",
            "tauri://drag-over",
            "arbitrary",
        ] {
            assert!(!allowed(event));
        }
        assert!(allowed("fling://sources/drop"));
        assert!(allowed("fling://online/event"));
    }
}
