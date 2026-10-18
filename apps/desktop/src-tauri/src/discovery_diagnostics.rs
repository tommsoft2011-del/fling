use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
const CAP: u64 = 1024 * 1024;
const ROTATED: &str = "discovery.log.1";
static LOGGER: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static IO_LOCK: Mutex<()> = Mutex::new(());
#[derive(Clone, Copy, Debug)]
pub enum Event {
    DaemonInitSucceeded,
    DaemonInitFailed,
    BrowsingStarted,
    RefreshRequested,
    RefreshSucceeded,
    RefreshFailed,
    RefreshStopFailed,
    RefreshBrowseFailed,
    RefreshStartConfirmationFailed,
    RefreshTimeout,
    PeerUpCount(u32),
    PeerDownCount(u32),
}
impl Event {
    fn label(self) -> &'static str {
        match self {
            Self::DaemonInitSucceeded => "daemon_init_succeeded",
            Self::DaemonInitFailed => "daemon_init_failed",
            Self::BrowsingStarted => "browsing_started",
            Self::RefreshRequested => "refresh_requested",
            Self::RefreshSucceeded => "refresh_started",
            Self::RefreshFailed => "refresh_failed",
            Self::RefreshStopFailed => "refresh_stop_failed",
            Self::RefreshBrowseFailed => "refresh_browse_failed",
            Self::RefreshStartConfirmationFailed => "refresh_start_confirmation_failed",
            Self::RefreshTimeout => "refresh_timeout",
            Self::PeerUpCount(_) => "peer_up_count",
            Self::PeerDownCount(_) => "peer_down_count",
        }
    }
    fn count(self) -> Option<u32> {
        match self {
            Self::PeerUpCount(n) | Self::PeerDownCount(n) => Some(n),
            _ => None,
        }
    }
}
fn append_to(path: &Path, event: Event) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let line = match event.count() {
        Some(n) => format!("{ts} {} {n}\n", event.label()),
        None => format!("{ts} {}\n", event.label()),
    };
    if path
        .metadata()
        .map(|m| m.len().saturating_add(line.len() as u64) > CAP)
        .unwrap_or(false)
    {
        let old = path.with_file_name(ROTATED);
        let _ = fs::remove_file(&old);
        fs::rename(path, old)?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    f.write_all(line.as_bytes())
}
pub fn initialize(app_data: &Path) {
    let path = app_data.join("discovery-diagnostics").join("discovery.log");
    let _ = fs::create_dir_all(path.parent().unwrap_or(app_data));
    *LOGGER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(path);
}
pub fn record(event: Event) {
    let _guard = IO_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(lock) = LOGGER.get() {
        if let Some(path) = lock.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            let _ = append_to(path, event);
        }
    }
}
pub fn export_text(app_data: &Path) -> std::io::Result<String> {
    let _guard = IO_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = app_data.join("discovery-diagnostics");
    let mut out = String::new();
    for name in [ROTATED, "discovery.log"] {
        let p = dir.join(name);
        if p.exists() {
            let mut bytes = Vec::new();
            fs::File::open(p)?.take(CAP).read_to_end(&mut bytes)?;
            out.push_str(&String::from_utf8_lossy(&bytes));
        }
    }
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn temp() -> PathBuf {
        std::env::temp_dir().join(format!("fling-diagnostics-test-{}", rand::random::<u64>()))
    }
    #[test]
    fn rotation_is_bounded_and_export_is_allowlisted() {
        let d = temp();
        let dir = d.join("discovery-diagnostics");
        let log = dir.join("discovery.log");
        fs::create_dir_all(&dir).unwrap();
        let line = format!(
            "{} refresh_failed\n",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let mut data = line.repeat(CAP as usize / line.len());
        data.truncate(CAP as usize);
        while data.len() < CAP as usize {
            data.push(' ');
        }
        fs::write(&log, data).unwrap();
        append_to(&log, Event::RefreshSucceeded).unwrap();
        assert!(log.metadata().unwrap().len() <= CAP);
        assert!(log.with_file_name(ROTATED).metadata().unwrap().len() <= CAP);
        let text = export_text(&d).unwrap();
        assert!(text.contains("refresh_failed") && text.contains("refresh_started"));
        assert!(text.len() <= (2 * CAP) as usize);
        assert!(
            !text.contains("/Users/") && !text.contains("device-id") && !text.contains("filename")
        );
        // A second rotation replaces the old backup instead of accumulating files.
        fs::write(&log, " ".repeat(CAP as usize)).unwrap();
        append_to(&log, Event::RefreshTimeout).unwrap();
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
        assert!(export_text(&d).unwrap().len() <= (2 * CAP) as usize);
        let _ = fs::remove_dir_all(d);
    }
    #[test]
    fn event_output_contains_only_enumerated_categories_and_counts() {
        let d = temp();
        let p = d.join("discovery-diagnostics").join("discovery.log");
        for event in [
            Event::DaemonInitSucceeded,
            Event::DaemonInitFailed,
            Event::BrowsingStarted,
            Event::RefreshRequested,
            Event::RefreshSucceeded,
            Event::RefreshFailed,
            Event::RefreshStopFailed,
            Event::RefreshBrowseFailed,
            Event::RefreshStartConfirmationFailed,
            Event::RefreshTimeout,
            Event::PeerUpCount(2),
            Event::PeerDownCount(0),
        ] {
            append_to(&p, event).unwrap();
        }
        let text = fs::read_to_string(p).unwrap();
        assert!(text.contains(" peer_up_count 2\n") && text.contains(" refresh_timeout\n"));
        assert!(!text.contains("error") && !text.contains("id="));
        for line in text.lines() {
            let fields: Vec<_> = line.split_whitespace().collect();
            assert!((2..=3).contains(&fields.len()));
            assert!(fields[0].parse::<u64>().is_ok());
            assert!(fields[1]
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_'));
            if fields.len() == 3 {
                assert!(fields[1] == "peer_up_count" || fields[1] == "peer_down_count");
                assert!(fields[2].parse::<u32>().is_ok());
            }
        }
        let _ = fs::remove_dir_all(d);
    }
}
