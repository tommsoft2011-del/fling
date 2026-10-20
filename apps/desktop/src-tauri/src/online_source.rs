//! Native disk sources. Only opaque IDs and display-relative metadata cross the WebView.
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
const SOURCE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_SOURCES: usize = 1024;
const MAX_DEPTH: usize = 64;
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceMetadata {
    pub source_id: String,
    pub name: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rel_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
}
struct Source {
    owner: String,
    file: File,
    metadata: SourceMetadata,
    expires: Instant,
    busy: bool,
}
#[derive(Default)]
pub struct OnlineSources {
    sources: Arc<Mutex<HashMap<String, Source>>>,
}
pub struct SourceLease {
    pub file: File,
    pub metadata: SourceMetadata,
}
pub struct BusyGuard {
    sources: Arc<Mutex<HashMap<String, Source>>>,
    owner: String,
    id: String,
}
impl Drop for BusyGuard {
    fn drop(&mut self) {
        if let Ok(mut sources) = self.sources.lock() {
            if let Some(source) = sources.get_mut(&self.id) {
                if source.owner == self.owner {
                    source.busy = false;
                }
            }
        }
    }
}
/// No absolute paths, parent components, ADS, or Windows-ambiguous names in display metadata.
pub fn safe_component(name: &str) -> bool {
    !name.is_empty()
        && !matches!(name, "." | "..")
        && !name.ends_with(['.', ' '])
        && !name
            .chars()
            .any(|c| c.is_control() || r#"/\:<>"|?*"#.contains(c))
        && !matches!(
            name.split('.')
                .next()
                .unwrap_or("")
                .to_ascii_uppercase()
                .as_str(),
            "CON"
                | "PRN"
                | "AUX"
                | "NUL"
                | "COM1"
                | "COM2"
                | "COM3"
                | "COM4"
                | "COM5"
                | "COM6"
                | "COM7"
                | "COM8"
                | "COM9"
                | "LPT1"
                | "LPT2"
                | "LPT3"
                | "LPT4"
                | "LPT5"
                | "LPT6"
                | "LPT7"
                | "LPT8"
                | "LPT9"
        )
}
fn display_name(path: &Path) -> Result<String, String> {
    let name = path
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or("文件名不能安全显示")?;
    if !safe_component(name) {
        return Err("文件名不支持安全传输".into());
    }
    Ok(name.into())
}
fn reparse(meta: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        meta.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        meta.file_type().is_symlink()
    }
}
fn open_source(
    path: &Path,
    rel_path: Option<String>,
    folder: Option<String>,
) -> Result<(File, SourceMetadata), String> {
    let entry = std::fs::symlink_metadata(path).map_err(|_| "无法读取所选文件")?;
    if reparse(&entry) || !entry.is_file() {
        return Err("不支持链接或特殊文件".into());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(0x20000);
    } // O_NOFOLLOW
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000);
    } // FILE_FLAG_OPEN_REPARSE_POINT
    let file = options.open(path).map_err(|_| "打开所选文件失败")?;
    let stat = file.metadata().map_err(|_| "读取文件信息失败")?;
    if reparse(&stat) || !stat.is_file() {
        return Err("仅支持普通磁盘文件".into());
    }
    Ok((
        file,
        SourceMetadata {
            source_id: format!("{:032x}", rand::random::<u128>()),
            name: display_name(path)?,
            size: stat.len(),
            rel_path,
            folder,
        },
    ))
}
fn collect(
    path: &Path,
    root: &Path,
    rel: Option<String>,
    folder: Option<String>,
    depth: usize,
    visited: &mut usize,
    pending: &mut Vec<(File, SourceMetadata)>,
) -> Result<(), String> {
    *visited += 1;
    if *visited > 16384 {
        return Err("文件夹条目超过 16384 个，未添加任何文件".into());
    }
    if depth > MAX_DEPTH {
        return Err("文件夹层级超过 64 层".into());
    }
    let stat = std::fs::symlink_metadata(path).map_err(|_| "读取所选项目失败")?;
    if reparse(&stat) {
        return Err("文件夹含链接或重解析点，未添加任何文件".into());
    }
    let canonical = path.canonicalize().map_err(|_| "所选项目已不可用")?;
    if !canonical.starts_with(root) {
        return Err("文件夹边界发生变化，请重新选择".into());
    }
    if stat.is_dir() {
        for entry in std::fs::read_dir(path).map_err(|_| "读取所选文件夹失败")? {
            let child = entry.map_err(|_| "读取文件夹条目失败")?.path();
            let name = display_name(&child)?;
            let next = rel.as_ref().map(|r| format!("{r}/{name}")).unwrap_or(name);
            collect(
                &child,
                root,
                Some(next),
                folder.clone(),
                depth + 1,
                visited,
                pending,
            )?;
        }
    } else {
        if pending.len() >= MAX_SOURCES {
            return Err("一次最多选择 1024 个文件，未添加任何文件".into());
        }
        pending.push(open_source(path, rel, folder)?);
    }
    Ok(())
}
impl OnlineSources {
    pub fn register(&self, owner: &str, path: &Path) -> Result<SourceMetadata, String> {
        let pending = vec![open_source(path, None, None)?];
        Ok(self.commit(owner, pending)?.remove(0))
    }
    /// All-or-nothing transaction, including cancellation (None) and enumeration errors.
    pub fn register_selection(
        &self,
        owner: &str,
        paths: Option<Vec<PathBuf>>,
    ) -> Result<Vec<SourceMetadata>, String> {
        let Some(paths) = paths else {
            return Ok(Vec::new());
        };
        if paths.len() > MAX_SOURCES {
            return Err("一次最多选择 1024 个项目".into());
        }
        let mut pending = Vec::new();
        let mut visited = 0;
        for path in paths {
            let stat = std::fs::symlink_metadata(&path).map_err(|_| "所选项目不可用")?;
            if reparse(&stat) {
                return Err("不支持链接或重解析点".into());
            }
            let root = path.canonicalize().map_err(|_| "所选项目不可用")?;
            let (rel, folder) = if stat.is_dir() {
                let name = display_name(&path)?;
                (Some(name.clone()), Some(name))
            } else {
                (None, None)
            };
            collect(&path, &root, rel, folder, 0, &mut visited, &mut pending)?;
        }
        self.commit(owner, pending)
    }
    fn commit(
        &self,
        owner: &str,
        pending: Vec<(File, SourceMetadata)>,
    ) -> Result<Vec<SourceMetadata>, String> {
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| "source registry poisoned")?;
        sources.retain(|_, s| s.busy || s.expires > Instant::now());
        if sources.len() + pending.len() > MAX_SOURCES {
            return Err("已选择文件达到 1024 个上限，请移除后重试".into());
        }
        let mut result = Vec::with_capacity(pending.len());
        for (file, mut metadata) in pending {
            while sources.contains_key(&metadata.source_id) {
                metadata.source_id = format!("{:032x}", rand::random::<u128>());
            }
            sources.insert(
                metadata.source_id.clone(),
                Source {
                    owner: owner.into(),
                    file,
                    metadata: metadata.clone(),
                    expires: Instant::now() + SOURCE_TTL,
                    busy: false,
                },
            );
            result.push(metadata);
        }
        Ok(result)
    }
    pub fn acquire(&self, owner: &str, id: &str) -> Result<SourceLease, String> {
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| "source registry poisoned")?;
        let source = sources.get_mut(id).ok_or("sourceId 无效，请重新选择文件")?;
        if source.owner != owner {
            return Err("sourceId 未授权".into());
        }
        if source.busy {
            return Err("文件正在发送".into());
        }
        if source.expires <= Instant::now() {
            sources.remove(id);
            return Err("sourceId 已过期，请重新选择文件".into());
        }
        let file = source.file.try_clone().map_err(|_| "复制源句柄失败")?;
        source.busy = true;
        Ok(SourceLease {
            file,
            metadata: source.metadata.clone(),
        })
    }
    pub fn busy_guard(&self, owner: &str, id: &str) -> BusyGuard {
        BusyGuard {
            sources: self.sources.clone(),
            owner: owner.into(),
            id: id.into(),
        }
    }
    pub fn purge_expired(&self) {
        if let Ok(mut sources) = self.sources.lock() {
            sources.retain(|_, s| s.busy || s.expires > Instant::now());
        }
    }
    pub fn release(&self, owner: &str, id: &str) {
        drop(self.busy_guard(owner, id));
    }
    pub fn remove_owner(&self, owner: &str) {
        if let Ok(mut sources) = self.sources.lock() {
            sources.retain(|_, s| s.owner != owner);
        }
    }
    pub fn remove(&self, owner: &str, id: &str) -> Result<(), String> {
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| "source registry poisoned")?;
        if let Some(s) = sources.get(id) {
            if s.owner != owner {
                return Err("sourceId 未授权".into());
            }
            if s.busy {
                return Err("文件正在发送".into());
            }
        }
        sources.remove(id);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_authorization_expiry_busy_and_retry() {
        let registry = OnlineSources::default();
        let path =
            std::env::temp_dir().join(format!("fling-source-test-{}", rand::random::<u64>()));
        std::fs::write(&path, b"test").unwrap();
        let meta = registry.register("main", &path).unwrap();
        assert!(registry.acquire("other", &meta.source_id).is_err());
        assert!(registry.acquire("main", "forged").is_err());
        assert!(registry.acquire("main", &meta.source_id).is_ok());
        assert!(registry.acquire("main", &meta.source_id).is_err());
        registry.release("main", &meta.source_id);
        assert!(registry.acquire("main", &meta.source_id).is_ok());
        registry.release("main", &meta.source_id);
        registry
            .sources
            .lock()
            .unwrap()
            .get_mut(&meta.source_id)
            .unwrap()
            .expires = Instant::now();
        assert!(registry.acquire("main", &meta.source_id).is_err());
        registry.remove("main", &meta.source_id).unwrap();
        assert!(registry.acquire("main", &meta.source_id).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn retained_source_survives_path_replacement_and_expiry_closes_idle_handles() {
        use std::io::Read;
        let dir = std::env::temp_dir().join(format!("fling-handle-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("original.bin");
        std::fs::write(&path, b"selected bytes").unwrap();
        let registry = OnlineSources::default();
        let metadata = registry.register("main", &path).unwrap();
        std::fs::rename(&path, dir.join("moved.bin")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        let mut lease = registry.acquire("main", &metadata.source_id).unwrap();
        let mut bytes = Vec::new();
        lease.file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"selected bytes");
        drop(lease);
        registry.release("main", &metadata.source_id);
        registry
            .sources
            .lock()
            .unwrap()
            .get_mut(&metadata.source_id)
            .unwrap()
            .expires = Instant::now();
        registry.purge_expired();
        assert!(registry.sources.lock().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn invalid_selection_does_not_poison_retry() {
        let registry = OnlineSources::default();
        assert!(registry
            .register("main", Path::new("/nonexistent/fling"))
            .is_err());
        assert!(registry.register("main", &std::env::temp_dir()).is_err());
        assert!(registry.sources.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let p =
                std::env::temp_dir().join(format!("fling-source-batch-{}", rand::random::<u64>()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn file(&self, name: &str) -> PathBuf {
            let p = self.0.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, b"ordinary file bytes sentinel").unwrap();
            p
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn multiselect_and_drop_register_distinct_opaque_ids_without_paths_or_bytes() {
        let fixture = Fixture::new();
        let a = fixture.file("中文.txt");
        let b = fixture.file("empty.bin");
        std::fs::write(&b, []).unwrap();
        let registry = OnlineSources::default();
        let batch = registry
            .register_selection("main", Some(vec![a.clone(), b]))
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert_ne!(batch[0].source_id, batch[1].source_id);
        for meta in &batch {
            assert_eq!(meta.source_id.len(), 32);
            let value = serde_json::to_value(meta).unwrap();
            assert_eq!(value.as_object().unwrap().len(), 3);
            let encoded = value.to_string();
            assert!(!encoded.contains(&fixture.0.to_string_lossy().to_string()));
            assert!(!encoded.contains("sentinel"));
            assert!(registry.acquire("foreign", &meta.source_id).is_err());
            assert!(registry.remove("foreign", &meta.source_id).is_err());
        }
        // The OS-drop ingress uses the exact same transactional registration, never a frontend path command.
        let dropped = registry.register_selection("main", Some(vec![a])).unwrap();
        assert_ne!(batch[0].source_id, dropped[0].source_id);
    }
    #[test]
    fn folder_recursive_unicode_and_same_names_keep_safe_relative_layout() {
        let fixture = Fixture::new();
        fixture.file("docs/子目录/a.txt");
        fixture.file("docs/a.txt");
        let registry = OnlineSources::default();
        let batch = registry
            .register_selection("main", Some(vec![fixture.0.join("docs")]))
            .unwrap();
        let mut paths: Vec<_> = batch.iter().map(|m| m.rel_path.clone().unwrap()).collect();
        paths.sort();
        assert_eq!(paths, vec!["docs/a.txt", "docs/子目录/a.txt"]);
        assert!(batch.iter().all(|m| m.folder.as_deref() == Some("docs")));
        assert!(batch.iter().all(|m| m.name == "a.txt"));
        for meta in batch {
            let lease = registry.acquire("main", &meta.source_id).unwrap();
            assert_eq!(lease.metadata.rel_path, meta.rel_path);
            registry.release("main", &meta.source_id);
        }
    }
    #[test]
    fn cancel_empty_and_partial_error_leave_existing_selection_untouched() {
        let fixture = Fixture::new();
        let file = fixture.file("existing.bin");
        let registry = OnlineSources::default();
        let existing = registry.register("main", &file).unwrap();
        assert!(registry
            .register_selection("main", None)
            .unwrap()
            .is_empty());
        assert!(registry
            .register_selection("main", Some(vec![]))
            .unwrap()
            .is_empty());
        assert!(registry
            .register_selection("main", Some(vec![file.clone(), fixture.0.join("missing")]))
            .is_err());
        assert_eq!(registry.sources.lock().unwrap().len(), 1);
        assert!(registry.acquire("main", &existing.source_id).is_ok());
        registry.release("main", &existing.source_id);
        assert_eq!(
            registry
                .register_selection("main", Some(vec![file]))
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn registry_capacity_error_is_atomic_and_recovers_after_remove() {
        let fixture = Fixture::new();
        let file = fixture.file("a.bin");
        let registry = OnlineSources::default();
        let batch = registry
            .register_selection("main", Some(vec![file.clone(); MAX_SOURCES]))
            .unwrap();
        assert!(registry
            .register_selection("main", Some(vec![file.clone()]))
            .is_err());
        assert_eq!(registry.sources.lock().unwrap().len(), MAX_SOURCES);
        registry.remove("main", &batch[0].source_id).unwrap();
        assert_eq!(
            registry
                .register_selection("main", Some(vec![file]))
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn busy_guard_recovers_error_and_unwind_and_expiry_never_drops_busy_entry() {
        let fixture = Fixture::new();
        let file = fixture.file("a.bin");
        let registry = OnlineSources::default();
        let meta = registry.register("main", &file).unwrap();
        let lease = registry.acquire("main", &meta.source_id).unwrap();
        assert!(registry.remove("main", &meta.source_id).is_err());
        registry
            .sources
            .lock()
            .unwrap()
            .get_mut(&meta.source_id)
            .unwrap()
            .expires = Instant::now();
        registry.purge_expired();
        assert_eq!(registry.sources.lock().unwrap().len(), 1);
        assert!(registry.acquire("main", &meta.source_id).is_err());
        drop(lease);
        let _ = std::panic::catch_unwind(|| {
            let _guard = registry.busy_guard("main", &meta.source_id);
            panic!("simulated worker failure");
        });
        registry.purge_expired();
        assert!(registry.sources.lock().unwrap().is_empty());
        assert!(registry.acquire("main", &meta.source_id).is_err());
    }
    #[test]
    fn window_cleanup_only_removes_its_own_capabilities() {
        let fixture = Fixture::new();
        let file = fixture.file("a.bin");
        let registry = OnlineSources::default();
        let first = registry.register("main", &file).unwrap();
        let other = registry.register("other", &file).unwrap();
        registry.remove_owner("main");
        assert!(registry.acquire("main", &first.source_id).is_err());
        assert!(registry.acquire("other", &other.source_id).is_ok());
    }
    #[test]
    fn display_components_reject_paths_ads_reserved_and_ambiguous_names() {
        for name in [
            "",
            ".",
            "..",
            "/root",
            "C:\\secret",
            "ads:stream",
            "a/b",
            "a\\b",
            "NUL",
            "COM1.txt",
            "trailing.",
            "space ",
            "control\n",
        ] {
            assert!(!safe_component(name), "{name:?}");
        }
        assert!(safe_component("中文 filename.txt"));
    }
    #[cfg(unix)]
    #[test]
    fn symlink_folder_loop_and_leaf_rejected_without_partial_registration() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new();
        fixture.file("docs/a.bin");
        symlink(fixture.0.join("docs"), fixture.0.join("docs/loop")).unwrap();
        let registry = OnlineSources::default();
        assert!(registry
            .register_selection("main", Some(vec![fixture.0.join("docs")]))
            .is_err());
        assert!(registry.sources.lock().unwrap().is_empty());
        symlink(fixture.0.join("docs/a.bin"), fixture.0.join("linked.bin")).unwrap();
        assert!(registry
            .register("main", &fixture.0.join("linked.bin"))
            .is_err());
    }
    #[test]
    fn excessive_depth_and_empty_folder_have_no_leaked_sources() {
        let fixture = Fixture::new();
        let registry = OnlineSources::default();
        let empty = fixture.0.join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert!(registry
            .register_selection("main", Some(vec![empty]))
            .unwrap()
            .is_empty());
        let mut deep = fixture.0.join("deep");
        for _ in 0..MAX_DEPTH + 1 {
            deep.push("d");
        }
        std::fs::create_dir_all(&deep).unwrap();
        assert!(registry
            .register_selection("main", Some(vec![fixture.0.join("deep")]))
            .is_err());
        assert!(registry.sources.lock().unwrap().is_empty());
    }
}
