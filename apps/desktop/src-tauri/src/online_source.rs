//! Native disk sources. The WebView never sees a path or file bytes.
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};
const SOURCE_TTL: Duration = Duration::from_secs(30 * 60);
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceMetadata {
    pub source_id: String,
    pub name: String,
    pub size: u64,
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
    sources: Mutex<HashMap<String, Source>>,
}
pub struct SourceLease {
    pub file: File,
    pub metadata: SourceMetadata,
}
impl OnlineSources {
    pub fn register(&self, owner: &str, path: &Path) -> Result<SourceMetadata, String> {
        let file = File::open(path).map_err(|e| format!("打开所选文件失败：{e}"))?;
        let stat = file
            .metadata()
            .map_err(|e| format!("读取文件信息失败：{e}"))?;
        if !stat.is_file() {
            return Err("仅支持普通磁盘文件".into());
        }
        let metadata = SourceMetadata {
            source_id: format!("{:032x}", rand::random::<u128>()),
            name: path
                .file_name()
                .ok_or("文件名不可用")?
                .to_string_lossy()
                .into_owned(),
            size: stat.len(),
        };
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| "source registry poisoned")?;
        sources.retain(|_, s| s.busy || s.expires > Instant::now());
        if sources.len() >= 64 {
            return Err("已选择的文件过多，请移除后重试".into());
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
        Ok(metadata)
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
        if source.expires <= Instant::now() {
            sources.remove(id);
            return Err("sourceId 已过期，请重新选择文件".into());
        }
        if source.busy {
            return Err("文件正在发送".into());
        }
        let file = source
            .file
            .try_clone()
            .map_err(|e| format!("复制源句柄失败：{e}"))?;
        source.busy = true;
        Ok(SourceLease {
            file,
            metadata: source.metadata.clone(),
        })
    }
    pub fn purge_expired(&self) {
        if let Ok(mut sources) = self.sources.lock() {
            sources.retain(|_, s| s.busy || s.expires > Instant::now());
        }
    }
    pub fn release(&self, owner: &str, id: &str) {
        if let Ok(mut sources) = self.sources.lock() {
            if let Some(s) = sources.get_mut(id) {
                if s.owner == owner {
                    s.busy = false;
                }
            }
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
