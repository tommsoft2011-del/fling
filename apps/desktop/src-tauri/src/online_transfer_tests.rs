//! Local engine regressions, not a Windows-to-Windows transport validation.
use super::*;
fn directory() -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("fling-native-test-{:016x}", rand::random::<u64>()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn manifest(data: &[u8]) -> FileManifest {
    FileManifest {
        id: format!("{:024x}", rand::random::<u64>()),
        name: "received.bin".into(),
        size: data.len() as u64,
        sha256: format!("{:x}", Sha256::digest(data)),
        chunk_size: NATIVE_CHUNK_SIZE as u32,
        total_chunks: (data.len() as u64).div_ceil(u64::from(NATIVE_CHUNK_SIZE)) as u32,
        kind: FileKind::File,
        mime_type: None,
        path: None,
    }
}
#[tokio::test]
async fn receive_sync_verify_commit_then_final_ack_and_no_overwrite() {
    let dir = directory();
    let data = vec![42; NATIVE_CHUNK_SIZE as usize + 19];
    let meta = manifest(&data);
    let receiver = Arc::new(OnlineReceiver::new(meta.clone(), &dir).unwrap());
    assert!(!receiver.check_complete().await.unwrap());
    let frame = encode_direct_file_frame(&meta.id, 0, &data, NATIVE_CHUNK_SIZE as u16, 2).unwrap();
    let seq = receiver.on_p2p_frame(&frame).await.unwrap();
    assert!(!receiver.target_path().exists());
    assert!(receiver.check_complete().await.unwrap());
    assert_eq!(std::fs::read(receiver.target_path()).unwrap(), data);
    let ack = chunk_ack_message(&receiver, seq, receiver.received_bytes().await, true);
    assert!(matches!(
        ack,
        FlingMessage::FileChunkAck {
            complete: Some(true),
            ..
        }
    ));
    assert!(!dir.join(format!(".{}.part", meta.id)).exists());
    assert!(receiver.check_complete().await.unwrap()); // idempotent terminal state
    let second = Arc::new(OnlineReceiver::new(manifest(&data), &dir).unwrap());
    let frame =
        encode_direct_file_frame(&second.manifest.id, 0, &data, NATIVE_CHUNK_SIZE as u16, 2)
            .unwrap();
    second.on_p2p_frame(&frame).await.unwrap();
    assert!(second.check_complete().await.is_err());
    second.abort().await;
    assert_eq!(std::fs::read(receiver.target_path()).unwrap(), data);
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn disk_corruption_rejected_and_abort_cleans_temporary_file() {
    let dir = directory();
    let data = b"verify actual disk";
    let receiver = Arc::new(OnlineReceiver::new(manifest(data), &dir).unwrap());
    receiver.on_relay_chunk(0, &B64.encode(data)).await.unwrap();
    // Mutate after append: verification must read disk rather than trust the incremental hash.
    std::fs::write(&receiver.temp_path, vec![0; data.len()]).unwrap();
    assert!(receiver.check_complete().await.is_err());
    assert!(!receiver.target_path().exists());
    receiver.abort().await;
    assert!(!receiver.temp_path.exists());
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn cancellation_is_terminal_and_retry_uses_fresh_temp() {
    let dir = directory();
    let data = vec![7; NATIVE_CHUNK_SIZE as usize * 2];
    let receiver = Arc::new(OnlineReceiver::new(manifest(&data), &dir).unwrap());
    receiver
        .on_relay_chunk(0, &B64.encode(&data[..NATIVE_CHUNK_SIZE as usize]))
        .await
        .unwrap();
    receiver.abort().await;
    assert!(!receiver.temp_path.exists());
    assert!(!receiver.target_path().exists());
    assert!(receiver
        .on_relay_chunk(1, &B64.encode(&data[NATIVE_CHUNK_SIZE as usize..]))
        .await
        .is_err());
    assert!(receiver.check_complete().await.is_err());
    let retry = Arc::new(OnlineReceiver::new(manifest(&data), &dir).unwrap());
    let frame = encode_direct_file_frame(&retry.manifest.id, 0, &data, NATIVE_CHUNK_SIZE as u16, 2)
        .unwrap();
    retry.on_p2p_frame(&frame).await.unwrap();
    assert!(retry.check_complete().await.unwrap());
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn empty_file_commits_and_early_completion_ack_is_sticky() {
    let dir = directory();
    let meta = manifest(b"");
    let receiver = Arc::new(OnlineReceiver::new(meta.clone(), &dir).unwrap());
    assert!(receiver.check_complete().await.unwrap());
    let sender = OnlineSender::new(meta, receiver.target_path()).unwrap();
    sender.on_complete();
    tokio::time::timeout(
        Duration::from_millis(100),
        sender.send_via_p2p(|_| Ok(()), || 0, |_, _| {}),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn sequence_gaps_and_relay_bounds_are_rejected() {
    let dir = directory();
    let data = vec![9; NATIVE_CHUNK_SIZE as usize * 2];
    let receiver = Arc::new(OnlineReceiver::new(manifest(&data), &dir).unwrap());
    let bad = encode_direct_file_frame(
        &receiver.manifest.id,
        1,
        &data[..NATIVE_CHUNK_SIZE as usize],
        NATIVE_CHUNK_SIZE as u16,
        1,
    )
    .unwrap();
    assert!(receiver.on_p2p_frame(&bad).await.is_err());
    assert!(receiver.on_relay_chunk(3, &B64.encode(b"x")).await.is_err());
    assert!(receiver.on_relay_chunk(0, &B64.encode(b"x")).await.is_err());
    receiver.abort().await;
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn retained_handle_manifest_is_bounded_and_cancellable() {
    let dir = directory();
    let source = dir.join("source.bin");
    std::fs::write(&source, b"retained handle").unwrap();
    let mut file = File::open(&source).unwrap();
    let meta = build_manifest_handle(
        &mut file,
        "source.bin".into(),
        FileKind::File,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(
        meta.sha256,
        format!("{:x}", Sha256::digest(b"retained handle"))
    );
    assert!(build_manifest_handle(
        &mut file,
        "source.bin".into(),
        FileKind::File,
        &AtomicBool::new(true)
    )
    .is_err());
    drop(file);
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn sync_failure_cannot_publish_or_produce_success() {
    let dir = directory();
    let data = b"sync-failure";
    let receiver = Arc::new(OnlineReceiver::new(manifest(data), &dir).unwrap());
    receiver.on_relay_chunk(0, &B64.encode(data)).await.unwrap();
    // A read-only descriptor causes sync/flush write capability to fail on
    // Windows. A missing handle is a deterministic equivalent before publication.
    receiver.file.lock().await.take();
    assert!(receiver.check_complete().await.is_err());
    assert!(!receiver.target_path().exists());
    receiver.abort().await;
    std::fs::remove_dir_all(dir).unwrap();
}
