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

#[tokio::test]
async fn consecutive_four_chunk_frames_ack_every_eight_chunks() {
    use crate::online_session::publish_progress_ack;
    let dir = directory();
    let data = vec![11; NATIVE_CHUNK_SIZE as usize * 20];
    let meta = manifest(&data);
    let receiver = OnlineReceiver::new(meta.clone(), &dir).unwrap();
    let cursors = Mutex::new(HashMap::from([(meta.id.clone(), 0)]));
    let published = std::sync::Mutex::new(Vec::new());
    for seq in (0..16).step_by(4) {
        let start = seq as usize * NATIVE_CHUNK_SIZE as usize;
        let end = start + 4 * NATIVE_CHUNK_SIZE as usize;
        let frame = encode_direct_file_frame(&meta.id, seq, &data[start..end], NATIVE_CHUNK_SIZE as u16, 4).unwrap();
        let next = receiver.on_p2p_frame(&frame).await.unwrap();
        let sent = publish_progress_ack(&receiver, &cursors, next, async {
            published.lock().unwrap().push(next);
            Ok(())
        }).await.unwrap();
        assert_eq!(sent, next % 8 == 0);
        assert_eq!(cursors.lock().await[&meta.id], (next / 8) * 8);
    }
    assert_eq!(*published.lock().unwrap(), vec![8, 16]);
    receiver.abort().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ack_cursor_does_not_advance_on_skip_or_failed_publish() {
    use crate::online_session::publish_progress_ack;
    let dir = directory();
    let receiver = OnlineReceiver::new(manifest(&vec![3; NATIVE_CHUNK_SIZE as usize * 24]), &dir).unwrap();
    let id = receiver.manifest().id.clone();
    let cursors = Mutex::new(HashMap::from([(id.clone(), 0)]));
    assert!(!publish_progress_ack(&receiver, &cursors, 4, async {
        panic!("no ACK is due: publisher must not be polled");
        #[allow(unreachable_code)] Ok(())
    }).await.unwrap());
    assert_eq!(cursors.lock().await[&id], 0);
    assert_eq!(publish_progress_ack(&receiver, &cursors, 8, async {
        Err("injected publish failure".into())
    }).await.unwrap_err(), "injected publish failure");
    assert_eq!(cursors.lock().await[&id], 0);
    assert!(publish_progress_ack(&receiver, &cursors, 8, async { Ok(()) }).await.unwrap());
    assert_eq!(cursors.lock().await[&id], 8);
    assert!(!publish_progress_ack(&receiver, &cursors, 12, async { panic!("below threshold") }).await.unwrap());
    assert_eq!(cursors.lock().await[&id], 8);
    assert!(publish_progress_ack(&receiver, &cursors, 16, async { Err("offline".into()) }).await.is_err());
    assert_eq!(cursors.lock().await[&id], 8);
    // A later frame still counts from the last successfully sent ACK.
    assert!(publish_progress_ack(&receiver, &cursors, 20, async { Ok(()) }).await.unwrap());
    assert_eq!(cursors.lock().await[&id], 20);
    receiver.abort().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn p2p_cumulative_acks_allow_sending_beyond_eight_mib_window() {
    use crate::online_session::publish_progress_ack;
    let dir = directory();
    // More than two windows, with a short final chunk as well.
    let data = vec![19; NATIVE_CHUNK_SIZE as usize * (SENDER_WINDOW_CHUNKS as usize * 2 + 3) + 17];
    let source = dir.join("source.bin");
    std::fs::write(&source, &data).unwrap();
    let meta = manifest(&data);
    let sender = OnlineSender::new(meta.clone(), &source).unwrap();
    let receiver = OnlineReceiver::new(meta.clone(), &dir).unwrap();
    let cursors = Mutex::new(HashMap::from([(meta.id.clone(), 0)]));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let progress = std::sync::atomic::AtomicU64::new(0);
    let pump = sender.send_via_p2p(
        |frame| tx.send(frame).map_err(|_| "frame channel closed".to_string()),
        || 0,
        |sent, _| { progress.store(sent, Ordering::Release); },
    );
    let receive = async {
        let mut ack_count = 0;
        loop {
            let frame = rx.recv().await.ok_or("sender stopped")?;
            let next = receiver.on_p2p_frame(&frame).await?;
            if receiver.check_complete().await? {
                // Completion can only be delivered after the real disk commit.
                assert!(receiver.is_committed());
                assert_eq!(std::fs::read(receiver.target_path()).unwrap(), data);
                sender.on_chunk_ack(next);
                sender.on_complete();
                break;
            }
            if publish_progress_ack(&receiver, &cursors, next, async {
                sender.on_chunk_ack(next);
                Ok(())
            }).await? { ack_count += 1; }
        }
        assert!(ack_count > SENDER_WINDOW_CHUNKS / ACK_INTERVAL_CHUNKS);
        Ok::<(), String>(())
    };
    let (sent, received) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(pump, receive)
    }).await.expect("ACKs must unblock the real sender beyond its 8 MiB window");
    sent.unwrap();
    received.unwrap();
    assert_eq!(progress.load(Ordering::Acquire), meta.size);
    assert!(meta.size > u64::from(SENDER_WINDOW_CHUNKS) * NATIVE_CHUNK_SIZE);
    drop(sender);
    drop(receiver);
    std::fs::remove_dir_all(dir).unwrap();
}
