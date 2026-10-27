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
    assert!(!dir.join(format!(".{}.tmp", meta.id)).exists());
    assert!(receiver.check_complete().await.unwrap()); // idempotent terminal state
    // Same name, second transfer: the target already exists, so the
    // rename must NOT overwrite it — it lands on received(1).bin and the
    // first file is untouched. The .tmp is gone and target_path() reports
    // the actual (suffixed) path, which is what the UI must display.
    let second = Arc::new(OnlineReceiver::new(manifest(&data), &dir).unwrap());
    let frame =
        encode_direct_file_frame(&second.manifest.id, 0, &data, NATIVE_CHUNK_SIZE as u16, 2)
            .unwrap();
    second.on_p2p_frame(&frame).await.unwrap();
    assert!(second.check_complete().await.unwrap());
    assert_eq!(second.target_path(), dir.join("received(1).bin"));
    assert_eq!(std::fs::read(dir.join("received(1).bin")).unwrap(), data);
    assert_eq!(std::fs::read(receiver.target_path()).unwrap(), data);
    assert!(!dir.join(format!(".{}.tmp", second.manifest.id)).exists());
    second.abort().await; // committed file must survive abort
    assert_eq!(std::fs::read(dir.join("received(1).bin")).unwrap(), data);
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn disk_corruption_rejected_and_abort_cleans_temporary_file() {
    let dir = directory();
    let data = b"verify actual disk";
    let receiver = Arc::new(OnlineReceiver::new(manifest(data), &dir).unwrap());
    // Receive bytes that differ from the manifest (same length): the
    // incremental hash must disagree with the manifest SHA at commit
    // time, so nothing is published and the .tmp is cleaned on abort.
    // NOTE: this no longer covers a post-write disk mutation — the new
    // design verifies the incremental hash, not a re-read of the .tmp
    // (same rule as ReceivedFileStore). Wire bytes are still fully
    // verified; only an external process rewriting the .tmp after the
    // write (or an unreported disk fault) would escape detection.
    receiver.on_relay_chunk(0, &B64.encode(vec![0u8; data.len()])).await.unwrap();
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
    let sender = OnlineSender::new(meta, &receiver.target_path()).unwrap();
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

#[tokio::test]
async fn registered_folder_layout_persists_exact_bytes_before_completion() {
    let root = directory();
    let data = b"folder payload";
    let mut meta = manifest(data); meta.name = "测试.txt".into(); meta.path = Some("docs/子目录/测试.txt".into());
    let receiver = OnlineReceiver::new(meta.clone(), &root).unwrap();
    receiver.on_relay_chunk(0, &B64.encode(data)).await.unwrap();
    assert!(receiver.check_complete().await.unwrap());
    assert_eq!(std::fs::read(root.join("docs/子目录/测试.txt")).unwrap(), data);
    assert!(receiver.is_committed());
    assert!(!root.join(format!("docs/子目录/.{}.tmp", meta.id)).exists());
    drop(receiver); std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn registered_folder_path_rejects_traversal_absolute_ads_and_mismatched_leaf() {
    let root = directory();
    for path in ["../received.bin", "/received.bin", "C:/received.bin", "docs//received.bin", "docs/../received.bin", "docs\\received.bin", "docs/other.bin", "NUL/received.bin", "docs./received.bin", "docs/ads:stream/received.bin"] {
        let mut meta = manifest(b"x"); meta.path = Some(path.into());
        assert!(OnlineReceiver::new(meta, &root).is_err(), "{path}");
    }
    assert!(std::fs::read_dir(&root).unwrap().next().is_none());
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(unix)]
#[test]
fn registered_folder_receive_does_not_follow_existing_symlink() {
    let root = directory(); let outside = directory();
    std::os::unix::fs::symlink(&outside, root.join("docs")).unwrap();
    let mut meta = manifest(b"x"); meta.path = Some("docs/received.bin".into());
    assert!(OnlineReceiver::new(meta, &root).is_err());
    assert!(std::fs::read_dir(&outside).unwrap().next().is_none());
    std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(outside).unwrap();
}
#[cfg(windows)]
#[test]
fn windows_junction_is_rejected_for_source_registration_and_folder_receive() {
    let root = directory(); let outside = directory();
    std::fs::write(outside.join("a.bin"), b"outside").unwrap();
    let link = root.join("docs");
    let result = std::process::Command::new("cmd").args(["/C", "mklink", "/J"]).arg(&link).arg(&outside).output().unwrap();
    assert!(result.status.success(), "junction fixture creation failed");
    let sources = crate::online_source::OnlineSources::default();
    assert!(sources.register_selection("main", Some(vec![link.clone()])).is_err());
    let mut meta = manifest(b"x"); meta.path = Some("docs/received.bin".into());
    assert!(OnlineReceiver::new(meta, &root).is_err());
    assert!(!outside.join("received.bin").exists());
    std::fs::remove_dir(link).unwrap(); std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(outside).unwrap();
}

#[tokio::test]
async fn diagnostic_phases_follow_real_sync_hash_and_atomic_commit() {
    use crate::online_diagnostics::Phase;
    let dir = directory();
    let data = b"observable durable bytes";
    let receiver = OnlineReceiver::new(manifest(data), &dir).unwrap();
    let phases = std::sync::Mutex::new(Vec::new());
    assert!(!receiver.check_complete_observed(|p| phases.lock().unwrap().push(p)).await.unwrap());
    assert!(phases.lock().unwrap().is_empty());
    receiver.on_relay_chunk(0, &B64.encode(data)).await.unwrap();
    assert!(receiver.check_complete_observed(|p| phases.lock().unwrap().push(p)).await.unwrap());
    assert_eq!(*phases.lock().unwrap(), vec![Phase::DiskSynced, Phase::HashVerified, Phase::AtomicCommit]);
    assert_eq!(std::fs::read(receiver.target_path()).unwrap(), data);
    assert!(receiver.check_complete_observed(|_| panic!("commit replay must be silent")).await.unwrap());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn diagnostic_hash_failure_never_reports_commit_and_sync_failure_is_silent() {
    use crate::online_diagnostics::Phase;
    let dir = directory();
    let data = b"corruption must not be success";
    let receiver = OnlineReceiver::new(manifest(data), &dir).unwrap();
    receiver.on_relay_chunk(0, &B64.encode(vec![0u8; data.len()])).await.unwrap();
    let phases = std::sync::Mutex::new(Vec::new());
    assert!(receiver.check_complete_observed(|p| phases.lock().unwrap().push(p)).await.is_err());
    assert_eq!(*phases.lock().unwrap(), vec![Phase::DiskSynced]);
    assert!(!receiver.target_path().exists());
    receiver.abort().await;
    let receiver = OnlineReceiver::new(manifest(data), &dir).unwrap();
    receiver.on_relay_chunk(0, &B64.encode(data)).await.unwrap();
    receiver.file.lock().await.take();
    assert!(receiver.check_complete_observed(|_| panic!("sync failed: no success milestone")).await.is_err());
    receiver.abort().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn malformed_ack_geometry_never_advances_confirmed_cursor() {
    let dir=directory();let data=vec![42;NATIVE_CHUNK_SIZE as usize+19];
    let source=dir.join("source.bin");std::fs::write(&source,&data).unwrap();
    let sender=OnlineSender::new(manifest(&data),&source).unwrap();
    for (seq,bytes) in [(u32::MAX,u64::MAX),(3,data.len() as u64),(1,1),(0,1),(2,data.len() as u64+1)] {
        assert!(!sender.valid_ack_geometry(seq,bytes));
        if sender.valid_ack_geometry(seq,bytes) {sender.on_chunk_ack(seq);}
        assert_eq!(sender.acked.load(Ordering::Acquire),0);
    }
    assert!(sender.valid_ack_geometry(0,0));
    assert!(sender.valid_ack_geometry(1,u64::from(NATIVE_CHUNK_SIZE)));
    assert!(sender.valid_ack_geometry(2,data.len() as u64));
    let empty=OnlineSender::new(manifest(b""),&source).unwrap();
    assert!(empty.valid_ack_geometry(0,0));assert!(!empty.valid_ack_geometry(1,0));
    drop(sender);drop(empty);std::fs::remove_dir_all(dir).unwrap();
}
