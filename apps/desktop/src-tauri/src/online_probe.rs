//! Internal Linux debug-only application acceptance runner. Uses actual
//! OnlineSession/OnlineRoom/native source and disk engine, without WebView actions.
//! Never compiled into a Windows portable or release build.
use std::{path::PathBuf, sync::{Arc, Mutex}, io::Write, time::{Duration,Instant}};
use tauri::{Manager, Listener};
use crate::online_native::NativeTaskConfig;
use crate::online_session::{OnlineSession,OnlineSessionRegistry};
use crate::online_source::{SourceLease,SourceMetadata};
static DROP_DIRECT_READY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Simulates signaling loss only. Never fabricates READY, selects TURN, changes
/// the 2.5s clock or bypasses the first-frame gate. Absent from Windows/release.
pub fn drop_signal(signal: &crate::online_direct::Signal) -> bool {
    DROP_DIRECT_READY.load(std::sync::atomic::Ordering::Acquire)
        && matches!(signal, crate::online_direct::Signal::Control(crate::online_route_choice::Envelope {
            fence: crate::online_route_choice::Fence {path:crate::online_route_choice::Path::Direct,..},
            message:crate::online_route_choice::Wire::Ready(_),
        }))
}
#[derive(Clone, serde::Deserialize)]
struct Spec {
    role:String, task:String, name:String,size:u64, source:Option<PathBuf>,
    output:PathBuf, log:PathBuf, cancel_after_bytes:Option<u64>,
    #[serde(default)] force_turn: bool,
}
fn token(cfg:&crate::FlingConfig,room:&str,identity:&str)->Result<String,String> {
    let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|e|e.to_string())?.as_secs();
    jsonwebtoken::encode(&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &serde_json::json!({"iss":cfg.api_key,"sub":identity,"name":identity,"nbf":now.saturating_sub(5),"exp":now+900,
            "video":{"roomJoin":true,"room":room,"canPublish":true,"canSubscribe":true,"canPublishData":true}}),
        &jsonwebtoken::EncodingKey::from_secret(cfg.api_secret.as_bytes())).map_err(|_|"probe token signing failed".into())
}
pub fn run(path:&str) {
    let result=(||->Result<(),String>{
        let spec:Spec=serde_json::from_slice(&std::fs::read(path).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        if !matches!(spec.role.as_str(),"sending"|"receiving") {return Err("invalid probe role".into());}
        std::fs::create_dir_all(&spec.output).map_err(|e|e.to_string())?;
        DROP_DIRECT_READY.store(spec.force_turn, std::sync::atomic::Ordering::Release);
        let log=Arc::new(Mutex::new(std::fs::File::create(&spec.log).map_err(|e|e.to_string())?));
        let mut context=tauri::generate_context!();context.config_mut().app.windows.clear();
        tauri::Builder::default().manage(OnlineSessionRegistry::new()).setup(move |app| {
            let log_event=log.clone();let event_clock=Instant::now();
            app.listen("fling://online/event",move|event|{
                let mut value:serde_json::Value=serde_json::from_str(event.payload()).unwrap_or(serde_json::Value::Null);
                value["probeElapsedMs"]=serde_json::json!(event_clock.elapsed().as_millis());
                let mut log=log_event.lock().unwrap();let _=writeln!(log,"{}",value);let _=log.flush();
            });
            let app=app.handle().clone();let worker_app=app.clone();let spec=spec.clone();let log=log.clone();
            tauri::async_runtime::spawn(async move {
                let started=Instant::now();
                let result=worker(worker_app,spec).await;
                let mut log=log.lock().unwrap();
                let _=writeln!(log,"{}",serde_json::json!({"probeResult":match &result {Ok(stage)=>stage.as_str(),Err(_)=>"error"},"error":result.as_ref().err(),"elapsedMs":started.elapsed().as_millis(),"version":env!("CARGO_PKG_VERSION"),"buildIdentity":env!("FLING_BUILD_IDENTITY"),"sameHost":true}));
                let _=log.flush();drop(log);
                app.exit(if result.is_ok(){0}else{1});
            });
            Ok(())
        }).run(context).map_err(|e|e.to_string())?;
        Ok(())
    })();
    if let Err(error)=result {eprintln!("internal app acceptance failed: {error}");std::process::exit(1);}
}
async fn worker(app:tauri::AppHandle,spec:Spec)->Result<String,String> {
    let cfg=crate::decrypt_embedded_config()?;
    let room=format!("native-direct-first-{}",spec.task);
    let identity=format!("probe-{}-{:x}",spec.role,rand::random::<u64>());
    let token=token(&cfg,&room,&identity)?;
    let native=NativeTaskConfig{task_id:spec.task.clone(),direction:spec.role.clone(),name:spec.name.clone(),size:spec.size};
    let servers=crate::online_p2p::default_ice_servers(cfg.turn_url.as_deref(),cfg.turn_username.as_deref(),cfg.turn_password.as_deref());
    let session_id=format!("{:032x}",rand::random::<u128>());
    let session=OnlineSession::connect(app.clone(),session_id.clone(),&cfg.server_url,&token,&spec.task,"native-direct-first-probe",spec.output.clone(),"internal-probe".into(),Some(native),servers).await?;
    app.state::<OnlineSessionRegistry>().insert(session_id,session.clone()).await;
    if spec.role=="sending" {
        let source=spec.source.ok_or("sender missing source")?;
        let file=std::fs::File::open(source).map_err(|e|e.to_string())?;
        let lease=SourceLease{file,metadata:SourceMetadata{source_id:"internal-probe".into(),name:spec.name,size:spec.size,rel_path:None,folder:None}};
        let status=session.send_source(lease).await?;
        let stage=status.stage.clone();session.disconnect().await?;
        if stage=="done" {Ok(stage)}else{Err(format!("sender {stage}: {}",status.error.unwrap_or_default()))}
    }else{
        tokio::time::timeout(Duration::from_secs(90),async {
            loop {
                let status=session.native.as_ref().unwrap().status.lock().await.clone();
                if status.terminal() {
                    let stage=status.stage.clone();
                    if stage=="done" {tokio::time::sleep(Duration::from_secs(2)).await;}
                    session.disconnect().await?;
                    return if stage=="done" || (stage=="cancelled" && spec.cancel_after_bytes.is_some()) {Ok(stage)}else{Err(format!("receiver {stage}: {}",status.error.unwrap_or_default()))};
                }
                if spec.cancel_after_bytes.is_some_and(|n|status.local_bytes>=n) {session.cancel_native(None,true).await?;}
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.map_err(|_|"receiver overall timeout".to_string())?
    }
}
