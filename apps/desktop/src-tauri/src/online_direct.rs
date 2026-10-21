//! Rust-owned online direct-first transport. The UI never sees SDP, ICE or frames.
//! Uses the same RouteHandler as deterministic protocol tests, with real native PCs.
use crate::connection_diagnostics::{Diagnostics as ConnectionDiagnostics, Observer, Stage, Outcome, Reason, control_stage};
use crate::online_p2p::{IceMode, OnlineP2p, P2pState, SignalingIceCandidate};
use crate::online_route_choice::{Effect, Envelope, Epoch, Event, Fence, Path, Phase, Role, RouteHandler};
use crate::online_route_runtime::{EffectExecutor, EffectFuture, RouteRuntime};
use livekit::webrtc::{data_channel::{DataChannel, DataChannelState}, peer_connection_factory::IceServer};
use serde::{Deserialize, Serialize};
use std::{collections::{HashMap, HashSet}, future::Future, pin::Pin, sync::{Arc, Weak, OnceLock, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant}};
use tokio::sync::{mpsc, watch, Mutex, Notify};

pub const ROUTE_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability { pub version: u32, pub epoch: Epoch }
impl Capability {
    pub fn fresh() -> Self { Self { version: ROUTE_VERSION, epoch: Epoch { attempt: rand::random(), generation: 1 } } }
    pub fn valid(self) -> bool { self.version == ROUTE_VERSION && self.epoch.generation == 1 }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Signal {
    Description { fence: Fence, answer: bool, ufrag: String, sdp: String },
    Ice { fence: Fence, ufrag: String, candidate: String },
    Control(Envelope),
}
pub enum TransportEvent { Started(Fence), TurnStarted, Frame { fence: Fence, bytes: Vec<u8> }, Failed(String) }
type SendFuture = Pin<Box<dyn Future<Output=Result<(), String>> + Send>>;
pub type SignalSender = Arc<dyn Fn(Signal) -> SendFuture + Send + Sync>;
struct Peer {
    link: Arc<OnlineP2p>, dc: Option<DataChannel>, remote_sdp: Option<String>,
    remote_ufrag: Option<String>, local_sdp: String, local_ufrag: String,
}
#[derive(Default)]
struct CandidateStore {
    bound: HashMap<Path, String>, buckets: HashMap<(Path, String), HashSet<String>>, count: usize,
}
impl CandidateStore {
    #[cfg(test)]
    fn insert(&mut self,path:Path,ufrag:&str,candidate:&str)->bool {self.admit(path,ufrag,candidate).is_ok()}
    fn admit(&mut self, path: Path, ufrag: &str, candidate: &str) -> Result<(),Reason> {
        if ufrag.is_empty() || ufrag.len()>256 || candidate.len()>4096 || self.count>=256 {return Err(Reason::Bounds);}
        if self.bound.get(&path).is_some_and(|v|v!=ufrag) {return Err(Reason::Generation);}
        let sig=serde_json::from_str::<SignalingIceCandidate>(candidate).map_err(|_|Reason::Malformed)?;
        crate::ice_syntax::validate(&sig.candidate).map_err(|_|Reason::InvalidSyntax)?;
        let tokens:Vec<_>=sig.candidate.split_ascii_whitespace().collect();
        if tokens.windows(2).any(|p|p[0]=="ufrag" && p[1]!=ufrag) {return Err(Reason::UfragMismatch);}
        if let Ok(value)=serde_json::from_str::<serde_json::Value>(candidate) {
            if value.get("usernameFragment").and_then(|v|v.as_str()).is_some_and(|v|v!=ufrag) {return Err(Reason::UfragMismatch);}
        }
        let key=(path,ufrag.to_owned());
        if !self.buckets.contains_key(&key) && self.buckets.len()>=8 {return Err(Reason::BucketLimit);}
        if !self.buckets.entry(key).or_default().insert(candidate.to_owned()) {return Err(Reason::Duplicate);}
        self.count+=1;Ok(())
    }
    fn bind(&mut self, path: Path, ufrag: &str) -> Result<Vec<String>, String> {
        if self.bound.get(&path).is_some_and(|v| v != ufrag) { return Err("conflicting remote SDP ufrag".into()); }
        self.bound.insert(path, ufrag.to_owned());
        self.buckets.retain(|(p,u),_| *p != path || u == ufrag);
        self.count = self.buckets.values().map(HashSet::len).sum();
        Ok(self.buckets.get(&(path,ufrag.to_owned())).map(|v|v.iter().cloned().collect()).unwrap_or_default())
    }
}

pub fn sdp_ufrag(sdp: &str) -> Result<String, String> {
    if sdp.len() > 128 * 1024 { return Err("oversized SDP".into()); }
    let mut value: Option<&str> = None;
    for line in sdp.lines() {
        if let Some(u) = line.trim_end_matches('\r').strip_prefix("a=ice-ufrag:") {
            if u.is_empty() || u.len()>256 || u.chars().any(char::is_whitespace) { return Err("invalid SDP ufrag".into()); }
            if value.is_some_and(|old| old != u) { return Err("multiple SDP ICE generations".into()); }
            value=Some(u);
        }
    }
    value.map(str::to_owned).ok_or_else(||"missing SDP ufrag".into())
}

pub struct DirectAttempt {
    diagnostics: Arc<ConnectionDiagnostics>,
    pub capability: Capability,
    role: Role, servers: Vec<IceServer>, send: SignalSender,
    runtime: OnceLock<RouteRuntime>, peers: Mutex<HashMap<Path, Peer>>,
    creating: Mutex<HashSet<Path>>, candidates: Mutex<CandidateStore>,
    pending_turn: Mutex<Option<Signal>>, turn_allowed: AtomicBool,
    completed: AtomicBool, terminal: AtomicBool, terminal_notice:watch::Sender<bool>, permit: Mutex<Option<Fence>>, changed: Notify,
    events: mpsc::Sender<TransportEvent>, failure: Mutex<Option<String>>,
}
struct Executor(Weak<DirectAttempt>);
impl EffectExecutor for Executor {
    fn effect_failed(&self,effect:&Effect,timed_out:bool) {
        if let Some(attempt)=self.0.upgrade() {
            let (path,stage)=match effect {
                Effect::Send(e)=>(e.fence.path,control_stage(e.message).unwrap_or(Stage::ControlReceive)),
                Effect::FirstFramePermit(f)=>(f.path,Stage::FirstFramePermit),
                _=>(Path::Direct,Stage::AttemptEnd),
            };
            attempt.diagnostics.scope(path).note(stage,Outcome::Failed,if timed_out {Reason::Timeout}else{Reason::OperationFailed});
        }
    }
    fn execute<'a>(&'a mut self, effect: Effect) -> EffectFuture<'a> {
        Box::pin(async move {
            let Some(attempt) = self.0.upgrade() else { return Err("attempt released".into()); };
            match effect {
                Effect::Send(envelope) => {
                    if envelope.message == crate::online_route_choice::Wire::AttemptAbort {
                        attempt.block_frames();
                        *attempt.permit.lock().await = None;
                        attempt.changed.notify_waiters();
                    }
                    if matches!(envelope.message, crate::online_route_choice::Wire::Prepare(_)
                        | crate::online_route_choice::Wire::Prepared(_) | crate::online_route_choice::Wire::StartAck) {
                        let selected = attempt.peers.lock().await.get(&envelope.fence.path)
                            .and_then(|p|p.dc.clone().map(|dc|(p.link.clone(),dc)));
                        let Some((link,dc))=selected else {return Err("selected path missing at commit/start".into());};
                        if !link.ready_proof(&dc).await.valid(envelope.fence.path) {
                            return Err("selected path no longer READY at commit/start".into());
                        }
                    }
                    let scope=attempt.diagnostics.scope(envelope.fence.path);
                    if let Some(stage)=control_stage(envelope.message) {scope.run(stage,(attempt.send)(Signal::Control(envelope))).await?;}
                    else {(attempt.send)(Signal::Control(envelope)).await?;}
                    if envelope.message == crate::online_route_choice::Wire::StartAck && attempt.role == Role::Receiver {
                        let _ = attempt.events.try_send(TransportEvent::Started(envelope.fence));
                    }
                },
                Effect::CreateTurnPeerConnection(_) => {
                    attempt.diagnostics.scope(Path::Turn).note(Stage::TurnDecision,Outcome::Ok,Reason::None);
                    let _=attempt.events.try_send(TransportEvent::TurnStarted);
                    attempt.turn_allowed.store(true, Ordering::Release);
                    let task=attempt.clone();
                    tokio::spawn(async move {
                        let result=if task.role==Role::Sender { task.create_offer(Path::Turn).await }
                        else if let Some(signal)=task.pending_turn.lock().await.take() { task.description(signal).await }
                        else { Ok(()) };
                        if let Err(e)=result { task.fail(e).await; }
                    });
                }
                Effect::FirstFramePermit(fence) => {
                    if attempt.terminal.load(Ordering::Acquire) { return Err("terminal attempt".into()); }
                    let selected=attempt.peers.lock().await.get(&fence.path)
                        .and_then(|p|p.dc.clone().map(|dc|(p.link.clone(),dc)));
                    let Some((link,dc))=selected else {return Err("selected channel missing at START_ACK".into());};
                    if !link.ready_proof(&dc).await.valid(fence.path) {
                        return Err("selected channel no longer READY at START_ACK".into());
                    }
                    attempt.diagnostics.scope(fence.path).note(Stage::FirstFramePermit,Outcome::Ok,Reason::None);
                    *attempt.permit.lock().await=Some(fence); attempt.changed.notify_waiters();
                }
                Effect::CloseUnselected(path) => {
                    if let Some(peer)=attempt.peers.lock().await.remove(&path) { peer.link.close(); }
                }
                Effect::ApplicationCompleted => {
                    attempt.completed.store(true, Ordering::Release);
                    *attempt.permit.lock().await = None;
                    attempt.changed.notify_waiters();
                }
                Effect::CloseAll(_) => attempt.close_peers().await,
                Effect::Failed => {
                    attempt.block_frames();
                    *attempt.permit.lock().await=None;
                    attempt.changed.notify_waiters();
                    let _=attempt.events.try_send(TransportEvent::Failed("安全连接建立失败，请重试".into()));
                }
                _ => {}
            }
            Ok(())
        })
    }
}
impl DirectAttempt {
    #[cfg(test)]
    pub fn new(role: Role, capability: Capability, servers: Vec<IceServer>, send: SignalSender)
        -> (Arc<Self>, mpsc::Receiver<TransportEvent>) {
        Self::new_observed(role,capability,servers,send,Arc::new(|_|{}))
    }
    pub fn new_observed(role:Role,capability:Capability,servers:Vec<IceServer>,send:SignalSender,observer:Observer) -> (Arc<Self>,mpsc::Receiver<TransportEvent>) {
        let diagnostics=ConnectionDiagnostics::new(role,observer);
        let (events,rx)=mpsc::channel(32);
        let (terminal_notice,_)=watch::channel(false);
        let attempt=Arc::new(Self { diagnostics, capability, role, servers, send, runtime: OnceLock::new(),
            peers:Mutex::new(HashMap::new()),creating:Mutex::new(HashSet::new()), candidates:Mutex::new(CandidateStore::default()),
            pending_turn:Mutex::new(None),turn_allowed:AtomicBool::new(false),completed:AtomicBool::new(false),terminal:AtomicBool::new(false),terminal_notice,
            permit:Mutex::new(None),changed:Notify::new(),events,failure:Mutex::new(None) });
        let runtime=RouteRuntime::spawn(RouteHandler::new(role,capability.epoch),Executor(Arc::downgrade(&attempt)),64);
        assert!(attempt.runtime.set(runtime).is_ok());
        (attempt,rx)
    }
    fn runtime(&self) -> &RouteRuntime { self.runtime.get().expect("initialized runtime") }
    fn fence(&self,path:Path) -> Fence { Fence {epoch:self.capability.epoch,seq:if path==Path::Direct {0}else{1},path} }
    fn matches(&self,fence:Fence) -> bool { fence==self.fence(fence.path) && !self.terminal.load(Ordering::Acquire) }
    pub async fn expect_offer(&self, origin:Instant) -> Result<(),String> {
        self.runtime().dispatch_setup_expected(self.capability.epoch,origin).await.map(|_|())
    }
    pub async fn start(self:&Arc<Self>) -> Result<(),String> {
        if self.role!=Role::Sender { return Err("receiver cannot initiate".into()); }
        self.expect_offer(Instant::now()).await?;
        match self.create_offer(Path::Direct).await { Ok(())=>Ok(()),Err(e)=>{self.fail(e.clone()).await;Err(e)} }
    }
    async fn create_offer(self:&Arc<Self>,path:Path) -> Result<(),String> {
        if self.terminal.load(Ordering::Acquire) { return Err("terminal attempt".into()); }
        if path==Path::Turn && !self.turn_allowed.load(Ordering::Acquire) { return Err("TURN before cutoff".into()); }
        if !self.creating.lock().await.insert(path) { return Ok(()); }
        // The direct attempt uses the STUN-only Direct configuration so
        // its candidate pool cannot contain a relay pair; see
        // mode_configuration in online_p2p.rs for the field evidence.
        let mode=if path==Path::Direct {IceMode::Direct}else{IceMode::Turn};
        let scope=self.diagnostics.scope(path);
        let (link,ice,sdp)=scope.bounded(Stage::OfferSetup,OnlineP2p::start_initiator_observed(self.servers.clone(),mode,Some(scope.clone()))).await?;
        let origin=link.gathering_origin();let ufrag=sdp_ufrag(&sdp)?;
        if self.terminal.load(Ordering::Acquire) {link.close();return Err("terminal attempt".into());}
        let link=Arc::new(link);
        self.peers.lock().await.insert(path,Peer { link:link.clone(),dc:None,remote_sdp:None,remote_ufrag:None,local_sdp:sdp.clone(),local_ufrag:ufrag.clone() });
        if path==Path::Direct {self.runtime().dispatch_gathering_started(self.capability.epoch,origin).await?;}
        // Offer publication must complete before forwarding its candidates.
        scope.bounded(Stage::OfferSend,(self.send)(Signal::Description {fence:self.fence(path),answer:false,ufrag:ufrag.clone(),sdp})).await?;
        self.forward_ice(path,ufrag,ice);
        self.watch_peer(path,link,None);
        Ok(())
    }
    pub async fn receive(self:&Arc<Self>,signal:Signal) -> Result<(),String> {
        if self.terminal.load(Ordering::Acquire) || self.is_completed() {return Ok(());}
        let result=match signal {
            Signal::Control(envelope) => {
                let scope=self.diagnostics.scope(envelope.fence.path);
                if let Some(stage)=control_stage(envelope.message) {scope.note(stage,Outcome::Observed,if self.matches(envelope.fence) || (envelope.fence.epoch==self.capability.epoch && envelope.fence.seq==1) {Reason::None}else{Reason::StaleFence});}
                scope.run(Stage::ControlReceive,async {self.runtime().dispatch(Event::Wire(envelope)).await.map(|_|())}).await
            },
            Signal::Ice {fence,ufrag,candidate} => {
                let scope=self.diagnostics.scope(fence.path);
                if !self.matches(fence) {scope.note(Stage::IceValidate,Outcome::Rejected,Reason::StaleFence);return Ok(());}
                let mut candidates=self.candidates.lock().await;
                let admission=candidates.admit(fence.path,&ufrag,&candidate);
                if let Err(reason)=admission {scope.note(Stage::IceValidate,Outcome::Rejected,reason);return Ok(());}
                scope.note(Stage::IceValidate,Outcome::Ok,Reason::None);
                let bound=candidates.bound.get(&fence.path).is_some_and(|u|u==&ufrag);drop(candidates);
                if bound {
                    let link=self.peers.lock().await.get(&fence.path).map(|p|p.link.clone());
                    if let Some(link)=link {scope.bounded(Stage::IceAdd,link.add_remote_ice(Some(candidate))).await?;}
                }
                Ok(())
            }
            signal@Signal::Description { .. } => self.description(signal).await,
        };
        if let Err(e)=&result {self.fail(e.clone()).await;}
        result
    }
    async fn description(self:&Arc<Self>,signal:Signal) -> Result<(),String> {
        let Signal::Description {fence,answer,ufrag,sdp}=signal else {return Ok(());};
        if !self.matches(fence) {return Ok(());}
        let scope=self.diagnostics.scope(fence.path);
        scope.run(Stage::DescriptionValidate,async {if sdp_ufrag(&sdp)?!=ufrag {return Err("SDP ufrag envelope mismatch".into());} Ok(())}).await?;
        if fence.path==Path::Turn && !self.turn_allowed.load(Ordering::Acquire) {
            if !answer && self.role==Role::Receiver {
                let mut pending=self.pending_turn.lock().await;
                if pending.is_none() {*pending=Some(Signal::Description {fence,answer,ufrag,sdp});}
            }
            return Ok(());
        }
        let path=fence.path;
        if answer {
            if self.role!=Role::Sender {return Err("unexpected answer".into());}
            let link={
                let peers=self.peers.lock().await;
                let Some(peer)=peers.get(&path) else {return Ok(());};
                if let Some(old)=&peer.remote_sdp {return if old==&sdp {Ok(())}else{Err("conflicting answer".into())};}
                peer.link.clone()
            };
            scope.bounded(Stage::AnswerSetup,link.handle_answer(&sdp)).await?;
            let mut peers=self.peers.lock().await;
            let peer=peers.get_mut(&path).ok_or("closed peer")?;
            peer.remote_sdp=Some(sdp);peer.remote_ufrag=Some(ufrag.clone());drop(peers);
        } else {
            if self.role!=Role::Receiver {return Err("unexpected offer".into());}
            {
                let peers=self.peers.lock().await;
                if let Some(peer)=peers.get(&path) {
                    if peer.remote_sdp.as_ref()!=Some(&sdp) {return Err("conflicting offer".into());}
                    let reply=Signal::Description {fence,answer:true,ufrag:peer.local_ufrag.clone(),sdp:peer.local_sdp.clone()};drop(peers);
                    return scope.bounded(Stage::AnswerSend,(self.send)(reply)).await;
                }
            }
            if !self.creating.lock().await.insert(path) {return Ok(());}
            let (frames_tx,frames_rx)=mpsc::channel(32);
            let mode=if path==Path::Direct {IceMode::Direct}else{IceMode::Turn};
            let (link,ice,local_sdp)=scope.bounded(Stage::AnswerSetup,OnlineP2p::accept_offer_observed(self.servers.clone(),mode,&sdp,frames_tx,Some(scope.clone()))).await?;
            let origin=link.gathering_origin();let local_ufrag=sdp_ufrag(&local_sdp)?;
            if self.terminal.load(Ordering::Acquire) {link.close();return Err("terminal attempt".into());}
            let link=Arc::new(link);
            self.peers.lock().await.insert(path,Peer {link:link.clone(),dc:None,remote_sdp:Some(sdp),remote_ufrag:Some(ufrag.clone()),local_sdp:local_sdp.clone(),local_ufrag:local_ufrag.clone()});
            if path==Path::Direct {self.runtime().dispatch_gathering_started(self.capability.epoch,origin).await?;}
            scope.bounded(Stage::AnswerSend,(self.send)(Signal::Description {fence,answer:true,ufrag:local_ufrag.clone(),sdp:local_sdp})).await?;
            self.forward_ice(path,local_ufrag,ice);
            self.watch_peer(path,link,Some(frames_rx));
        }
        let pending=self.candidates.lock().await.bind(path,&ufrag)?;
        let link=self.peers.lock().await.get(&path).map(|p|p.link.clone());
        if let Some(link)=link {for candidate in pending {scope.bounded(Stage::IceAdd,link.add_remote_ice(Some(candidate))).await?;}}
        Ok(())
    }
    fn forward_ice(self:&Arc<Self>,path:Path,ufrag:String,mut rx:mpsc::UnboundedReceiver<Option<String>>) {
        let task=Arc::downgrade(self);
        tokio::spawn(async move {
            while let Some(candidate)=rx.recv().await {
                let Some(task)=task.upgrade() else {break;};
                if task.terminal.load(Ordering::Acquire) {break;}
                let Some(candidate)=candidate else {continue;};
                if let Err(e)=task.diagnostics.scope(path).bounded(Stage::IceSend,(task.send)(Signal::Ice {fence:task.fence(path),ufrag:ufrag.clone(),candidate})).await {
                    task.fail(e).await;break;
                }
            }
        });
    }
    fn watch_peer(self:&Arc<Self>,path:Path,link:Arc<OnlineP2p>,frames:Option<mpsc::Receiver<Vec<u8>>>) {
        let monitor=Arc::downgrade(self);let observed_link=link.clone();
        tokio::spawn(async move {
            loop {
                let Some(task)=monitor.upgrade() else {break;};
                if task.terminal.load(Ordering::Acquire) || !task.peers.lock().await.get(&path).is_some_and(|p|Arc::ptr_eq(&p.link,&observed_link)) {break;}
                let scope=task.diagnostics.scope(path);drop(task);
                let summary=observed_link.diagnostic_pair_state().await;
                let Some(task)=monitor.upgrade() else {break;};
                if task.terminal.load(Ordering::Acquire) {break;}
                scope.pair(summary);drop(task);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let weak=Arc::downgrade(self);
        tokio::spawn(async move {
            let dc=match link.wait_open().await {Ok(dc)=>dc,Err(_)=>{
                if let Some(task)=weak.upgrade() {let _=task.runtime().dispatch(Event::PathFailed {epoch:task.capability.epoch,path}).await;} return;
            }};
            let Some(task)=weak.upgrade() else {link.close();return;};
            task.diagnostics.scope(path).note(Stage::ChannelOpen,Outcome::Ok,Reason::None);
            if task.terminal.load(Ordering::Acquire) {link.close();return;}
            if let Some(peer)=task.peers.lock().await.get_mut(&path) {peer.dc=Some(dc.clone());}else{return;}
            if let Some(mut frames)=frames {
                let frame_task=Arc::downgrade(&task);
                tokio::spawn(async move {
                    while let Some(bytes)=frames.recv().await {
                        let Some(task)=frame_task.upgrade() else {break;};
                        if task.terminal.load(Ordering::Acquire) {break;}
                        let fence=Fence {epoch:task.capability.epoch,seq:1,path};
                        match task.runtime().dispatch(Event::Frame {fence}).await {
                            Ok(effects) if effects.contains(&Effect::AcceptFrame(fence)) => {
                                if task.events.send(TransportEvent::Frame {fence,bytes}).await.is_err() {break;}
                            }
                            _=>{} // An unselected/pre-START channel never reaches the file engine.
                        }
                    }
                });
            }
            drop(task);
            loop {
                let Some(task)=weak.upgrade() else {break;};
                if task.terminal.load(Ordering::Acquire) || !task.peers.lock().await.contains_key(&path) {break;}
                if matches!(link.state().await,P2pState::Failed|P2pState::Closed) || dc.state()!=DataChannelState::Open {
                    let _=task.runtime().dispatch(Event::PathFailed {epoch:task.capability.epoch,path}).await;break;
                }
                let proof=link.ready_proof(&dc).await;
                if proof.valid(path) {
                    task.diagnostics.scope(path).note(Stage::ReadyProof,Outcome::Ok,Reason::None);
                    if task.runtime().dispatch(Event::OwnReady {epoch:task.capability.epoch,path,proof}).await.is_err() {break;}
                }
                drop(task);tokio::time::sleep(Duration::from_millis(40)).await;
            }
        });
    }
    pub async fn wait_selected(&self) -> Result<(Fence,Arc<OnlineP2p>,DataChannel),String> {
        tokio::time::timeout(Duration::from_secs(20),async {
            loop {
                let changed=self.changed.notified();
                if self.terminal.load(Ordering::Acquire) {return Err(self.failure.lock().await.clone().unwrap_or_else(||"安全连接失败".into()));}
                if let Some(fence)=*self.permit.lock().await {
                    let peers=self.peers.lock().await;let peer=peers.get(&fence.path).ok_or("selected peer closed")?;
                    return Ok((fence,peer.link.clone(),peer.dc.clone().ok_or("selected DC missing")?));
                }
                changed.await;
            }
        }).await.map_err(|_|"selected path timeout".to_string())?
    }
    pub async fn active_fence(&self) -> Option<Fence> {
        if self.terminal.load(Ordering::Acquire) {return None;}
        let (phase,path,_)=self.runtime().snapshot().await.ok()?;
        if phase!=Phase::Active {return None;}
        Some(Fence {epoch:self.capability.epoch,seq:1,path:path?})
    }
    pub async fn selected_peer(&self, fence:Fence) -> Option<Arc<OnlineP2p>> {
        if self.active_fence().await!=Some(fence) {return None;}
        self.peers.lock().await.get(&fence.path).map(|p|p.link.clone())
    }
    pub async fn confirm_ack(&self,fence:Fence,bytes:u64) -> bool {
        if self.active_fence().await!=Some(fence) {return false;}
        self.runtime().dispatch(Event::Wire(Envelope {fence,message:crate::online_route_choice::Wire::Ack {bytes}})).await.is_ok()
    }
    /// Called only after the session's durable and task-level ACK boundary.
    /// Keep the active receive fence for duplicate final-frame ACK replay.
    pub async fn complete(&self, fence:Fence) -> Result<(),String> {
        self.runtime().dispatch(Event::ApplicationCompleted {fence}).await?;
        if self.is_completed() {Ok(())} else {Err("transport failed before final completion".into())}
    }
    pub fn is_completed(&self) -> bool {self.completed.load(Ordering::Acquire)}
    pub fn is_terminal(&self) -> bool {self.terminal.load(Ordering::Acquire)}
    pub fn terminal_watch(&self) -> watch::Receiver<bool> {self.terminal_notice.subscribe()}
    pub fn block_frames(&self) {
        if !self.terminal.swap(true,Ordering::AcqRel) {
            self.diagnostics.scope(Path::Direct).note(Stage::AttemptEnd,Outcome::Observed,Reason::None);
            self.diagnostics.flush();
        }
        self.terminal_notice.send_replace(true);
    }
    pub async fn fail(&self,error:String) {
        // Serialize against ApplicationCompleted; a late ICE/close callback
        // must not independently turn a completed task into a terminal failure.
        *self.failure.lock().await=Some(error);
        let _=self.runtime().dispatch(Event::Timeout {epoch:self.capability.epoch}).await;
    }
    async fn close_peers(&self) {
        self.block_frames();
        *self.permit.lock().await=None;
        for (_,peer) in self.peers.lock().await.drain() {peer.link.close();}
        self.changed.notify_waiters();
    }
    pub async fn close(&self) {
        if self.is_completed() {self.close_peers().await;}
        else {self.fail("连接已关闭".into()).await;}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn candidate_rejection_categories_preserve_strict_filters() {
        let mut store=CandidateStore::default();
        assert_eq!(store.admit(Path::Turn,"","{}"),Err(Reason::Bounds));
        assert_eq!(store.admit(Path::Turn,"abc","not-json"),Err(Reason::Malformed));
        assert_eq!(store.admit(Path::Turn,"abc",r#"{"candidate":"secret"}"#),Err(Reason::InvalidSyntax));
        let candidate=serde_json::json!({"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host ufrag abc"}).to_string();
        assert_eq!(store.admit(Path::Direct,"wrong",&candidate),Err(Reason::UfragMismatch));
        assert_eq!(store.admit(Path::Direct,"abc",&candidate),Ok(()));
        assert_eq!(store.admit(Path::Direct,"abc",&candidate),Err(Reason::Duplicate));
        store.bind(Path::Direct,"abc").unwrap();
        assert_eq!(store.admit(Path::Direct,"old",&candidate),Err(Reason::Generation));
    }
    #[test]
    fn sdp_and_candidate_generation_fences() {
        assert_eq!(sdp_ufrag("v=0\r\na=ice-ufrag:abc\r\n").unwrap(),"abc");
        assert!(sdp_ufrag("a=ice-ufrag:a\na=ice-ufrag:b").is_err());
        assert!(sdp_ufrag("v=0").is_err());
        let mut candidates=CandidateStore::default();
        let candidate=serde_json::json!({"candidate":"candidate:1 1 udp 1 127.0.0.1 9 typ host ufrag abc"}).to_string();
        assert!(!candidates.insert(Path::Direct,"wrong",&candidate));
        assert!(candidates.insert(Path::Direct,"abc",&candidate));
        assert!(!candidates.insert(Path::Direct,"abc",&candidate));
        assert_eq!(candidates.bind(Path::Direct,"abc").unwrap(),vec![candidate.clone()]);
        assert!(candidates.bind(Path::Direct,"old").is_err());
        assert!(!candidates.insert(Path::Direct,"old",&candidate));
    }
}

#[cfg(test)]
mod native_tests {
    use super::*;
    use crate::online_route_choice::Wire;
    fn sender(tx:mpsc::UnboundedSender<Signal>) -> SignalSender {
        Arc::new(move |signal| {let tx=tx.clone();Box::pin(async move {
            // Exercise the production serde wire contract, not shared objects.
            let json=serde_json::to_vec(&signal).unwrap();
            let signal=serde_json::from_slice(&json).unwrap();
            tx.send(signal).map_err(|_|"test signaling closed".into())
        })})
    }
    #[tokio::test(flavor="multi_thread",worker_threads=4)]
    async fn actual_completed_receiver_replays_frames_then_closes_without_abort_or_failed() {
        let capability=Capability::fresh();
        let (stx,mut srx)=mpsc::unbounded_channel();let (rtx,mut rrx)=mpsc::unbounded_channel();
        let seen=Arc::new(std::sync::Mutex::new(Vec::new()));let observed=seen.clone();
        let (s,_sevents)=DirectAttempt::new(Role::Sender,capability,vec![],sender(stx));
        let (r,mut events)=DirectAttempt::new_observed(Role::Receiver,capability,vec![],sender(rtx),Arc::new(move |e|observed.lock().unwrap().push(serde_json::to_value(e).unwrap())));
        r.expect_offer(Instant::now()).await.unwrap();
        let task=r.clone();let sr=tokio::spawn(async move {while let Some(signal)=srx.recv().await {task.receive(signal).await.unwrap();}});
        let task=s.clone();let rs=tokio::spawn(async move {while let Some(signal)=rrx.recv().await {task.receive(signal).await.unwrap();}});
        s.start().await.unwrap();let (fence,_link,dc)=s.wait_selected().await.unwrap();
        r.complete(fence).await.unwrap();
        assert!(r.is_completed());assert!(!r.is_terminal());
        for _ in 0..2 {
            dc.send(b"replayed-final-frame",true).unwrap();
            tokio::time::timeout(Duration::from_secs(3),async {
                loop {match events.recv().await.unwrap() {
                    TransportEvent::Frame {fence:f,bytes}=>{assert_eq!(f,fence);assert_eq!(bytes,b"replayed-final-frame");break;},
                    TransportEvent::Started(_)=>{},_=>panic!("completed receiver must not fail"),
                }}
            }).await.unwrap();
        }
        assert_eq!(r.active_fence().await,Some(fence));
        assert!(r.confirm_ack(fence,19).await);assert!(r.confirm_ack(fence,19).await);
        r.fail("late ICE callback".into()).await;
        assert!(!r.is_terminal(),"late callback must not destroy final ACK replay window");
        r.close().await;r.close().await;
        assert!(r.is_terminal());assert!(r.peers.lock().await.is_empty());
        assert!(r.is_completed());
        assert_eq!(r.runtime().snapshot().await.unwrap().0,Phase::Active);
        assert!(!seen.lock().unwrap().iter().any(|e|e["stage"]=="attempt_abort"));
        while let Ok(event)=events.try_recv() {assert!(!matches!(event,TransportEvent::Failed(_)));}
        sr.abort();rs.abort();s.close().await;
    }
    #[tokio::test(flavor="multi_thread",worker_threads=4)]
    async fn actual_native_peers_require_ready_commit_start_ack_before_frame() {
        let capability=Capability::fresh();
        let (stx,mut srx)=mpsc::unbounded_channel();let (rtx,mut rrx)=mpsc::unbounded_channel();
        let observed=Arc::new(std::sync::Mutex::new(Vec::new()));let captured=observed.clone();
        let (s,_sevents)=DirectAttempt::new_observed(Role::Sender,capability,vec![],sender(stx),Arc::new(move |e|captured.lock().unwrap().push(serde_json::to_value(e).unwrap())));
        let (r,mut revents)=DirectAttempt::new(Role::Receiver,capability,vec![],sender(rtx));
        r.expect_offer(Instant::now()).await.unwrap();
        let rtask=r.clone();let sr=tokio::spawn(async move {while let Some(signal)=srx.recv().await {rtask.receive(signal).await.unwrap();}});
        let stask=s.clone();let rs=tokio::spawn(async move {while let Some(signal)=rrx.recv().await {stask.receive(signal).await.unwrap();}});
        let start=Instant::now();s.start().await.unwrap();
        assert!(s.permit.lock().await.is_none());
        let (fence,link,dc)=s.wait_selected().await.unwrap();
        assert_eq!(fence.path,Path::Direct);assert_eq!(fence.epoch,capability.epoch);
        assert!(start.elapsed()>=Duration::from_millis(2500));
        let proof=link.ready_proof(&dc).await;assert!(proof.valid(Path::Direct),"actual selected native proof: {proof:?}");
        assert_eq!(s.active_fence().await,Some(fence));assert_eq!(r.active_fence().await,Some(fence));
        dc.send(b"actual-native-frame",true).unwrap();
        let frame=tokio::time::timeout(Duration::from_secs(3),async {
            loop {match revents.recv().await.unwrap() {TransportEvent::Frame{fence,bytes}=>break(fence,bytes),TransportEvent::Started(_)=>{},TransportEvent::TurnStarted=>panic!("successful Direct must not start TURN"),TransportEvent::Failed(e)=>panic!("{e}")}}
        }).await.unwrap();
        assert_eq!(frame,(fence,b"actual-native-frame".to_vec()));
        {
            let events=observed.lock().unwrap();
            for stage in ["peer_create","channel_create","offer_create","offer_set","offer_send","answer_apply","channel_wait","channel_open","ready_proof","prepare","commit","start","first_frame_permit"] {
                assert!(events.iter().any(|e|e["stage"]==stage && e["outcome"]=="ok"),"missing production diagnostic: {stage}");
            }
            assert!(events.iter().any(|e|e["stage"]=="pair_observe" && e["pair"]["selected"]==true));
        }
        let old=Fence {epoch:Epoch{generation:0,..capability.epoch},..fence};
        assert!(!s.confirm_ack(old,100).await);assert!(s.confirm_ack(fence,23).await);
        assert_eq!(s.runtime().snapshot().await.unwrap().2,23);
        s.fail("test selected path failure".into()).await;
        assert!(s.is_terminal());assert!(s.active_fence().await.is_none());
        // No late READY or START_ACK may resurrect this attempt.
        s.receive(Signal::Control(Envelope {fence,message:Wire::StartAck})).await.unwrap();
        assert!(s.permit.lock().await.is_none());
        sr.abort();rs.abort();r.close().await;
    }
    #[tokio::test]
    async fn initial_offer_publish_failure_and_lost_abort_have_bounded_remote_failure() {
        let capability=Capability::fresh();
        let send:SignalSender=Arc::new(|_|Box::pin(async {Err("injected publication failure".into())}));
        let (s,_)=DirectAttempt::new(Role::Sender,capability,vec![],send.clone());
        let (r,mut events)=DirectAttempt::new(Role::Receiver,capability,vec![],send);
        r.expect_offer(Instant::now()-Duration::from_millis(14_950)).await.unwrap();
        assert!(s.start().await.is_err());assert!(s.is_terminal());
        let event=tokio::time::timeout(Duration::from_secs(1),events.recv()).await.unwrap().unwrap();
        assert!(matches!(event,TransportEvent::Failed(_)));assert!(r.is_terminal());
        assert!(r.peers.lock().await.is_empty());assert!(!r.turn_allowed.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod terminal_notice_tests {
    use super::*;
    #[tokio::test]
    async fn terminal_flush_reports_exact_ice_totals_once_outside_frame_queue() {
        let saved=Arc::new(std::sync::Mutex::new(Vec::new()));let captured=saved.clone();
        let send:SignalSender=Arc::new(|_|Box::pin(async{Ok(())}));
        let (route,_events)=DirectAttempt::new_observed(Role::Sender,Capability::fresh(),vec![],send,Arc::new(move |e|captured.lock().unwrap().push(serde_json::to_value(e).unwrap())));
        for _ in 0..3 {route.diagnostics.scope(Path::Turn).note(Stage::IceValidate,Outcome::Rejected,Reason::InvalidSyntax);}
        route.block_frames();route.block_frames();
        let events=saved.lock().unwrap();
        assert_eq!(events.iter().filter(|e|e["stage"]=="ice_validate" && e["count"]==3).count(),1);
        assert_eq!(events.iter().filter(|e|e["stage"]=="attempt_end").count(),1);
    }
    #[tokio::test]
    async fn full_frame_event_queue_cannot_drop_terminal_fact() {
        let cap=Capability::fresh();
        let send:SignalSender=Arc::new(|_|Box::pin(async{Err("publication unavailable".into())}));
        let (route,events)=DirectAttempt::new(Role::Receiver,cap,vec![],send);
        let fence=Fence{epoch:cap.epoch,seq:1,path:Path::Direct};
        for _ in 0..32 {route.events.try_send(TransportEvent::Started(fence)).unwrap_or_else(|_|panic!("queue filled too early"));}
        let notice=route.terminal_watch();
        assert!(!*notice.borrow());
        tokio::time::timeout(Duration::from_secs(3),route.fail("selected path failed".into())).await.unwrap();
        assert!(route.is_terminal());assert!(route.peers.lock().await.is_empty());
        assert_eq!(events.len(),32,"bounded data queue remains full in this counterexample");
        assert!(*notice.borrow(),"priority notice is independent of queue capacity");
        assert!(*route.terminal_watch().borrow(),"late subscribers see sticky terminal state");
    }
}
