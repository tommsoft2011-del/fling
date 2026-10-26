//! Direct-first transaction policy. Runtime must serialize `handle` on one task.
//! All times are coordinator-local monotonic milliseconds, never remote timestamps.
//! Shared policy for production native transport and deterministic protocol tests.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Give trickled host/server-reflexive candidate checks enough time to
/// complete before paying for a TURN allocation. Field logs showed multiple
/// Direct pairs still in progress at the old 2.5s cutoff.
pub const DIRECT_CUTOFF_MS: u64 = 5_000;
/// Bounded grace after the cutoff when our own Direct path is fully proven
/// (valid ReadyProof recorded) but the peer's pre-decision Ready is still in
/// flight: the decision waits for the peer Ready instead of escalating to
/// TURN on a hair-missed cutoff. No grace when our own Direct is unproven,
/// so a genuinely unreachable peer still escalates at DIRECT_CUTOFF_MS.
pub const PEER_READY_GRACE_MS: u64 = 2_000;
/// Covers the extended Direct window, optional peer-ready grace and TURN
/// startup when the receiver began waiting before the sender's offer.
pub const SETUP_LIMIT_MS: u64 = 20_000;
const HANDSHAKE_LIMIT_MS: u64 = 15_000;
const RETRANSMIT_MS: u64 = 200;
const MAX_CANDIDATES: usize = 256;
const MAX_CANDIDATE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Path { Direct, Turn }
impl Path { fn other(self) -> Self { if self == Self::Direct { Self::Turn } else { Self::Direct } } }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Epoch { pub attempt: u64, pub generation: u64 }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Fence { pub epoch: Epoch, pub seq: u64, pub path: Path }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role { Sender, Receiver }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReadyProof {
    pub dc_open: bool, pub pc_connected: bool, pub selected: bool,
    pub nominated: bool, pub succeeded: bool, pub known: bool, pub relay: bool,
}
impl ReadyProof {
    pub fn valid(self, path: Path) -> bool {
        self.dc_open && self.pc_connected && self.selected && self.nominated
            && self.succeeded && self.known && self.relay == (path == Path::Turn)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Wire {
    Ready(ReadyProof), Decision, DecisionAck, Request, RequestAck,
    Prepare(ReadyProof), Prepared(ReadyProof), Commit, Committed,
    Start, StartAck, StartConfirmed, Abort, AttemptAbort, Ack { bytes: u64 },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Envelope { pub fence: Fence, pub message: Wire }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase { Idle, Gathering, Locked, Requesting, Preparing, Committing, Starting, Active, Failed }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OfferFailure { CreateOffer, SetLocalDescription, InitialOfferSend }
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Trusted Rust acceptance of the session/manifest, before ACK publication.
    /// This starts only a failure watchdog, NEVER a route-selection clock.
    SetupExpected { epoch: Epoch },
    /// Called only following successful setLocalDescription; now is sampled immediately there.
    GatheringStarted { epoch: Epoch, now: u64 },
    OwnReady { epoch: Epoch, path: Path, proof: ReadyProof },
    Wire(Envelope),
    OfferFailed { epoch: Epoch, failure: OfferFailure },
    PathFailed { epoch: Epoch, path: Path },
    /// Trusted application boundary: durable file ACK AND final task ACK succeeded.
    ApplicationCompleted { fence: Fence },
    Frame { fence: Fence },
    Tick,
    Timeout { epoch: Epoch },
    Retry(Epoch),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Send(Envelope), CreateTurnPeerConnection(Epoch), FirstFramePermit(Fence),
    AcceptFrame(Fence), CloseUnselected(Path), CloseAll(Epoch),
    DirectStarted, Cutoff(Path), Failed, IgnoreStale, ApplicationCompleted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteHandler {
    role: Role, epoch: Epoch, phase: Phase, selected: Option<Path>,
    setup_at: Option<u64>,
    started_at: Option<u64>, last_now: u64, decision_at: Option<u64>,
    own_direct_at: Option<u64>, peer_direct_at: Option<u64>,
    own: HashMap<Path, ReadyProof>, peer: HashMap<Path, ReadyProof>,
    decision_ack: bool, receiver_prepared: bool, receiver_committed: bool,
    close_confirmed: bool, bytes: u64, application_completed: bool,
    pending: Option<Envelope>, last_send: u64,
    expected_ufrags: HashMap<Path, String>,
    candidates: HashMap<(Path, String), HashSet<String>>, candidate_count: usize,
}
impl RouteHandler {
    pub fn new(role: Role, epoch: Epoch) -> Self {
        Self { role, epoch, phase: Phase::Idle, selected: None, started_at: None,
            setup_at: None,
            last_now: 0, decision_at: None, own_direct_at: None, peer_direct_at: None,
            own: HashMap::new(), peer: HashMap::new(), decision_ack: false,
            receiver_prepared: false, receiver_committed: false, close_confirmed: false,
            bytes: 0, application_completed: false, pending: None, last_send: 0, expected_ufrags: HashMap::new(),
            candidates: HashMap::new(), candidate_count: 0 }
    }
    pub fn phase(&self) -> Phase { self.phase }
    pub fn selected(&self) -> Option<Path> { self.selected }
    pub fn epoch(&self) -> Epoch { self.epoch }
    pub fn confirmed_bytes(&self) -> u64 { self.bytes }
    pub fn bind_setup_clock(&mut self, epoch: Epoch) -> bool {
        if epoch != self.epoch || self.phase != Phase::Idle || self.setup_at.is_some() { return false; }
        self.last_now = 0;
        true
    }
    /// Rebase only before this attempt has a gathering clock. Runtime calls
    /// this exactly once with the immutable native source Instant; queued
    /// pre-start events cannot establish or reset a decision deadline.
    pub fn bind_source_clock(&mut self, epoch: Epoch) -> bool {
        if epoch != self.epoch || self.phase != Phase::Idle { return false; }
        // Runtime must expire the old setup clock BEFORE rebasing. It is then
        // disarmed by successful gathering, not converted into a path deadline.
        self.setup_at = None;
        self.last_now = 0;
        true
    }
    pub fn fence(&self) -> Option<Fence> { self.selected.map(|path| Fence { epoch: self.epoch, seq: 1, path }) }
    pub fn permits_frame(&self, fence: Fence) -> bool {
        self.role == Role::Sender && !self.application_completed && self.phase == Phase::Active && self.fence() == Some(fence)
    }
    pub fn accepts_frame(&self, fence: Fence) -> bool {
        self.role == Role::Receiver && self.phase == Phase::Active && self.fence() == Some(fence)
    }
    pub fn may_close(&self, path: Path) -> bool {
        self.close_confirmed && self.phase == Phase::Active && self.selected == Some(path.other())
    }
    pub fn confirm_bytes(&mut self, fence: Fence, bytes: u64) -> bool {
        if self.phase != Phase::Active || self.fence() != Some(fence) { return false; }
        self.bytes = self.bytes.max(bytes); true
    }
    /// Returns only the matching previously buffered candidates; mismatching buckets are discarded.
    pub fn bind_ufrag(&mut self, epoch: Epoch, path: Path, ufrag: &str) -> Vec<String> {
        if epoch != self.epoch || ufrag.is_empty() || ufrag.len() > 256 { return vec![]; }
        if self.expected_ufrags.get(&path).is_some_and(|old| old != ufrag) { return vec![]; }
        self.expected_ufrags.insert(path, ufrag.to_owned());
        self.candidates.retain(|(p,u), _| *p != path || u == ufrag);
        self.candidate_count = self.candidates.values().map(HashSet::len).sum();
        self.candidates.get(&(path, ufrag.to_owned())).map(|v| v.iter().cloned().collect()).unwrap_or_default()
    }
    /// Buffer/dedupe only. Applying ICE requires a bound matching remote SDP ufrag.
    pub fn insert_candidate(&mut self, epoch: Epoch, path: Path, ufrag: &str, candidate: &str) -> bool {
        if epoch != self.epoch || self.phase == Phase::Failed || ufrag.is_empty() || ufrag.len() > 256
            || candidate.len() > MAX_CANDIDATE_BYTES || self.candidate_count >= MAX_CANDIDATES
            || (path == Path::Turn && self.selected != Some(Path::Turn))
            || self.expected_ufrags.get(&path).is_some_and(|expected| expected != ufrag) { return false; }
        let key = (path, ufrag.to_owned());
        if !self.candidates.contains_key(&key) && self.candidates.len() >= 8 { return false; }
        if !self.candidates.entry(key).or_default().insert(candidate.to_owned()) { return false; }
        self.candidate_count += 1; true
    }
    fn wire(&self, path: Path, seq: u64, message: Wire) -> Envelope {
        Envelope { fence: Fence { epoch: self.epoch, seq, path }, message }
    }
    fn selected_wire(&self, message: Wire) -> Envelope {
        self.wire(self.selected.expect("selection required"), 1, message)
    }
    fn send_pending(&mut self, message: Wire, now: u64) -> Vec<Effect> {
        let msg = self.selected_wire(message); self.pending = Some(msg); self.last_send = now;
        vec![Effect::Send(msg)]
    }
    fn ready_effect(&self, path: Path) -> Option<Effect> {
        let proof = *self.own.get(&path)?;
        Some(Effect::Send(self.wire(path, if path == Path::Direct { 0 } else { 1 }, Wire::Ready(proof))))
    }
    fn fail(&mut self) -> Vec<Effect> {
        if self.phase == Phase::Failed { return vec![]; }
        let abort = self.wire(Path::Direct, 0, Wire::AttemptAbort);
        self.phase = Phase::Failed; self.pending = None;
        vec![Effect::Send(abort), Effect::CloseAll(self.epoch), Effect::Failed]
    }
    fn maybe_request(&mut self, now: u64) -> Vec<Effect> {
        let Some(path) = self.selected else { return vec![]; };
        if self.role == Role::Sender && self.phase == Phase::Locked && self.decision_ack
            && self.own.get(&path).is_some_and(|p| p.valid(path))
            && self.peer.get(&path).is_some_and(|p| p.valid(path)) {
            self.phase = Phase::Requesting; return self.send_pending(Wire::Request, now);
        }
        vec![]
    }
    /// `now` MUST be sampled by the local serialized executor at event observation.
    /// Remote payloads never supply readiness timestamps. A peer READY received
    /// at/after the cutoff still counts while the PEER_READY_GRACE_MS grace is
    /// open (own Direct proven, peer Ready outstanding); outside grace, a READY
    /// that has not been received by decision time does not count.
    pub fn handle(&mut self, event: Event, now: u64) -> Vec<Effect> {
        // Completion is absorbing for transport failures, but committed receiver
        // frames may still request idempotent final-ACK replay until room teardown.
        // No remote wire message can establish this trusted local boundary.
        if self.application_completed && !matches!(event, Event::Retry(_)) {
            return match event {
                Event::Frame { fence } if self.accepts_frame(fence) => vec![Effect::AcceptFrame(fence)],
                _ => vec![],
            };
        }
        if now < self.last_now { return self.fail(); }
        self.last_now = now;
        if let Event::Retry(epoch) = event {
            if epoch.attempt <= self.epoch.attempt || epoch.generation <= self.epoch.generation { return vec![Effect::IgnoreStale]; }
            let old = self.epoch; let bytes = self.bytes; let role = self.role;
            *self = Self::new(role, epoch); self.bytes = bytes; self.last_now = now;
            return vec![Effect::CloseAll(old)];
        }
        if self.phase == Phase::Failed { return vec![]; }
        // Apply on EVERY event, not just Tick: a queued late offer or duplicate
        // expectation may not beat an already expired watchdog.
        if self.phase == Phase::Idle && self.setup_at.is_some_and(|start| now.saturating_sub(start) >= SETUP_LIMIT_MS) {
            return self.fail();
        }
        match event {
            Event::SetupExpected { epoch } => {
                if epoch != self.epoch { return vec![Effect::IgnoreStale]; }
                if self.phase == Phase::Idle { self.setup_at.get_or_insert(now); }
                vec![]
            }
            Event::GatheringStarted { epoch, now: start } => {
                if epoch != self.epoch { return vec![Effect::IgnoreStale]; }
                if self.phase != Phase::Idle { return vec![]; }
                if start > now { return self.fail(); }
                self.setup_at = None;
                self.started_at = Some(start); self.phase = Phase::Gathering;
                vec![Effect::DirectStarted]
            }
            Event::OfferFailed { epoch, .. } | Event::Timeout { epoch } => {
                if epoch != self.epoch { vec![Effect::IgnoreStale] } else { self.fail() }
            }
            Event::ApplicationCompleted { fence } => {
                if self.phase != Phase::Active || self.fence() != Some(fence) {
                    return vec![Effect::IgnoreStale];
                }
                self.application_completed = true;
                self.pending = None;
                vec![Effect::ApplicationCompleted]
            }
            Event::PathFailed { epoch, path } => {
                if epoch != self.epoch { return vec![Effect::IgnoreStale]; }
                if self.selected == Some(path) { self.fail() } else {
                    self.own.remove(&path); self.own_direct_at = None; vec![]
                }
            }
            Event::OwnReady { epoch, path, proof } => {
                if epoch != self.epoch { return vec![Effect::IgnoreStale]; }
                if self.phase == Phase::Idle || !proof.valid(path)
                    || (path == Path::Turn && self.selected != Some(Path::Turn)) { return vec![]; }
                self.own.insert(path, proof);
                if path == Path::Direct && self.role == Role::Sender && self.phase == Phase::Gathering
                    && self.started_at.is_some_and(|s| now.saturating_sub(s) < DIRECT_CUTOFF_MS) {
                    self.own_direct_at.get_or_insert(now);
                }
                let mut fx = self.ready_effect(path).into_iter().collect::<Vec<_>>();
                fx.extend(self.maybe_request(now)); fx
            }
            Event::Tick => {
                if self.phase == Phase::Idle { return vec![]; }
                let Some(start) = self.started_at else { return vec![]; };
                if self.phase == Phase::Active {
                    // Receiver must not retain the unselected PC indefinitely if
                    // START_ACK or its final confirmation is lost and no frame follows.
                    if self.role == Role::Receiver && !self.close_confirmed
                        && self.decision_at.is_some_and(|t| now.saturating_sub(t) >= HANDSHAKE_LIMIT_MS) {
                        return self.fail();
                    }
                    return vec![];
                }
                if now.saturating_sub(start) >= HANDSHAKE_LIMIT_MS { return self.fail(); }
                if self.role == Role::Sender && self.phase == Phase::Gathering
                    && now.saturating_sub(start) >= DIRECT_CUTOFF_MS {
                    // Grace: our Direct is fully proven but the peer's Ready
                    // has not landed yet. Hold the decision (fall through to
                    // retransmission) until it arrives or the grace expires;
                    // escalating here would abandon a healthy Direct path
                    // over a sub-second signalling delay.
                    let in_grace = self.own_direct_at.is_some() && self.peer_direct_at.is_none()
                        && now.saturating_sub(start) < DIRECT_CUTOFF_MS + PEER_READY_GRACE_MS;
                    if !in_grace {
                        let path = if self.own_direct_at.is_some() && self.peer_direct_at.is_some() { Path::Direct } else { Path::Turn };
                        self.selected = Some(path); self.phase = Phase::Locked; self.decision_at = Some(now);
                        let mut fx = vec![Effect::Cutoff(path)];
                        if path == Path::Turn { fx.push(Effect::CreateTurnPeerConnection(self.epoch)); }
                        fx.extend(self.send_pending(Wire::Decision, now)); return fx;
                    }
                }
                if now.saturating_sub(self.last_send) < RETRANSMIT_MS { return vec![]; }
                self.last_send = now;
                let mut fx = self.pending.map(Effect::Send).into_iter().collect::<Vec<_>>();
                if self.role == Role::Receiver || self.phase == Phase::Gathering {
                    if let Some(f) = self.ready_effect(self.selected.unwrap_or(Path::Direct)) { fx.push(f); }
                }
                fx
            }
            Event::Frame { fence } => {
                if !self.accepts_frame(fence) { return vec![Effect::IgnoreStale]; }
                let mut fx = vec![Effect::AcceptFrame(fence)];
                if !self.close_confirmed { self.close_confirmed = true; fx.push(Effect::CloseUnselected(fence.path.other())); }
                fx
            }
            Event::Wire(envelope) => self.on_wire(envelope, now),
            Event::Retry(_) => unreachable!(),
        }
    }
    fn on_wire(&mut self, e: Envelope, now: u64) -> Vec<Effect> {
        use Wire::*;
        if e.fence.epoch != self.epoch { return vec![Effect::IgnoreStale]; }
        if e.message == AttemptAbort {
            // Explicit attempt-level abort has a canonical, path-independent
            // fence. A lost Decision must not prevent terminal convergence.
            let valid = e.fence.seq == 0 && e.fence.path == Path::Direct;
            return if valid { self.fail() } else { vec![Effect::IgnoreStale] };
        }
        if e.message == Abort {
            let valid = self.fence().map_or(e.fence.seq == 0 && e.fence.path == Path::Direct,
                |current| current == e.fence);
            return if valid { self.fail() } else { vec![Effect::IgnoreStale] };
        }
        if let Ready(proof) = e.message {
            if !proof.valid(e.fence.path) { return vec![]; }
            let pre_direct = e.fence.seq == 0 && e.fence.path == Path::Direct;
            if !pre_direct && self.fence() != Some(e.fence) { return vec![Effect::IgnoreStale]; }
            self.peer.insert(e.fence.path, proof);
            if self.role == Role::Sender && self.phase == Phase::Gathering && pre_direct
                && self.started_at.is_some_and(|s| now.saturating_sub(s) < DIRECT_CUTOFF_MS + PEER_READY_GRACE_MS) {
                self.peer_direct_at.get_or_insert(now);
            }
            return self.maybe_request(now);
        }
        if e.message == Decision && self.role == Role::Receiver {
            if e.fence.seq != 1 || self.phase == Phase::Idle { return vec![Effect::IgnoreStale]; }
            if let Some(path) = self.selected {
                if path != e.fence.path { return self.fail(); }
                return vec![Effect::Send(self.selected_wire(DecisionAck))];
            }
            self.selected = Some(e.fence.path); self.phase = Phase::Locked; self.decision_at = Some(now);
            let mut fx = vec![Effect::Send(self.selected_wire(DecisionAck))];
            if e.fence.path == Path::Turn { fx.push(Effect::CreateTurnPeerConnection(self.epoch)); }
            if let Some(ready) = self.ready_effect(e.fence.path) { fx.push(ready); }
            return fx;
        }
        if self.fence() != Some(e.fence) { return vec![Effect::IgnoreStale]; }
        let path = e.fence.path;
        match (self.role, e.message) {
            (Role::Sender, DecisionAck) => { self.decision_ack = true; self.maybe_request(now) }
            (Role::Receiver, Request) => {
                if !self.own.get(&path).is_some_and(|p| p.valid(path)) { return vec![]; }
                if self.phase == Phase::Locked { self.phase = Phase::Requesting; }
                vec![Effect::Send(self.selected_wire(RequestAck))]
            }
            (Role::Sender, RequestAck) if self.phase == Phase::Requesting => {
                self.phase = Phase::Preparing;
                self.send_pending(Prepare(self.own[&path]), now)
            }
            (Role::Receiver, Prepare(proof)) => {
                if !proof.valid(path) || !self.own.get(&path).is_some_and(|p| p.valid(path))
                    || self.phase == Phase::Locked { return vec![]; }
                self.peer.insert(path, proof); self.receiver_prepared = true;
                if self.phase == Phase::Requesting { self.phase = Phase::Preparing; }
                vec![Effect::Send(self.selected_wire(Prepared(self.own[&path])))]
            }
            (Role::Sender, Prepared(proof)) if self.phase == Phase::Preparing => {
                if !proof.valid(path) { return self.fail(); }
                self.peer.insert(path, proof); self.phase = Phase::Committing;
                self.send_pending(Commit, now)
            }
            (Role::Receiver, Commit) if self.receiver_prepared => {
                self.receiver_committed = true;
                if self.phase == Phase::Preparing { self.phase = Phase::Committing; }
                vec![Effect::Send(self.selected_wire(Committed))]
            }
            (Role::Sender, Committed) if self.phase == Phase::Committing => {
                self.phase = Phase::Starting; self.send_pending(Start, now)
            }
            (Role::Receiver, Start) if self.receiver_committed => {
                self.phase = Phase::Active;
                vec![Effect::Send(self.selected_wire(StartAck))]
            }
            (Role::Sender, StartAck) if self.phase == Phase::Starting => {
                self.phase = Phase::Active; self.pending = None; self.close_confirmed = true;
                vec![Effect::Send(self.selected_wire(StartConfirmed)), Effect::FirstFramePermit(e.fence), Effect::CloseUnselected(path.other())]
            }
            (Role::Sender, StartAck) if self.phase == Phase::Active => {
                vec![Effect::Send(self.selected_wire(StartConfirmed))]
            }
            (Role::Receiver, StartConfirmed) if self.phase == Phase::Active && !self.close_confirmed => {
                self.close_confirmed = true; vec![Effect::CloseUnselected(path.other())]
            }
            (Role::Sender, Ack { bytes }) if self.phase == Phase::Active => {
                self.bytes = self.bytes.max(bytes); vec![]
            }
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const E: Epoch = Epoch { attempt: 1, generation: 1 };
    const CUTOFF: u64 = DIRECT_CUTOFF_MS;
    const GRACE_END: u64 = DIRECT_CUTOFF_MS + PEER_READY_GRACE_MS;
    const DP: ReadyProof = ReadyProof { dc_open: true, pc_connected: true, selected: true, nominated: true, succeeded: true, known: true, relay: false };
    const TP: ReadyProof = ReadyProof { relay: true, ..DP };
    #[derive(Clone, Debug)]
    struct Pair { s: RouteHandler, r: RouteHandler, pool: Vec<(bool, Envelope)>, permits: usize, turns: usize }
    impl Pair {
        fn new() -> Self {
            let mut p = Self { s: RouteHandler::new(Role::Sender, E), r: RouteHandler::new(Role::Receiver, E), pool: vec![], permits: 0, turns: 0 };
            p.s.handle(Event::GatheringStarted { epoch: E, now: 0 }, 0);
            p.r.handle(Event::GatheringStarted { epoch: E, now: 0 }, 0); p
        }
        fn effects(&mut self, sender: bool, fx: Vec<Effect>) {
            for f in fx { match f {
                Effect::Send(e) => { if !self.pool.contains(&(sender,e)) { self.pool.push((sender,e)); } }
                Effect::FirstFramePermit(_) => { assert!(sender); self.permits += 1; }
                Effect::CreateTurnPeerConnection(_) => self.turns += 1,
                _ => {}
            }}
            self.check();
        }
        fn local(&mut self, sender: bool, event: Event, now: u64) {
            // Production samples a monotonic attempt clock for every queued
            // event. Keep later lifecycle tests monotonic when their times
            // are unrelated to the longer Direct cutoff.
            let now = if sender { now.max(self.s.last_now) } else { now.max(self.r.last_now) };
            let fx = if sender { self.s.handle(event,now) } else { self.r.handle(event,now) };
            self.effects(sender,fx);
        }
        fn deliver(&mut self, i: usize, now: u64) {
            let (from_s,e) = self.pool[i];
            let now = if from_s { now.max(self.r.last_now) } else { now.max(self.s.last_now) };
            let fx = if from_s { self.r.handle(Event::Wire(e),now) } else { self.s.handle(Event::Wire(e),now) };
            self.effects(!from_s,fx);
        }
        fn drain(&mut self, now: u64) {
            let mut i=0;
            while i<self.pool.len() { self.deliver(i,now); i+=1; assert!(i<100); }
        }
        fn ready(&mut self, path: Path, now: u64) {
            let proof=if path==Path::Direct { DP } else { TP };
            self.local(true,Event::OwnReady{epoch:E,path,proof},now);
            self.local(false,Event::OwnReady{epoch:E,path,proof},now);
        }
        fn check(&self) {
            assert!(self.permits<=1,"duplicate app sender permit");
            if let (Some(s),Some(r))=(self.s.selected(),self.r.selected()) { assert_eq!(s,r); }
            // Remote failure notification may still be in flight. It must never
            // activate another path; processing Abort revokes the sender permit.
            if let Some(f)=self.s.fence() { if self.s.permits_frame(f) {
                assert!(self.r.accepts_frame(f) || self.r.phase()==Phase::Failed);
            } }
            for h in [&self.s,&self.r] {
                if h.may_close(Path::Direct)||h.may_close(Path::Turn) { assert_eq!(h.phase(),Phase::Active); }
            }
        }
        fn turn_active() -> Self {
            let mut p=Self::new();p.local(true,Event::Tick,CUTOFF);p.drain(CUTOFF);
            p.ready(Path::Turn,CUTOFF+100);p.drain(CUTOFF+100);
            assert_eq!(p.s.phase(),Phase::Active);assert_eq!(p.r.phase(),Phase::Active);assert_eq!(p.permits,1);p
        }
        fn direct_active() -> Self {
            let mut p=Self::new();p.ready(Path::Direct,CUTOFF-1);p.drain(CUTOFF-1);
            p.local(true,Event::Tick,CUTOFF);p.drain(CUTOFF);
            assert_eq!(p.s.selected(),Some(Path::Direct));assert_eq!(p.s.phase(),Phase::Active);p
        }
    }
    #[test]
    fn completed_turn_ignores_selected_unselected_close_and_peer_abort() {
        for sender in [true,false] {
            let mut p=Pair::turn_active();
            let h=if sender {&mut p.s}else{&mut p.r};
            let fence=h.fence().unwrap();
            assert_eq!(h.handle(Event::ApplicationCompleted {fence},2700),vec![Effect::ApplicationCompleted]);
            for event in [Event::PathFailed {epoch:E,path:Path::Direct},
                Event::PathFailed {epoch:E,path:Path::Turn},Event::Timeout {epoch:E},
                Event::Wire(Envelope {fence:Fence {epoch:E,seq:0,path:Path::Direct},message:Wire::AttemptAbort}),
                Event::Tick,Event::ApplicationCompleted {fence}] {
                assert!(h.handle(event,2800).is_empty(),"completed teardown must not emit Abort/Failed");
                assert_eq!(h.phase(),Phase::Active);
            }
            assert!(!h.permits_frame(fence),"no new sender bytes after completion");
            if !sender {assert_eq!(h.handle(Event::Frame {fence},2900),vec![Effect::AcceptFrame(fence)]);}
        }
    }
    #[test]
    fn turn_failure_before_application_completion_still_aborts() {
        for sender in [true,false] {
            for bytes in [0,14_040] {
                let mut p=Pair::turn_active();let h=if sender {&mut p.s}else{&mut p.r};
                let fence=h.fence().unwrap();
                assert!(h.confirm_bytes(fence,bytes));
                let effects=h.handle(Event::PathFailed {epoch:E,path:Path::Turn},2700);
                assert!(effects.contains(&Effect::Failed));
                assert!(effects.iter().any(|e|matches!(e,Effect::Send(Envelope {message:Wire::AttemptAbort,..}))));
                assert_eq!(h.phase(),Phase::Failed);
                assert!(h.handle(Event::ApplicationCompleted {fence},2800).is_empty());
                assert!(!h.application_completed,"byte accounting is not durable/task completion");
            }
        }
    }
    #[test]
    fn stale_or_early_completion_cannot_hide_selected_failure() {
        let mut h=RouteHandler::new(Role::Sender,E);
        let fence=Fence {epoch:E,seq:1,path:Path::Turn};
        assert_eq!(h.handle(Event::ApplicationCompleted {fence},0),vec![Effect::IgnoreStale]);
        let mut p=Pair::turn_active();
        for bad in [Fence {epoch:Epoch {generation:0,..E},..fence},Fence {path:Path::Direct,..fence},Fence {seq:0,..fence}] {
            assert_eq!(p.s.handle(Event::ApplicationCompleted {fence:bad},2700),vec![Effect::IgnoreStale]);
        }
        assert!(p.s.handle(Event::PathFailed {epoch:E,path:Path::Turn},2800).contains(&Effect::Failed));
    }
    #[test]
    fn completion_preserves_duplicate_final_frame_and_ack_fences() {
        let mut p=Pair::turn_active();let f=p.r.fence().unwrap();
        assert_eq!(p.r.handle(Event::ApplicationCompleted {fence:f},2700),vec![Effect::ApplicationCompleted]);
        for _ in 0..3 {
            assert_eq!(p.r.handle(Event::Frame {fence:f},2800),vec![Effect::AcceptFrame(f)]);
            assert!(p.r.handle(Event::Wire(Envelope {fence:f,message:Wire::Start}),2800).is_empty());
        }
        assert!(p.r.handle(Event::Frame {fence:Fence {path:Path::Direct,..f}},2800).is_empty());
        assert_eq!(p.r.fence(),Some(f));
    }
    #[test] fn direct_needs_peer_selection_not_candidate_open() {
        let mut p=Pair::new();p.ready(Path::Direct,100);assert_eq!(p.permits,0);
        p.drain(100);assert_eq!(p.permits,0);p.local(true,Event::Tick,CUTOFF);
        assert_eq!(p.permits,0);p.drain(CUTOFF);assert_eq!(p.permits,1);
    }
    #[test] fn backup_is_independent_and_candidate_ready_is_not_active() {
        let mut p=Pair::new();p.ready(Path::Turn,100);assert_eq!(p.turns,0);assert!(p.pool.is_empty());
        p.local(true,Event::Tick,2_500);assert_eq!(p.turns,0);
        p.local(true,Event::Tick,CUTOFF-1);assert_eq!(p.turns,0);
        p.local(true,Event::Tick,CUTOFF);assert_eq!(p.turns,1);p.drain(CUTOFF);assert_eq!(p.turns,2);
        p.ready(Path::Turn,CUTOFF+100);assert_eq!(p.permits,0);p.drain(CUTOFF+100);assert_eq!(p.permits,1);
    }
    #[test] fn direct_wins_every_backup_boundary_before_selection() {
        for t in [0,1,CUTOFF-2,CUTOFF-1] { let mut p=Pair::new();p.ready(Path::Direct,t);p.drain(t);
            p.local(true,Event::Tick,CUTOFF);p.drain(CUTOFF);assert_eq!(p.s.selected(),Some(Path::Direct));assert_eq!(p.turns,0); }
    }
    #[test] fn peer_ready_within_grace_still_wins_direct() {
        // Own READY is local at 100. The peer READY received before, at, or
        // after the cutoff — while the grace window is open — still counts.
        for t in [CUTOFF-1,CUTOFF,CUTOFF+1,GRACE_END-1] {
            let mut p=Pair::new();p.ready(Path::Direct,100);p.drain(t);
            p.local(true,Event::Tick,t.max(CUTOFF));assert_eq!(p.s.selected(),Some(Path::Direct));
        }
    }
    #[test] fn grace_holds_decision_until_peer_ready_arrives() {
        let mut p=Pair::new();p.ready(Path::Direct,100);
        // Deliver only the sender's Ready; hold the peer's in the pool.
        let held=p.pool.iter().position(|(from_s,e)| !from_s && matches!(e.message, Wire::Ready(_))).unwrap();
        for i in 0..p.pool.len() { if i!=held { p.deliver(i,100); } }
        p.local(true,Event::Tick,CUTOFF);
        assert_eq!(p.s.selected(),None);assert_eq!(p.turns,0);
        p.local(true,Event::Tick,GRACE_END-1);
        assert_eq!(p.s.selected(),None);assert_eq!(p.turns,0);
        // Peer Ready lands inside the grace: next tick decides Direct.
        p.deliver(held,GRACE_END-1);
        p.local(true,Event::Tick,GRACE_END-1);
        assert_eq!(p.s.selected(),Some(Path::Direct));assert_eq!(p.turns,0);
    }
    #[test] fn grace_expires_to_turn_when_peer_ready_never_arrives() {
        let mut p=Pair::new();p.ready(Path::Direct,100);
        let held=p.pool.iter().position(|(from_s,e)| !from_s && matches!(e.message, Wire::Ready(_))).unwrap();
        for i in 0..p.pool.len() { if i!=held { p.deliver(i,100); } }
        p.local(true,Event::Tick,CUTOFF);assert_eq!(p.s.selected(),None);
        p.local(true,Event::Tick,GRACE_END);
        assert_eq!(p.s.selected(),Some(Path::Turn));assert_eq!(p.turns,1);
    }
    #[test] fn no_grace_without_own_direct_ready() {
        // Nothing proven locally: the decision stays at the plain cutoff.
        let mut p=Pair::new();
        p.local(true,Event::Tick,CUTOFF);
        assert_eq!(p.s.selected(),Some(Path::Turn));assert_eq!(p.turns,1);
    }
    #[test] fn acceptance_direct_ready_while_backup_select_is_in_flight() {
        let mut p=Pair::new();p.local(true,Event::Tick,CUTOFF);p.ready(Path::Direct,CUTOFF+1);p.drain(CUTOFF+1);
        assert_eq!(p.s.selected(),Some(Path::Turn));assert_eq!(p.r.selected(),Some(Path::Turn));
        p.ready(Path::Turn,CUTOFF+100);p.drain(CUTOFF+100);assert_eq!(p.permits,1);
    }
    #[test] fn acceptance_direct_ready_while_backup_selected_is_in_flight() {
        let mut p=Pair::new();p.local(true,Event::Tick,CUTOFF);p.drain(CUTOFF);p.ready(Path::Turn,CUTOFF+100);
        // Hold every receiver-to-sender acknowledgement while processing outbound preparation.
        for i in 0..p.pool.len() { if p.pool[i].0 { p.deliver(i,CUTOFF+100); } }
        p.ready(Path::Direct,CUTOFF+101);assert_eq!(p.permits,0);p.drain(CUTOFF+101);
        assert_eq!(p.s.selected(),Some(Path::Turn));assert_eq!(p.permits,1);
    }
    #[test] fn ambiguous_publish_is_replayed_not_rolled_back() {
        let mut p=Pair::new();p.local(true,Event::Tick,CUTOFF);
        let decision=p.pool[0];let fx=p.s.handle(Event::Tick,CUTOFF+200);
        assert!(fx.contains(&Effect::Send(decision.1)));p.effects(true,fx);
        assert_eq!(p.s.selected(),Some(Path::Turn));assert_eq!(p.permits,0);
        p.drain(CUTOFF+200);p.ready(Path::Turn,CUTOFF+300);p.drain(CUTOFF+300);assert_eq!(p.permits,1);
        let pool=p.pool.clone();for _ in 0..3 { for i in 0..pool.len() { p.deliver(i,CUTOFF+300); } }
        assert_eq!(p.permits,1);assert_eq!(p.s.phase(),Phase::Active);assert_eq!(p.r.phase(),Phase::Active);
    }
    #[test] fn all_waiting_states_expire_and_late_messages_cannot_resurrect() {
        let p=Pair::turn_active();
        for phase in [Phase::Idle,Phase::Gathering,Phase::Locked,Phase::Requesting,Phase::Preparing,Phase::Committing,Phase::Starting] {
            for role in [Role::Sender,Role::Receiver] {
                let mut h=RouteHandler::new(role,E);h.phase=phase;h.started_at=Some(0);
                h.handle(Event::Timeout{epoch:E},15000);assert_eq!(h.phase(),Phase::Failed);
                for (_,e) in &p.pool { h.handle(Event::Wire(*e),15000);assert_eq!(h.phase(),Phase::Failed); }
            }
        }
        let mut p=Pair::new();p.local(true,Event::Tick,CUTOFF);p.local(true,Event::Tick,CUTOFF+HANDSHAKE_LIMIT_MS);p.drain(CUTOFF+HANDSHAKE_LIMIT_MS);
        assert_eq!(p.s.phase(),Phase::Failed);assert_eq!(p.r.phase(),Phase::Failed);
    }
    #[test] fn ready_proof_requires_every_field_and_turn_relay() {
        for i in 0..7 { let mut proof=DP;match i {0=>proof.dc_open=false,1=>proof.pc_connected=false,2=>proof.selected=false,3=>proof.nominated=false,4=>proof.succeeded=false,5=>proof.known=false,_=>proof.relay=true}
            let mut p=Pair::new();p.local(true,Event::OwnReady{epoch:E,path:Path::Direct,proof},10);p.drain(10);
            p.local(true,Event::Tick,CUTOFF);assert_eq!(p.s.selected(),Some(Path::Turn)); }
    }
    #[test] fn offer_errors_and_selected_failure_fail_closed() {
        for failure in [OfferFailure::CreateOffer,OfferFailure::SetLocalDescription,OfferFailure::InitialOfferSend] {
            let mut p=Pair::new();p.local(true,Event::OfferFailed{epoch:E,failure},1);p.drain(1);
            assert_eq!(p.s.phase(),Phase::Failed);assert_eq!(p.r.phase(),Phase::Failed);assert_eq!(p.permits,0);
        }
        let mut p=Pair::turn_active();p.local(true,Event::PathFailed{epoch:E,path:Path::Turn},2700);p.drain(2700);
        assert_eq!(p.s.phase(),Phase::Failed);assert_eq!(p.r.phase(),Phase::Failed);
        assert!(!p.s.permits_frame(p.s.fence().unwrap()));
    }
    #[test] fn retry_fences_every_old_event_and_preserves_confirmed_bytes() {
        let mut p=Pair::direct_active();let old=p.s.fence().unwrap();
        p.s.confirm_bytes(old,100);p.s.confirm_bytes(old,20);assert_eq!(p.s.confirmed_bytes(),100);
        p.local(true,Event::Timeout{epoch:E},2600);p.local(false,Event::Timeout{epoch:E},2600);
        let next=Epoch{attempt:2,generation:2};p.local(true,Event::Retry(next),2600);p.local(false,Event::Retry(next),2600);
        for (_,e) in p.pool.clone() { p.s.handle(Event::Wire(e),2600);p.r.handle(Event::Wire(e),2600); }
        for event in [Event::OwnReady{epoch:E,path:Path::Direct,proof:DP},Event::PathFailed{epoch:E,path:Path::Direct},Event::OfferFailed{epoch:E,failure:OfferFailure::CreateOffer},Event::Frame{fence:old},Event::Timeout{epoch:E}] {
            p.s.handle(event,2600);assert_eq!(p.s.phase(),Phase::Idle);
        }
        assert_eq!(p.s.confirmed_bytes(),100);assert!(!p.s.permits_frame(old));assert!(!p.r.accepts_frame(old));
    }
    #[test] fn unselected_path_closes_only_after_start_ack_and_progress_fenced() {
        let mut p=Pair::turn_active();let selected=p.s.fence().unwrap();let mut other=selected;other.path=Path::Direct;
        assert!(!p.s.confirm_bytes(other,999));assert!(!p.r.accepts_frame(other));
        assert!(p.s.may_close(Path::Direct));assert!(!p.s.may_close(Path::Turn));
        assert!(p.s.confirm_bytes(selected,5));
        p.local(false,Event::Frame{fence:other},2700);assert_eq!(p.s.confirmed_bytes(),5);
        let mut pre=Pair::new();pre.local(true,Event::Tick,CUTOFF);pre.drain(CUTOFF);
        assert!(!pre.s.may_close(Path::Direct));assert!(!pre.r.may_close(Path::Direct));
    }
    #[test] fn ack_before_activation_and_wrong_epoch_are_ignored() {
        let mut p=Pair::new();p.local(true,Event::Tick,CUTOFF);let f=p.s.fence().unwrap();
        p.local(true,Event::Wire(Envelope{fence:f,message:Wire::Ack{bytes:100}}),CUTOFF);
        assert_eq!(p.s.confirmed_bytes(),0);assert_eq!(p.permits,0);
        p.drain(CUTOFF);p.ready(Path::Turn,CUTOFF+100);p.drain(CUTOFF+100);
        let mut old=f;old.epoch.generation=0;
        p.local(true,Event::Wire(Envelope{fence:old,message:Wire::Ack{bytes:999}}),2600);
        assert_eq!(p.s.confirmed_bytes(),0);
    }
    #[test] fn candidate_buckets_are_bounded_deduped_and_ufrag_fenced() {
        let mut h=RouteHandler::new(Role::Receiver,E);
        assert!(h.insert_candidate(E,Path::Direct,"a","candidate-1"));
        assert!(!h.insert_candidate(E,Path::Direct,"a","candidate-1"));
        assert!(h.insert_candidate(E,Path::Direct,"b","candidate-old"));
        assert_eq!(h.bind_ufrag(E,Path::Direct,"a"),vec!["candidate-1"]);
        assert!(!h.insert_candidate(E,Path::Direct,"b","candidate-old"));
        assert!(!h.insert_candidate(Epoch{attempt:0,..E},Path::Direct,"a","old"));
        assert!(!h.insert_candidate(E,Path::Turn,"a","early-turn"));
        for i in 0..300 { h.insert_candidate(E,Path::Direct,"a",&format!("c{i}")); }
        assert_eq!(h.candidate_count,MAX_CANDIDATES);
    }
    #[test] fn missing_ready_and_lost_ack_have_bounded_failure_or_same_transaction_retry() {
        let mut p=Pair::new();p.ready(Path::Direct,100); // Drop READY: not observed by arbiter.
        // Own Direct is proven, so the grace holds the decision first; with
        // the peer READY never observed, escalation is still bounded.
        p.local(true,Event::Tick,CUTOFF);assert_eq!(p.s.selected(),None);
        p.local(true,Event::Tick,GRACE_END);assert_eq!(p.s.selected(),Some(Path::Turn));p.drain(GRACE_END);
        p.ready(Path::Turn,GRACE_END+100);p.drain(GRACE_END+100);assert_eq!(p.permits,1);
        // A fresh transaction whose decision is lost forever cannot gain permission.
        let mut q=Pair::new();q.local(true,Event::Tick,CUTOFF);
        for t in ((CUTOFF+200)..(CUTOFF+HANDSHAKE_LIMIT_MS)).step_by(200) { q.local(true,Event::Tick,t);assert_eq!(q.permits,0); }
        q.local(true,Event::Tick,CUTOFF+HANDSHAKE_LIMIT_MS);assert_eq!(q.s.phase(),Phase::Failed);
    }
    #[test] fn receiver_cannot_choose_route_or_gain_sender_permit() {
        let mut p=Pair::new();p.local(false,Event::Tick,CUTOFF);assert_eq!(p.r.selected(),None);assert_eq!(p.turns,0);
        let p=Pair::turn_active();assert!(!p.r.permits_frame(p.r.fence().unwrap()));
    }
    #[test] fn same_epoch_wrong_path_or_old_sequence_abort_cannot_kill_selected_route() {
        let mut p=Pair::turn_active();let selected=p.s.fence().unwrap();
        for fence in [Fence{path:Path::Direct,..selected},Fence{seq:0,..selected},Fence{seq:2,..selected}] {
            p.local(true,Event::Wire(Envelope{fence,message:Wire::Abort}),2700);
            p.local(false,Event::Wire(Envelope{fence,message:Wire::Abort}),2700);
            assert_eq!(p.s.phase(),Phase::Active);assert_eq!(p.r.phase(),Phase::Active);
        }
        p.local(true,Event::Wire(Envelope{fence:selected,message:Wire::Abort}),2700);
        assert_eq!(p.s.phase(),Phase::Failed);
    }
    #[test] fn attempt_abort_converges_even_if_turn_decision_was_lost() {
        let mut p=Pair::new();
        p.local(true,Event::Tick,CUTOFF); // Decision deliberately not delivered.
        p.local(true,Event::Timeout{epoch:E},CUTOFF+100);
        let abort=p.pool.iter().position(|(sender,e)| *sender && e.message==Wire::AttemptAbort).unwrap();
        p.deliver(abort,CUTOFF+100);
        assert_eq!(p.s.phase(),Phase::Failed);
        assert_eq!(p.r.phase(),Phase::Failed);
        assert_eq!(p.permits,0);
        // A delayed decision may not resurrect either side.
        p.drain(CUTOFF+100);
        assert_eq!(p.r.phase(),Phase::Failed);
    }
    #[test] fn attempt_abort_is_canonical_and_old_epoch_cannot_kill_retry() {
        let mut p=Pair::turn_active();let selected=p.s.fence().unwrap();
        p.local(true,Event::Wire(Envelope{fence:selected,message:Wire::AttemptAbort}),2700);
        assert_eq!(p.s.phase(),Phase::Active); // attempt abort never carries a path decision fence.
        let abort=Envelope{fence:Fence{epoch:E,seq:0,path:Path::Direct},message:Wire::AttemptAbort};
        p.local(true,Event::Wire(abort),2700);assert_eq!(p.s.phase(),Phase::Failed);
        p.local(true,Event::Retry(Epoch{attempt:2,generation:2}),2700);
        p.local(true,Event::Wire(abort),2700);assert_eq!(p.s.phase(),Phase::Idle);
    }
    #[test] fn initial_offer_and_abort_loss_must_have_bounded_remote_failure() {
        // Mirrors actual signaling: manifest was ACKed, sender createOffer or
        // initial publication fails, and Abort publication is also lost. No
        // successful remote setLocalDescription ever established a clock.
        let mut sender=RouteHandler::new(Role::Sender,E);
        let mut receiver=RouteHandler::new(Role::Receiver,E);
        // Explicit trusted expectation is the production lifecycle trigger;
        // mere construction/unauthenticated packets must not arm a timer.
        receiver.handle(Event::SetupExpected{epoch:E},0);
        sender.handle(Event::OfferFailed{epoch:E,failure:OfferFailure::InitialOfferSend},0);
        assert_eq!(sender.phase(),Phase::Failed);
        receiver.handle(Event::Tick,SETUP_LIMIT_MS);
        assert_eq!(receiver.phase(),Phase::Failed,
            "a peer awaiting the initial offer needs an attempt-level setup watchdog independent of the Direct cutoff clock");
    }
    #[test] fn setup_expectation_is_idempotent_and_late_offer_cannot_resurrect() {
        let mut h=RouteHandler::new(Role::Receiver,E);
        h.handle(Event::SetupExpected{epoch:E},100);
        h.handle(Event::SetupExpected{epoch:E},SETUP_LIMIT_MS-1);
        let effects=h.handle(Event::GatheringStarted{epoch:E,now:SETUP_LIMIT_MS+100},SETUP_LIMIT_MS+100);
        assert_eq!(h.phase(),Phase::Failed);
        assert!(effects.contains(&Effect::CloseAll(E)));
        assert!(effects.contains(&Effect::Failed));
        for event in [Event::SetupExpected{epoch:E},Event::GatheringStarted{epoch:E,now:SETUP_LIMIT_MS+101},Event::Tick] {
            h.handle(event,SETUP_LIMIT_MS+101);assert_eq!(h.phase(),Phase::Failed);
        }
        assert_eq!(h.selected(),None);
    }
    #[test] fn setup_timeout_never_selects_turn_or_starts_gathering_clock() {
        for role in [Role::Sender,Role::Receiver] {
            let mut h=RouteHandler::new(role,E);
            h.handle(Event::SetupExpected{epoch:E},0);
            assert!(h.handle(Event::Tick,2500).is_empty());
            assert_eq!(h.phase(),Phase::Idle);assert_eq!(h.selected(),None);
            assert_eq!(h.started_at,None);
            h.handle(Event::GatheringStarted{epoch:E,now:SETUP_LIMIT_MS-1},SETUP_LIMIT_MS-1);
            h.handle(Event::Tick,SETUP_LIMIT_MS);
            assert_eq!(h.phase(),Phase::Gathering);assert_eq!(h.selected(),None);
            h.handle(Event::Tick,SETUP_LIMIT_MS-1+CUTOFF);
            assert_eq!(h.selected(),if role==Role::Sender {Some(Path::Turn)}else{None});
        }
    }
    #[test] fn retry_requires_fresh_setup_and_fences_lost_offer_attempt() {
        let mut h=RouteHandler::new(Role::Receiver,E);
        h.handle(Event::SetupExpected{epoch:E},0);h.handle(Event::Tick,SETUP_LIMIT_MS);
        assert_eq!(h.phase(),Phase::Failed);
        let next=Epoch{attempt:2,generation:2};
        h.handle(Event::Retry(next),SETUP_LIMIT_MS);
        h.handle(Event::SetupExpected{epoch:next},SETUP_LIMIT_MS+1);
        for event in [Event::SetupExpected{epoch:E},Event::Timeout{epoch:E},Event::GatheringStarted{epoch:E,now:SETUP_LIMIT_MS+2}] {
            h.handle(event,SETUP_LIMIT_MS+2);assert_eq!(h.phase(),Phase::Idle);
        }
        h.handle(Event::GatheringStarted{epoch:next,now:SETUP_LIMIT_MS+3},SETUP_LIMIT_MS+3);
        assert_eq!(h.phase(),Phase::Gathering);
        h.handle(Event::Tick,30_001);assert_eq!(h.phase(),Phase::Gathering);
    }
    #[test] fn unconfirmed_receiver_start_has_bounded_failure_instead_of_early_close() {
        let mut p=Pair::turn_active();
        // Simulate loss of the final confirmation and the absence of all file frames.
        p.r.close_confirmed=false;
        let deadline=CUTOFF+100+15_000;
        p.local(false,Event::Tick,deadline);assert_eq!(p.r.phase(),Phase::Failed);
        p.drain(deadline);assert_eq!(p.s.phase(),Phase::Failed);
    }
    #[test] fn bounded_exhaustive_handler_safety_under_arbitrary_duplicates_and_reordering() {
        // Replayable pool is not consumed: models arbitrary duplicate delivery and reorder.
        // Explore reachable intermediate states from every prefix of a real dual-peer trace.
        // Depth is finite; this is evidence, not a proof for arbitrary executions.
        let mut base=Pair::new();base.local(true,Event::Tick,CUTOFF);base.drain(CUTOFF);
        base.ready(Path::Turn,CUTOFF+100);
        let mut direct=Pair::new();direct.ready(Path::Direct,CUTOFF-1);direct.drain(CUTOFF-1);
        direct.local(true,Event::Tick,CUTOFF);
        let mut frontier=vec![base,direct];let mut seen=HashSet::new();let mut visited=0usize;
        let mut reached_active=false;
        for _depth in 0..12 {
            let mut next=vec![];
            for p in frontier {
                p.check();visited+=1;
                reached_active |= p.s.phase()==Phase::Active;
                for i in 0..p.pool.len() {
                    let mut q=p.clone();q.deliver(i,CUTOFF+100);
                    let key=format!("{:?}|{:?}|{:?}|{}",q.s,q.r,q.pool,q.permits);
                    if seen.insert(key) { next.push(q); }
                }
                for sender in [true,false] {
                    let mut q=p.clone();q.local(sender,Event::Timeout{epoch:E},2600);
                    q.check();
                    let key=format!("{:?}|{:?}|{:?}|{}",q.s,q.r,q.pool,q.permits);
                    if seen.insert(key) { next.push(q); }
                    for path in [Path::Direct,Path::Turn] {
                        let mut q=p.clone();q.local(sender,Event::PathFailed{epoch:E,path},2600);
                        let key=format!("{:?}|{:?}|{:?}|{}",q.s,q.r,q.pool,q.permits);
                        if seen.insert(key) { next.push(q); }
                    }
                }
            }
            frontier=next;
        }
        for p in &frontier { p.check(); }
        eprintln!("explored {visited} dual-peer effect/message states, frontier {}",frontier.len());
        assert!(visited>100);
        assert!(reached_active,"exploration must reach a genuine bilateral activation");
    }
}
