use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub(crate) struct RefreshTicket(Arc<TicketState>);
#[derive(Debug)]
struct TicketState {
    round_id: u64,
    started: Instant,
    terminal: AtomicBool,
    counts: Mutex<crate::discovery_diagnostics::PhaseCounts>,
}
impl RefreshTicket {
    pub(crate) fn new(round_id: u64) -> Self {
        Self(Arc::new(TicketState {
            round_id,
            started: Instant::now(),
            terminal: AtomicBool::new(false),
            counts: Mutex::new(Default::default()),
        }))
    }
    pub(crate) fn round_id(&self) -> u64 {
        self.0.round_id
    }
    pub(crate) fn started(&self) -> Instant {
        self.0.started
    }
    pub(crate) fn finish_once(&self) -> bool {
        self.0
            .terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub(crate) fn is_terminal(&self) -> bool {
        self.0.terminal.load(Ordering::Acquire)
    }
    pub(crate) fn update_counts(&self, counts: crate::discovery_diagnostics::PhaseCounts) {
        let mut snapshot = self.0.counts.lock().unwrap_or_else(|e| e.into_inner());
        if !self.is_terminal() {
            *snapshot = counts;
        }
    }
    pub(crate) fn counts(&self) -> crate::discovery_diagnostics::PhaseCounts {
        *self.0.counts.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod ticket_tests {
    use super::*;
    #[test]
    fn exactly_one_terminal_winner_and_late_worker_suppressed() {
        for _ in 0..100 {
            let ticket = RefreshTicket::new(1);
            let a = ticket.clone();
            let b = ticket.clone();
            let a = std::thread::spawn(move || a.finish_once());
            let b = std::thread::spawn(move || b.finish_once());
            assert_ne!(a.join().unwrap(), b.join().unwrap());
        }
        let ticket = RefreshTicket::new(2);
        assert!(ticket.finish_once());
        assert!(!ticket.finish_once());
    }
    #[test]
    fn timeout_keeps_observed_snapshot_and_rejects_late_updates() {
        let ticket = RefreshTicket::new(3);
        let counts = crate::discovery_diagnostics::PhaseCounts {
            resolved_services: 2,
            valid_services: 1,
            lan_eligible_peers: 0,
            ui_emitted_peers: 0,
        };
        ticket.update_counts(counts);
        assert!(ticket.finish_once());
        ticket.update_counts(Default::default());
        assert_eq!(ticket.counts(), counts);
        assert!(!ticket.finish_once());
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RoundCounts {
    pub valid_services: u32,
    pub lan_eligible_peers: u32,
    pub ui_emitted_peers: u32,
}

/// State for one explicitly timed mDNS observation window. Resolved service
/// count is intentionally not part of the public response; diagnostics records
/// every resolve event, while validServices counts unique service fullnames.
#[derive(Debug)]
pub(crate) struct DiscoveryRound {
    started: Instant,
    confirmed_at: Option<Instant>,
    resolved_services: u32,
    valid_fullnames: HashSet<String>,
    eligible_ids: HashSet<String>,
    emitted_ids: HashSet<String>,
}
impl DiscoveryRound {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            started: now,
            confirmed_at: None,
            resolved_services: 0,
            valid_fullnames: HashSet::new(),
            eligible_ids: HashSet::new(),
            emitted_ids: HashSet::new(),
        }
    }
    pub(crate) fn confirm(&mut self, now: Instant) {
        self.confirmed_at.get_or_insert(now);
    }
    pub(crate) fn resolved(&mut self) {
        self.resolved_services = self.resolved_services.saturating_add(1);
    }
    pub(crate) fn resolved_services(&self) -> u32 {
        self.resolved_services
    }
    pub(crate) fn valid_service(&mut self, fullname: &str) {
        self.valid_fullnames.insert(fullname.to_owned());
    }
    pub(crate) fn eligible_peer(&mut self, id: &str) {
        self.eligible_ids.insert(id.to_owned());
    }
    pub(crate) fn emitted_peer(&mut self, id: &str, emit_succeeded: bool) {
        if emit_succeeded {
            self.emitted_ids.insert(id.to_owned());
        }
    }
    pub(crate) fn counts(&self) -> RoundCounts {
        RoundCounts {
            valid_services: bounded(self.valid_fullnames.len()),
            lan_eligible_peers: bounded(self.eligible_ids.len()),
            ui_emitted_peers: bounded(self.emitted_ids.len()),
        }
    }
    pub(crate) fn confirmed(&self) -> bool {
        self.confirmed_at.is_some()
    }
    pub(crate) fn complete(&self, now: Instant) -> bool {
        self.confirmed_at
            .is_some_and(|t| now.duration_since(t) >= Duration::from_secs(3))
    }
    pub(crate) fn confirmation_expired(&self, now: Instant) -> bool {
        self.confirmed_at.is_none() && now.duration_since(self.started) >= Duration::from_secs(5)
    }
}
fn bounded(n: usize) -> u32 {
    n.min(u32::MAX as usize) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    fn at(seconds: u64) -> Instant {
        Instant::now() + Duration::from_secs(seconds)
    }
    #[test]
    fn waits_for_three_seconds_after_confirmation_and_zero_result_is_valid() {
        let mut r = DiscoveryRound::new(at(0));
        assert!(!r.complete(at(8)));
        r.confirm(at(8));
        assert!(!r.complete(at(10)));
        assert!(r.complete(at(11)));
        assert_eq!(r.counts(), RoundCounts::default());
    }
    #[test]
    fn unique_service_and_peer_counts_and_only_successful_emissions() {
        let mut r = DiscoveryRound::new(at(0));
        r.confirm(at(0));
        r.valid_service("a");
        r.valid_service("a");
        r.valid_service("b");
        r.eligible_peer("peer");
        r.eligible_peer("peer");
        r.emitted_peer("peer", false);
        r.emitted_peer("peer", true);
        assert_eq!(
            r.counts(),
            RoundCounts {
                valid_services: 2,
                lan_eligible_peers: 1,
                ui_emitted_peers: 1
            }
        );
    }
    #[test]
    fn confirmation_timeout_at_five_seconds() {
        let r = DiscoveryRound::new(at(0));
        assert!(!r.confirmation_expired(at(4)));
        assert!(r.confirmation_expired(at(5)));
    }
    #[test]
    fn repeated_start_confirmation_does_not_extend_the_round() {
        let now = Instant::now();
        let mut round = DiscoveryRound::new(now);
        round.confirm(now);
        round.confirm(now + Duration::from_secs(2));
        assert!(round.complete(now + Duration::from_secs(3)));
    }
}
