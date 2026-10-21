//! Native single-file receiver setup watchdog lifecycle, independent of UI and networking.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const INITIAL_EFFECT_TIMEOUT: Duration = Duration::from_secs(2);

/// A failure bound, never a gathering/route-selection clock. Publication errors
/// and ambiguous timed-out sends are terminal locally, not a relay trigger.
pub async fn bounded_initial_effect<T>(
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::time::timeout(INITIAL_EFFECT_TIMEOUT, future)
        .await
        .map_err(|_| "原生连接建立副作用超时，请重试".to_string())?
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupState {
    Dormant,
    WaitingForOffer,
    AcceptingOffer,
    OfferReady,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfferAdmission {
    Begin(Instant),
    Duplicate,
}

#[derive(Clone)]
pub struct SetupLifecycle(Arc<Mutex<Inner>>);
struct Inner {
    state: SetupState,
    deadline: Option<Instant>,
    timeout_claimed: bool,
}

impl SetupLifecycle {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner {
            state: SetupState::Dormant,
            deadline: None,
            timeout_claimed: false,
        })))
    }
    /// Start only after native task validation and manifest validation. This is the sole clock origin.
    pub fn start(&self, now: Instant) -> bool {
        let mut inner = self.0.lock().expect("setup lifecycle poisoned");
        if inner.state != SetupState::Dormant {
            return false;
        }
        inner.state = SetupState::WaitingForOffer;
        inner.deadline = Some(now + SETUP_TIMEOUT);
        true
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.0.lock().expect("setup lifecycle poisoned").deadline
    }
    /// Production watchdog task and tests use the same autonomous wait path.
    /// A successful description can disarm the deadline while this sleeps;
    /// only an unclaimed terminal timeout authorizes cleanup on wake.
    pub async fn wait_for_timeout(&self) -> bool {
        let Some(deadline) = self.deadline() else {
            return false;
        };
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        self.timeout(Instant::now())
    }
    /// Exactly one offer can claim this session. Duplicates are ignored, not
    /// interpreted as a new connection or an error that cancels the valid PC.
    pub fn admit_offer(&self, now: Instant) -> Result<OfferAdmission, &'static str> {
        let mut inner = self.0.lock().expect("setup lifecycle poisoned");
        if inner.state == SetupState::TimedOut {
            return Err("setup deadline expired");
        }
        if inner.state == SetupState::OfferReady {
            return Ok(OfferAdmission::Duplicate);
        }
        let deadline = inner.deadline.ok_or("setup lifecycle not started")?;
        if now >= deadline {
            inner.state = SetupState::TimedOut;
            return Err("setup deadline expired");
        }
        if inner.state == SetupState::AcceptingOffer {
            return Ok(OfferAdmission::Duplicate);
        }
        inner.state = SetupState::AcceptingOffer;
        Ok(OfferAdmission::Begin(deadline))
    }
    #[cfg(test)]
    fn begin_offer(&self, now: Instant) -> Result<Instant, &'static str> {
        match self.admit_offer(now)? {
            OfferAdmission::Begin(deadline) => Ok(deadline),
            OfferAdmission::Duplicate => Err("offer already claimed"),
        }
    }
    /// Call only after setLocalDescription has successfully completed.
    pub fn offer_ready(&self, now: Instant) -> bool {
        let mut inner = self.0.lock().expect("setup lifecycle poisoned");
        if inner.state != SetupState::AcceptingOffer || inner.deadline.is_none_or(|d| now >= d) {
            if inner.state == SetupState::AcceptingOffer {
                inner.state = SetupState::TimedOut;
            }
            return false;
        }
        inner.state = SetupState::OfferReady;
        inner.deadline = None;
        true
    }
    pub fn timeout(&self, now: Instant) -> bool {
        let mut inner = self.0.lock().expect("setup lifecycle poisoned");
        if inner.deadline.is_some_and(|d| now >= d)
            && matches!(
                inner.state,
                SetupState::WaitingForOffer | SetupState::AcceptingOffer
            )
        {
            inner.state = SetupState::TimedOut;
        }
        // ICE/late-offer checks may have fenced first. They must not steal
        // the one notification that drives local terminal UI and cleanup.
        if inner.state == SetupState::TimedOut && !inner.timeout_claimed {
            inner.timeout_claimed = true;
            return true;
        }
        false
    }
    pub fn active(&self, now: Instant) -> bool {
        let mut inner = self.0.lock().expect("setup lifecycle poisoned");
        if inner.deadline.is_some_and(|d| now >= d)
            && matches!(
                inner.state,
                SetupState::WaitingForOffer | SetupState::AcceptingOffer
            )
        {
            inner.state = SetupState::TimedOut;
        }
        matches!(
            inner.state,
            SetupState::WaitingForOffer | SetupState::AcceptingOffer | SetupState::OfferReady
        )
    }
    pub fn state(&self) -> SetupState {
        self.0.lock().expect("setup lifecycle poisoned").state
    }
}
impl Default for SetupLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn t(s: u64) -> Instant {
        Instant::now() + Duration::from_secs(s)
    }
    #[test]
    fn lost_offer_and_terminal_are_bounded_without_messages() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        assert!(setup.start(now));
        assert!(setup.timeout(now + SETUP_TIMEOUT));
        assert_eq!(setup.state(), SetupState::TimedOut);
        assert!(setup
            .begin_offer(now + SETUP_TIMEOUT + Duration::from_millis(1))
            .is_err());
    }
    #[test]
    fn late_offer_and_duplicate_offer_are_fenced() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        setup.start(now);
        assert!(setup.begin_offer(now + SETUP_TIMEOUT).is_err());
        assert_eq!(setup.state(), SetupState::TimedOut);
        let fresh = SetupLifecycle::new();
        fresh.start(now);
        fresh.begin_offer(now).unwrap();
        assert!(fresh.begin_offer(now).is_err());
    }
    #[test]
    fn initial_failure_deadline_edges_and_retry_instance_fencing() {
        let first = SetupLifecycle::new();
        let now = Instant::now();
        first.start(now);
        assert_eq!(first.state(), SetupState::WaitingForOffer);
        assert!(first.begin_offer(now).is_ok());
        assert!(!first.offer_ready(now + SETUP_TIMEOUT));
        assert_eq!(first.state(), SetupState::TimedOut);
        let retry = SetupLifecycle::new();
        retry.start(t(0));
        assert!(retry.active(Instant::now()));
        assert_eq!(retry.state(), SetupState::WaitingForOffer);
        assert!(!first.active(Instant::now()));
    }
    #[test]
    fn successful_local_description_stops_watchdog_and_duplicates_stay_blocked() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        setup.start(now);
        setup.begin_offer(now).unwrap();
        assert!(setup.offer_ready(now + Duration::from_secs(1)));
        assert_eq!(setup.state(), SetupState::OfferReady);
        assert!(setup.deadline().is_none());
        assert!(setup.begin_offer(now).is_err());
    }
    #[test]
    fn deadline_observed_by_ice_must_not_consume_terminal_cleanup_notification() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        setup.start(now);
        assert!(!setup.active(now + SETUP_TIMEOUT));
        assert_eq!(setup.state(), SetupState::TimedOut);
        assert!(
            setup.timeout(now + SETUP_TIMEOUT),
            "ICE deadline observation must not strand the UI by stealing the watchdog notification"
        );
        assert!(
            !setup.timeout(now + SETUP_TIMEOUT),
            "only one task owns terminal cleanup"
        );
    }
    #[test]
    fn deadline_observed_by_late_offer_must_still_notify_watchdog() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        setup.start(now);
        assert!(setup.begin_offer(now + SETUP_TIMEOUT).is_err());
        assert!(
            setup.timeout(now + SETUP_TIMEOUT),
            "late offer must not suppress autonomous terminal cleanup"
        );
        assert!(!setup.timeout(now + SETUP_TIMEOUT));
    }
    #[test]
    fn production_offer_admission_ignores_duplicates_without_new_allocation() {
        let setup = SetupLifecycle::new();
        let now = Instant::now();
        assert!(setup.admit_offer(now).is_err());
        setup.start(now);
        assert_eq!(
            setup.admit_offer(now),
            Ok(OfferAdmission::Begin(now + SETUP_TIMEOUT))
        );
        assert_eq!(setup.admit_offer(now), Ok(OfferAdmission::Duplicate));
        assert!(setup.offer_ready(now));
        assert_eq!(
            setup.admit_offer(now + SETUP_TIMEOUT),
            Ok(OfferAdmission::Duplicate)
        );
        assert_eq!(setup.state(), SetupState::OfferReady);
    }
    #[tokio::test]
    async fn production_wait_expires_without_offer_abort_or_tick() {
        let setup = SetupLifecycle::new();
        setup.start(Instant::now() - SETUP_TIMEOUT + Duration::from_millis(40));
        assert!(
            tokio::time::timeout(Duration::from_secs(2), setup.wait_for_timeout())
                .await
                .unwrap()
        );
        assert_eq!(setup.state(), SetupState::TimedOut);
        assert!(!setup.wait_for_timeout().await);
        assert!(setup.admit_offer(Instant::now()).is_err());
    }
    #[tokio::test]
    async fn production_wait_honors_disarm_and_old_session_cannot_fail_retry() {
        let old = SetupLifecycle::new();
        old.start(Instant::now() - SETUP_TIMEOUT);
        let retry = SetupLifecycle::new();
        let origin = Instant::now();
        retry.start(origin);
        retry.begin_offer(origin).unwrap();
        assert!(retry.offer_ready(origin));
        assert!(old.wait_for_timeout().await);
        assert!(!retry.wait_for_timeout().await);
        assert_eq!(retry.state(), SetupState::OfferReady);
        assert!(retry.active(Instant::now()));
    }
    #[tokio::test]
    async fn explicit_initial_publication_failure_is_not_success_or_fallback() {
        let result = bounded_initial_effect(async {
            Err::<(), _>("injected initial send failure".to_string())
        })
        .await;
        assert_eq!(result.unwrap_err(), "injected initial send failure");
    }
    #[tokio::test]
    async fn hung_initial_publication_has_a_bounded_error() {
        let result = tokio::time::timeout(
            Duration::from_secs(4),
            bounded_initial_effect(std::future::pending::<Result<(), String>>()),
        )
        .await;
        assert!(result.unwrap().unwrap_err().contains("超时"));
    }
}
