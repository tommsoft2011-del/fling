//! Serialized direct-first transaction executor, shared by production and tests.
//! online_direct supplies the actual native setLocalDescription source Instant,
//! bounded signaling effects, distinct PCs, frame gates and failure notifications.
use crate::online_route_choice::{Effect, Epoch, Event, Phase, RouteHandler};
use std::{future::Future, pin::Pin, time::{Duration, Instant}};
use tokio::sync::{mpsc, oneshot};

pub type EffectFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
const EFFECT_LIMIT: Duration = Duration::from_secs(2);

/// Effects are serialized. An error (including ambiguous publication) fails the
/// whole attempt; later original effects MUST NOT run. Cleanup is best-effort,
/// bounded, and continues even if sending Abort fails. Local cleanup is not a
/// proof that the remote peer received Abort.
pub trait EffectExecutor: Send + 'static {
    fn execute<'a>(&'a mut self, effect: Effect) -> EffectFuture<'a>;
    fn effect_failed(&self, _effect: &Effect, _timed_out: bool) {}
}

#[derive(Clone)]
pub struct RouteRuntime {
    tx: mpsc::Sender<Command>,
}
type Reply = oneshot::Sender<Result<Vec<Effect>, String>>;
enum Command {
    Event(Event, Reply),
    SetupExpected { epoch: Epoch, origin: Instant, reply: Reply },
    GatheringStarted { epoch: Epoch, origin: Instant, reply: Reply },
    Snapshot(oneshot::Sender<(Phase, Option<crate::online_route_choice::Path>, u64)>),
}

async fn execute_bounded(executor: &mut impl EffectExecutor, effect: Effect) -> Result<(), String> {
    let result=tokio::time::timeout(EFFECT_LIMIT, executor.execute(effect.clone())).await;
    match result {
        Ok(result)=>{if result.is_err() {executor.effect_failed(&effect,false);} result}
        Err(_)=>{executor.effect_failed(&effect,true);Err("route effect timed out".into())}
    }
}

impl RouteRuntime {
    pub fn spawn(handler: RouteHandler, executor: impl EffectExecutor, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel(capacity.max(1));
        // Capture before task scheduling, not when the spawned task first runs.
        let initial_clock = Instant::now();
        tokio::spawn(async move {
            let mut clock = initial_clock;
            let mut handler = handler;
            let mut executor = executor;
            // One serialized executor pulse drives failures and coordinator
            // cutoff. This is not an independently deciding peer/path timer.
            let mut pulse = tokio::time::interval(Duration::from_millis(20));
            pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let command = tokio::select! {
                    biased;
                    command = rx.recv() => match command { Some(c) => c, None => break },
                    _ = pulse.tick() => {
                        let (reply, _ignored) = oneshot::channel();
                        Command::Event(Event::Tick, reply)
                    }
                };
                let (event, reply) = match command {
                    Command::Snapshot(reply) => {
                        let _ = reply.send((handler.phase(), handler.selected(), handler.confirmed_bytes()));
                        continue;
                    }
                    Command::SetupExpected { epoch, origin, reply } => {
                        if origin > Instant::now() {
                            (Event::Timeout { epoch }, reply)
                        } else if handler.bind_setup_clock(epoch) {
                            clock = origin;
                            // Source time is retained even if this command was
                            // queued for longer than the entire setup budget.
                            let mut effects = handler.handle(Event::SetupExpected { epoch }, 0);
                            effects.extend(handler.handle(Event::Tick, elapsed_ms(clock)));
                            for effect in effects.iter().cloned() {
                                let _ = execute_bounded(&mut executor, effect).await;
                            }
                            let _ = reply.send(Ok(effects));
                            continue;
                        } else {
                            // Duplicate expectation must not renew setup time.
                            (Event::Tick, reply)
                        }
                    }
                    Command::GatheringStarted { epoch, origin, reply } => {
                        if handler.epoch() != epoch || handler.phase() != Phase::Idle {
                            // A duplicate/stale start must never rebase the clock.
                            let _ = reply.send(Ok(vec![Effect::IgnoreStale]));
                            continue;
                        }
                        // Evaluate the independent setup deadline on its OLD
                        // clock before switching to the immutable gathering
                        // origin. Late success may not resurrect the attempt.
                        let expired = handler.handle(Event::Tick, elapsed_ms(clock));
                        if handler.phase() == Phase::Failed {
                            for effect in expired.iter().cloned() {
                                let _ = execute_bounded(&mut executor, effect).await;
                            }
                            let _ = reply.send(Ok(expired));
                            continue;
                        }
                        if origin > Instant::now() {
                            (Event::Timeout { epoch }, reply)
                        } else {
                            // Preserve the actual source Instant, including queue
                            // delay. Relative milliseconds now have this exact
                            // origin, avoiding offset-rounding at the cutoff.
                            clock = origin;
                            assert!(handler.bind_source_clock(epoch));
                            (Event::GatheringStarted { epoch, now: 0 }, reply)
                        }
                    }
                    Command::Event(Event::GatheringStarted { .. }, reply) => {
                        // A numeric event cannot identify the successful native
                        // side effect's clock origin. Do not guess/reset it.
                        let _ = reply.send(Err("gathering requires source Instant".into()));
                        continue;
                    }
                    Command::Event(Event::SetupExpected { .. }, reply) => {
                        let _ = reply.send(Err("setup expectation requires source Instant".into()));
                        continue;
                    }
                    Command::Event(event, reply) => (event, reply),
                };
                // A busy command queue may not starve the cutoff/watchdog pulse.
                // Observation is sampled once, before any resulting network wait.
                let now = elapsed_ms(clock);
                let mut effects = handler.handle(Event::Tick, now);
                effects.extend(handler.handle(event, now));
                let mut error = None;
                for effect in effects.iter().cloned() {
                    if let Err(e) = execute_bounded(&mut executor, effect).await {
                        error.get_or_insert(e);
                        // Terminal-event effects already include CloseAll/Failed.
                        // Abort publication failure must not skip the rest of that
                        // list: re-entering an absorbing Failed handler returns []!
                        if handler.phase() != Phase::Failed { break; }
                    }
                }
                if let Some(error) = error {
                    // Whole-attempt failure, not failure of an unselected path.
                    // Transition first, then attempt ALL cleanup effects even if
                    // Abort publication fails. Never grant a later frame permit.
                    let cleanup = handler.handle(Event::Timeout { epoch: handler.epoch() }, elapsed_ms(clock));
                    for effect in cleanup.iter().cloned() {
                        let _ = execute_bounded(&mut executor, effect).await;
                    }
                    effects.extend(cleanup);
                    let _ = reply.send(Err(error));
                } else {
                    let _ = reply.send(Ok(effects));
                }
            }
        });
        Self { tx }
    }

    /// Called only by trusted Rust session acceptance, before manifest ACK.
    /// The source Instant is not reconstructed on queue delivery.
    pub async fn dispatch_setup_expected(&self, epoch: Epoch, origin: Instant) -> Result<Vec<Effect>, String> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Command::SetupExpected { epoch, origin, reply }).await
            .map_err(|_| "route runtime stopped".to_string())?;
        rx.await.map_err(|_| "route runtime stopped".to_string())?
    }

    /// Call with the Instant sampled immediately after successful native
    /// setLocalDescription, distinct from trusted setup expectation.
    pub async fn dispatch_gathering_started(&self, epoch: Epoch, origin: Instant) -> Result<Vec<Effect>, String> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Command::GatheringStarted { epoch, origin, reply }).await
            .map_err(|_| "route runtime stopped".to_string())?;
        rx.await.map_err(|_| "route runtime stopped".to_string())?
    }

    pub async fn dispatch(&self, event: Event) -> Result<Vec<Effect>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Event(event, tx)).await.map_err(|_| "route runtime stopped".to_string())?;
        rx.await.map_err(|_| "route runtime stopped".to_string())?
    }

    pub async fn snapshot(&self) -> Result<(Phase, Option<crate::online_route_choice::Path>, u64), String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::Snapshot(tx)).await.map_err(|_| "route runtime stopped".to_string())?;
        rx.await.map_err(|_| "route runtime stopped".to_string())
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u64::MAX as u128) as u64
}

pub struct EffectFn<F>(pub F);
impl<F> EffectExecutor for EffectFn<F>
where F: FnMut(Effect) -> Result<(), String> + Send + 'static {
    fn execute<'a>(&'a mut self, effect: Effect) -> EffectFuture<'a> {
        Box::pin(async move { (self.0)(effect) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::online_route_choice::{Path, Role};
    use std::sync::{Arc, Mutex};
    const E: Epoch = Epoch { attempt: 1, generation: 1 };
    fn sender(executor: impl EffectExecutor) -> RouteRuntime {
        RouteRuntime::spawn(RouteHandler::new(Role::Sender, E), executor, 4)
    }

    #[tokio::test]
    async fn terminal_event_abort_publish_failure_still_closes_all_and_notifies() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let runtime = sender(EffectFn(move |effect: Effect| {
            captured.lock().unwrap().push(effect.clone());
            if matches!(effect, Effect::Send(_)) { Err("abort publication failed".into()) } else { Ok(()) }
        }));
        runtime.dispatch_gathering_started(E, Instant::now()).await.unwrap();
        assert!(runtime.dispatch(Event::Timeout {epoch:E}).await.is_err());
        let seen = seen.lock().unwrap();
        assert!(seen.contains(&Effect::CloseAll(E)), "terminal cleanup may not be skipped when Abort fails");
        assert!(seen.contains(&Effect::Failed), "the remote-loss watchdog must notify its local owner");
    }

    #[tokio::test]
    async fn runtime_serializes_events_and_reports_effect_failure() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let runtime = sender(EffectFn(move |effect: Effect| { captured.lock().unwrap().push(effect); Ok(()) }));
        runtime.dispatch_gathering_started(E, Instant::now()).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(), (Phase::Gathering, None, 0));
        let failing = sender(EffectFn(|_| Err("injected transport failure".to_owned())));
        assert_eq!(failing.dispatch_gathering_started(E, Instant::now()).await.unwrap_err(), "injected transport failure");
        assert!(!seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn initial_effect_error_must_terminally_fail_before_route_selection() {
        let runtime = sender(EffectFn(|_| Err("injected initial signaling failure".to_owned())));
        assert!(runtime.dispatch_gathering_started(E, Instant::now()).await.is_err());
        let (phase, _, _) = runtime.snapshot().await.unwrap();
        assert_eq!(phase, Phase::Failed,
            "an initial ambiguous/failed effect must not leave an independent gathering timer alive");
    }

    #[tokio::test]
    async fn cutoff_must_include_time_since_actual_set_local_description_not_dispatch() {
        let runtime = sender(EffectFn(|_| Ok(())));
        let actual_origin = Instant::now();
        tokio::time::sleep(Duration::from_millis(crate::online_route_choice::DIRECT_CUTOFF_MS + 200)).await;
        runtime.dispatch_gathering_started(E, actual_origin).await.unwrap();
        runtime.dispatch(Event::Tick).await.unwrap();
        let (_, selected, _) = runtime.snapshot().await.unwrap();
        assert_eq!(selected, Some(Path::Turn),
            "the Direct cutoff must not restart when the start event is dequeued");
    }

    #[tokio::test]
    async fn duplicate_or_stale_start_cannot_reset_original_source_instant() {
        let runtime = sender(EffectFn(|_| Ok(())));
        let actual_origin = Instant::now() - Duration::from_millis(crate::online_route_choice::DIRECT_CUTOFF_MS + 200);
        runtime.dispatch_gathering_started(E, actual_origin).await.unwrap();
        runtime.dispatch_gathering_started(E, Instant::now()).await.unwrap();
        runtime.dispatch_gathering_started(Epoch { generation: 0, ..E }, Instant::now()).await.unwrap();
        runtime.dispatch(Event::Tick).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap().1, Some(Path::Turn));
    }

    #[tokio::test]
    async fn retry_binds_new_source_clock_without_inheriting_old_elapsed_time() {
        let runtime = sender(EffectFn(|_| Ok(())));
        runtime.dispatch_gathering_started(E, Instant::now() - Duration::from_millis(crate::online_route_choice::DIRECT_CUTOFF_MS + 200)).await.unwrap();
        runtime.dispatch(Event::Tick).await.unwrap();
        runtime.dispatch(Event::Timeout { epoch: E }).await.unwrap();
        let next = Epoch { attempt: 2, generation: 2 };
        runtime.dispatch(Event::Retry(next)).await.unwrap();
        // A queued old callback retains its captured old epoch.
        runtime.dispatch_gathering_started(E, Instant::now() - Duration::from_secs(20)).await.unwrap();
        runtime.dispatch_gathering_started(next, Instant::now()).await.unwrap();
        runtime.dispatch(Event::Tick).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(), (Phase::Gathering, None, 0));
    }

    #[tokio::test]
    async fn numeric_start_is_rejected_instead_of_reconstructing_origin() {
        let runtime = sender(EffectFn(|_| Ok(())));
        assert!(runtime.dispatch(Event::GatheringStarted { epoch: E, now: 0 }).await.is_err());
        assert!(runtime.dispatch(Event::SetupExpected { epoch: E }).await.is_err());
        assert_eq!(runtime.snapshot().await.unwrap().0, Phase::Idle);
    }

    #[tokio::test]
    async fn setup_offer_and_terminal_loss_expires_without_remote_packets() {
        let seen=Arc::new(Mutex::new(Vec::new()));let captured=seen.clone();
        let runtime=RouteRuntime::spawn(RouteHandler::new(Role::Receiver,E),EffectFn(move |e| {
            captured.lock().unwrap().push(e);Ok(())
        }),4);
        runtime.dispatch_setup_expected(E,Instant::now()-Duration::from_millis(crate::online_route_choice::SETUP_LIMIT_MS-50)).await.unwrap();
        // No manual Tick and no signal is delivered. The local watchdog itself
        // must fence the attempt and author the terminal UI cleanup effect.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Failed,None,0));
        let effects=seen.lock().unwrap();
        assert!(effects.contains(&Effect::CloseAll(E)));assert!(effects.contains(&Effect::Failed));
        assert!(!effects.iter().any(|e|matches!(e,Effect::CreateTurnPeerConnection(_)|Effect::FirstFramePermit(_))));
    }

    #[tokio::test]
    async fn delayed_expectation_and_late_offer_cannot_rebase_expired_setup() {
        let runtime=RouteRuntime::spawn(RouteHandler::new(Role::Receiver,E),EffectFn(|_|Ok(())),4);
        runtime.dispatch_setup_expected(E,Instant::now()-Duration::from_millis(crate::online_route_choice::SETUP_LIMIT_MS+1)).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap().0,Phase::Failed);
        runtime.dispatch_setup_expected(E,Instant::now()).await.unwrap();
        runtime.dispatch_gathering_started(E,Instant::now()).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Failed,None,0));
    }

    #[tokio::test]
    async fn setup_watchdog_does_not_consume_gathering_budget_and_retry_has_new_origin() {
        let runtime=sender(EffectFn(|_|Ok(())));
        runtime.dispatch_setup_expected(E,Instant::now()-Duration::from_secs(10)).await.unwrap();
        runtime.dispatch_setup_expected(E,Instant::now()).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Idle,None,0));
        runtime.dispatch_gathering_started(E,Instant::now()).await.unwrap();
        runtime.dispatch(Event::Tick).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Gathering,None,0));
        runtime.dispatch(Event::Timeout{epoch:E}).await.unwrap();
        let next=Epoch{attempt:2,generation:2};runtime.dispatch(Event::Retry(next)).await.unwrap();
        runtime.dispatch_setup_expected(next,Instant::now()).await.unwrap();
        runtime.dispatch_setup_expected(E,Instant::now()-Duration::from_secs(30)).await.unwrap();
        runtime.dispatch_gathering_started(E,Instant::now()-Duration::from_secs(30)).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Idle,None,0));
        runtime.dispatch_gathering_started(next,Instant::now()).await.unwrap();
        assert_eq!(runtime.snapshot().await.unwrap(),(Phase::Gathering,None,0));
    }

    #[tokio::test]
    async fn duplicate_setup_cannot_extend_missing_offer_deadline() {
        let runtime=RouteRuntime::spawn(RouteHandler::new(Role::Receiver,E),EffectFn(|_|Ok(())),4);
        runtime.dispatch_setup_expected(E,Instant::now()-Duration::from_millis(crate::online_route_choice::SETUP_LIMIT_MS-50)).await.unwrap();
        runtime.dispatch_setup_expected(E,Instant::now()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(runtime.snapshot().await.unwrap().0,Phase::Failed);
    }

    #[tokio::test]
    async fn ambiguous_publish_fails_locally_and_cleanup_runs_even_when_abort_fails() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let runtime = sender(EffectFn(move |effect: Effect| {
            captured.lock().unwrap().push(effect.clone());
            if matches!(effect, Effect::Send(_)) { Err("ambiguous publication".into()) } else { Ok(()) }
        }));
        runtime.dispatch_gathering_started(E, Instant::now() - Duration::from_millis(crate::online_route_choice::DIRECT_CUTOFF_MS + 200)).await.unwrap();
        assert!(runtime.dispatch(Event::Tick).await.is_err());
        assert_eq!(runtime.snapshot().await.unwrap().0, Phase::Failed);
        runtime.dispatch(Event::Tick).await.unwrap();
        let seen = seen.lock().unwrap();
        assert!(seen.contains(&Effect::CloseAll(E)));
        assert!(seen.contains(&Effect::Failed));
        assert!(!seen.iter().any(|e| matches!(e, Effect::FirstFramePermit(_))));
    }

    #[tokio::test]
    async fn side_effect_error_before_decision_publication_stops_remaining_effects() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let runtime = sender(EffectFn(move |effect: Effect| {
            captured.lock().unwrap().push(effect.clone());
            if matches!(effect, Effect::CreateTurnPeerConnection(_)) { Err("create failed after partial allocation".into()) } else { Ok(()) }
        }));
        runtime.dispatch_gathering_started(E, Instant::now() - Duration::from_millis(crate::online_route_choice::DIRECT_CUTOFF_MS + 200)).await.unwrap();
        assert!(runtime.dispatch(Event::Tick).await.is_err());
        assert_eq!(runtime.snapshot().await.unwrap().0, Phase::Failed);
        let seen = seen.lock().unwrap();
        assert!(seen.contains(&Effect::CloseAll(E)));
        assert!(!seen.iter().any(|e| matches!(e, Effect::Send(crate::online_route_choice::Envelope { message: crate::online_route_choice::Wire::Decision, .. }))));
    }

    struct Hanging;
    impl EffectExecutor for Hanging {
        fn execute<'a>(&'a mut self, effect: Effect) -> EffectFuture<'a> {
            Box::pin(async move {
                if effect == Effect::DirectStarted { std::future::pending::<()>().await; }
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn blocked_effect_has_bounded_terminal_failure() {
        let runtime = sender(Hanging);
        let result = tokio::time::timeout(Duration::from_secs(4), runtime.dispatch_gathering_started(E, Instant::now())).await;
        assert!(result.unwrap().is_err());
        assert_eq!(runtime.snapshot().await.unwrap().0, Phase::Failed);
    }
}
