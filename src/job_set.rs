use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::job::{PeriodicJob, run_periodic};
use crate::schedule::Schedule;

/// Outcome of a [`JobSet::shutdown`] call. Each spawned job ends up in
/// exactly one of the three vectors.
///
/// `aborted` and `panicked` being non-empty are alert-worthy signals;
/// `aborted` means a job didn't finish its current tick within the
/// shutdown grace, and `panicked` means a tick body panicked at some
/// point during the run.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ShutdownReport {
    /// Jobs that observed cancellation and exited cleanly within the
    /// grace window.
    pub clean: Vec<&'static str>,
    /// Jobs that didn't finish their current tick within the grace
    /// window and were aborted at their next yield point. The names
    /// are also logged as `warn!(job, "aborted after shutdown grace")`
    /// at abort time.
    pub aborted: Vec<&'static str>,
    /// Jobs whose tick body panicked. The names are also logged as
    /// `error!(job, "panicked")` at join time.
    pub panicked: Vec<&'static str>,
}

/// Lifecycle owner for a set of background jobs.
///
/// Two registration verbs:
///
/// - [`Self::periodic`] — tick-shaped work, wrapped in the periodic
///   driver (cadence + jitter + first-tick policy + per-tick error
///   logging).
/// - [`Self::task`] — long-running work that isn't tick-shaped (a
///   reconnect loop, a background listener). Receives the cancellation
///   token as its first argument; the body is responsible for
///   observing it.
///
/// Call [`Self::shutdown`] to cancel and join. If the `JobSet` is
/// dropped without an explicit shutdown, its `Drop` impl cancels the
/// shared token and emits a warn-line. Without that guard, spawned
/// jobs would hold internal clones of the token and run indefinitely.
///
/// ## Tick bodies must be idempotent
///
/// On `shutdown`, jobs that don't finish their current tick within
/// the grace window are aborted at their next yield point, no
/// cleanup. Designs that rely on a tick running to completion will
/// observe partial work after a crash or shutdown. Make ticks
/// idempotent, the next tick should converge on the same end state.
pub struct JobSet {
    token: CancellationToken,
    tasks: JoinSet<()>,
    names: HashMap<Id, &'static str>,
}

impl JobSet {
    /// Construct an empty set. Use one set per process, the shared
    /// cancellation token signals every job at once.
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            tasks: JoinSet::new(),
            names: HashMap::new(),
        }
    }

    /// Cancellation token shared by every job in this set. Use this
    /// when non-`easi-job` code (e.g. an axum handler that should
    /// refuse new work after Ctrl-C) needs to observe the same
    /// shutdown signal.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Spawn a periodic job under `name`, driven on the cadence described
    /// by `schedule`. Errors are logged `error!(job = name, "tick failed")`
    /// and the loop continues.
    ///
    /// `name` is the identifier emitted as `job = "<name>"` in every log
    /// line (and in the [`ShutdownReport`]) — pick a snake_case identifier,
    /// since it appears in alerts and dashboards. It's a separate argument
    /// from `schedule` (mirroring [`task`](Self::task)) so one `Schedule`
    /// can drive several differently-named jobs.
    ///
    /// `job` is anything implementing [`PeriodicJob`]. For short,
    /// stateless bodies pass a closure wrapped in
    /// [`job_fn`](crate::job_fn). For jobs that carry cross-tick state
    /// (or want a named, testable type) implement [`PeriodicJob`] on a
    /// struct.
    pub fn periodic(&mut self, name: &'static str, schedule: Schedule, job: impl PeriodicJob) {
        let token = self.token.clone();
        let abort = self.tasks.spawn(run_periodic(name, schedule, token, job));
        self.names.insert(abort.id(), name);
    }

    /// Spawn a single long-running future. Use this for work that
    /// isn't tick-shaped like connection reconnect loops, listeners,
    /// long-running streams.
    ///
    /// The closure receives the cancellation token shared by the set.
    /// The body is responsible for `select!`-ing on it (or otherwise
    /// observing it) to exit cleanly during shutdown. A task that
    /// ignores the token is aborted at its next yield point after the
    /// shutdown grace expires, no async cleanup runs.
    pub fn task<F, Fut>(&mut self, name: &'static str, work: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let token = self.token.clone();
        let abort = self.tasks.spawn(work(token));
        self.names.insert(abort.id(), name);
    }

    /// Cancel all jobs and wait up to `grace` for them to finish.
    /// Any job still running after grace gets `JoinHandle::abort()`
    /// and a `warn!(job, "aborted after shutdown grace")` log line.
    /// Panics inside a job's future are logged
    /// `error!(job, "panicked")`.
    ///
    /// Returns a [`ShutdownReport`] classifying each job. The report
    /// can be discarded (`let _ = jobs.shutdown(grace).await;`) for
    /// callers who don't need it.
    ///
    /// On an empty set, returns immediately without sleeping.
    pub async fn shutdown(mut self, grace: Duration) -> ShutdownReport {
        // Cancel the shared token; spawned tasks observe this on their
        // next yield.
        self.token.cancel();
        if self.tasks.is_empty() {
            return ShutdownReport::default();
        }

        let mut report = ShutdownReport::default();
        let deadline = tokio::time::Instant::now() + grace;

        // Phase 1: drain tasks as they finish, up to the grace deadline.
        // `timeout_at` polls each completion before the timer, so a task
        // that finishes within grace is always classified by its real
        // outcome.
        loop {
            match tokio::time::timeout_at(deadline, self.tasks.join_next_with_id()).await {
                Ok(None) => return report, // everything drained within grace
                Ok(Some(res)) => classify(&self.names, res, false, &mut report),
                Err(_elapsed) => break, // grace expired
            }
        }

        // Phase 2: grace expired, abort whatever is still running and
        // drain it. Each task still reports its real outcome, so one
        // that finished a hair after the deadline returns `Ok` and is
        // classified clean, not aborted.
        self.tasks.abort_all();
        while let Some(res) = self.tasks.join_next_with_id().await {
            classify(&self.names, res, true, &mut report);
        }

        report
    }
}

/// Sort one joined task into the report. `after_grace` distinguishes a
/// task aborted by the post-grace `abort_all` (which warns) from one
/// that completed or panicked on its own.
fn classify(
    names: &HashMap<Id, &'static str>,
    res: Result<(Id, ()), JoinError>,
    after_grace: bool,
    report: &mut ShutdownReport,
) {
    let lookup = |id: Id| names.get(&id).copied().unwrap_or("<unknown>");
    match res {
        Ok((id, ())) => report.clean.push(lookup(id)),
        Err(e) if e.is_panic() => {
            let name = lookup(e.id());
            tracing::error!(job = name, "panicked");
            report.panicked.push(name);
        }
        Err(e) => {
            let name = lookup(e.id());
            if after_grace {
                tracing::warn!(job = name, "aborted after shutdown grace");
            }
            report.aborted.push(name);
        }
    }
}

impl Default for JobSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Drop guard. If a `JobSet` is dropped without [`Self::shutdown`]
/// being awaited, the token is cancelled here (so well-behaved tasks
/// see the signal) and the inner [`JoinSet`]'s own `Drop` then aborts
/// any task still running, closing the leak where a task that ignores
/// the token would otherwise run forever. This is the hard-abort
/// "you forgot `shutdown`" path; it cannot await graceful completion.
///
/// The graceful path (`shutdown(grace).await`) consumes `self` and
/// drains `tasks`, so this `Drop` then sees an empty, already-cancelled
/// set and stays silent.
impl Drop for JobSet {
    fn drop(&mut self) {
        if !self.token.is_cancelled() {
            self.token.cancel();
            if !self.tasks.is_empty() {
                tracing::warn!(
                    handles = self.tasks.len(),
                    "JobSet dropped without shutdown — cancelled token. \
                     Remaining tasks aborted on drop without graceful join"
                );
            }
        }
        // `self.tasks` (a JoinSet) aborts any remaining tasks on drop.
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BoxError, FirstTick, job_fn};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Spawn a closure-job that increments `counter` per tick.
    fn spawn_counting(
        set: &mut JobSet,
        name: &'static str,
        every: Duration,
        counter: Arc<AtomicUsize>,
    ) {
        set.periodic(
            name,
            Schedule::new(every).with_jitter(0.0),
            job_fn(move |_ct| {
                let c = counter.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_joins_clean_jobs_within_grace() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut jobs = JobSet::new();
        spawn_counting(&mut jobs, "a", Duration::from_secs(10), counter.clone());
        spawn_counting(&mut jobs, "b", Duration::from_secs(10), counter.clone());
        spawn_counting(&mut jobs, "c", Duration::from_secs(10), counter.clone());

        // Three jobs × one tick each between t=10 and t=15
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3, "one tick per job");

        // Shutdown is near-instant — cancellation hits the inter-tick sleep
        let shutdown = jobs.shutdown(Duration::from_secs(60));
        let report = tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("clean cancellation should complete in zero virtual time");

        assert_eq!(report.clean.len(), 3, "all 3 jobs joined cleanly");
        assert!(report.aborted.is_empty(), "no aborts on clean shutdown");
        assert!(report.panicked.is_empty(), "no panics");

        // No further ticks after shutdown
        let after = counter.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            after,
            "no ticks after shutdown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_slow_job_after_grace() {
        let work_completed = Arc::new(AtomicBool::new(false));
        let wc = work_completed.clone();

        let mut jobs = JobSet::new();
        jobs.periodic(
            "slow_job",
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            job_fn(move |_ct| {
                let wc = wc.clone();
                async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    wc.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }),
        );

        // Let the immediate tick start its 60s sleep
        tokio::time::sleep(Duration::from_millis(1)).await;

        // 5s grace < 60s sleep → must abort
        let report = jobs.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.aborted, vec!["slow_job"]);
        assert!(report.clean.is_empty());
        assert!(report.panicked.is_empty());

        // Advance well past when the work would have completed naturally.
        // If the abort succeeded, work_completed stays false.
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert!(
            !work_completed.load(Ordering::SeqCst),
            "slow job should have been aborted, not completed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_reports_panicked_job() {
        struct Boom;
        impl PeriodicJob for Boom {
            async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
                panic!("boom in tick");
            }
        }

        let mut jobs = JobSet::new();
        jobs.periodic(
            "boom",
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            Boom,
        );

        // Let the immediate tick run and panic.
        tokio::time::sleep(Duration::from_millis(1)).await;

        let report = jobs.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.panicked, vec!["boom"]);
        assert!(report.clean.is_empty());
        assert!(report.aborted.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_classifies_each_job_by_its_own_outcome() {
        let mut jobs = JobSet::new();
        // Fast: its single tick finishes well within the grace window.
        jobs.periodic(
            "fast",
            Schedule::new(Duration::from_secs(100))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            job_fn(|_ct| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            }),
        );
        // Slow: its single tick outlasts the grace window.
        jobs.periodic(
            "slow",
            Schedule::new(Duration::from_secs(100))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            job_fn(|_ct| async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            }),
        );

        // Let both immediate ticks start.
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Each job is classified by its own outcome — the slow job does
        // not drag the fast one into `aborted`. This guards against the
        // old set-difference reconciliation that mislabeled jobs.
        let report = jobs.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.clean, vec!["fast"], "fast finished within grace");
        assert_eq!(report.aborted, vec!["slow"], "slow aborted at grace");
        assert!(report.panicked.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_empty_set_returns_immediately() {
        let jobs = JobSet::new();
        // Long grace — but with no handles, shutdown should return at t=0.
        let report = tokio::time::timeout(
            Duration::from_millis(1),
            jobs.shutdown(Duration::from_secs(60)),
        )
        .await
        .expect("empty shutdown should not block");
        assert!(report.clean.is_empty());
        assert!(report.aborted.is_empty());
        assert!(report.panicked.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn task_receives_cancellation_token_and_observes_shutdown() {
        let exited = Arc::new(AtomicBool::new(false));
        let exited_for_task = exited.clone();

        let mut jobs = JobSet::new();
        jobs.task("listener", move |ct| {
            let exited = exited_for_task;
            async move {
                ct.cancelled().await;
                exited.store(true, Ordering::SeqCst);
            }
        });

        // Task is sleeping on cancellation; shouldn't have exited yet.
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!exited.load(Ordering::SeqCst), "task waiting on token");

        let report = jobs.shutdown(Duration::from_secs(5)).await;
        assert!(exited.load(Ordering::SeqCst), "task exited after cancel");
        assert_eq!(report.clean, vec!["listener"]);
        assert!(report.aborted.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn drop_without_shutdown_cancels_token() {
        // Capture the token from the set so we can observe its state
        // after the set is dropped.
        let token = {
            let mut jobs = JobSet::new();
            let token = jobs.cancellation_token();
            jobs.task("listener", move |ct| async move {
                ct.cancelled().await;
            });
            // Drop `jobs` here — the Drop guard cancels the token, and
            // the inner JoinSet aborts the still-running task (hard
            // abort, no graceful join).
            token
        };

        // Token is cancelled synchronously inside Drop.
        assert!(token.is_cancelled(), "Drop guard should cancel the token");
    }
}

