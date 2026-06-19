//! The periodic job and the driver that runs it.

use std::future::Future;
use std::time::Duration;

use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::schedule::{FirstTick, Schedule};

/// Boxed error type returned by tick bodies. `Send + Sync` so the
/// driver future stays `Send` for the multi-threaded runtime.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Periodic work driven by [`JobSet::periodic`](crate::JobSet::periodic).
///
/// The driver calls [`tick`](PeriodicJob::tick) **serially** on the
/// schedule's cadence. A tick always finishes before the next begins,
/// so `&mut self` is exclusive per tick. Hold cross-tick state in plain
/// fields; no `Arc`/atomics/`Mutex` are needed for state that only the
/// job itself touches:
///
/// ```
/// # use easi_job::{BoxError, CancellationToken, PeriodicJob};
/// struct Backoff { failures: usize }
///
/// impl PeriodicJob for Backoff {
///     async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
///         // self.failures persists across ticks — no Arc, no atomics.
///         self.failures += 1;
///         Ok(())
///     }
/// }
/// ```
///
/// Stateless jobs can implement the trait when you want a named, 
/// unit-testable type or cross-tick mutable state.
///
/// The returned future must be `Send` so the driver can run on the
/// multi-threaded runtime; an `async fn` body satisfies this as long as
/// it holds no `!Send` value across an `.await`.
pub trait PeriodicJob: Send + 'static {
    /// Run one tick. `ct` is a per-tick child of the set's cancellation
    /// token. `select!` on it to opt into mid-tick cancellation
    /// (otherwise the driver lets the tick run to completion).
    fn tick(&mut self, ct: CancellationToken) -> impl Future<Output = Result<(), BoxError>> + Send;
}

/// A closure wrapped as a [`PeriodicJob`]. Construct with [`job_fn`].
///
/// Coherence prevents a blanket `impl PeriodicJob for F: FnMut(..)`
/// from coexisting with user trait impls (the compiler can't prove a
/// user's type isn't `FnMut`), so closures route through this adapter.
/// It's the same pattern tower uses for `service_fn`.
#[derive(Debug, Clone, Copy)]
pub struct JobFn<F>(F);

/// Wrap a closure so it can be passed where a [`PeriodicJob`] is
/// expected:
///
/// ```no_run
/// # use std::time::Duration;
/// # use easi_job::{job_fn, JobSet, Schedule};
/// let mut jobs = JobSet::new();
/// jobs.periodic(
///     "cache_sweep",
///     Schedule::new(Duration::from_secs(60)),
///     job_fn(|_ct| async move { /* sweep */ Ok(()) }),
/// );
/// ```
pub fn job_fn<F, Fut>(f: F) -> JobFn<F>
where
    F: FnMut(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    JobFn(f)
}

impl<F, Fut> PeriodicJob for JobFn<F>
where
    F: FnMut(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    fn tick(&mut self, ct: CancellationToken) -> impl Future<Output = Result<(), BoxError>> + Send {
        (self.0)(ct)
    }
}

/// Internal driver: runs `job` on the cadence described by `schedule`,
/// logging under `name`, honouring `token` for cancellation.
///
/// 1. Apply the first-tick policy (initial wait, possibly zero).
/// 2. Loop: call `job.tick(child_token)`, log outcome, sleep
///    every jitter, repeat. The tick future is *not* aborted by
///    the driver on cancellation, it runs to completion. The job's
///    tick body can `select!` on the child token to opt into
///    mid-tick cancellation.
/// 3. On cancellation observed between ticks, break and emit
///    `info!(job, "stopped")`.
///
/// ## Cadence is inter-tick delay, not fixed-rate
///
/// The driver sleeps `every` *after* each tick completes, so the period
/// is `tick_duration + every` and ticks never overlap — there is no
/// drift compensation. This is deliberate: serialized, non-overlapping
/// ticks are what make `&mut self` on [`PeriodicJob`] sound and keep
/// non-reentrant sweeps safe. If you need fixed-rate firing regardless
/// of tick duration, then consider `tokio::time::interval` instead.
pub(crate) async fn run_periodic<J>(
    name: &'static str,
    schedule: Schedule,
    token: CancellationToken,
    mut job: J,
) where
    J: PeriodicJob,
{
    let initial_wait = match schedule.first_tick {
        FirstTick::Immediate => Duration::ZERO,
        // apply jitter to the first tick as well, so the initial wait isn't always exactly the same
        FirstTick::SkipImmediate => jittered(schedule.every, schedule.jitter_fraction),
        FirstTick::After(d) => d,
    };

    if !initial_wait.is_zero() {
        select! {
            // `biased` disabled the default random polling order. Instead it polls 
            // the branches in the order they are written, giving priority to the 
            // first one.
            biased;
            // stop if cancellation triggered straight away
            () = token.cancelled() => {
                tracing::info!(job = name, "stopped");
                return;
            }
            // delay by the initial wait
            () = sleep(initial_wait) => {}
        }
    }

    loop {
        match job.tick(token.child_token()).await {
            Ok(()) => tracing::trace!(job = name, "ok"),
            // Pass the error as `&dyn Error` so tracing records the full
            // `.source()` chain, not just the top-level Display.
            Err(e) => tracing::error!(
                job = name,
                error = &*e as &(dyn std::error::Error + 'static),
                "tick failed"
            ),
        }

        select! {
            biased;
            () = token.cancelled() => break,
            () = sleep(jittered(schedule.every, schedule.jitter_fraction)) => {}
        }
    }

    tracing::info!(job = name, "stopped");
}

/// Apply `fraction` jitter to `base` using a non-cryptographic RNG
/// (`fastrand`). Clamps `fraction` to `[0.0, 1.0]` and the result to
/// `>= 0`.
fn jittered(base: Duration, fraction: f32) -> Duration {
    let fraction = fraction.clamp(0.0, 1.0);
    if fraction == 0.0 {
        return base;
    }
    
    let r = fastrand::f32().mul_add(2.0, -1.0);
    let multiplier = r.mul_add(fraction, 1.0);
    let secs = base.as_secs_f64() * f64::from(multiplier);
    Duration::from_secs_f64(secs.max(0.0))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[test]
    fn jittered_zero_fraction_returns_base() {
        let base = Duration::from_secs(100);
        assert_eq!(jittered(base, 0.0), base);
        assert_eq!(jittered(base, -0.5), base);
    }

    #[test]
    fn jittered_stays_within_bounds() {
        let base = Duration::from_secs(100);
        for _ in 0..1000 {
            let r = jittered(base, 0.1);
            let secs = r.as_secs_f64();
            assert!(secs >= 90.0, "{secs} below lower bound");
            assert!(secs <= 110.0, "{secs} above upper bound");
        }
    }

    #[test]
    fn jittered_clamps_fraction_above_one() {
        let base = Duration::from_secs(100);
        for _ in 0..100 {
            let r = jittered(base, 5.0);
            let secs = r.as_secs_f64();
            assert!(secs >= 0.0);
            assert!(secs <= 200.0);
        }
    }

    /// Spawn `run_periodic` and return the counter + cancel token.
    /// Caller drives virtual time via `tokio::time::sleep`.
    fn spawn_counting(
        schedule: Schedule,
    ) -> (
        Arc<AtomicUsize>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let counter = Arc::new(AtomicUsize::new(0));
        let token = CancellationToken::new();
        let counter_for_work = counter.clone();
        let token_for_task = token.clone();
        let job = job_fn(move |_ct| {
            let c = counter_for_work.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let handle = tokio::spawn(run_periodic("test", schedule, token_for_task, job));
        (counter, token, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn skip_immediate_first_tick_at_one_period() {
        let (counter, token, handle) =
            spawn_counting(Schedule::new(Duration::from_secs(10)).with_jitter(0.0));

        tokio::time::sleep(Duration::from_secs(9)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0, "no tick before t=10");

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1, "first tick at t=10");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2, "second tick at t=20");

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn immediate_first_tick_at_t_zero() {
        let (counter, token, handle) = spawn_counting(
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
        );

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1, "immediate tick at t=0");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2, "second tick at t=10");

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn after_first_tick_at_specified_duration() {
        let (counter, token, handle) = spawn_counting(
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::After(Duration::from_secs(3)))
                .with_jitter(0.0),
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "no tick before initial wait"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1, "first tick at t=3");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2, "second tick at t=13");

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_sleep_breaks_promptly() {
        let (counter, token, handle) =
            spawn_counting(Schedule::new(Duration::from_secs(60)).with_jitter(0.0));

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        token.cancel();
        // Cancellation arm of select! resolves immediately; no time
        // advance needed for the task to finish.
        handle.await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_work_lets_work_finish() {
        let work_started = Arc::new(AtomicBool::new(false));
        let work_completed = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::new();

        let ws = work_started.clone();
        let wc = work_completed.clone();
        let token_for_task = token.clone();
        let job = job_fn(move |_ct| {
            let ws = ws.clone();
            let wc = wc.clone();
            async move {
                ws.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(5)).await;
                wc.store(true, Ordering::SeqCst);
                Ok(())
            }
        });
        let handle = tokio::spawn(run_periodic(
            "test",
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            token_for_task,
            job,
        ));

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(work_started.load(Ordering::SeqCst), "work started");
        assert!(!work_completed.load(Ordering::SeqCst), "work still running");

        token.cancel();

        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(
            work_completed.load(Ordering::SeqCst),
            "work runs to completion despite cancel — driver does not abort it"
        );

        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn error_in_work_continues_loop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let token = CancellationToken::new();
        let counter_for_work = counter.clone();
        let token_for_task = token.clone();
        let job = job_fn(move |_| {
            let c = counter_for_work.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n.is_multiple_of(2) {
                    Err::<(), BoxError>("simulated".into())
                } else {
                    Ok(())
                }
            }
        });
        let handle = tokio::spawn(run_periodic(
            "test",
            Schedule::new(Duration::from_secs(10))
                .with_first_tick(FirstTick::Immediate)
                .with_jitter(0.0),
            token_for_task,
            job,
        ));

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1, "first tick (errored)");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2, "second tick after error");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3, "third tick");

        token.cancel();
        handle.await.unwrap();
    }

    /// A `PeriodicJob` impl keeps mutable state across ticks via
    /// `&mut self`. No `Arc`/atomics. We mirror the count into a shared
    /// atomic only so the test thread can observe it.
    #[tokio::test(start_paused = true)]
    async fn trait_job_holds_state_across_ticks() {
        struct Counting {
            ticks: usize,
            mirror: Arc<AtomicUsize>,
        }

        impl PeriodicJob for Counting {
            async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
                self.ticks += 1;
                self.mirror.store(self.ticks, Ordering::SeqCst);
                Ok(())
            }
        }

        let mirror = Arc::new(AtomicUsize::new(0));
        let token = CancellationToken::new();
        let token_for_task = token.clone();
        let job = Counting {
            ticks: 0,
            mirror: mirror.clone(),
        };
        let handle = tokio::spawn(async move {
            run_periodic(
                "test",
                Schedule::new(Duration::from_secs(10))
                    .with_first_tick(FirstTick::Immediate)
                    .with_jitter(0.0),
                token_for_task,
                job,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(mirror.load(Ordering::SeqCst), 1, "first tick");

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            mirror.load(Ordering::SeqCst),
            2,
            "state carried to second tick"
        );

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            mirror.load(Ordering::SeqCst),
            3,
            "state carried to third tick"
        );

        token.cancel();
        handle.await.unwrap();
    }
}
