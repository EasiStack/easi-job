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