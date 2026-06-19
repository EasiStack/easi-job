//! A runnable tour of `easi-job`. Run it with:
//!
//! `main` registers six background jobs, lets them tick for ~4 seconds, then
//! shuts the whole set down gracefully and prints the `ShutdownReport`. Each
//! job's name (in its console lines, the framework logs, and the report)
//! matches the `add_*` function that builds it.
//!
//! The first four demos cross the two ways to register tick-shaped work (a
//! `job_fn` closure vs. a [`PeriodicJob`] trait impl) with whether the job
//! holds state across ticks:
//!
//! ```text
//!               |  stateless                  |  stateful
//!  -------------+-----------------------------+----------------------------
//!  job_fn       |  1. add_stateless_closure   |  2. add_stateful_closure
//!  PeriodicJob  |  3. add_stateless_trait     |  4. add_stateful_trait
//! ```
//!
//! The last two demos show lifecycle features rather than the state axis:
//! 5. [`add_listener_task`] — non-tick work that observes the cancellation token.
//! 6. [`add_slow_aborting_job`] — a tick that outlives the grace and is aborted.

use std::time::Duration;

use easi_job::{BoxError, CancellationToken, FirstTick, JobSet, PeriodicJob, Schedule, job_fn};

#[tokio::main]
async fn main() {
    // Install a console subscriber so the framework's own `tracing` lines show
    // up next to the jobs' `println!`s. In a real app you'd configure this once
    // at startup (env-filter, JSON, etc.).
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    println!("starting jobs — they tick for ~4s, then we shut down...");

    // One JobSet owns every background job; its shared cancellation token is
    // what `shutdown` later trips to stop them all at once.
    let mut jobs = JobSet::new();
    add_stateless_closure(&mut jobs); // job_fn, no captured state
    add_stateful_closure(&mut jobs); // job_fn with a captured counter
    add_stateless_trait(&mut jobs); // PeriodicJob holding only immutable deps
    add_stateful_trait(&mut jobs); // PeriodicJob with &mut self state
    add_listener_task(&mut jobs); // non-tick background task
    add_slow_aborting_job(&mut jobs); // slow tick, aborted at shutdown

    // Let the jobs run for a while before we tear them down.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Graceful shutdown: cancel the shared token and wait up to `grace` for
    // every job to finish its current tick. The returned report classifies
    // each job as clean / aborted / panicked.
    println!("shutting down (1s grace)...");
    let report = jobs.shutdown(Duration::from_secs(1)).await;

    println!("=== shutdown report ===");
    println!("clean    : {:?}", report.clean);
    println!("aborted  : {:?}", report.aborted);
    println!("panicked : {:?}", report.panicked);
}

/// Demo 1 (stateless closure) — the terse form: wrap a closure with
/// [`job_fn`] and hand it to [`JobSet::periodic`]. It captures nothing
/// mutable, so it holds no state between ticks. `FirstTick::Immediate` fires
/// the first tick at t=0; jitter is off so the cadence is exactly 1s.
fn add_stateless_closure(jobs: &mut JobSet) {
    jobs.periodic(
        "stateless_closure",
        Schedule::new(Duration::from_secs(1))
            .with_first_tick(FirstTick::Immediate)
            .with_jitter(0.0),
        job_fn(|_ct| async move {
            println!("{}   stateless_closure: tick", ts());
            Ok(())
        }),
    );
}

/// Demo 2 (stateful closure) — a closure that does keep state: the tick
/// counter `n` is captured by `move` and survives across ticks. It also doubles
/// as the error-handling demo. It fails every third tick, and you'll see the
/// driver log `ERROR ... tick failed` and keep looping, one bad tick never
/// stops the job.
fn add_stateful_closure(jobs: &mut JobSet) {
    let mut n: u64 = 0;
    jobs.periodic(
        "stateful_closure",
        Schedule::new(Duration::from_millis(1200))
            .with_first_tick(FirstTick::Immediate)
            .with_jitter(0.0),
        job_fn(move |_ct| {
            n += 1; // captured state — persists across ticks.
            let this = n;
            async move {
                if this.is_multiple_of(3) {
                    println!("{}   stateful_closure: tick #{this} -> returning Err", ts());
                    return Err(format!("simulated failure on tick {this}").into());
                }
                println!(
                    "{}   stateful_closure: tick #{this} ok (count={this})",
                    ts()
                );
                Ok(())
            }
        }),
    );
}

/// Demo 3 (stateless trait) — a [`PeriodicJob`] trait impl that holds no
/// cross-tick state, only immutable. 
struct StatelessCleanup {
    scratch_dir: &'static str,
}

impl PeriodicJob for StatelessCleanup {
    async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
        println!("{}   stateless_trait: sweeping {}", ts(), self.scratch_dir);
        Ok(())
    }
}

fn add_stateless_trait(jobs: &mut JobSet) {
    jobs.periodic(
        "stateless_trait",
        // Default schedule: `SkipImmediate` first tick (waits one period before
        // the first run) and 10% jitter, the production-friendly defaults.
        Schedule::new(Duration::from_millis(1500)),
        StatelessCleanup {
            scratch_dir: "/tmp/jobs-demo",
        },
    );
}

/// Demo 4 (stateful trait) — a [`PeriodicJob`] trait impl that carries state
/// across ticks. Because the driver calls ticks serially, `tick` can keep plain
/// `&mut self` fields with no `Arc`/atomics. Each tick sees the previous
/// tick's mutations.
struct StatefulRunningTotal {
    sum: u64,
    step: u64,
}

impl PeriodicJob for StatefulRunningTotal {
    async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
        self.sum += self.step; // persists across ticks — no synchronisation.
        println!("{}   stateful_trait: running total = {}", ts(), self.sum);
        Ok(())
    }
}

fn add_stateful_trait(jobs: &mut JobSet) {
    // Cadence / first-tick / jitter are all `const` builders, so a Schedule can
    // even live as a `const`. Since it holds no name, the same one could
    // be reused for several differently-named jobs.
    const SCHEDULE: Schedule = Schedule::new(Duration::from_millis(900))
        .with_first_tick(FirstTick::Immediate)
        .with_jitter(0.0);
    jobs.periodic("stateful_trait", SCHEDULE, StatefulRunningTotal { sum: 0, step: 10 });
}

/// Demo 5 (lifecycle) — work that isn't tick-shaped: a listener, a reconnect
/// loop, etc. Use [`JobSet::task`]: it hands the closure the set's cancellation
/// token, and the body is responsible for observing it. Here it simply parks
/// until shutdown, then exits cleanly.
fn add_listener_task(jobs: &mut JobSet) {
    jobs.task("listener_task", |ct| async move {
        println!("{}   listener_task: waiting for shutdown signal", ts());
        ct.cancelled().await;
        println!(
            "{}   listener_task: cancellation observed, exiting cleanly",
            ts()
        );
    });
}

/// Demo 6 (lifecycle) — a deliberately slow job. Its tick sleeps 10s, far
/// longer than the 1s shutdown grace, so it can't finish in time and ends up
/// in `report.aborted`. The driver never aborts a tick mid-flight on its own;
/// only the post-grace `abort_all` does, at the tick's next yield point.
fn add_slow_aborting_job(jobs: &mut JobSet) {
    jobs.periodic(
        "slow_aborting_job",
        Schedule::new(Duration::from_secs(30))
            .with_first_tick(FirstTick::Immediate)
            .with_jitter(0.0),
        job_fn(|ct| async move {
            println!("{}   slow_aborting_job: starting a long (10s) tick", ts());
            // A well-behaved long tick would `select!` on this per-tick child
            // token to bail out early; we deliberately ignore it so the grace
            // window expires and the abort path is demonstrated.
            let _ = ct;
            tokio::time::sleep(Duration::from_secs(10)).await;
            println!(
                "{}   slow_aborting_job: finished (you won't see this — it's aborted)",
                ts()
            );
            Ok(())
        }),
    );
}

/// Generate local time
fn ts() -> String {
    chrono::Local::now().format("%H:%M:%S%.3f").to_string()
}
