<p align="center">  
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/EasiStack/easi-job/refs/heads/main/.github/easistack-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/EasiStack/easi-job/refs/heads/main/.github/easistack-logo-light.svg">
    <img alt="easiStack" src="https://raw.githubusercontent.com/EasiStack/easi-job/refs/heads/main/.github/easistack-logo-light.svg" width="350" height="70" style="max-width: 100%;">
  </picture>
</p>

# easi-job
In-process periodic-job primitive with graceful shutdown.

[![Crates.io](https://img.shields.io/crates/v/easi-job)](https://crates.io/crates/easi-job)
![Documentation](https://docs.rs/easi-job/badge.svg)


`easi-job` runs recurring background work inside your async process and stops
it cleanly. You register jobs on a `JobSet`. Each one either a **periodic**
job that ticks on a fixed cadence, or a long-running **task**. On shutdown
the set cancels them all, waits a grace period for in-flight ticks to drain,
and reports which finished cleanly and which had to be aborted. It's built for
the everyday server needs, like cleanup tasks that should start with the process
and drain gracefully on Ctrl-C.


## Two registration verbs

`JobSet` has exactly two methods for adding work:

### `periodic` — tick-shaped work

`periodic` accepts anything implementing the `PeriodicJob` trait. For a
short, stateless body, wrap a closure with `job_fn`:

```rust
use std::time::Duration;
use easi_job::{job_fn, JobSet, Schedule};

let mut jobs = JobSet::new();

jobs.periodic(
    "cache_sweep",
    Schedule::new(Duration::from_secs(60)),
    job_fn(|_ct| async move {
        // your work
        Ok(())
    }),
);

// ... server runs ...

// On shutdown: cancel all jobs, wait up to 5s for in-flight ticks
// to finish, abort and warn any that don't.
let report = jobs.shutdown(Duration::from_secs(5)).await;
tracing::info!(
    clean = report.clean.len(),
    aborted = report.aborted.len(),
    panicked = report.panicked.len(),
    "background-job shutdown"
);
```

### `task` — long-running, non-tick-shaped work

For reconnect loops, listeners, or any work that isn't
tick on cadence shaped. The closure receives the cancellation token
and is responsible for observing it:

```rust
jobs.task("pg_listen", |ct| async move {
    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            row = listen.recv() => handle(row).await,
        }
    }
});
```

## Design: Closure or trait?

`periodic` is built on the same "trait core + function adapter" pattern
tower uses for `Service` / `service_fn`. The trait is the real
abstraction; `job_fn` lets a closure stand in where one is expected.

Pick along two axes:

|                              | terse / anonymous            | named / unit-testable |
| ---------------------------- | ---------------------------- | --------------------- |
| **stateless**                | `job_fn(\|_ct\| async {…})`  | `impl PeriodicJob`    |
| **mutable cross-tick state** | Invalid with closure         | `impl PeriodicJob`    |

Please note **`impl PeriodicJob`** is required when a tick mutates state *across*
  ticks: the driver calls ticks serially (a tick always finishes before
  the next begins), so `&mut self` is exclusive per tick and plain
  fields replace `Arc`/atomics/`Mutex`. It also gives you a named type
  you can unit-test `tick()` on directly.

### Stateless: either form works

```rust
use easi_job::{job_fn, BoxError, CancellationToken, PeriodicJob, Schedule};

// Closure
jobs.periodic(
    "cache_sweep",
    Schedule::new(Duration::from_secs(60)),
    job_fn(move |_ct| {
        let cache = cache.clone();
        async move { cache.sweep().await?; Ok(()) }
    }),
);

// Trait — a named, testable type holding its (immutable) deps:
struct CacheSweep { cache: Arc<Cache> }
impl PeriodicJob for CacheSweep {
    async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
        self.cache.sweep().await?;      // read-only through &mut self
        Ok(())
    }
}
jobs.periodic("cache_sweep", Schedule::new(Duration::from_secs(60)),
              CacheSweep { cache });
```

### Mutable cross-tick state: trait only

A closure can't hold state across ticks (the spawned tick future can't
borrow the closure's state across an `.await`), so cross-tick state must
go through `Arc` + atomics with a closure. The trait keeps it in a plain
field:

```rust
struct Backoff { failures: usize }

impl PeriodicJob for Backoff {
    async fn tick(&mut self, _ct: CancellationToken) -> Result<(), BoxError> {
        if do_work().await.is_err() {
            self.failures += 1;        // persists across ticks, no Arc/atomics
        } else {
            self.failures = 0;
        }
        Ok(())
    }
}

jobs.periodic(
    "backoff_sweep",
    Schedule::new(Duration::from_secs(60)),
    Backoff { failures: 0 },
);
```

## Shutdown semantics

`JobSet::shutdown(grace).await -> ShutdownReport`:

1. Cancels the shared `CancellationToken`. Jobs sleeping between
   ticks observe this immediately and exit cleanly. Tasks
   `select!`-ing on the token wake and return.
2. Drains the internal `JoinSet` up to a shared `grace` deadline.
   Each task is classified by its *own* outcome as it completes. 
3. Any task still running when the deadline expires is aborted
   (`JoinSet::abort_all`) plus a `warn!(job, "aborted after shutdown
   grace")` log line.
4. Returns a `ShutdownReport { clean, aborted, panicked }`
   classifying each job. The report can be discarded
   (`let _ = jobs.shutdown(grace).await;`) for callers who don't
   need it.

**Tick bodies must be idempotent.** Abort after grace kills the
future at its next yield point with no chance to run cleanup. 
Designs that rely on a tick running to completion will observe
partial work after shutdown or crash. Make ticks idempotent, the
next tick should converge on the same end state.

If you genuinely need async teardown that survives shutdown, run
the cleanup in a sibling `task("name", |ct| async move {
ct.cancelled().await; do_cleanup().await; })`. The registration
shutdown awaits it like any other.

### Drop guard

If a `JobSet` is dropped without `shutdown(grace).await` being
called, the `Drop` impl cancels the shared token, emits a warn-line,
and the inner `JoinSet` aborts any task still running on drop. This is
the hard-abort "you forgot `shutdown`" path. It cannot await graceful
completion, so prefer the explicit `shutdown(grace).await`. The
graceful path consumes `self` and drains the set, so the warn line
never fires when shutdown was awaited.

## Logging conventions

Every line emitted by the runner carries `job = "<name>"`:

- `trace!(job, "ok")` — successful tick
- `error!(job, error, "tick failed")` — tick returned `Err`
- `info!(job, "stopped")` — loop exited cleanly after cancel
- `warn!(job, "aborted after shutdown grace")` — abort fallback
- `error!(job, "panicked")` — tick body panicked
- `warn!(handles, "JobSet dropped without shutdown — …")` — Drop
  guard fired (i.e. caller forgot to await `shutdown`)

## When NOT to use `easi-job`

- You need durability across crashes.
- You need cron expressions.
- Your job holds `!Send` state, you could use `tokio::task::spawn_local` instead.
