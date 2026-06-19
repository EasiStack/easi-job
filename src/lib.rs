#![warn(missing_docs)]
//! Run recurring background work inside your async process and shut it
//! down cleanly.
//!
//! You register jobs on a [`JobSet`]. Each job is one of two shapes: a
//! *periodic* job that ticks on a fixed cadence, or a long-running *task*.
//! The set owns a shared cancellation token, so a single
//! [`shutdown`](JobSet::shutdown) call stops every job at once, waits up to a
//! grace period for in-flight ticks to finish, and returns a
//! [`ShutdownReport`] saying which jobs ended cleanly and which had to be
//! aborted.
//!
//! It targets the everyday server needs like clean up jobs that should start
//! with the process and drain gracefully on Ctrl-C.
//!
//! ## Two registration verbs
//!
//! [`JobSet::periodic`] runs tick-shaped work on a [`Schedule`] (cadence +
//! jitter + first-tick policy), logging any per-tick error and carrying on. It
//! accepts any [`PeriodicJob`]. For the common case, wrap a closure with
//! [`job_fn`]:
//! ```no_run
//! # use std::time::Duration;
//! # use easi_job::{job_fn, JobSet, Schedule};
//! let mut jobs = JobSet::new();
//! jobs.periodic(
//!     "cleanup",
//!     Schedule::new(Duration::from_secs(60)),
//!     job_fn(|_ct| async move { /* sweep */ Ok(()) }),
//! );
//! ```
//! For work that mutates state *across* ticks, implement [`PeriodicJob`] on a
//! struct: the driver runs ticks serially, so `&mut self` holds the state with
//! no `Arc`/atomics. (A `job_fn` closure can still capture an `Arc<State>` for
//! shared, immutable dependencies.)
//!
//! [`JobSet::task`] runs a single long-running future that isn't tick-shaped.
//! For example a reconnect loop, a listener. It receives the set's cancellation
//! token and is responsible for observing it so it can exit on shutdown:
//! ```no_run
//! # use std::time::Duration;
//! # use easi_job::JobSet;
//! let mut jobs = JobSet::new();
//! jobs.task("listener", |ct| async move {
//!     ct.cancelled().await;
//!     // ... cleanup ...
//! });
//! ```
//!
//! ## Scope: a primitive, not a job queue
//!
//! `easi-job` is intentionally small and in-process: no persistence, no
//! cross-process dispatch, no retries beyond the next tick. If the process
//! crashes mid-tick that tick's work is lost, so tick bodies must be
//! idempotent. The next tick should converge on the same end state.
//!
//! See the README for side-by-side closure-vs-trait examples and the full
//! shutdown semantics.

mod job;
mod job_set;
mod schedule;

pub use job::{BoxError, JobFn, PeriodicJob, job_fn};
pub use job_set::{JobSet, ShutdownReport};
pub use schedule::{FirstTick, Schedule};

// Re-exported because it appears in the public API. `PeriodicJob::tick`
// and `JobSet::task` both take one. Consumers can name it without
// taking a direct `tokio-util` dependency or matching its version.
pub use tokio_util::sync::CancellationToken;
