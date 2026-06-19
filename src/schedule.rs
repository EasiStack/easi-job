//! The [`Schedule`] cadence builder which defines how often a periodic job ticks.

use std::time::Duration;

/// Cadence for a periodic job: how often it ticks, its first-tick policy, and
/// jitter.
///
/// The fields aren't part of the public API. Construct via [`Schedule::new`]
/// and override the defaults with the `with_*` builder methods. A schedule
/// (even a `const` one) can be reused across jobs.
///
/// ```
/// # use std::time::Duration;
/// # use easi_job::{FirstTick, Schedule};
///
/// // Every minute, with SkipImmediate first tick and 10% jitter defaults.
/// let s = Schedule::new(Duration::from_secs(60));
///
/// // Can be constant and reusable.
/// const DAILY: Schedule = Schedule::new(Duration::from_secs(86_400))
///     .with_first_tick(FirstTick::Immediate)
///     .with_jitter(0.0);
/// ```
#[derive(Debug, Clone)]
pub struct Schedule {
    // Fields are `pub(crate)` so they stay out of the public API.
    pub(crate) every: Duration,
    pub(crate) first_tick: FirstTick,
    pub(crate) jitter_fraction: f32,
}

impl Schedule {
    /// Construct with [`FirstTick::SkipImmediate`] and 10% jitter.
    #[must_use]
    pub const fn new(every: Duration) -> Self {
        Self {
            every,
            first_tick: FirstTick::SkipImmediate,
            jitter_fraction: 0.1,
        }
    }

    /// Override the first-tick policy.
    #[must_use]
    pub const fn with_first_tick(mut self, first_tick: FirstTick) -> Self {
        self.first_tick = first_tick;
        self
    }

    /// Override the jitter fraction. Pass `0.0` to disable. The value is
    /// clamped to `[0.0, 1.0]` here (and `NaN` is treated as `0.0`) so
    /// the stored fraction always matches what the driver applies.
    #[must_use]
    pub const fn with_jitter(mut self, fraction: f32) -> Self {
        self.jitter_fraction = if fraction.is_nan() || fraction < 0.0 {
            0.0
        } else if fraction > 1.0 {
            1.0
        } else {
            fraction
        };
        self
    }
}

/// First-tick offset policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstTick {
    /// Run a tick immediately on entry, then wait `every` between
    /// subsequent ticks.
    Immediate,
    /// Wait `every` (jittered) before the first tick. The default to avoid
    /// boot-time DB burst when many jobs start together.
    SkipImmediate,
    /// Wait this fixed duration before the first tick. Useful for
    /// staggered boots when multiple replicas start together.
    After(Duration),
}
