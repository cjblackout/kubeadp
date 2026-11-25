use std::time::{Duration, SystemTime};

/// Lightweight Xorshift64 pseudo-random generator for jitter without external crates.
#[derive(Debug)]
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new() -> Self {
        let nanos = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as u64,
            Err(_) => 123456789,
        };
        let state = if nanos == 0 { 0xdeadbeefcafe } else { nanos };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a float in [0.0, 1.0)
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// `BackoffManager` is now a thin façade: it still stores `base_backoff_secs`,
/// `max_backoff_secs`, and `healthy_run_secs` for compatibility, but the
/// **actual delay** is computed by `SlideWindowBackoff` which tracks a sliding
/// window of restart timestamps (fix #4).
///
/// `on_run_finished` is still called (and still returns whether the run was
/// healthy) so that the crash-tracker `clear()` logic in `session.rs` is
/// unchanged, but it no longer resets the backoff internally.
#[derive(Debug)]
pub struct BackoffManager {
    pub base_backoff_secs: f64,
    pub max_backoff_secs: f64,
    pub healthy_run_secs: f64,
    /// Kept for compatibility / tests that inspect this field.
    pub current_backoff_secs: f64,
    rng: SimpleRng,
}

impl BackoffManager {
    pub fn new(base: f64, max: f64, healthy: f64) -> Self {
        let base = base.max(0.01);
        Self {
            base_backoff_secs: base,
            max_backoff_secs: max.max(base),
            healthy_run_secs: healthy.max(0.0),
            current_backoff_secs: base,
            rng: SimpleRng::new(),
        }
    }

    /// Resets the backoff to base.
    pub fn reset(&mut self) {
        self.current_backoff_secs = self.base_backoff_secs;
    }

    /// Called when a run finishes. Returns true if the run was healthy (>= healthy_run_secs).
    /// NOTE: Does NOT reset backoff anymore (the sliding window handles that).
    pub fn on_run_finished(&mut self, uptime_secs: f64) -> bool {
        uptime_secs >= self.healthy_run_secs
    }

    /// Calculates the backoff duration with jitter, and advances `current_backoff_secs`
    /// for tests/compat that inspect the field.
    /// Jitter adds up to +25% variation.
    pub fn compute_and_advance_backoff(&mut self) -> Duration {
        let backoff = self.current_backoff_secs;
        let jitter = self.rng.next_f64() * 0.25;
        let sleep_secs = (backoff * (1.0 + jitter)).min(self.max_backoff_secs);
        self.current_backoff_secs = (self.current_backoff_secs * 2.0).min(self.max_backoff_secs);
        Duration::from_secs_f64(sleep_secs)
    }
}

/// Fix #4: Sliding-window backoff.
///
/// Stores a ring of ALL restart timestamps within a `backoff_window_secs`
/// window. The delay for the next restart is:
///
///   delay = min(base * 2^(n-1), max_backoff)  where n = resets in window
///
/// A lone crash after a long healthy run still gets only the base delay
/// (because previous restarts aged out of the window).
///
/// Jitter (+0..+25%) is applied on top.
#[derive(Debug, Clone)]
pub struct SlideWindowBackoff {
    pub base_backoff_secs: f64,
    pub max_backoff_secs: f64,
    pub backoff_window_secs: f64,
    /// Timestamps (seconds since epoch, f64) of all restarts within the window.
    restarts: Vec<f64>,
    rng_state: u64,
}

impl SlideWindowBackoff {
    pub fn new(base: f64, max: f64, window: f64) -> Self {
        let base = base.max(0.01);
        // Seed from time.
        let rng_state = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xdeadbeef);
        Self {
            base_backoff_secs: base,
            max_backoff_secs: max.max(base),
            backoff_window_secs: window.max(1.0),
            restarts: Vec::new(),
            rng_state,
        }
    }

    /// Constructor used for testing with an explicit RNG seed (for determinism).
    #[cfg(test)]
    pub fn new_with_seed(base: f64, max: f64, window: f64, seed: u64) -> Self {
        let base = base.max(0.01);
        Self {
            base_backoff_secs: base,
            max_backoff_secs: max.max(base),
            backoff_window_secs: window.max(1.0),
            restarts: Vec::new(),
            rng_state: seed,
        }
    }

    fn xorshift64(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        (self.xorshift64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Record a restart at `now_secs` and return the delay to wait before the
    /// *next* attempt.
    pub fn record_restart_and_delay(&mut self, now_secs: f64) -> Duration {
        // Prune timestamps outside the window.
        let cutoff = now_secs - self.backoff_window_secs;
        self.restarts.retain(|&t| t >= cutoff);

        // n = number of restarts in window BEFORE this one.
        let n = self.restarts.len();

        // Record this restart.
        self.restarts.push(now_secs);

        // Compute delay: base * 2^n, capped at max.
        let raw = if n == 0 {
            self.base_backoff_secs
        } else {
            // 2^n, guarded against overflow
            let exp = n.min(62) as u32;
            self.base_backoff_secs * (1u64 << exp) as f64
        };
        let delay_no_jitter = raw.min(self.max_backoff_secs);

        // Add +0..+25% jitter.
        let jitter = self.next_f64() * 0.25;
        let delay_secs = (delay_no_jitter * (1.0 + jitter)).min(self.max_backoff_secs);

        Duration::from_secs_f64(delay_secs)
    }

    /// Count of restarts currently within the window (given current time).
    pub fn restarts_in_window(&self, now_secs: f64) -> usize {
        let cutoff = now_secs - self.backoff_window_secs;
        self.restarts.iter().filter(|&&t| t >= cutoff).count()
    }
}

#[derive(Debug, Clone)]
pub struct CrashWindowTracker {
    pub max_crashes: usize,
    pub crash_window_secs: f64,
    pub crashes: Vec<f64>,
}

impl CrashWindowTracker {
    pub fn new(max_crashes: usize, crash_window_secs: f64) -> Self {
        Self {
            max_crashes,
            crash_window_secs: crash_window_secs.max(0.1),
            crashes: Vec::new(),
        }
    }

    /// Records a crash event at time `now_secs`.
    /// Purges timestamps outside the sliding window.
    /// Returns true if the session has failed (crashes in window >= max_crashes).
    /// If `max_crashes` is 0, crashes are tracked but the session never fails (unlimited).
    pub fn record_crash(&mut self, now_secs: f64) -> bool {
        let cutoff = now_secs - self.crash_window_secs;
        self.crashes.retain(|&t| t >= cutoff);
        self.crashes.push(now_secs);
        if self.max_crashes == 0 {
            false
        } else {
            self.crashes.len() >= self.max_crashes
        }
    }

    /// Clears crash history on healthy runs.
    pub fn clear(&mut self) {
        self.crashes.clear();
    }

    pub fn crash_count(&self, now_secs: f64) -> usize {
        let cutoff = now_secs - self.crash_window_secs;
        self.crashes.iter().filter(|&&t| t >= cutoff).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_growth_and_cap() {
        let mut bm = BackoffManager::new(1.0, 8.0, 10.0);
        assert_eq!(bm.current_backoff_secs, 1.0);

        let d1 = bm.compute_and_advance_backoff();
        assert!(d1.as_secs_f64() >= 1.0 && d1.as_secs_f64() <= 1.26);
        assert_eq!(bm.current_backoff_secs, 2.0);

        let d2 = bm.compute_and_advance_backoff();
        assert!(d2.as_secs_f64() >= 2.0 && d2.as_secs_f64() <= 2.55);
        assert_eq!(bm.current_backoff_secs, 4.0);

        let _d3 = bm.compute_and_advance_backoff();
        assert_eq!(bm.current_backoff_secs, 8.0);

        let _d4 = bm.compute_and_advance_backoff();
        assert_eq!(bm.current_backoff_secs, 8.0); // Capped at max
    }

    #[test]
    fn test_backoff_reset_on_healthy_run() {
        let mut bm = BackoffManager::new(2.0, 60.0, 30.0);
        let _ = bm.compute_and_advance_backoff();
        let _ = bm.compute_and_advance_backoff();
        assert_eq!(bm.current_backoff_secs, 8.0);

        // Short run (< 30s) does not report healthy
        let is_healthy = bm.on_run_finished(10.0);
        assert!(!is_healthy);

        // Healthy run (>= 30s) reports healthy
        let is_healthy = bm.on_run_finished(35.0);
        assert!(is_healthy);
    }

    #[test]
    fn test_crash_window_accounting() {
        let mut tracker = CrashWindowTracker::new(3, 10.0);

        // Crash 1 at t=1.0
        assert!(!tracker.record_crash(1.0));
        assert_eq!(tracker.crash_count(1.0), 1);

        // Crash 2 at t=5.0
        assert!(!tracker.record_crash(5.0));
        assert_eq!(tracker.crash_count(5.0), 2);

        // Crash at t=12.0: Crash 1 at t=1.0 has expired (cutoff = 12 - 10 = 2.0)
        // So crashes are now [5.0, 12.0], total count = 2 -> not failed
        assert!(!tracker.record_crash(12.0));
        assert_eq!(tracker.crash_count(12.0), 2);

        // Crash 4 at t=13.0: crashes are [5.0, 12.0, 13.0], count = 3 >= max_crashes -> FAILED!
        assert!(tracker.record_crash(13.0));
        assert_eq!(tracker.crash_count(13.0), 3);
    }

    #[test]
    fn test_crash_window_clear_on_healthy() {
        let mut tracker = CrashWindowTracker::new(3, 10.0);
        tracker.record_crash(1.0);
        tracker.record_crash(2.0);
        assert_eq!(tracker.crash_count(2.0), 2);

        tracker.clear();
        assert_eq!(tracker.crash_count(2.0), 0);
    }

    #[test]
    fn test_crash_window_tracker_zero_never_fails() {
        let mut tracker = CrashWindowTracker::new(0, 10.0);
        for t in 0..20 {
            assert!(!tracker.record_crash(t as f64));
        }
        assert_eq!(tracker.crash_count(19.0), 11);
    }

    // -----------------------------------------------------------------------
    // Fix #4: SlideWindowBackoff unit tests with injected clock
    // -----------------------------------------------------------------------

    #[test]
    fn test_slide_window_first_crash_gets_base_delay() {
        // Seed=1 → deterministic jitter.
        let mut swb = SlideWindowBackoff::new_with_seed(2.0, 60.0, 600.0, 1);
        // First crash: n=0 in window → delay = base (2.0) + jitter.
        let d = swb.record_restart_and_delay(1000.0);
        assert!(
            d.as_secs_f64() >= 2.0 && d.as_secs_f64() <= 2.51,
            "first delay should be ~base: {:?}",
            d
        );
    }

    #[test]
    fn test_slide_window_exponential_growth() {
        let mut swb = SlideWindowBackoff::new_with_seed(1.0, 64.0, 600.0, 42);
        // n=0 → base=1
        let d0 = swb.record_restart_and_delay(1000.0);
        // n=1 → 2^1=2
        let d1 = swb.record_restart_and_delay(1001.0);
        // n=2 → 2^2=4
        let d2 = swb.record_restart_and_delay(1002.0);
        // n=3 → 2^3=8
        let d3 = swb.record_restart_and_delay(1003.0);

        // Values grow; allow for jitter
        assert!(d0.as_secs_f64() < d1.as_secs_f64() + 0.5, "d0<d1 expected");
        assert!(d1.as_secs_f64() < d2.as_secs_f64() + 1.0, "d1<d2 expected");
        assert!(d2.as_secs_f64() < d3.as_secs_f64() + 2.0, "d2<d3 expected");
        // All within [base, max+jitter]
        for d in [d0, d1, d2, d3] {
            assert!(
                d.as_secs_f64() >= 1.0 && d.as_secs_f64() <= 80.0,
                "delay out of range: {:?}",
                d
            );
        }
    }

    #[test]
    fn test_slide_window_aged_out_resets_to_base() {
        let mut swb = SlideWindowBackoff::new_with_seed(2.0, 60.0, 600.0, 7);
        // Several crashes at t=0..3 → high n.
        for i in 0..5 {
            swb.record_restart_and_delay(i as f64);
        }
        // After 700s all previous restarts age out (window=600s).
        let d = swb.record_restart_and_delay(700.0);
        // n-in-window=0 before this call → base delay again.
        assert!(
            d.as_secs_f64() >= 2.0 && d.as_secs_f64() <= 2.51,
            "aged-out should reset to base: {:?}",
            d
        );
    }

    #[test]
    fn test_slide_window_cap_at_max() {
        let mut swb = SlideWindowBackoff::new_with_seed(1.0, 8.0, 600.0, 99);
        // 10 crashes in rapid succession → delay capped at max (8.0).
        for i in 0..10 {
            let d = swb.record_restart_and_delay(i as f64);
            assert!(
                d.as_secs_f64() <= 10.0, // 8.0 + 25% jitter = 10.0
                "delay must not exceed max+jitter: {:?}",
                d
            );
        }
    }

    #[test]
    fn test_slide_window_restarts_in_window_count() {
        let mut swb = SlideWindowBackoff::new_with_seed(1.0, 60.0, 10.0, 13);
        swb.record_restart_and_delay(100.0);
        swb.record_restart_and_delay(105.0);
        swb.record_restart_and_delay(108.0);
        // At t=111.0, only timestamps >= 101.0 are in window (10s).
        // t=100.0 is out, t=105, t=108 remain.
        assert_eq!(swb.restarts_in_window(111.0), 2);
    }
}
