use std::time::Duration;

/// Cooldown durations a failure earns an exit. Penalties (in the worker's `penalize`) read
/// these off `Policy`; the ladder no longer carries durations.
///
/// A **rate-limit** (CF 1015) is a *brief rest of one IP*, not a punishment: the clearance is still
/// valid, the IP just needs its request rate to fall back under the window, then it serves again
/// with no re-solve. So this is short — long enough for the window to clear, short enough that a
/// big run doesn't deplete the warm set into a stall (the failure mode of the old 10-minute value:
/// on a 50k-page run the IPs trip 1015 in a cascade and a 10-minute bench collapses the pool faster
/// than it recovers). A re-trip just rests again, so this self-paces aggregate throughput to the
/// sustainable rate. It is deliberately **shorter** than the burn cooldown — a rate-limit is far
/// more transient than a (reputation) block. Tune via `Policy.rate_limit_cooldown` if a host's
/// window differs.
pub const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);
pub const BURN_COOLDOWN: Duration = Duration::from_secs(5 * 60);
pub const TRANSIENT_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct Policy {
    pub max_attempts: u32,
    pub rate_limit_cooldown: Duration,
    pub burn_cooldown: Duration,
    pub transient_cooldown: Duration,
    pub no_checkbox_deadline: Duration,
    /// Total solve budget for a **warming** solve — much shorter than a headed `fetch_browser`'s
    /// `--timeout`. Warming is speculative bulk work across the whole catalog, so a single stuck
    /// exit must not hold one of the few (B) browser slots for the full minute: give up fast and
    /// let the slot try a warmable exit. (A headed fetch the caller explicitly asked for still
    /// gets the full `--timeout`.)
    pub warm_timeout: Duration,
    pub lease_timeout: Duration,
    pub probe_concurrency: usize,
    pub slim_timeout: Duration,
    pub slim_connect_timeout: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            max_attempts: 4,
            rate_limit_cooldown: RATE_LIMIT_COOLDOWN,
            burn_cooldown: BURN_COOLDOWN,
            transient_cooldown: TRANSIENT_COOLDOWN,
            no_checkbox_deadline: Duration::from_secs(20),
            warm_timeout: Duration::from_secs(20),
            lease_timeout: Duration::from_secs(60),
            probe_concurrency: 64,
            slim_timeout: crate::slim::DEFAULT_TIMEOUT,
            slim_connect_timeout: crate::slim::DEFAULT_CONNECT_TIMEOUT,
        }
    }
}
