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
/// Escalation bounds for the **domain-level** backoff a confirmed structural redirect mismatch
/// earns (see `worker::Shared::note_redirect_mismatch`) — deliberately much longer than any
/// per-exit cooldown above: this is a *config* problem, not exit noise, so the floor starts higher
/// and the ceiling is a rare re-check rather than a fast retry.
pub const REDIRECT_MISMATCH_BACKOFF_BASE: Duration = Duration::from_secs(60);
pub const REDIRECT_MISMATCH_BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);

/// Tunables for retry/cooldown behaviour, the headed-solve deadlines, pacing, and probing. Read off
/// [`Config::policy`](crate::Config::policy); [`Policy::default`] is sensible for most runs.
#[derive(Clone, Debug)]
pub struct Policy {
    /// The per-request rotation budget — how many exits a *raw challenge* burns through before
    /// giving up (winnable failures re-queue without spending it, so they retry forever).
    pub max_attempts: u32,
    /// How long a rate-limited (CF 1015) exit rests before it's leasable again. Deliberately short.
    pub rate_limit_cooldown: Duration,
    /// How long a reputation-blocked (CF 1020) exit is benched — longer than a rate-limit.
    pub burn_cooldown: Duration,
    /// How long a transient blip (5xx / odd-transport) benches an exit before a retry elsewhere.
    pub transient_cooldown: Duration,
    /// How long the headed solver waits for the challenge checkbox to appear before giving up.
    pub no_checkbox_deadline: Duration,
    /// Total solve budget for a **warming** solve — much shorter than a headed `fetch_browser`'s
    /// `--timeout`. Warming is speculative bulk work across the whole catalog, so a single stuck
    /// exit must not hold one of the few (B) browser slots for the full minute: give up fast and
    /// let the slot try a warmable exit. (A headed fetch the caller explicitly asked for still
    /// gets the full `--timeout`.)
    pub warm_timeout: Duration,
    /// How long a resource waits on a persistently-resting pool before failing [`Resting`](crate::FetchError::Resting).
    pub lease_timeout: Duration,
    /// How many exits are health-probed concurrently.
    pub probe_concurrency: usize,
    /// Per-request timeout for a slim HTTP fetch.
    pub slim_timeout: Duration,
    /// Connect timeout for a slim HTTP fetch.
    pub slim_connect_timeout: Duration,
    /// Base of the escalating **domain-level** backoff a confirmed structural redirect mismatch
    /// earns (see `REDIRECT_MISMATCH_BACKOFF_BASE`'s docs).
    pub redirect_mismatch_backoff_base: Duration,
    /// Ceiling of the domain-level redirect-mismatch backoff.
    pub redirect_mismatch_backoff_max: Duration,
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
            redirect_mismatch_backoff_base: REDIRECT_MISMATCH_BACKOFF_BASE,
            redirect_mismatch_backoff_max: REDIRECT_MISMATCH_BACKOFF_MAX,
        }
    }
}
