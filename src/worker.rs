use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chromiumoxide::Page;
use futures::future::BoxFuture;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::host_of;
use crate::solver::browser::{Browser, Cleared, SolveConfig};
use crate::solver::session::ChromeExec;
use crate::wait_full_load;

use crate::classify::Reason;
use crate::clearance::Clearance;
use crate::client::{Config, Domain, FetchError, FetchResult, Outcome, Resource, per_min_interval};
use crate::egress::{Availability, ExitStatus, Lease};
use crate::introspect::Introspector;
use crate::ladder::{self, ChallengeAction, HeadedAction, Step};
use crate::policy::Policy;
use crate::pool::ExitPool;
use crate::slim::{self, Request};
use crate::store::Persistence;

/// Bounds a serving worker's **give-up** check when the whole pool is resting: absent any exit
/// state change, the worker re-checks this often to reap pending work rather than hang. This is a
/// rare safety path (every non-wonky exit cooling), *not* the hot path — a servable exit wakes its
/// worker immediately via the per-exit signal, so this never paces normal serving.
const REAP_FALLBACK: Duration = Duration::from_secs(1);

/// How long the background maintainer waits, when nothing is warmable right now, before re-checking
/// for a freed/cooled-down exit absent a `leasable_signal` wake. Background upkeep, not latency-
/// sensitive, so a coarse tick that catches a cooldown lapsing is fine.
const WARM_IDLE_FALLBACK: Duration = Duration::from_secs(1);

/// The browser-free solve seam (warming): produce a clearance for `url` on the leased exit
/// (`exit_key`). The production impl launches a headed browser; tests inject a fake so the
/// whole serving/warming loop runs hermetically — no browser, no CF. The B permit is acquired
/// by the caller (the maintainer), so it bounds live browsers regardless of this seam.
pub(crate) type SolveFn = Arc<
    dyn Fn(String, String) -> BoxFuture<'static, Result<(Clearance, u32), Reason>> + Send + Sync,
>;

/// The slim seam (serving): given `(url, exit_key, has_clearance)`, return the served body bytes or
/// a failure reason. Tests inject a fake (warm exit serves, cold exit challenges) so the serving
/// loop runs without real HTTP; production uses [`slim::fetch`].
pub(crate) type SlimFn = Arc<dyn Fn(String, String, bool) -> Result<Vec<u8>, Reason> + Send + Sync>;

/// A lock-free **pool-wide** rate pacer (one per aggregate-capped domain): a GCRA/leaky-bucket with
/// no burst credit. `acquire` CAS-advances a shared monotonic "next-allowed" clock by `interval` and
/// returns how long the caller must sleep before sending, so the total send rate across all workers
/// stays ≤ `1/interval` regardless of exit count. Monotonic (`Instant` base), so an NTP step can't
/// bunch slots; no burst (an idle pool resets `slot` to *now*, never accumulating allowance).
struct AggregatePacer {
    epoch: Instant,
    next_nanos: AtomicU64,
    interval_nanos: u64,
}

impl AggregatePacer {
    fn new(interval: Duration) -> AggregatePacer {
        AggregatePacer {
            epoch: Instant::now(),
            next_nanos: AtomicU64::new(0),
            interval_nanos: interval.as_nanos() as u64,
        }
    }

    fn acquire(&self) -> Duration {
        loop {
            let now = self.epoch.elapsed().as_nanos() as u64;
            let cur = self.next_nanos.load(Ordering::Relaxed);
            let slot = cur.max(now); // idle pool → start at now (no burst credit)
            if self
                .next_nanos
                .compare_exchange_weak(
                    cur,
                    slot + self.interval_nanos,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Duration::from_nanos(slot - now);
            }
        }
    }
}

/// State every worker (serving or the maintainer) shares. The browser semaphore is the **B** cap:
/// at most B live browsers at once, drawn by the maintainer and by headed `fetch_browser`. Slim
/// clients are pooled per proxy (the reusable client per exit is what fixed the EMFILE blowup)
/// and shared across workers.
pub(crate) struct Shared {
    pub cfg: Config,
    pub egress: Arc<ExitPool>,
    pub persistence: Arc<Persistence>,
    pub introspect: Arc<Introspector>,
    browser_permits: Arc<Semaphore>,
    slim_clients: Mutex<HashMap<String, wreq::Client>>,
    slim_ever_succeeded: AtomicBool,
    /// The startup fingerprint canary's verdict: the installed Chrome major matches the pinned slim
    /// TLS profile. When true, a persistent slim challenge on a fresh clearance is a **per-URL** CF
    /// challenge (escalate to headed), not a broken triple — so escalation is allowed even before
    /// slim has served once (a batch of only per-URL-hard URLs still gets them). False (pin mismatch
    /// or Chrome unreadable) falls back to the empirical `slim_ever_succeeded` gate.
    fingerprint_ok: bool,
    /// The Chrome executable every solve launches — the `setpriv --pdeathsig` wrapper that binds
    /// Chrome's lifetime to ours (its `Drop` removes the wrapper on a clean exit). Owned here so it
    /// outlives any worker that might launch a browser.
    chrome_exec: ChromeExec,
    /// Configured domains (solve flag + per-IP / aggregate rates), **exact-host matched**.
    /// Seeded from `cfg.domains` and grown at runtime by `mark_solve_host`. The resolvers below route
    /// each request and pace it.
    domains: Mutex<Vec<Domain>>,
    /// Pool-wide aggregate pacers, one per aggregate-capped domain, created lazily.
    aggregate_pacers: Mutex<HashMap<String, Arc<AggregatePacer>>>,
    /// Fired once by `shutdown()` so a worker asleep on a far-future aggregate slot wakes and exits
    /// instead of hanging the drain.
    shutdown: Arc<Notify>,
    /// Test-only overrides so the serving/warming loop runs without a real browser or HTTP.
    /// `None` in production (the real headed solve / real slim run).
    solve_override: Option<SolveFn>,
    slim_override: Option<SlimFn>,
    /// Monotonic id stamped on every fetch's tracing span, so a single resource is greppable
    /// across the exits it serves on.
    req_counter: AtomicU64,
    /// Per-**domain** (not per-exit) confirmed structural mismatches — see [`RedirectMismatch`].
    /// Keyed by the configured host. Empty in the overwhelmingly common case.
    redirect_mismatches: Mutex<HashMap<String, RedirectMismatch>>,
}

/// Confirmed-count and backoff state for a solve domain whose solve keeps landing on a different
/// host than configured, where the landed-host clearance has been independently verified (not just
/// suspected) not to work against the configured host. Distinct from
/// [`ExitData::record_challenge`](crate::store): that's "one exit is flaky"; this is "every exit
/// that tries this domain hits the same wall" — a config error, not exit noise.
#[derive(Default)]
struct RedirectMismatch {
    /// How many times a self-verify has confirmed the mismatch **while independently trusted**
    /// (never incremented by ordinary per-exit flakiness, and never by an untrusted or non-challenge
    /// failure — see `note_redirect_mismatch`). Drives the operator-visible give-up
    /// (`MisconfiguredHost`). Reset to absent by the next self-verify that *succeeds* — a live
    /// streak, not a permanent verdict, so a config fix (or the site changing its redirect) heals it.
    confirmed: u32,
    /// Total self-verify failures (any reason, trusted or not) — drives the maintainer's `retry_after`
    /// backoff. Unlike `confirmed` this needs no trust: even if the real cause is a broken fingerprint
    /// (every solve-host replay challenges), re-solving in a browser can't fix it, so backing off
    /// warming is only ever correct — and keying the backoff on `confirmed` instead would leave the
    /// untrusted and non-challenge cases un-backed-off, re-solving them at full speed forever.
    failures: u32,
    /// The host solving actually landed on, for the operator-facing message.
    landed: String,
    /// The maintainer won't offer this domain to `lease_to_warm_any` before this instant —
    /// escalating with `failures`, mirroring the per-exit challenge cooldown.
    retry_after: Option<Instant>,
    /// Set the instant *any* self-verify fails — trusted or not. Unlike `confirmed`, this needs no
    /// trust: it only excludes the domain from `may_pull`'s "warm for every solve domain"
    /// requirement, so a domain that will never warm can't force every *other* domain to wait on
    /// it forever. Gating this on trust too would deadlock: trust itself comes from a domain
    /// successfully serving, which can't happen while an unwarmable domain still blocks serving.
    unwarmable_for_now: bool,
}

/// Confirmed occurrences before a domain is treated as misconfigured (backs off warming, then fails
/// jobs targeting it) rather than a one-off blip. Kept small: the signal is already high-precision
/// (a redirect *and* an independently-verified replay failure), so there's little to gain from
/// waiting for more confirmations, and a lot to lose in wasted solves. Unlike the backoff duration
/// (`Policy::redirect_mismatch_backoff_base`/`_max`), this count isn't exposed as a tunable — there's
/// no run-to-run reason to want more or fewer confirmations before trusting the signal.
const REDIRECT_MISMATCH_THRESHOLD: u32 = 2;

impl Shared {
    pub fn new(
        cfg: Config,
        egress: Arc<ExitPool>,
        persistence: Arc<Persistence>,
        introspect: Arc<Introspector>,
        fingerprint_ok: bool,
        chrome_exec: ChromeExec,
    ) -> Arc<Self> {
        let browser_permits = Arc::new(Semaphore::new(cfg.browsers.max(1)));
        let domains = Mutex::new(cfg.domains.clone());
        // A solve workload has exits "waiting to warm" (`cold`); a pure-raw one doesn't. Tell the
        // dashboard so it splits leasable-idle exits into idle (warm/free) vs cold accordingly.
        introspect.set_solving(cfg.domains.iter().any(|d| d.solve));
        Arc::new(Shared {
            cfg,
            egress,
            persistence,
            introspect,
            browser_permits,
            slim_clients: Mutex::new(HashMap::new()),
            slim_ever_succeeded: AtomicBool::new(false),
            fingerprint_ok,
            chrome_exec,
            domains,
            aggregate_pacers: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            solve_override: None,
            slim_override: None,
            req_counter: AtomicU64::new(0),
            redirect_mismatches: Mutex::new(HashMap::new()),
        })
    }

    /// Register `host` as a solve domain (explicit registration; no rate). A no-op if a domain
    /// already covers it.
    pub(crate) fn mark_solve_host(&self, host: &str) {
        if host.is_empty() {
            return;
        }
        self.introspect.set_solving(true); // warming now applies → dashboard splits idle vs cold
        let mut domains = self.domains.lock().unwrap();
        if !domains.iter().any(|d| d.host == host) {
            domains.push(Domain::solve(host));
        }
    }

    /// The configured domain for `host` — an **exact host match**, or `None` if `host` is not
    /// configured. Matching is exact on purpose: `example.com` does **not** cover
    /// `www.example.com`; register the precise host you fetch. An unmatched host is a caller
    /// config error, not a raw fallback (see [`WorkerPool::submit`], which rejects it `Unconfigured`).
    fn domain_for(&self, host: &str) -> Option<Domain> {
        self.domains
            .lock()
            .unwrap()
            .iter()
            .find(|d| d.host == host)
            .cloned()
    }

    /// The registered solve domains (hosts with `solve = true`) — what the maintainer warms every
    /// exit for. Empty means a pure-raw workload (nothing to warm).
    pub(crate) fn solve_domains(&self) -> Vec<String> {
        self.domains
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.solve)
            .map(|d| d.host.clone())
            .collect()
    }

    /// The solve host matching `host` exactly (the clearance key the maintainer warmed under), or
    /// `None` if `host` is configured raw or not configured at all.
    pub(crate) fn solve_domain_for(&self, host: &str) -> Option<String> {
        self.domain_for(host).filter(|d| d.solve).map(|d| d.host)
    }

    /// The registered solve domains a worker actually needs to be warm for before pulling *any*
    /// job — every solve domain **except** one whose latest self-verify just failed (see
    /// `RedirectMismatch::unwarmable_for_now`). Deliberately **not** gated on the trusted
    /// `confirmed` streak: trust itself can only be established by *some* domain successfully
    /// serving, which can't happen while an unwarmable domain still blocks every domain's serving —
    /// gating this on trust would deadlock the whole pool the first time a broken domain is
    /// registered before anything else has ever served.
    fn solve_domains_needing_warmth(&self) -> Vec<String> {
        let mismatches = self.redirect_mismatches.lock().unwrap();
        self.solve_domains()
            .into_iter()
            .filter(|host| !mismatches.get(host).is_some_and(|m| m.unwarmable_for_now))
            .collect()
    }

    /// The registered solve domains **minus** any currently backed off by a confirmed structural
    /// mismatch — what the maintainer actually offers to `lease_to_warm_any`. A backed-off domain
    /// reappears once its `retry_after` lapses (one more attempt: either it clears the mismatch —
    /// e.g. the operator fixed the config and restarted — or re-confirms and backs off further).
    fn warmable_solve_domains(&self) -> Vec<String> {
        let now = Instant::now();
        let mismatches = self.redirect_mismatches.lock().unwrap();
        self.solve_domains()
            .into_iter()
            .filter(|host| {
                mismatches
                    .get(host)
                    .and_then(|m| m.retry_after)
                    .is_none_or(|until| now >= until)
            })
            .collect()
    }

    /// Record a redirect self-verify failure for `host` (landed on `landed`). Always marks the
    /// domain `unwarmable_for_now` (see the field docs — no trust needed; it just stops other domains
    /// waiting on it) **and** backs off the maintainer's warming with an escalating `retry_after`, so
    /// a domain that never validates isn't re-solved in a browser at full speed forever — regardless
    /// of trust, since re-solving can't fix either a config error or a broken fingerprint. Only bumps
    /// the **confirmed**, operator-visible streak (→ `MisconfiguredHost`) when `trusted` independently
    /// confirms slim itself works (the same `fingerprint_ok || slim_ever_succeeded` gate escalation
    /// uses) — otherwise a broken fingerprint triple would look identical, and blaming this domain
    /// would hide the real fix.
    fn note_redirect_mismatch(&self, host: &str, landed: &str, trusted: bool) {
        let mut mismatches = self.redirect_mismatches.lock().unwrap();
        let entry = mismatches.entry(host.to_string()).or_default();
        entry.unwarmable_for_now = true;
        entry.landed = landed.to_string();
        // Back off warming on *every* failure — keyed on `failures`, not `confirmed`, so the
        // untrusted and non-challenge cases (which never bump `confirmed`) still get backed off
        // instead of re-solved on loop. The backoff paces retries; it says nothing about blame.
        let pol = self.policy();
        entry.failures = entry.failures.saturating_add(1);
        entry.retry_after = Some(
            Instant::now()
                + crate::store::timeout_cooldown(
                    entry.failures,
                    pol.redirect_mismatch_backoff_base,
                    pol.redirect_mismatch_backoff_max,
                ),
        );
        // The operator-facing "structurally misconfigured" verdict is higher-precision: only a
        // trusted *challenge* self-verify counts toward it (see this method's doc and the callers).
        if !trusted {
            return;
        }
        entry.confirmed = entry.confirmed.saturating_add(1);
        if entry.confirmed >= REDIRECT_MISMATCH_THRESHOLD {
            tracing::error!(
                configured = host,
                landed,
                confirmed = entry.confirmed,
                "domain confirmed structurally misconfigured — solve keeps redirecting to a host \
                 whose clearance never validates against the configured one; register the landed \
                 host as its own domain instead"
            );
        }
    }

    /// A self-verify against `host` succeeded despite an earlier redirect — clear any accumulated
    /// mismatch streak. Self-healing: a config fix (or the site's redirect changing) un-backs-off the
    /// domain on the very next successful warm, same as a per-exit challenge streak resetting on serve.
    fn reset_redirect_mismatch(&self, host: &str) {
        self.redirect_mismatches.lock().unwrap().remove(host);
    }

    /// The landed host if `host` is confirmed structurally misconfigured (past the threshold), else
    /// `None`. Gates both the job-level give-up (`ladder::decide_challenge`) and the pre-flight
    /// rejection in `WorkerPool::submit`.
    fn confirmed_misconfigured(&self, host: &str) -> Option<String> {
        self.redirect_mismatches
            .lock()
            .unwrap()
            .get(host)
            .filter(|m| m.confirmed >= REDIRECT_MISMATCH_THRESHOLD)
            .map(|m| m.landed.clone())
    }

    /// Whether `host`'s latest self-verify just failed (trusted or not — see
    /// `RedirectMismatch::unwarmable_for_now`). A worker that pulls a job for a solve host with no
    /// clearance normally just hands it back for a warm peer at no cost (a brief, self-correcting
    /// race). That free retry is *only* safe because `may_pull` otherwise guarantees the exit was
    /// warm for every solve domain before pulling anything — which no longer holds for a domain this
    /// flag excludes from that gate. `serve_html` uses this to route such a pull down the challenge
    /// ladder instead (spending the rotation budget so it can't spin the retry queue forever) — but
    /// releasing the exit `Ok`, *not* cooled: the domain is broken pool-wide, not this exit, and the
    /// single per-exit cooldown would poison a healthy domain sharing it.
    fn is_redirect_suspect(&self, host: &str) -> bool {
        self.redirect_mismatches
            .lock()
            .unwrap()
            .get(host)
            .is_some_and(|m| m.unwarmable_for_now)
    }

    /// Every solve domain confirmed structurally misconfigured right now, as `(configured, landed)`
    /// pairs — surfaced so a caller's end-of-run summary can name the fix without needing `-v debug`.
    /// Empty in the common case.
    pub(crate) fn misconfigured_domains(&self) -> Vec<(String, String)> {
        self.redirect_mismatches
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m)| m.confirmed >= REDIRECT_MISMATCH_THRESHOLD)
            .map(|(host, m)| (host.clone(), m.landed.clone()))
            .collect()
    }

    /// Acquire this domain's slot in the **pool-wide** aggregate pacer, returning how long to sleep
    /// before sending (`None` if the domain has no aggregate cap). Lock-free GCRA: a CAS advances a
    /// shared monotonic "next-allowed" clock by `60s / aggregate`, so total send rate across *all*
    /// exits stays ≤ the cap — the defense against a per-account/key limit. Call once per request,
    /// **before leasing an exit**, so a worker awaiting its turn holds nothing.
    pub(crate) fn aggregate_wait(&self, host: &str) -> Option<Duration> {
        let d = self.domain_for(host)?;
        let per_min = d.aggregate?;
        let pacer = {
            let mut pacers = self.aggregate_pacers.lock().unwrap();
            pacers
                .entry(d.host.clone())
                .or_insert_with(|| Arc::new(AggregatePacer::new(per_min_interval(per_min))))
                .clone()
        };
        Some(pacer.acquire())
    }

    fn next_req(&self) -> u64 {
        self.req_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn policy(&self) -> &Policy {
        &self.cfg.policy
    }

    fn slim_client(&self, proxy: Option<&str>) -> Result<wreq::Client, Reason> {
        let key = proxy.unwrap_or_default();
        let mut clients = self.slim_clients.lock().unwrap();
        if let Some(c) = clients.get(key) {
            return Ok(c.clone());
        }
        let p = self.policy();
        let client = slim::build_client(proxy, p.slim_timeout, p.slim_connect_timeout)?;
        clients.insert(key.to_string(), client.clone());
        Ok(client)
    }

    fn event(&self, msg: impl Into<String>) {
        self.introspect.event(msg);
    }
}

/// A unit of caller work. `Html` is one browser-free fetch (carrying its input slot + the batch's
/// bounded results channel — never a per-call oneshot, so a million-URL batch pins O(exits)
/// senders); `Headed` carries a type-erased executor so the worker stays generic over the caller's
/// extract closure. Both carry `attempts` — the **rotation budget**: a serve failure decrements it
/// and re-queues the job for another exit; at zero the request gives up.
pub(crate) enum Job {
    Html {
        resource: Resource,
        index: usize,
        started: Instant,
        results: async_channel::Sender<FetchResult<Vec<u8>>>,
        attempts: u32,
    },
    Headed {
        url: String,
        exec: HeadedExec,
        attempts: u32,
    },
}

pub(crate) type HeadedExec =
    Box<dyn FnOnce(Result<HeadedSession, FetchError>) -> BoxFuture<'static, ()> + Send>;

/// A cleared, live page handed to a `fetch` caller's extractor. The browser stays alive
/// (owned by the worker) until the executor returns.
pub(crate) struct HeadedSession {
    pub page: Page,
    pub clicks: u32,
    pub exit: String,
}

/// A **serving worker**, bound 1:1 to one exit (`code`) for life. It pulls a `Job` only once its
/// exit can serve it (ready + idle + not cooling, and — for a solve workload — warm for the solve
/// domains), serves on *its* exit, and re-queues the job for a peer on failure. It never solves in
/// a browser: warming is the maintainer's job. When the whole pool is resting it reaps pending work
/// (fails it) rather than hang.
struct Worker {
    shared: Arc<Shared>,
    code: String,
    /// Fresh work (bounded → O(exits) memory) and re-queued work (unbounded → a re-queue never
    /// deadlocks against a full fresh queue). Retries are drained first.
    work_rx: async_channel::Receiver<Job>,
    retry_rx: async_channel::Receiver<Job>,
    retry_tx: async_channel::Sender<Job>,
    /// Set on shutdown. A worker parked on a cooling exit (never reaching `recv_job`, so it can't
    /// observe the closed work queue) checks this each loop and exits within one fallback tick.
    closing: Arc<AtomicBool>,
}

/// What a serve failure resolves to, once the exit's been penalized and the lease released.
enum Fail {
    /// Re-queue the job for another exit, carrying this rotation budget. A **winnable** obstacle
    /// (rate-limit/block/timeout/unreachable) leaves the budget untouched (retry forever); the one
    /// give-up-able exit-quality reason (an unregistered-CF raw challenge) decrements it.
    Requeue { attempts: u32 },
    /// A genuinely unwinnable-by-retry failure — deliver this terminal give-up to the caller.
    GiveUp(FetchError),
}

impl Worker {
    async fn run(self) {
        let notify = self.shared.egress.exit_notify(&self.code);
        loop {
            if self.closing.load(Ordering::Relaxed) {
                return;
            }
            // Phase 1 — wait until our exit can pull work, or reap pending work if the pool is
            // resting. Register the wake *before* checking so a change landing in the gap isn't lost.
            let wake = notify.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if !self.may_pull() {
                if self.shared.egress.availability() != Availability::Available {
                    // The whole pool is resting (every non-wonky exit cooling): drain and fail
                    // pending work rather than hang. Keep reaping while stuck.
                    if self.reap_one().await {
                        continue;
                    }
                }
                // Sleep precisely to the pace deadline if that's what's holding us; otherwise the
                // coarse reap fallback (a servable exit wakes its worker immediately via `notify`).
                let wait = self.pace_wait().unwrap_or(REAP_FALLBACK);
                tokio::select! {
                    _ = wake => {}
                    _ = tokio::time::sleep(wait) => {}
                }
                continue;
            }

            // Phase 2 — pull a job (retries first) and serve it on our exit. `recv` may block on an
            // empty queue; our exit stays Idle meanwhile (claim happens only once we hold a job), so
            // the maintainer/prober can still act on it.
            let Some(job) = self.recv_job().await else {
                return; // both queues closed → shutdown
            };
            self.handle(job).await;
        }
    }

    /// Whether our exit can pull work right now: leasable (ready + idle + not cooling + under cap)
    /// and — when there are solve domains — warm for all of them (a raw workload needs no warmth).
    /// This is the pull-gate that makes the tail-latency guarantee hold: a cold/warming worker never
    /// claims a resource, so warm idle workers finish the stragglers. "All of them" is
    /// [`Shared::solve_domains_needing_warmth`] — every solve domain minus one whose self-verify
    /// just failed, so a domain that can provably never warm can't starve every *other* domain's
    /// jobs forever, waiting on a readiness this exit (or any exit) will never reach.
    fn may_pull(&self) -> bool {
        if !self.shared.egress.is_claimable(&self.code) {
            return false;
        }
        // Pacing: if this exit is spacing out requests under a per-IP ceiling, don't pull yet.
        if self.shared.egress.paced_until(&self.code).is_some() {
            return false;
        }
        let domains = self.shared.solve_domains_needing_warmth();
        domains.is_empty()
            || domains
                .iter()
                .all(|d| self.shared.egress.exit_warm_for(&self.code, d))
    }

    /// If this exit is blocked *only* by pacing, how long until it may serve again — so the worker
    /// sleeps precisely to that deadline instead of the coarse reap fallback (no poll, no slop).
    fn pace_wait(&self) -> Option<Duration> {
        self.shared
            .egress
            .paced_until(&self.code)
            .map(|until| until.saturating_duration_since(Instant::now()))
    }

    /// Pull a job, retries before fresh work. `None` once both channels are closed (shutdown).
    async fn recv_job(&self) -> Option<Job> {
        tokio::select! {
            biased;
            r = self.retry_rx.recv() => r.ok(),
            r = self.work_rx.recv() => r.ok(),
        }
    }

    /// The pool is resting (every non-wonky exit cooling). Drain one pending job; **give up only if
    /// it's been unable to make progress for the whole `lease_timeout`** — a persistently dead /
    /// unwarmable pool. A transient resting wave (a burst of cooldowns that will lift) is winnable,
    /// so the job is put back to keep waiting. Returns whether a job was actually failed.
    async fn reap_one(&self) -> bool {
        let Some(job) = self
            .retry_rx
            .try_recv()
            .or_else(|_| self.work_rx.try_recv())
            .ok()
        else {
            return false;
        };
        let past_deadline = match &job {
            Job::Html { started, .. } => started.elapsed() >= self.shared.cfg.policy.lease_timeout,
            Job::Headed { .. } => true, // a headed capture has no wait budget — fail on a dead pool
        };
        if !past_deadline {
            self.requeue(job).await; // keep waiting for the pool to recover
            return false;
        }
        let err = match self.shared.egress.availability() {
            Availability::Resting(retry_after) => FetchError::Resting { retry_after },
            Availability::Available => FetchError::GaveUp(Reason::Unavailable),
        };
        tracing::error!("gave up — pool resting past the lease timeout");
        deliver_err(job, err).await;
        true
    }

    /// Route a pulled job onto our exit. Claims the exit; if the claim races (rare — a single
    /// worker owns its exit's serving, so it normally holds between the gate and here) or the job's
    /// host needs a clearance this exit lacks (a multi-domain edge), the job is handed back rather
    /// than held hostage while warming.
    async fn handle(&self, job: Job) {
        // Aggregate (pool-wide) pacing happens *before* we lease an exit, so a worker awaiting its
        // slot holds nothing (exit stays Idle). Cancellable on shutdown (register-then-recheck) so
        // a far-future slot can't hang the drain at end-of-run.
        let host = match &job {
            Job::Html { resource, .. } => host_of(&resource.url).unwrap_or_default(),
            Job::Headed { url, .. } => host_of(url).unwrap_or_default(),
        };
        if let Some(wait) = self.shared.aggregate_wait(&host) {
            let sd = self.shared.shutdown.notified();
            tokio::pin!(sd);
            sd.as_mut().enable();
            if self.closing.load(Ordering::Relaxed) {
                self.requeue(job).await;
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = sd => {
                    self.requeue(job).await;
                    return;
                }
            }
        }

        let Some(lease) = self.shared.egress.claim(&self.code) else {
            self.requeue(job).await;
            return;
        };
        let req = self.shared.next_req();
        match job {
            Job::Html {
                resource,
                index,
                started,
                results,
                attempts,
            } => {
                let span = tracing::info_span!("fetch", req, url = %short_url(&resource.url));
                self.serve_html(lease, resource, index, started, results, attempts)
                    .instrument(span)
                    .await;
            }
            Job::Headed {
                url,
                exec,
                attempts,
            } => {
                let span = tracing::info_span!("fetch", req, url = %short_url(&url), headed = true);
                self.serve_headed_job(lease, &url, exec, attempts)
                    .instrument(span)
                    .await;
            }
        }
    }

    /// Serve one browser-free fetch on the held lease. A solve host replays this exit's clearance;
    /// a raw host is sent as-is. Success delivers the body; a failure penalizes the exit and either
    /// re-queues the job (rotation budget remaining) or gives up.
    async fn serve_html(
        &self,
        lease: Lease,
        resource: Resource,
        index: usize,
        started: Instant,
        results: async_channel::Sender<FetchResult<Vec<u8>>>,
        attempts: u32,
    ) {
        let host = host_of(&resource.url).unwrap_or_default();
        let exit_key = lease.key();
        // Solve hosts replay a clearance keyed by the registered solve host (what the maintainer
        // warmed) — the same exact host, since routing is exact-matched.
        let domain = self.shared.solve_domain_for(&host);
        let is_solve = domain.is_some();
        let clearance = domain
            .as_ref()
            .and_then(|d| self.shared.egress.warm(&exit_key, d));

        // A solve host we're not actually warm for. The common case is a brief, self-correcting
        // race (multi-domain edge / a runtime registration) — hand it back for a warm peer, no
        // penalty. But a domain whose latest self-verify just failed (`is_redirect_suspect`) is
        // excluded from `may_pull`'s "warm for every solve domain" gate precisely so it can't starve
        // *other* domains — which means a worker can land here for it repeatedly, indefinitely, with
        // no clearance ever coming. Run that case down the challenge ladder (spend the rotation
        // budget, eventually give up) so it can't spin the retry queue forever — but release the
        // exit `Ok`, *not* cooled: this domain is broken pool-wide (that's why it's out of the warmth
        // gate), so it's not this exit's fault, and the single per-exit cooldown would poison a
        // healthy domain sharing the exit. Rotating away wouldn't help anyway.
        if is_solve && clearance.is_none() {
            if self.shared.is_redirect_suspect(&host) {
                lease.release(ExitStatus::Ok);
                self.resolve_challenge(exit_key, resource, index, started, results, attempts)
                    .await;
                return;
            }
            lease.release(ExitStatus::Ok);
            self.requeue(Job::Html {
                resource,
                index,
                started,
                results,
                attempts,
            })
            .await;
            return;
        }

        let span = tracing::info_span!("exit", code = %lease.code());
        async {
            let label = exit_label(&exit_key);
            let proxy = (!exit_key.is_empty()).then(|| exit_key.clone());
            self.shared.egress.mark_serving(&exit_key);
            self.shared.egress.record_request(&exit_key);
            // Per-IP pacing: commit this exit to the domain's per-exit interval before the request
            // goes out, so spacing is between request *starts* and a failed request still counts.
            // Keyed by the matched domain (any configured domain, solve or raw).
            if let Some(d) = self.shared.domain_for(&host)
                && let Some(per_min) = d.per_ip
            {
                self.shared.egress.mark_served(
                    &self.code,
                    &d.host,
                    Instant::now() + per_min_interval(per_min),
                );
            }
            let req = resource.to_request();
            match slim_request(
                &self.shared,
                &req,
                &exit_key,
                &host,
                proxy.as_deref(),
                clearance.as_ref(),
            )
            .await
            {
                Ok(body) => {
                    tracing::info!(bytes = body.len(), raw = !is_solve, "slim served");
                    self.shared.event(format!("served {host} via {label}"));
                    let outcome = self.outcome(body, 0, started, false, label);
                    lease.release(ExitStatus::Ok);
                    self.deliver(index, &resource, &results, Ok(outcome)).await;
                }
                Err(reason) if is_solve && reason == Reason::Challenged => {
                    // A challenge on a solve host despite holding a clearance — a stale/loaded
                    // cookie. `challenged` re-warms and retries slim while the rotation budget
                    // lasts, escalating or giving up only once it's spent (see its docs).
                    self.challenged(lease, resource, index, started, results, attempts)
                        .await;
                }
                Err(reason) => match self.apply_failure(lease, reason, &host, &label, attempts) {
                    Fail::Requeue { attempts } => {
                        self.requeue(Job::Html {
                            resource,
                            index,
                            started,
                            results,
                            attempts,
                        })
                        .await;
                    }
                    Fail::GiveUp(err) => {
                        self.deliver(index, &resource, &results, Err(err)).await;
                    }
                },
            }
        }
        .instrument(span)
        .await;
    }

    /// A live solve-host challenge on a *held* clearance (a stale/loaded cookie). Bench **this exit**
    /// with the escalating per-host cooldown — the fault is this exit's cookie, so cooling it and
    /// rotating is right — then run the shared ladder. The clearance key (`record_slim_challenge`'s,
    /// and the ladder's) is `resource`'s own host, since routing is exact-matched.
    async fn challenged(
        &self,
        lease: Lease,
        resource: Resource,
        index: usize,
        started: Instant,
        results: async_channel::Sender<FetchResult<Vec<u8>>>,
        attempts: u32,
    ) {
        let host = host_of(&resource.url).unwrap_or_default();
        let exit_key = lease.key();
        let pol = self.shared.policy();
        self.shared.egress.record_slim_challenge(
            &exit_key,
            &host,
            pol.transient_cooldown,
            pol.burn_cooldown,
        );
        lease.release(ExitStatus::Cooled);
        self.resolve_challenge(exit_key, resource, index, started, results, attempts)
            .await;
    }

    /// The shared challenge ladder, after the exit has already been released by the caller: retry
    /// slim while the rotation budget lasts, escalate to a headed fetch once it's spent (if the
    /// fingerprint triple is trustworthy), or give up (`FingerprintMismatch`, or `MisconfiguredHost`
    /// once the domain is confirmed structurally broken). Two callers with different exit dispositions
    /// share this: a live challenge (`challenged`, exit cooled) and a redirect-suspect no-clearance
    /// pull (`serve_html`, exit released `Ok` — its brokenness is domain-wide, not the exit's).
    async fn resolve_challenge(
        &self,
        exit_key: String,
        resource: Resource,
        index: usize,
        started: Instant,
        results: async_channel::Sender<FetchResult<Vec<u8>>>,
        attempts: u32,
    ) {
        let host = host_of(&resource.url).unwrap_or_default();
        let escalate_allowed =
            self.shared.fingerprint_ok || self.shared.slim_ever_succeeded.load(Ordering::Relaxed);
        let left = attempts.saturating_sub(1);
        let misconfigured = self.shared.confirmed_misconfigured(&host);
        match ladder::decide_challenge(escalate_allowed, left, misconfigured.is_some()) {
            ChallengeAction::RetrySlim => {
                tracing::debug!("clearance challenged · re-warming and retrying slim");
                self.requeue(Job::Html {
                    resource,
                    index,
                    started,
                    results,
                    attempts: left,
                })
                .await;
            }
            ChallengeAction::Escalate => {
                tracing::info!(
                    "clearance challenged past the budget · escalating to a headed fetch"
                );
                self.requeue(self.escalate_to_headed(resource, index, started, results))
                    .await;
            }
            ChallengeAction::GiveUp => {
                let label = exit_label(&exit_key);
                let err = replay_giveup(&self.shared, Reason::Challenged, label);
                self.deliver(index, &resource, &results, Err(err)).await;
            }
            ChallengeAction::GiveUpMisconfigured => {
                let landed = misconfigured.unwrap_or_default();
                tracing::error!(
                    configured = host,
                    landed,
                    "gave up — domain confirmed misconfigured (solve keeps redirecting to a host \
                     that never validates)"
                );
                let err = FetchError::MisconfiguredHost { host, landed };
                self.deliver(index, &resource, &results, Err(err)).await;
            }
        }
    }

    /// Apply an **exit-quality** slim failure (5xx/timeout/rate-limit, or a *raw*-host challenge) to
    /// the exit. A *solve*-host challenge is handled by the caller (stale clearance). Releases the
    /// lease and re-queues for a *different* exit (this one is cooled/killed so its worker won't
    /// re-pull) — because with a healthy pool every one of these is **winnable**, so we never give
    /// up on them. The sole exception is a **raw** challenge: the host needs solving but isn't
    /// registered, and no amount of rotation clears it, so that one runs down the rotation budget
    /// and gives up ("register it as a solve domain").
    fn apply_failure(
        &self,
        lease: Lease,
        reason: Reason,
        host: &str,
        label: &str,
        attempts: u32,
    ) -> Fail {
        let exit_key = lease.key();
        let can_rotate = self.shared.egress.can_rotate();
        self.shared
            .event(format!("{reason:?} on {host} via {label}"));

        match reason {
            // Unwinnable-by-retry: an unregistered CF host challenges on every exit. Bench this IP,
            // rotate, and give up once the budget's exhausted — the fix is to register the host.
            Reason::Challenged => {
                self.shared
                    .egress
                    .record_transient(&exit_key, self.shared.policy().transient_cooldown);
                lease.release(ExitStatus::Cooled);
                let left = attempts.saturating_sub(1);
                if left > 0 && can_rotate {
                    tracing::debug!(
                        attempts_left = left,
                        "raw challenge · rotating to a clean exit"
                    );
                    Fail::Requeue { attempts: left }
                } else {
                    tracing::error!(
                        "raw challenged on every exit · this host is configured raw but is \
                         Cloudflare-protected — reconfigure it as a solve domain"
                    );
                    Fail::GiveUp(FetchError::GaveUp(Reason::Challenged))
                }
            }
            // Winnable: a rate-limited IP rests, a blocked/timed-out/unreachable one rotates to a
            // clean exit. Cool this exit and retry forever on another — never a give-up.
            r => {
                penalize(
                    &self.shared.egress,
                    self.shared.policy(),
                    Step::Slim,
                    &exit_key,
                    host,
                    r,
                );
                let status = if r == Reason::Unreachable {
                    ExitStatus::Dead
                } else {
                    ExitStatus::Cooled
                };
                lease.release(status);
                tracing::debug!(?r, "rejected · retrying on another exit");
                Fail::Requeue { attempts }
            }
        }
    }

    async fn requeue(&self, job: Job) {
        let _ = self.retry_tx.send(job).await; // unbounded → immediate
    }

    /// Turn a browser-free `Job::Html` into a **headed fetch of its own URL** — the escalation for a
    /// slim challenge that a cookie can't clear (a per-URL CF challenge). The extractor solves headed
    /// on some exit (`serve_headed_job`), waits for the page to settle, and delivers its HTML into
    /// the *same* results slot, so escalation is invisible to the caller — just a slower, solved
    /// fetch. Given its own rotation budget; a genuine dead end (the browser can't clear it either)
    /// surfaces as `GaveUp`.
    fn escalate_to_headed(
        &self,
        resource: Resource,
        index: usize,
        started: Instant,
        results: async_channel::Sender<FetchResult<Vec<u8>>>,
    ) -> Job {
        let url = resource.url.clone();
        let budget = self.shared.cfg.timeout;
        let exec: HeadedExec = Box::new(move |res| {
            Box::pin(async move {
                let result = match res {
                    Ok(session) => {
                        let html = wait_full_load(&session.page, budget).await;
                        // Don't blindly trust the clear: the page may have *settled into* a
                        // rate-limit/block/challenge after the solve loop declared it cleared (e.g. a
                        // post-clear 429). Re-run the same classifier the solve loop uses; on a
                        // failure reason, give up rather than deliver an error page as a 200 (the
                        // caller then defers it, not writes it). A content-shaped stub — a valid 200
                        // that's just body-less — is not an error `classify` can see; that stays the
                        // caller's content check.
                        let title = page_title_of(&html);
                        match crate::classify::from_page(&title, &html) {
                            Some(reason) => Err(FetchError::GaveUp(reason)),
                            None => Ok(Outcome {
                                value: html.into_bytes(),
                                clicks: session.clicks,
                                elapsed: started.elapsed(),
                                solve_required: true,
                                exit: Some(session.exit),
                            }),
                        }
                    }
                    Err(e) => Err(e),
                };
                let _ = results
                    .send(FetchResult {
                        index,
                        url: resource.url,
                        key: resource.key,
                        result,
                    })
                    .await;
            })
        });
        Job::Headed {
            url,
            exec,
            attempts: self.shared.cfg.policy.max_attempts,
        }
    }

    async fn deliver(
        &self,
        index: usize,
        resource: &Resource,
        results: &async_channel::Sender<FetchResult<Vec<u8>>>,
        result: Result<Outcome<Vec<u8>>, FetchError>,
    ) {
        let _ = results
            .send(FetchResult {
                index,
                url: resource.url.clone(),
                key: resource.key.clone(),
                result,
            })
            .await;
    }

    // ── headed role (fetch_browser) ─────────────────────────────────────────────────────

    /// Handle a `Job::Headed` on our exit: solve headed under a B permit, hand the live page to the
    /// caller's executor, then tear the browser down. On a solve failure, re-queue for another exit
    /// (budget remaining) or hand the caller the give-up.
    async fn serve_headed_job(&self, lease: Lease, url: &str, exec: HeadedExec, attempts: u32) {
        let exit_key = lease.key();
        let label = exit_label(&exit_key);
        let permit = self
            .shared
            .browser_permits
            .clone()
            .acquire_owned()
            .await
            .expect("permits open");
        self.shared.egress.mark_solving(&exit_key);
        let outcome = self.solve_live(url, &exit_key).await;
        match outcome {
            Ok((browser, page, clicks)) => {
                tracing::info!(clicks, "solved challenge in browser");
                self.shared.egress.mark_serving(&exit_key);
                let session = HeadedSession {
                    page: page.clone(),
                    clicks,
                    exit: label,
                };
                exec(Ok(session)).await;
                let _ = page.close().await;
                browser.close().await;
                drop(permit);
                lease.release(ExitStatus::Ok);
            }
            Err(reason) => {
                drop(permit);
                self.shared
                    .event(format!("solve failed {reason:?} via {label}"));
                let host = host_of(url).unwrap_or_default();
                let can_rotate = self.shared.egress.can_rotate();
                let action = headed_decision(
                    &self.shared.egress,
                    self.shared.policy(),
                    can_rotate,
                    &exit_key,
                    &host,
                    reason,
                    attempts.saturating_sub(1),
                );
                match action {
                    HeadedAction::Rotate(status) => {
                        tracing::warn!(?reason, "solve failed · re-queuing on another exit");
                        lease.release(status);
                        self.requeue(Job::Headed {
                            url: url.to_string(),
                            exec,
                            attempts: attempts.saturating_sub(1),
                        })
                        .await;
                    }
                    HeadedAction::Fail(status) => {
                        tracing::error!(?reason, "solve failed · gave up");
                        lease.release(status);
                        exec(Err(FetchError::GaveUp(reason))).await;
                    }
                }
            }
        }
    }

    /// The headed solve that keeps the page alive for the caller's extractor. Always a real browser.
    async fn solve_live(&self, url: &str, exit_key: &str) -> Result<(Browser, Page, u32), Reason> {
        let proxy = (!exit_key.is_empty()).then(|| exit_key.to_string());
        let profile = self.shared.persistence.profile_dir(exit_key);
        let artifacts = self.shared.persistence.artifact_dir();
        // A headed fetch the caller explicitly asked for gets the full --timeout budget.
        let cfg = solve_config(
            &self.shared.cfg,
            &artifacts,
            self.shared.cfg.timeout,
            self.shared.chrome_exec.path().to_path_buf(),
        );
        let mut browser = match Browser::launch(
            proxy.as_deref(),
            &profile,
            &cfg,
            self.shared.introspect.clone(),
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = ?e, "browser launch failed");
                return Err(Reason::Unavailable);
            }
        };
        self.shared
            .introspect
            .set_exit(browser.id(), exit_code_of(&self.shared.egress, exit_key));
        match browser.solve(&cfg, url).await {
            Ok(Cleared {
                page,
                clearance,
                clicks,
            }) => {
                let host = host_of(url).unwrap_or_default();
                record_solve(&self.shared, exit_key, &host, clearance).await;
                Ok((browser, page, clicks))
            }
            Err(reason) => {
                browser.close().await;
                Err(reason)
            }
        }
    }

    // ── shared helpers ──────────────────────────────────────────────────────────────────

    fn outcome<T>(
        &self,
        value: T,
        clicks: u32,
        started: Instant,
        solve_required: bool,
        label: String,
    ) -> Outcome<T> {
        Outcome {
            value,
            clicks,
            elapsed: started.elapsed(),
            solve_required,
            exit: Some(label),
        }
    }
}

/// Send one slim request (replaying `clearance` if present), free-standing so both the serving
/// worker's normal replay and a warm-time self-verify (see [`record_solve`]) share it. `host` is the
/// domain key for the per-host challenge streak (`ExitPool::record_success`) — the request's own
/// host for a normal replay, the configured host for a self-verify probe.
async fn slim_request(
    shared: &Arc<Shared>,
    req: &Request,
    exit_key: &str,
    host: &str,
    proxy: Option<&str>,
    clearance: Option<&Clearance>,
) -> Result<Vec<u8>, Reason> {
    let started = Instant::now();
    let result = match &shared.slim_override {
        Some(f) => f(req.url.clone(), exit_key.to_string(), clearance.is_some()),
        None => slim::fetch(&shared.slim_client(proxy)?, req, clearance).await,
    };
    if result.is_ok() {
        shared
            .egress
            .record_success(exit_key, host, started.elapsed());
        shared.slim_ever_succeeded.store(true, Ordering::Relaxed);
    }
    result
}

/// Bank a freshly-lifted clearance — but if the solve **redirected** to a different host than
/// configured, don't trust it blindly: a stored clearance is what lets a serving worker claim real
/// traffic for this domain, and slim replays against the *configured* host with no rewrite (see
/// `slim::Request`'s docs), so a clearance scoped to the landed host may not actually validate there.
/// Verify with one extra slim probe against the configured host before banking — cheap, and only
/// paid in the rare redirect case. A harmless redirect (the probe succeeds) banks normally; a
/// confirmed mismatch (the probe is challenged) is treated exactly like an ordinary solve-host
/// challenge on this exit (drop it, bench with the escalating cooldown) *and*, when slim is
/// independently known to work, counted toward the domain-level structural-misconfiguration signal
/// (see [`Shared::note_redirect_mismatch`]) — the thing a single flaky exit could never prove on its
/// own. Called by both the maintainer's warm loop and a headed escalation's own solve.
async fn record_solve(shared: &Arc<Shared>, exit_key: &str, host: &str, clearance: Clearance) {
    shared.egress.check_fingerprint(&clearance.user_agent);
    if clearance.host.is_empty() || clearance.host == host {
        // A clean solve on the configured host is the self-heal path when a site *stops*
        // redirecting: clear any prior mismatch streak, else the domain stays wrongly flagged
        // `MisconfiguredHost` forever (a redirected-then-verified solve heals via the Ok branch
        // below — both banking paths must reset, or recovery-without-redirect never heals).
        shared.reset_redirect_mismatch(host);
        shared.egress.record_clearance(exit_key, host, clearance);
        return;
    }
    let landed = clearance.host.clone();
    tracing::warn!(
        configured = host,
        %landed,
        "solve redirected to a different host; verifying the clearance against the configured host \
         before trusting it as warm"
    );
    let proxy = (!exit_key.is_empty()).then(|| exit_key.to_string());
    let verify = Request {
        url: format!("https://{host}/"),
        ..Default::default()
    };
    // Reuses the standard pooled slim client (its normal timeout, not a shortened one) — this is a
    // rare path and a dedicated shorter-timeout client per proxy would duplicate the pool `slim_client`
    // already bounds FDs with.
    match slim_request(
        shared,
        &verify,
        exit_key,
        host,
        proxy.as_deref(),
        Some(&clearance),
    )
    .await
    {
        Ok(_) => {
            tracing::info!(
                configured = host,
                %landed,
                "redirected clearance verified against the configured host — banking it"
            );
            shared.reset_redirect_mismatch(host);
            shared.egress.record_clearance(exit_key, host, clearance);
        }
        Err(Reason::Challenged) => {
            let pol = shared.policy();
            shared.egress.record_slim_challenge(
                exit_key,
                host,
                pol.transient_cooldown,
                pol.burn_cooldown,
            );
            let trusted =
                shared.fingerprint_ok || shared.slim_ever_succeeded.load(Ordering::Relaxed);
            shared.note_redirect_mismatch(host, &landed, trusted);
        }
        Err(reason) => {
            // Not a challenge, but the probe still failed to validate the redirected clearance
            // against the configured host, so we can't bank it. Treat it like an untrusted
            // mismatch: mark the domain unwarmable (so it can't hold the `may_pull` gate hostage and
            // hang every *other* domain's workers) and back off re-warming — but never accuse it
            // (`MisconfiguredHost` means a *challenge* that never clears, not a transient obstacle).
            tracing::debug!(
                configured = host,
                ?reason,
                "redirect self-verify inconclusive (non-challenge) — not banking; backing off warming"
            );
            shared.note_redirect_mismatch(host, &landed, false);
        }
    }
}

/// The background **maintainer**: warms cold exits so serving workers have warm exits to serve on.
/// It walks the catalog fastest-first (`lease_to_warm_any` picks the lowest-latency cold exit),
/// solves once under a B permit to bank a `(exit, host)` clearance, and releases the exit warm.
/// Persistent — it keeps the whole catalog warm for the solve domains as exits free/cool/recover.
struct Maintainer {
    shared: Arc<Shared>,
    closing: Arc<AtomicBool>,
}

impl Maintainer {
    async fn run(self) {
        let leasable = self.shared.egress.leasable_signal();
        loop {
            if self.closing.load(Ordering::Relaxed) {
                return;
            }
            let domains = self.shared.warmable_solve_domains();
            if domains.is_empty() {
                // Either a pure-raw workload (nothing to warm) or every solve domain is currently
                // backed off by a confirmed structural mismatch — either way, nothing to do right
                // now. Wait for a (possibly runtime-registered) solve domain, a backoff lapsing, or
                // a shutdown, cheaply.
                let wake = leasable.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                if self.closing.load(Ordering::Relaxed) {
                    return;
                }
                tokio::select! {
                    _ = wake => {}
                    _ = tokio::time::sleep(WARM_IDLE_FALLBACK) => {}
                }
                continue;
            }

            let permit = self
                .shared
                .browser_permits
                .clone()
                .acquire_owned()
                .await
                .expect("permits open");
            if self.closing.load(Ordering::Relaxed) {
                return;
            }
            match self.shared.egress.lease_to_warm_any(&domains) {
                Some((lease, host)) => {
                    self.warm_one(lease, &host, permit).await;
                }
                None => {
                    // Everything leasable is already warm for every domain — nothing to do now.
                    drop(permit);
                    let wake = leasable.notified();
                    tokio::pin!(wake);
                    wake.as_mut().enable();
                    tokio::select! {
                        _ = wake => {}
                        _ = tokio::time::sleep(WARM_IDLE_FALLBACK) => {}
                    }
                }
            }
        }
    }

    /// Solve `host` on the leased (cold) exit once, bank the clearance, and hand the exit back warm.
    /// The exit's serving worker then wakes (its per-exit signal fires on the clearance delta) and
    /// starts serving. On a solve failure the exit is penalized + released, exactly as the ladder
    /// would route it.
    async fn warm_one(&self, lease: Lease, host: &str, permit: OwnedSemaphorePermit) {
        let exit_key = lease.key();
        let label = exit_label(&exit_key);
        let url = format!("https://{host}/");
        let span = tracing::info_span!("warm", code = %lease.code(), host = %host);
        async {
            self.shared.egress.mark_solving(&exit_key);
            let solved = self.solve(&url, &exit_key).await;
            // The browser solve is done and the browser closed — release the scarce B permit *now*,
            // before `record_solve`. Its redirect self-verify is a plain slim probe (not a browser
            // op) under the full slim timeout; holding B across it would let a few concurrently
            // redirecting solves stall all catalog warming, defeating the short warm timeout.
            drop(permit);
            match solved {
                Ok((clearance, clicks)) => {
                    tracing::info!(clicks, "warmed host in browser");
                    let how = if clicks > 0 {
                        format!("solved in {clicks} click(s)")
                    } else {
                        "cleared".into()
                    };
                    record_solve(&self.shared, &exit_key, host, clearance).await;
                    self.shared.event(format!("{how} {host} via {label}"));
                    lease.release(ExitStatus::Ok); // idle, now warm — release wakes its worker
                }
                Err(reason) => {
                    self.shared
                        .event(format!("warm failed {reason:?} via {label}"));
                    penalize(
                        &self.shared.egress,
                        self.shared.policy(),
                        Step::Headed,
                        &exit_key,
                        host,
                        reason,
                    );
                    let status = if reason == Reason::Unreachable {
                        ExitStatus::Dead
                    } else {
                        ExitStatus::Cooled
                    };
                    if matches!(reason, Reason::Timeout | Reason::Unavailable) {
                        tracing::debug!(
                            ?reason,
                            "warm solve failed (transient; exit retried later)"
                        );
                    } else {
                        tracing::warn!(?reason, "warm solve failed");
                    }
                    lease.release(status);
                }
            }
        }
        .instrument(span)
        .await;
    }

    /// The warming solve: the injected fake in tests, else a real headed browser solve on the
    /// leased exit's proxy under the short `warm_timeout`.
    async fn solve(&self, url: &str, exit_key: &str) -> Result<(Clearance, u32), Reason> {
        if let Some(f) = &self.shared.solve_override {
            return f(url.to_string(), exit_key.to_string()).await;
        }
        let proxy = (!exit_key.is_empty()).then(|| exit_key.to_string());
        let profile = self.shared.persistence.profile_dir(exit_key);
        let artifacts = self.shared.persistence.artifact_dir();
        let cfg = solve_config(
            &self.shared.cfg,
            &artifacts,
            self.shared.cfg.policy.warm_timeout,
            self.shared.chrome_exec.path().to_path_buf(),
        );
        let mut browser = match Browser::launch(
            proxy.as_deref(),
            &profile,
            &cfg,
            self.shared.introspect.clone(),
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = ?e, "browser launch failed");
                return Err(Reason::Unavailable);
            }
        };
        self.shared
            .introspect
            .set_exit(browser.id(), exit_code_of(&self.shared.egress, exit_key));
        let result = match browser.solve(&cfg, url).await {
            Ok(Cleared {
                page,
                clearance,
                clicks,
            }) => {
                let _ = page.close().await;
                Ok((clearance, clicks))
            }
            Err(reason) => Err(reason),
        };
        browser.close().await;
        result
    }
}

/// Deliver a terminal error into `job`'s own result slot — a `Job::Html`'s results channel, or a
/// `Job::Headed`'s executor. Shared by a worker's dead-pool reap and `WorkerPool::submit`'s
/// pre-flight rejection, since both fail a job before (or without) ever serving it.
async fn deliver_err(job: Job, err: FetchError) {
    match job {
        Job::Html {
            resource,
            index,
            results,
            ..
        } => {
            let _ = results
                .send(FetchResult {
                    index,
                    url: resource.url,
                    key: resource.key,
                    result: Err(err),
                })
                .await;
        }
        Job::Headed { exec, .. } => exec(Err(err)).await,
    }
}

/// The one-worker-per-exit serving pool plus the background maintainer. Serving workers pull `Job`s
/// from the bounded fresh-work queue (+ an unbounded retry queue for re-queued failures); the
/// maintainer (B tasks) warms cold exits. In-flight fresh work is bounded to the exit count by
/// construction, and live browsers to B by the shared permit.
pub(crate) struct WorkerPool {
    work_tx: async_channel::Sender<Job>,
    shared: Arc<Shared>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    serving: usize,
    closing: Arc<AtomicBool>,
}

impl WorkerPool {
    pub fn spawn(shared: Arc<Shared>) -> WorkerPool {
        let codes = shared.egress.exit_codes();
        let serving = codes.len().max(1);
        let maintainers = shared.cfg.browsers.max(1);
        // Fresh work is bounded to the exit count (breadth) for O(exits) memory; retries are
        // unbounded (a re-queue must never block, and live jobs are ≤ exits anyway).
        let (work_tx, work_rx) = async_channel::bounded(serving);
        let (retry_tx, retry_rx) = async_channel::unbounded();
        let closing = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(serving + maintainers);
        for code in codes {
            let worker = Worker {
                shared: shared.clone(),
                code,
                work_rx: work_rx.clone(),
                retry_rx: retry_rx.clone(),
                retry_tx: retry_tx.clone(),
                closing: closing.clone(),
            };
            handles.push(tokio::spawn(worker.run()));
        }
        for _ in 0..maintainers {
            let maintainer = Maintainer {
                shared: shared.clone(),
                closing: closing.clone(),
            };
            handles.push(tokio::spawn(maintainer.run()));
        }
        WorkerPool {
            work_tx,
            shared,
            handles: Mutex::new(handles),
            serving,
            closing,
        }
    }

    /// The serving-worker count = the exit count = how many requests can be in flight at once.
    pub fn count(&self) -> usize {
        self.serving
    }

    pub async fn submit(&self, job: Job) -> Result<(), FetchError> {
        // Routing gate — the single choke point for *all* work, browser-free or headed (the
        // feeder, `Client::fetch_browser`, and the test harness all submit here). Every fetched
        // host must match a configured `Domain` (exact host, solve or raw); an unconfigured host
        // is a caller error, not a silent raw fetch. Reject it into its own result slot *before*
        // it enters the queue or leases an exit — a fast, deterministic failure independent of
        // pool state.
        let host = match &job {
            Job::Html { resource, .. } => host_of(&resource.url).unwrap_or_default(),
            Job::Headed { url, .. } => host_of(url).unwrap_or_default(),
        };
        let Some(domain) = self.shared.domain_for(&host) else {
            deliver_err(job, FetchError::Unconfigured { host }).await;
            return Ok(());
        };
        // A domain already confirmed structurally misconfigured (see `record_solve`'s
        // self-verify): no exit will ever warm for it, so a job would otherwise queue forever (or,
        // for a headed fetch, launch a browser doomed to fail every time). Same pre-flight spirit
        // as `Unconfigured`.
        if domain.solve
            && let Some(landed) = self.shared.confirmed_misconfigured(&domain.host)
        {
            deliver_err(job, FetchError::MisconfiguredHost { host, landed }).await;
            return Ok(());
        }
        self.work_tx
            .send(job)
            .await
            .map_err(|_| FetchError::Other(anyhow!("worker pool is shut down")))
    }

    pub async fn shutdown(&self) {
        self.work_tx.close(); // serving workers drain fresh + retry, then exit
        self.shared.shutdown.notify_waiters(); // wake workers asleep on an aggregate-pace slot
        self.closing.store(true, Ordering::Relaxed);
        self.shared.egress.leasable_signal().notify_waiters(); // wake idle maintainers to exit
        self.shared.egress.wake_all_workers(); // wake workers parked on a cooling exit
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut self.handles.lock().unwrap());
        for h in handles {
            let _ = h.await;
        }
    }

    /// Interrupt: **abandon** in-flight work rather than draining it. Aborting a worker/maintainer
    /// task drops its stack — including any `Browser` it holds — and `kill_on_drop` `SIGKILL`s
    /// Chrome synchronously, so browsers die without waiting for the batch to wind down.
    pub async fn abort(&self) {
        self.closing.store(true, Ordering::Relaxed);
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut self.handles.lock().unwrap());
        for h in &handles {
            h.abort();
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

fn solve_config(
    cfg: &Config,
    artifact_dir: &Path,
    timeout: Duration,
    chrome: PathBuf,
) -> SolveConfig {
    SolveConfig {
        real_display: cfg.real_display,
        cdp_click: cfg.cdp_click,
        no_click: cfg.no_click,
        move_mouse: cfg.move_mouse,
        connect_grace: cfg.connect_grace,
        timeout,
        no_checkbox_deadline: cfg.policy.no_checkbox_deadline,
        width: cfg.width,
        height: cfg.height,
        capture_dir: cfg.capture_dir.clone(),
        artifact_dir: artifact_dir.to_path_buf(),
        chrome,
    }
}

/// The `<title>` text of a settled page's HTML — enough for `classify::from_page` to spot a
/// challenge/error title. Best-effort string scan (no HTML parser); empty if there's no title.
fn page_title_of(html: &str) -> String {
    html.split_once("<title>")
        .and_then(|(_, rest)| rest.split_once("</title>"))
        .map(|(title, _)| title.trim().to_string())
        .unwrap_or_default()
}

pub(crate) fn exit_label(exit_key: &str) -> String {
    if exit_key.is_empty() {
        return "direct".to_string();
    }
    let host = exit_key.strip_prefix("socks5h://").unwrap_or(exit_key);
    host.split('.').next().unwrap_or(host).to_string()
}

/// The catalog `code` for an exit key (proxy URL), for stamping a browser card. Falls back to the
/// key itself when the exit isn't found.
fn exit_code_of(pool: &ExitPool, exit_key: &str) -> String {
    pool.exit_codes()
        .into_iter()
        .find(|c| c == exit_key)
        .unwrap_or_else(|| exit_label(exit_key))
}

/// The URL minus its scheme, for a compact `fetch` span field (`example.com/search/`).
fn short_url(url: &str) -> &str {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
}

/// Apply a failure's consequence to the exit — the *single* place a reason turns into an exit
/// side effect, so the ladder can stay pure routing. A challenge means the stored slim clearance
/// is stale (drop it); on the headed step a challenge is treated as a burn. Every cooling reason
/// records its cooldown here (with the right badge reason).
pub(crate) fn penalize(
    pool: &ExitPool,
    policy: &Policy,
    step: Step,
    exit_key: &str,
    host: &str,
    reason: Reason,
) {
    match (step, reason) {
        (Step::Slim, Reason::Challenged) => pool.drop_clearance(exit_key, host),
        (Step::Headed, Reason::Challenged) => pool.record_block(exit_key, policy.burn_cooldown),
        (_, Reason::RateLimited) => pool.record_rate_limit(exit_key, policy.rate_limit_cooldown),
        (_, Reason::Blocked) => pool.record_block(exit_key, policy.burn_cooldown),
        (_, Reason::Timeout) => pool.record_timeout(
            exit_key,
            policy.transient_cooldown,
            policy.rate_limit_cooldown,
        ),
        (_, Reason::Unavailable) => pool.record_transient(exit_key, policy.transient_cooldown),
        (_, Reason::Unreachable) => {} // no cooldown — the ladder marks it Dead
    }
}

/// Penalize a headed-step reason and route rotate/fail via the headed ladder. A fixed
/// (un-rotatable) egress has no rotations left, so it always fails rather than spinning on the
/// same exit.
pub(crate) fn headed_decision(
    pool: &ExitPool,
    policy: &Policy,
    can_rotate: bool,
    exit_key: &str,
    host: &str,
    reason: Reason,
    attempts_left: u32,
) -> HeadedAction {
    penalize(pool, policy, Step::Headed, exit_key, host, reason);
    ladder::decide_headed(reason, rotations_left(can_rotate, attempts_left))
}

/// How many rotation attempts remain. A fixed (single-exit) egress can't rotate, so it has none.
pub(crate) fn rotations_left(can_rotate: bool, attempts: u32) -> u32 {
    if can_rotate { attempts } else { 0 }
}

/// A give-up while serving a *freshly-warmed* exit: if slim has never once succeeded, the
/// fingerprint triple is the prime suspect (a banked cookie that slim can't replay) — say so.
fn replay_giveup(shared: &Shared, reason: Reason, label: String) -> FetchError {
    if shared.slim_ever_succeeded.load(Ordering::Relaxed) {
        tracing::error!(?reason, "gave up serving browser-free after re-warming");
        FetchError::GaveUp(reason)
    } else {
        tracing::error!(
            ?reason,
            "slim has NEVER succeeded — suspect a broken fingerprint triple"
        );
        FetchError::FingerprintMismatch {
            exit: label,
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    //! The one-worker-per-exit serving loop + background maintainer, driven hermetically: the
    //! `solve` and `slim` seams are faked (a warm exit serves, a cold one challenges), so the whole
    //! model runs with no browser and no HTTP. These prove the headline behaviours — the catalog
    //! warms and everything serves, a raw host never solves, one challenged exit can't block the
    //! scrape, an unwarmable host gives up rather than hangs, and a cold/failing exit never blocks a
    //! warm one from finishing work (the tail-latency guarantee).
    use super::*;
    use std::sync::atomic::AtomicUsize;

    const RATE_HOST: &str = "h.test";

    fn fake_clearance() -> Clearance {
        Clearance::new(
            vec![("cf_clearance".into(), "t".into())],
            "UA".into(),
            None,
            String::new(),
        )
    }

    /// A serving exit returns a page iff it holds a clearance (warm); otherwise it challenges —
    /// exactly CF's behaviour.
    fn warm_serves_cold_challenges() -> SlimFn {
        Arc::new(|_url, _exit, has_clearance| {
            if has_clearance {
                Ok(format!("<html>{}</html>", "x".repeat(3000)).into_bytes())
            } else {
                Err(Reason::Challenged)
            }
        })
    }

    /// Always serves, warm or cold — for the raw path, where no clearance is held and a CF
    /// challenge should never happen.
    fn always_serves() -> SlimFn {
        Arc::new(|_url, _exit, _has_clearance| Ok(b"<html>raw</html>".to_vec()))
    }

    fn test_shared(
        exit_codes: Vec<String>,
        cfg: Config,
        solve: SolveFn,
        slim: SlimFn,
    ) -> Arc<Shared> {
        let egress = crate::pool::ExitPool::manual_no_monitor(exit_codes);
        let browser_permits = Arc::new(Semaphore::new(cfg.browsers.max(1)));
        let domains = Mutex::new(cfg.domains.clone());
        Arc::new(Shared {
            cfg,
            egress,
            persistence: Arc::new(Persistence::open(None, "Chrome147")),
            introspect: Introspector::new(),
            browser_permits,
            slim_clients: Mutex::new(HashMap::new()),
            slim_ever_succeeded: AtomicBool::new(false),
            // Off in tests: escalation routes through the real-browser headed path (no fake seam),
            // so tests that exercise a solve-host challenge rely on `slim_ever_succeeded` (an
            // actual fake slim success) rather than this canary to trust their evidence.
            fingerprint_ok: false,
            chrome_exec: ChromeExec::direct("/usr/bin/true"),
            domains,
            aggregate_pacers: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            solve_override: Some(solve),
            slim_override: Some(slim),
            req_counter: AtomicU64::new(0),
            redirect_mismatches: Mutex::new(HashMap::new()),
        })
    }

    /// Drive a batch of `urls` through the pool (one worker per exit + the maintainer), draining
    /// results concurrently so the bounded work queue never deadlocks against the bounded results
    /// channel. Returns one result per input.
    async fn run_batch(shared: Arc<Shared>, urls: Vec<String>) -> Vec<FetchResult<Vec<u8>>> {
        let attempts = shared.cfg.policy.max_attempts;
        let pool = WorkerPool::spawn(shared);
        let cap = pool.count().max(1);
        let (tx, rx) = async_channel::bounded(cap);
        let drain = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Ok(fr) = rx.recv().await {
                out.push(fr);
            }
            out
        });
        for (index, url) in urls.into_iter().enumerate() {
            pool.submit(Job::Html {
                resource: Resource::from(url),
                index,
                started: Instant::now(),
                results: tx.clone(),
                attempts,
            })
            .await
            .unwrap();
        }
        drop(tx);
        let out = drain.await.unwrap();
        pool.shutdown().await;
        out
    }

    fn codes(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("e{i}")).collect()
    }

    /// `h.test` takes the warm/solve path (the solve-set holds `"h.test"`, matched exactly).
    fn cfg_with(browsers: usize) -> Config {
        Config {
            browsers,
            domains: vec![Domain::solve(RATE_HOST)],
            ..Config::default()
        }
    }

    fn raw_cfg(browsers: usize) -> Config {
        Config {
            browsers,
            ..Config::default()
        }
    }

    fn counting_solve(solves: Arc<AtomicUsize>) -> SolveFn {
        Arc::new(move |_url, _exit| {
            let solves = solves.clone();
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                solves.fetch_add(1, Ordering::SeqCst);
                Ok((fake_clearance(), 0u32))
            })
        })
    }

    fn total_solves(shared: &Shared) -> u64 {
        shared
            .egress
            .snapshot()
            .stats
            .iter()
            .map(|s| s.stats.solves)
            .sum()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn maintainer_warms_the_catalog_and_serves_everything() {
        let solves = Arc::new(AtomicUsize::new(0));
        let shared = test_shared(
            codes(20),
            cfg_with(4),
            counting_solve(solves.clone()),
            warm_serves_cold_challenges(),
        );
        let urls: Vec<String> = (0..200).map(|i| format!("https://h.test/p{i}")).collect();
        let out = run_batch(shared.clone(), urls).await;

        assert_eq!(out.len(), 200, "exactly one result per input");
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "every request served browser-free off the warmed catalog"
        );
        let solved = total_solves(&shared);
        // The maintainer warms toward the whole catalog (persistent, fastest-first), so solves is
        // bounded by the exit count — never O(requests).
        assert!(
            (1..=20).contains(&solved),
            "solves are bounded by the catalog, not the 200 requests: {solved}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_domain_warms_each_domain_and_serves() {
        let solve: SolveFn = Arc::new(|_url, _exit| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                Ok((fake_clearance(), 0u32))
            })
        });
        let mut cfg = cfg_with(4);
        cfg.domains = (0..4).map(|i| Domain::solve(format!("h{i}.x"))).collect();
        let shared = test_shared(codes(20), cfg, solve, warm_serves_cold_challenges());
        let urls: Vec<String> = (0..160)
            .map(|i| format!("https://h{}.x/p{i}", i % 4))
            .collect();
        let out = run_batch(shared.clone(), urls).await;

        assert_eq!(out.len(), 160);
        assert!(out.iter().all(|r| r.result.is_ok()), "every host served");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unwarmable_host_gives_up_after_the_lease_timeout() {
        // Every solve fails (blocked) → no clearance is ever banked → exits cool out → the pool
        // stays resting. A persistently dead/unwarmable pool gives up after `lease_timeout` (rather
        // than hanging forever) — shortened here so the test is fast.
        let solve: SolveFn = Arc::new(|_url, _exit| Box::pin(async { Err(Reason::Blocked) }));
        let mut cfg = cfg_with(2);
        cfg.policy.lease_timeout = Duration::from_millis(150);
        let shared = test_shared(codes(3), cfg, solve, warm_serves_cold_challenges());
        let out = run_batch(shared, vec!["https://h.test/p".into()]).await;
        assert_eq!(out.len(), 1);
        assert!(
            out[0].result.is_err(),
            "an unwarmable host gives up, not hangs"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn raw_host_is_served_without_any_solve() {
        let solves = Arc::new(AtomicUsize::new(0));
        let mut cfg = raw_cfg(2);
        cfg.domains = vec![Domain::raw("api.raw")];
        let shared = test_shared(
            codes(4),
            cfg,
            counting_solve(solves.clone()),
            always_serves(),
        );
        let urls: Vec<String> = (0..20).map(|i| format!("https://api.raw/p{i}")).collect();
        let out = run_batch(shared.clone(), urls).await;

        assert_eq!(out.len(), 20);
        assert!(out.iter().all(|r| r.result.is_ok()), "raw host served");
        assert_eq!(total_solves(&shared), 0, "the raw path never solves");
        assert_eq!(solves.load(Ordering::SeqCst), 0);
    }

    fn one_exit_challenges() -> SlimFn {
        Arc::new(|_url, exit, _has_clearance| {
            if exit == "socks5h://e0" {
                Err(Reason::Challenged)
            } else {
                Ok(b"<html>raw</html>".to_vec())
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_challenged_exit_does_not_block_the_scrape() {
        let solves = Arc::new(AtomicUsize::new(0));
        let mut cfg = raw_cfg(2);
        cfg.domains = vec![Domain::raw("shop.io")];
        let shared = test_shared(
            codes(4),
            cfg,
            counting_solve(solves.clone()),
            one_exit_challenges(),
        );
        let urls: Vec<String> = (0..20).map(|i| format!("https://shop.io/p{i}")).collect();
        let out = run_batch(shared.clone(), urls).await;

        assert_eq!(out.len(), 20);
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "every request rotated off the one bad exit and served raw"
        );
        assert_eq!(
            solves.load(Ordering::SeqCst),
            0,
            "the raw path never solves"
        );
        assert!(shared.solve_domain_for("shop.io").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unconfigured_host_is_rejected_before_any_work() {
        let solves = Arc::new(AtomicUsize::new(0));
        let shared = test_shared(
            codes(4),
            raw_cfg(2), // no domains — nothing is configured
            counting_solve(solves.clone()),
            warm_serves_cold_challenges(),
        );
        let out = run_batch(shared.clone(), vec!["https://shop.cf/p".into()]).await;

        assert_eq!(out.len(), 1);
        assert!(
            matches!(&out[0].result, Err(FetchError::Unconfigured { host }) if host == "shop.cf"),
            "an unconfigured host fails Unconfigured up front — never silently fetched raw"
        );
        assert_eq!(
            total_solves(&shared),
            0,
            "no exit was leased, no browser launched"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn headed_job_on_an_unconfigured_host_is_also_rejected_up_front() {
        // `Job::Headed` (what `Client::fetch_browser` submits) must hit the same pre-flight gate
        // as `Job::Html` — it used to fall straight through to the work queue, launching a browser
        // on an unconfigured host instead of failing fast like the browser-free path.
        let shared = test_shared(
            codes(2),
            raw_cfg(2), // no domains — nothing is configured
            counting_solve(Arc::new(AtomicUsize::new(0))),
            warm_serves_cold_challenges(),
        );
        let pool = WorkerPool::spawn(shared.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let exec: HeadedExec = Box::new(move |res| {
            Box::pin(async move {
                let _ = tx.send(res);
            })
        });
        pool.submit(Job::Headed {
            url: "https://shop.cf/".into(),
            exec,
            attempts: 3,
        })
        .await
        .unwrap();

        match rx.await.unwrap() {
            Err(FetchError::Unconfigured { host }) => assert_eq!(host, "shop.cf"),
            other => panic!(
                "a headed job on an unconfigured host must also fail Unconfigured up front, got {}",
                match other {
                    Ok(_) => "Ok(_)".to_string(),
                    Err(e) => format!("Err({e:?})"),
                }
            ),
        }
        assert_eq!(
            total_solves(&shared),
            0,
            "no exit was leased, no browser launched"
        );
        pool.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn raw_host_that_is_actually_cf_gives_up_challenged() {
        // Configured raw, but the origin is Cloudflare-protected (challenges every exit). The raw
        // path can't solve, so the resource exhausts its rotation and gives up `Challenged` — the
        // signal to reconfigure it as a solve domain.
        let solves = Arc::new(AtomicUsize::new(0));
        let mut cfg = raw_cfg(2);
        cfg.domains = vec![Domain::raw("shop.cf")];
        let shared = test_shared(
            codes(4),
            cfg,
            counting_solve(solves.clone()),
            warm_serves_cold_challenges(),
        );
        let out = run_batch(shared.clone(), vec!["https://shop.cf/p".into()]).await;

        assert_eq!(out.len(), 1);
        assert!(
            matches!(out[0].result, Err(FetchError::GaveUp(Reason::Challenged))),
            "a configured-raw host that is actually CF gives up Challenged"
        );
        assert_eq!(
            total_solves(&shared),
            0,
            "the raw path never launches a browser"
        );
    }

    /// A solve that fails on one specific exit (`e1`) but succeeds elsewhere. Proves the
    /// tail-latency guarantee: the request is finished by the exit that warms (`e0`), never blocked
    /// waiting on the exit that stays cold — a cold/warming worker never claims the resource.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cold_exit_never_blocks_a_warm_one_from_finishing() {
        let solve: SolveFn = Arc::new(|_url, exit| {
            Box::pin(async move {
                if exit == "socks5h://e1" {
                    Err(Reason::Blocked) // e1 never warms
                } else {
                    Ok((fake_clearance(), 0u32))
                }
            })
        });
        let shared = test_shared(codes(2), cfg_with(2), solve, warm_serves_cold_challenges());
        let out = run_batch(
            shared,
            (0..10).map(|i| format!("https://h.test/p{i}")).collect(),
        )
        .await;
        assert_eq!(out.len(), 10);
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "the warm exit finishes every request; the cold one never blocks it"
        );
    }

    /// A per-IP rate ceiling paces one exit: N requests on a single exit take at least (N-1)
    /// intervals, and every request still serves (a fully-paced warm set spaces out, never gives up).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pacing_holds_one_exit_to_its_interval() {
        let solve: SolveFn =
            Arc::new(|_url, _exit| Box::pin(async { Ok((fake_clearance(), 0u32)) }));
        let mut cfg = cfg_with(1);
        // 750/min = 80ms spacing between requests on the one exit.
        cfg.domains = vec![Domain::solve(RATE_HOST).per_ip(750)];
        let shared = test_shared(codes(1), cfg, solve, warm_serves_cold_challenges());
        let started = Instant::now();
        let out = run_batch(
            shared,
            (0..4).map(|i| format!("https://h.test/p{i}")).collect(),
        )
        .await;
        let elapsed = started.elapsed();
        assert_eq!(out.len(), 4);
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "paced requests still all serve"
        );
        // 4 requests, 3 gaps of 80ms ≈ 240ms; allow generous slack for timer jitter but well above
        // the unpaced ~0ms.
        assert!(
            elapsed >= Duration::from_millis(160),
            "pacing must space the requests (took {elapsed:?}, expected ≥ ~240ms)"
        );
    }

    /// An **aggregate** cap holds the *whole pool* to N req/min regardless of exit count: a raw host
    /// with many exits still can't exceed the pool-wide rate (the Algolia-per-key case).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn aggregate_cap_holds_the_whole_pool_to_the_rate() {
        // 6000/min = 10ms between sends, pool-wide. 8 requests → ≥ 7 gaps ≈ 70ms, even with 8 exits
        // that could otherwise fire in parallel.
        let mut cfg = raw_cfg(2);
        cfg.domains = vec![Domain::raw("api.raw").aggregate(6000)];
        let shared = test_shared(
            codes(8),
            cfg,
            counting_solve(Arc::new(AtomicUsize::new(0))),
            always_serves(),
        );
        let started = Instant::now();
        let out = run_batch(
            shared,
            (0..8).map(|i| format!("https://api.raw/p{i}")).collect(),
        )
        .await;
        let elapsed = started.elapsed();
        assert_eq!(out.len(), 8);
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "all served under the aggregate cap"
        );
        assert!(
            elapsed >= Duration::from_millis(50),
            "the pool-wide cap must space sends across all exits (took {elapsed:?}, expected ≥ ~70ms)"
        );
    }

    /// A solve host where warming succeeds but slim challenges even *with* a fresh clearance, and
    /// slim never once serves — the broken-fingerprint-triple shape. That's unwinnable by retry, so
    /// the resource gives up as `FingerprintMismatch` (after the rotation budget) rather than
    /// looping forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn broken_fingerprint_gives_up_as_mismatch() {
        let solve: SolveFn =
            Arc::new(|_url, _exit| Box::pin(async { Ok((fake_clearance(), 0u32)) }));
        let always_challenges: SlimFn = Arc::new(|_url, _exit, _warm| Err(Reason::Challenged));
        // More exits than the rotation budget: each challenge benches its exit (escalating), so the
        // give-up comes from the budget on fresh exits, not from waiting out a cooldown.
        let shared = test_shared(codes(6), cfg_with(2), solve, always_challenges);
        let out = run_batch(shared, vec!["https://h.test/p".into()]).await;
        assert_eq!(out.len(), 1);
        assert!(
            matches!(out[0].result, Err(FetchError::FingerprintMismatch { .. })),
            "slim never serving despite fresh cookies is flagged as a fingerprint mismatch, not an endless loop"
        );
    }

    /// A solve that redirects to a different host, where the landed-host clearance still validates
    /// fine against the *configured* host (a harmless redirect) — the self-verify should bank it
    /// normally, exactly as an unredirected solve, with no domain flagged misconfigured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn harmless_redirect_is_verified_and_banked_normally() {
        let solve: SolveFn = Arc::new(|_url, _exit| {
            Box::pin(async {
                Ok((
                    Clearance::new(
                        vec![("cf_clearance".into(), "t".into())],
                        "UA".into(),
                        None,
                        "redirected.test".into(), // lands elsewhere, but the clearance still works
                    ),
                    0u32,
                ))
            })
        });
        let shared = test_shared(codes(4), cfg_with(4), solve, warm_serves_cold_challenges());
        let urls: Vec<String> = (0..20).map(|i| format!("https://h.test/p{i}")).collect();
        let out = run_batch(shared.clone(), urls).await;

        assert_eq!(out.len(), 20);
        assert!(
            out.iter().all(|r| r.result.is_ok()),
            "a harmless redirect still serves fine"
        );
        assert!(
            shared.misconfigured_domains().is_empty(),
            "a redirect whose clearance validates is never flagged misconfigured"
        );
    }

    /// The structural-misconfiguration shape end to end: "bad.test"'s solve always redirects to a
    /// host whose clearance never validates against "bad.test" (confirmed by the self-verify), while
    /// "good.test" shares the same exit pool and works fine throughout. Proves: the domain gets
    /// flagged (with the landed host, for the operator-facing summary), a job submitted afterward
    /// fails **fast** with `MisconfiguredHost` rather than hanging or silently escalating to a real
    /// browser forever, and the healthy domain is never affected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn structurally_misconfigured_domain_gives_up_fast_without_affecting_a_healthy_one() {
        let solve: SolveFn = Arc::new(|url, _exit| {
            Box::pin(async move {
                if url.contains("bad.test") {
                    Ok((
                        Clearance::new(
                            vec![("cf_clearance".into(), "t".into())],
                            "UA".into(),
                            None,
                            "redirected.test".into(),
                        ),
                        0u32,
                    ))
                } else {
                    Ok((fake_clearance(), 0u32))
                }
            })
        });
        let slim: SlimFn = Arc::new(|url, _exit, has_clearance| {
            if url.contains("bad.test") {
                Err(Reason::Challenged) // never validates, warm or not
            } else if has_clearance {
                Ok(b"<html>good</html>".to_vec())
            } else {
                Err(Reason::Challenged)
            }
        });

        let mut cfg = cfg_with(4);
        cfg.domains = vec![Domain::solve("good.test"), Domain::solve("bad.test")];
        // Every exit's fake solve is instant, so — unlike a real run, where solves are staggered by
        // real browser time — all exits can hit "bad.test"'s broken self-verify in lockstep and cool
        // simultaneously (one cooldown per exit, not per domain, is deliberate — see CLAUDE.md).
        // Shrink the cooldown so that harmless thundering-herd doesn't make the test slow.
        cfg.policy.transient_cooldown = Duration::from_millis(2);
        cfg.policy.burn_cooldown = Duration::from_millis(10);
        // The domain-level backoff (once trust lands, escalating from `base`) would otherwise wait
        // up to a minute between confirmations — shrink it so reaching the confirm threshold doesn't
        // depend on winning a race against the first backoff window.
        cfg.policy.redirect_mismatch_backoff_base = Duration::from_millis(2);
        cfg.policy.redirect_mismatch_backoff_max = Duration::from_millis(10);
        let shared = test_shared(codes(6), cfg, solve, slim);
        let pool = WorkerPool::spawn(shared.clone());

        // Drive a few "good.test" requests through so slim independently proves itself healthy —
        // the gate (`fingerprint_ok || slim_ever_succeeded`) that lets a "bad.test" self-verify
        // failure be trusted as domain-specific evidence rather than a possibly-broken fingerprint
        // triple no fresh clearance could ever satisfy.
        let (tx, rx) = async_channel::bounded(4);
        for i in 0..4 {
            pool.submit(Job::Html {
                resource: Resource::from(format!("https://good.test/p{i}")),
                index: i,
                started: Instant::now(),
                results: tx.clone(),
                attempts: shared.cfg.policy.max_attempts,
            })
            .await
            .unwrap();
        }
        drop(tx);
        let mut good_results = Vec::new();
        while let Ok(fr) = rx.recv().await {
            good_results.push(fr);
        }
        assert!(
            good_results.iter().all(|r| r.result.is_ok()),
            "the healthy domain serves fine throughout"
        );

        // Give the maintainer a chance to confirm "bad.test" is structurally broken. Polled rather
        // than a fixed sleep to avoid flakiness while staying fast — it settles in ~milliseconds
        // normally; the generous ceiling is headroom against a heavily contended test machine
        // (many parallel tests' tokio runtimes competing for CPU), not the expected wait.
        let confirmed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if shared
                    .misconfigured_domains()
                    .iter()
                    .any(|(h, _)| h == "bad.test")
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            confirmed.is_ok(),
            "bad.test should be confirmed misconfigured"
        );
        assert_eq!(
            shared.misconfigured_domains(),
            vec![("bad.test".to_string(), "redirected.test".to_string())],
            "reports the landed host so the operator knows what to register instead"
        );

        // A job submitted *after* confirmation fails fast — no hang, no silent headed escalation.
        let (tx2, rx2) = async_channel::bounded(1);
        pool.submit(Job::Html {
            resource: Resource::from("https://bad.test/p".to_string()),
            index: 0,
            started: Instant::now(),
            results: tx2,
            attempts: shared.cfg.policy.max_attempts,
        })
        .await
        .unwrap();
        let fr = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .expect("a confirmed-misconfigured domain must fail fast, not hang")
            .unwrap();
        assert!(
            matches!(
                &fr.result,
                Err(FetchError::MisconfiguredHost { host, landed })
                    if host == "bad.test" && landed == "redirected.test"
            ),
            "got {:?}",
            fr.result.err()
        );

        pool.shutdown().await;
    }
}
