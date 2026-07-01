use std::collections::HashMap;
use std::path::Path;
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

use mara_core::host_of;
use mara_solver::browser::{Browser, Cleared, SolveConfig};

use crate::classify::Reason;
use crate::clearance::Clearance;
use crate::client::{Config, Domain, FetchError, FetchResult, Outcome, Resource, per_min_interval};
use crate::egress::{Availability, ExitStatus, Lease};
use crate::introspect::Introspector;
use crate::ladder::{self, HeadedAction, Step};
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
    /// Configured domains (solve flag + per-IP / aggregate rates), suffix-matched (longest wins).
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
}

impl Shared {
    pub fn new(
        cfg: Config,
        egress: Arc<ExitPool>,
        persistence: Arc<Persistence>,
        introspect: Arc<Introspector>,
    ) -> Arc<Self> {
        let browser_permits = Arc::new(Semaphore::new(cfg.browsers.max(1)));
        let domains = Mutex::new(cfg.domains.clone());
        Arc::new(Shared {
            cfg,
            egress,
            persistence,
            introspect,
            browser_permits,
            slim_clients: Mutex::new(HashMap::new()),
            slim_ever_succeeded: AtomicBool::new(false),
            domains,
            aggregate_pacers: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            solve_override: None,
            slim_override: None,
            req_counter: AtomicU64::new(0),
        })
    }

    /// Register `host` as a solve domain (explicit registration; no rate). A no-op if a domain
    /// already covers it.
    pub(crate) fn mark_solve_host(&self, host: &str) {
        if host.is_empty() {
            return;
        }
        let mut domains = self.domains.lock().unwrap();
        if !domains.iter().any(|d| d.host == host) {
            domains.push(Domain::solve(host));
        }
    }

    /// The configured domain covering `host` — **longest suffix wins** (exact match, or `host`
    /// ends with `.{domain}`), or `None` if no domain matches (a raw host).
    fn domain_for(&self, host: &str) -> Option<Domain> {
        self.domains
            .lock()
            .unwrap()
            .iter()
            .filter(|d| host == d.host || host.ends_with(&format!(".{}", d.host)))
            .max_by_key(|d| d.host.len())
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

    /// The solve-domain covering `host` (the clearance key the maintainer warmed under), or `None`
    /// if `host` is raw or matches only a non-solve domain.
    pub(crate) fn solve_domain_for(&self, host: &str) -> Option<String> {
        self.domain_for(host).filter(|d| d.solve).map(|d| d.host)
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
    /// claims a resource, so warm idle workers finish the stragglers.
    fn may_pull(&self) -> bool {
        if !self.shared.egress.is_claimable(&self.code) {
            return false;
        }
        // Pacing: if this exit is spacing out requests under a per-IP ceiling, don't pull yet.
        if self.shared.egress.paced_until(&self.code).is_some() {
            return false;
        }
        let domains = self.shared.solve_domains();
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
        self.deliver_err(job, err).await;
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
        // Solve hosts replay a clearance keyed by the registered solve-*domain* (what the maintainer
        // warmed), not the exact request host — so a suffix registration covers subdomains.
        let domain = self.shared.solve_domain_for(&host);
        let is_solve = domain.is_some();
        let clearance = domain
            .as_ref()
            .and_then(|d| self.shared.egress.warm(&exit_key, d));

        // A solve host we're not actually warm for (multi-domain edge / a runtime registration
        // race): don't hostage the resource — hand it back for a warm peer, no penalty.
        if is_solve && clearance.is_none() {
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
            match self
                .slim_request(&req, &exit_key, proxy.as_deref(), clearance.as_ref())
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
                    // A challenge on a solve host despite a fresh clearance. Drop it and bench the
                    // exit with an **escalating** cooldown (via `record_slim_challenge`): a one-off
                    // stale cookie cools briefly and recovers (its streak resets on the next serve),
                    // but a CF-flagged IP that never serves climbs to a long bench so the
                    // fastest-first maintainer stops re-warming it on loop. The *resource* is
                    // winnable, so it doesn't burn the rotation budget — retry forever on a good
                    // exit. The exception is a **broken fingerprint triple**: if slim has *never*
                    // once served, run down the budget and give up (FingerprintMismatch).
                    let key = domain.as_deref().unwrap_or(&host); // clearance keyed by the solve-domain
                    let pol = self.shared.policy();
                    self.shared.egress.record_slim_challenge(
                        &exit_key,
                        key,
                        pol.transient_cooldown,
                        pol.burn_cooldown,
                    );
                    lease.release(ExitStatus::Cooled);
                    if self.shared.slim_ever_succeeded.load(Ordering::Relaxed) {
                        tracing::debug!("stale clearance · re-warming and retrying");
                        self.requeue(Job::Html {
                            resource,
                            index,
                            started,
                            results,
                            attempts,
                        })
                        .await;
                    } else {
                        let left = attempts.saturating_sub(1);
                        if left > 0 {
                            self.requeue(Job::Html {
                                resource,
                                index,
                                started,
                                results,
                                attempts: left,
                            })
                            .await;
                        } else {
                            let err = replay_giveup(&self.shared, reason, label); // → FingerprintMismatch
                            self.deliver(index, &resource, &results, Err(err)).await;
                        }
                    }
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
                        "raw challenged on every exit · register the host as a solve domain"
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

    async fn deliver_err(&self, job: Job, err: FetchError) {
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
        let cfg = solve_config(&self.shared.cfg, &artifacts, self.shared.cfg.timeout);
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
                tracing::warn!(error = %e, "browser launch failed");
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
                self.record_solve(exit_key, &host_of(url).unwrap_or_default(), clearance);
                Ok((browser, page, clicks))
            }
            Err(reason) => {
                browser.close().await;
                Err(reason)
            }
        }
    }

    // ── shared helpers ──────────────────────────────────────────────────────────────────

    /// Bank a freshly-lifted clearance on the exit (surfacing any fingerprint drift first).
    fn record_solve(&self, exit_key: &str, host: &str, clearance: Clearance) {
        self.shared.egress.check_fingerprint(&clearance.user_agent);
        self.shared
            .egress
            .record_clearance(exit_key, host, clearance);
    }

    async fn slim_request(
        &self,
        req: &Request,
        exit_key: &str,
        proxy: Option<&str>,
        clearance: Option<&Clearance>,
    ) -> Result<Vec<u8>, Reason> {
        let started = Instant::now();
        let result = match &self.shared.slim_override {
            Some(f) => f(req.url.clone(), exit_key.to_string(), clearance.is_some()),
            None => slim::fetch(&self.shared.slim_client(proxy)?, req, clearance).await,
        };
        if result.is_ok() {
            self.shared
                .egress
                .record_success(exit_key, started.elapsed());
            self.shared
                .slim_ever_succeeded
                .store(true, Ordering::Relaxed);
        }
        result
    }

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
            let domains = self.shared.solve_domains();
            if domains.is_empty() {
                // Pure-raw workload: nothing to warm. Wait for a (possibly runtime-registered)
                // solve domain or a shutdown, cheaply.
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
            match self.solve(&url, &exit_key).await {
                Ok((clearance, clicks)) => {
                    tracing::info!(clicks, "warmed host in browser");
                    let how = if clicks > 0 {
                        format!("solved in {clicks} click(s)")
                    } else {
                        "cleared".into()
                    };
                    self.record_solve(&exit_key, host, clearance);
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
        drop(permit);
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
                tracing::warn!(error = %e, "browser launch failed");
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

    fn record_solve(&self, exit_key: &str, host: &str, clearance: Clearance) {
        self.shared.egress.check_fingerprint(&clearance.user_agent);
        self.shared
            .egress
            .record_clearance(exit_key, host, clearance);
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

fn solve_config(cfg: &Config, artifact_dir: &Path, timeout: Duration) -> SolveConfig {
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
    }
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

/// The URL minus its scheme, for a compact `fetch` span field (`fragrantica.com/search/`).
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

    const RATE_HOST: &str = "test";

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
            domains,
            aggregate_pacers: Mutex::new(HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            solve_override: Some(solve),
            slim_override: Some(slim),
            req_counter: AtomicU64::new(0),
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

    /// `*.test` hosts take the warm/solve path (the solve-set holds `"test"`, which suffix-matches).
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
        let shared = test_shared(
            codes(4),
            raw_cfg(2),
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
        let shared = test_shared(
            codes(4),
            raw_cfg(2),
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
    async fn unregistered_cf_host_gives_up_without_warming() {
        let solves = Arc::new(AtomicUsize::new(0));
        let shared = test_shared(
            codes(4),
            raw_cfg(2),
            counting_solve(solves.clone()),
            warm_serves_cold_challenges(),
        );
        let out = run_batch(shared.clone(), vec!["https://shop.cf/p".into()]).await;

        assert_eq!(out.len(), 1);
        assert!(
            matches!(out[0].result, Err(FetchError::GaveUp(Reason::Challenged))),
            "an unregistered CF host gives up Challenged — register it in solve_domains"
        );
        assert_eq!(total_solves(&shared), 0, "no browser was launched");
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
}
