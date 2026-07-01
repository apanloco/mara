use std::sync::Mutex;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::clearance::Clearance;
use crate::egress::{Availability, ExitStatus, Lease};
use crate::introspect::{ExitRow, Introspector};
use crate::store::{Cooling, ExitData, Persistence, StoreSnapshot};

/// Re-probe interval for a **confirmed** (`Ready`/`Wonky`) exit — an occasional re-confirm.
const REPROBE_AFTER: Duration = Duration::from_secs(60);
/// Re-probe interval for an **unconfirmed** (`Probing`) exit — we're actively trying to confirm
/// it, so retry fast instead of leaving a relay whose first probe failed (e.g. a startup-congestion
/// blip) stuck `probing` for a full minute.
const PROBE_RETRY: Duration = Duration::from_secs(5);
/// After this many consecutive failed probes, a still-`Probing` exit is demoted to `Wonky` — it's
/// unreachable, not "being checked", so it leaves the probing bucket (and re-probes slowly).
const PROBE_FAILS_TO_WONKY: u32 = 3;
/// Probe connect-timeout when no latency cap is set (any latency is acceptable, so wait a while
/// to confirm reachability). With a cap, the timeout is the cap instead — see [`connect_probe_for`].
const PROBE_TIMEOUT_UNCAPPED: Duration = Duration::from_secs(5);

/// A generic exit descriptor: where the exit is and how to reach it. Source-agnostic —
/// the Mullvad catalog produces these, and so does a manual `--exit`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExitRecord {
    pub country: String,
    pub code: String,
    /// The SOCKS endpoint (`host:port`), or `None` for the direct exit.
    pub socks: Option<String>,
}

impl ExitRecord {
    /// The proxy URL to dial, or `None` for the direct exit (no SOCKS endpoint). The store
    /// key for direct then derives as `""` (see [`Exit::key`]) and its lease carries no proxy.
    pub(crate) fn proxy_url(&self) -> Option<String> {
        self.socks.as_ref().map(|s| format!("socks5h://{s}"))
    }
}

fn socks_host(url: &str) -> String {
    url.strip_prefix("socks5h://")
        .or_else(|| url.strip_prefix("socks5://"))
        .unwrap_or(url)
        .to_string()
}

fn manual_exit(url: String) -> Exit {
    Exit {
        rec: ExitRecord {
            country: "manual".into(),
            code: url.clone(),
            socks: Some(socks_host(&url)),
        },
        health: ExitHealth::Ready,
        activity: Activity::Idle,
        latency: None,
        last_probe: None,
        probe_failures: 0,
        over_cap: false,
        data: ExitData::default(),
        last_disposition: None,
    }
}

/// The exit's durable disposition — set by the probe and lease lifecycle. A slim **timeout**
/// also demotes a `Ready` exit back to `Probing` (it stopped answering — unconfirmed), so it
/// can't be leased again until a probe re-confirms it. *Cooling* is deliberately NOT here: it
/// lives in `ExitData` as the single cooldown field, so an exit is never simultaneously
/// "Ready in `health`" and "cooling in `data`" by hand-sync.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExitHealth {
    Probing,
    Ready,
    Wonky,
}

/// What the exit is doing *right now*, set by the single worker that holds its lease.
/// Orthogonal to health and warmth. `Idle` means free-to-lease; the other two both mean
/// "held by a worker" (so not leasable) — `Serving` a slim request, `Solving` a headed browser.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Activity {
    Idle,
    Serving,
    Solving,
}

/// One exit, all of its per-exit state in one place. The facets are orthogonal —
/// **health** (durable disposition), **activity** (what it's doing now), and **warmth +
/// cooldown** (both in `data`) — mutated by a single owner. The dashboard projects them
/// to one badge by priority.
pub(crate) struct Exit {
    rec: ExitRecord,
    health: ExitHealth,
    activity: Activity,
    latency: Option<Duration>,
    last_probe: Option<Instant>,
    /// Consecutive failed probes (reset on any success). Drives the back-off-to-`Wonky` so a
    /// persistently-unreachable relay leaves the `Probing` bucket instead of looking like it's
    /// still being checked.
    probe_failures: u32,
    /// The last probe **timed out** over the cap — benched as `slow` but with no usable `latency`
    /// measurement (UI shows `n/a`, not a fake number). Cleared on any measured probe.
    over_cap: bool,
    data: ExitData,
    /// The last UI [`Disposition`] streamed for this exit. A delta is emitted only when the
    /// disposition changes (see [`note_row`]) — so a single mutation sends one small row, not the
    /// whole catalog, and a stat-only tick (which doesn't change the disposition) sends nothing.
    last_disposition: Option<Disposition>,
}

/// The badge-relevant projection of an exit — what the dashboard actually *shows* as its state,
/// excluding monotonic counters (raw stats ride along in the row but don't trigger a delta) and
/// raw latency (bucketed, so jitter is quiet but a real shift surfaces). Equality decides whether
/// a mutation is worth a delta.
#[derive(Clone, Copy, PartialEq)]
struct Disposition {
    health: ExitHealth,
    activity: Activity,
    over_latency: bool,
    cooling: bool,
    cooling_reason: Option<Cooling>,
    warm: bool,
    paced: bool,
    latency_bucket: Option<u64>,
}

impl Exit {
    fn key(&self) -> String {
        self.rec.proxy_url().unwrap_or_default()
    }

    /// Leasable = ready, idle, and not cooling — the one place the three facets combine
    /// for the lease path.
    fn leasable(&self) -> bool {
        self.health == ExitHealth::Ready
            && self.activity == Activity::Idle
            && !self.data.is_cooling()
    }

    fn due(&self, now: Instant) -> bool {
        match self.data.cooling_until() {
            Some(until) => now >= until,
            // An unconfirmed (`Probing`) exit retries fast; a settled one re-confirms slowly.
            None => {
                let interval = if self.health == ExitHealth::Probing {
                    PROBE_RETRY
                } else {
                    REPROBE_AFTER
                };
                self.last_probe
                    .is_none_or(|t| now.duration_since(t) >= interval)
            }
        }
    }

    /// Apply a probe outcome. This is the **single writer of `latency`** and the sole place a
    /// probe turns into state, so the latency update can never be written apart from the health
    /// transition it implies. Returns whether the exit just (re-)confirmed `Ready` (the caller
    /// then wakes lease waiters). Emits its own log lines under the monitor's `exit{code}`
    /// span, so they carry no redundant `code`.
    fn observe_probe(&mut self, now: Instant, outcome: ProbeOutcome) -> bool {
        self.last_probe = Some(now);
        let latency = match outcome {
            ProbeOutcome::Ok { latency } => latency,
            // Over the cap, no usable measurement: bench as `slow` with latency unknown (→ `n/a`),
            // not a fake number. It answered (just too slowly), so it doesn't count toward wonky;
            // `over_cap` makes it read `slow` and `Ready` takes it out of the `probing` bucket.
            ProbeOutcome::TooSlow => {
                self.probe_failures = 0;
                self.over_cap = true;
                self.latency = None;
                if self.activity == Activity::Idle {
                    self.health = ExitHealth::Ready;
                }
                return false; // not leasable → don't wake lease waiters
            }
            // A failed connect. While the exit is held by a worker, health is the worker's, not the
            // probe's — leave it. Otherwise count the failure and, once it's clearly unreachable,
            // demote `Probing` → `Wonky` so it stops looking like it's still being confirmed.
            ProbeOutcome::Transient => {
                if self.activity == Activity::Idle {
                    self.probe_failures = self.probe_failures.saturating_add(1);
                    if self.health == ExitHealth::Probing
                        && self.probe_failures >= PROBE_FAILS_TO_WONKY
                    {
                        self.health = ExitHealth::Wonky;
                        tracing::warn!(
                            failures = self.probe_failures,
                            "probe unreachable · benched (probing → wonky)"
                        );
                    }
                }
                return false; // latency untouched
            }
        };
        self.probe_failures = 0;
        self.over_cap = false;
        if let Some(prev) = self.latency.replace(latency)
            && latency_shifted(prev, latency)
        {
            tracing::info!(
                from_ms = prev.as_millis() as u64,
                to_ms = latency.as_millis() as u64,
                activity = ?self.activity,
                "re-probe latency shift",
            );
        }
        // While the exit is held by a worker its health is the worker's to change, not the
        // probe's — the probe only refreshes the latency above. A reachable probe of an *idle*
        // exit (re-)confirms it Ready.
        if self.activity != Activity::Idle {
            return false;
        }
        let was_ready = self.health == ExitHealth::Ready;
        self.health = ExitHealth::Ready;
        if !was_ready {
            tracing::info!(
                latency_ms = latency.as_millis() as u64,
                "probe confirmed ready"
            );
        }
        !was_ready
    }

    /// Benched by the latency cap: either a probe that timed out over the cap (`over_cap`, no
    /// measurement) or a measured latency above it. Either way it reads `slow` and isn't leasable.
    fn over_latency(&self, max_latency: Option<Duration>) -> bool {
        self.over_cap || matches!((max_latency, self.latency), (Some(cap), Some(l)) if l > cap)
    }

    /// The badge-relevant projection (no counters, latency bucketed) — drives delta emission.
    fn disposition(&self, max_latency: Option<Duration>) -> Disposition {
        Disposition {
            health: self.health,
            activity: self.activity,
            over_latency: self.over_latency(max_latency),
            cooling: self.data.is_cooling(),
            cooling_reason: self.data.cooling(),
            warm: self.data.has_warm(),
            paced: self.data.is_paced(),
            // Bucket to 25ms so jitter stays quiet but a real shift (or going slow) surfaces.
            latency_bucket: self.latency.map(|d| d.as_millis() as u64 / 25),
        }
    }

    /// The full UI row — everything the dashboard shows for this exit, in one projection (the
    /// badge facets ∪ the cumulative stats). Sent as a delta when the disposition changes.
    fn row(&self, now: Instant, max_latency: Option<Duration>) -> ExitRow {
        let health = match self.health {
            ExitHealth::Probing => "probing",
            ExitHealth::Ready => "ready",
            ExitHealth::Wonky => "wonky",
        };
        let activity = match self.activity {
            Activity::Idle => "idle",
            Activity::Serving => "serving",
            Activity::Solving => "solving",
        };
        ExitRow {
            code: self.rec.code.clone(),
            country: self.rec.country.clone(),
            health: health.to_string(),
            activity: activity.to_string(),
            latency_ms: self.latency.map(|d| d.as_millis() as u64),
            last_probe_unix: self
                .last_probe
                .map(|t| crate::clearance::now_unix() - now.duration_since(t).as_secs_f64()),
            proxy_url: self.rec.proxy_url().unwrap_or_default(),
            over_latency: self.over_latency(max_latency),
            warm: self.data.has_warm(),
            paced: self.data.is_paced(),
            cooling: self.data.is_cooling(),
            cooling_reason: self.data.cooling(),
            stats: self.data.stats(),
        }
    }
}

/// Diff an exit's disposition against the last one streamed; if it changed, record the new one and
/// return the row to emit. The single decision point for "does this mutation deserve a delta?" —
/// called by the pool's mutation funnel and the monitor sweep, holding the `exits` lock. Emission
/// (the broadcast) happens *after* the lock is released.
fn note_row(e: &mut Exit, now: Instant, max_latency: Option<Duration>) -> Option<ExitRow> {
    let d = e.disposition(max_latency);
    if e.last_disposition == Some(d) {
        return None;
    }
    e.last_disposition = Some(d);
    Some(e.row(now, max_latency))
}

/// The verdict of a single probe of one exit.
pub enum ProbeOutcome {
    /// Connected, with the measured connect-RTT.
    Ok { latency: Duration },
    /// Connect didn't finish within the cap-tied timeout (only with a latency cap set): over the
    /// cap, but **no usable measurement** — benched as `slow` with latency shown `n/a`, not a fake number.
    TooSlow,
    /// Connect errored (refused/reset/DNS) — the relay is down. Don't touch latency/health.
    Transient,
}

/// An injected per-exit liveness probe: given a proxy URL, decide the exit's [`ProbeOutcome`].
/// Both Mullvad and manual pools use [`connect_probe_for`] — a plain TCP-connect to the SOCKS
/// endpoint. (Reaching a `*.relays.mullvad.net` SOCKS port already proves it's a Mullvad
/// relay, so no `am.i.mullvad` round-trip is needed on the hot path.)
pub type Probe = Arc<dyn Fn(String) -> BoxFuture<'static, ProbeOutcome> + Send + Sync>;

/// The probe connect-timeout and whether a timeout means "too slow" (vs "retry"). Tied to the
/// latency cap: ~2× the cap (floored at 1 s, capped at 10 s) when set — no point waiting longer
/// than the slowest latency we'd lease — and a timeout is then the verdict "slower than the cap".
/// Uncapped, any latency is acceptable, so it's a flat timeout and a miss just means retry.
fn probe_timeout(max_latency: Option<Duration>) -> (Duration, bool) {
    match max_latency {
        Some(cap) => (
            (cap * 2).clamp(Duration::from_secs(1), Duration::from_secs(10)),
            true,
        ),
        None => (PROBE_TIMEOUT_UNCAPPED, false),
    }
}

/// Build the liveness probe — a TCP-connect to the SOCKS endpoint, timed. Confirms the proxy is
/// accepting connections and yields a clean reach-RTT; no external HTTP dependency.
///
/// The connect timeout is **tied to the latency cap**: there's no point waiting longer than the
/// slowest latency we'd ever lease. With `--max-exit-latency` set, the timeout is ~2× the cap and a
/// timeout is the verdict *"slower than the cap"* → [`ProbeOutcome::TooSlow`], which benches the
/// exit as `slow` with **no measured latency** (shown `n/a`), **not** a transient failure that would
/// leave it stuck `probing`. This is
/// what keeps the probe wave draining fast (a slow/far relay frees its concurrency slot in ~2× the
/// cap, not the old fixed 10 s) so the 64-wide pool refills against the next exits instead of
/// stalling on stragglers. A real connection *error* (refused/reset/DNS) is always `Transient`
/// (the relay is down — retry later). Without a cap, any latency is acceptable, so the timeout is a
/// flat [`PROBE_TIMEOUT_UNCAPPED`] and a timeout just means retry.
pub(crate) fn connect_probe_for(max_latency: Option<Duration>) -> Probe {
    let (timeout, slow_on_timeout) = probe_timeout(max_latency);
    Arc::new(move |url: String| {
        Box::pin(async move {
            let addr = socks_host(&url);
            let started = Instant::now();
            match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
                Ok(Ok(_)) => ProbeOutcome::Ok {
                    latency: started.elapsed(),
                },
                Ok(Err(_)) => ProbeOutcome::Transient, // refused/reset/DNS — the relay is down
                // Timed out: with a cap this is "slower than we'll accept" → `TooSlow` (reads `slow`,
                // frees the slot now, no fake latency); without a cap we can't judge → retry.
                Err(_) => {
                    if slow_on_timeout {
                        ProbeOutcome::TooSlow
                    } else {
                        ProbeOutcome::Transient
                    }
                }
            }
        })
    })
}

/// The single synthetic exit for direct (no-proxy) egress. Its `socks: None` makes
/// `proxy_url()` return `None`, so its lease carries no proxy and its store key is `""`.
fn direct_exit() -> Exit {
    Exit {
        rec: ExitRecord {
            country: "direct".into(),
            code: "direct".into(),
            socks: None,
        },
        health: ExitHealth::Ready,
        activity: Activity::Idle,
        latency: None,
        last_probe: None,
        probe_failures: 0,
        over_cap: false,
        data: ExitData::default(),
        last_disposition: None,
    }
}

/// A probe that always succeeds — for the direct exit, which has no proxy to connect to.
fn always_ok_probe(_url: String) -> BoxFuture<'static, ProbeOutcome> {
    Box::pin(async {
        ProbeOutcome::Ok {
            latency: Duration::ZERO,
        }
    })
}

pub struct ExitPool {
    exits: Mutex<Vec<Exit>>,
    max_latency: Option<Duration>,
    lease_timeout: Duration,
    probe_concurrency: usize,
    probe: Probe,
    ready: Arc<Notify>,
    persistence: Arc<Persistence>,
    introspect: Arc<Introspector>,
    monitor: Mutex<Option<JoinHandle<()>>>,
    /// Per-exit **worker wakes**, keyed by `code`. In the one-worker-per-exit model each exit has
    /// exactly one interested serving worker; whenever an exit's disposition changes (a clearance
    /// banked by the maintainer, a cooldown lapsing, a probe re-confirming it) its worker is woken
    /// so it re-evaluates whether it can now serve — a *per-exit* signal, not a broadcast, so a
    /// 500-exit catalog doesn't fan every change out to every worker.
    worker_wakes: Mutex<std::collections::HashMap<String, Arc<Notify>>>,
}

impl Drop for ExitPool {
    fn drop(&mut self) {
        if let Some(h) = self.monitor.lock().unwrap().take() {
            h.abort();
        }
    }
}

impl ExitPool {
    pub fn manual(
        urls: Vec<String>,
        introspect: Arc<Introspector>,
        persistence: Arc<Persistence>,
        max_latency: Option<Duration>,
        lease_timeout: Duration,
        probe_concurrency: usize,
    ) -> Arc<Self> {
        let exits = urls.into_iter().map(manual_exit).collect();
        ExitPool::spawn(
            exits,
            connect_probe_for(max_latency),
            max_latency,
            lease_timeout,
            probe_concurrency,
            persistence,
            introspect,
        )
    }

    /// Direct egress: a pool of one always-ready exit with no proxy. `can_rotate()` is then
    /// false (a lone exit can't be rotated away from), which makes the ladder fail rather
    /// than spin on the same IP — matching the old dedicated direct path.
    pub fn direct(
        introspect: Arc<Introspector>,
        persistence: Arc<Persistence>,
        lease_timeout: Duration,
    ) -> Arc<Self> {
        ExitPool::spawn(
            vec![direct_exit()],
            Arc::new(always_ok_probe),
            None,
            lease_timeout,
            1,
            persistence,
            introspect,
        )
    }

    /// Whether a worker can rotate to a *different* exit on a bad reason. False for direct or
    /// any single-exit pool: there is nowhere else to go.
    pub fn can_rotate(&self) -> bool {
        self.exit_count() > 1
    }

    /// Build a pool from pre-built exits and an injected liveness probe, and start the
    /// background monitor. Sources (Mullvad catalog, manual) call this with their own
    /// exits and probe.
    pub(crate) fn spawn(
        exits: Vec<Exit>,
        probe: Probe,
        max_latency: Option<Duration>,
        lease_timeout: Duration,
        probe_concurrency: usize,
        persistence: Arc<Persistence>,
        introspect: Arc<Introspector>,
    ) -> Arc<Self> {
        let pool = Arc::new(ExitPool {
            exits: Mutex::new(exits),
            max_latency,
            lease_timeout,
            probe_concurrency,
            probe,
            ready: Arc::new(Notify::new()),
            persistence,
            introspect,
            monitor: Mutex::new(None),
            worker_wakes: Mutex::new(std::collections::HashMap::new()),
        });
        pool.sweep(); // seed the dashboard with every exit's first row
        let handle = tokio::spawn(ExitPool::monitor(Arc::downgrade(&pool)));
        *pool.monitor.lock().unwrap() = Some(handle);
        pool
    }

    pub(crate) fn catalog_exit(rec: ExitRecord) -> Exit {
        Exit {
            rec,
            health: ExitHealth::Probing,
            activity: Activity::Idle,
            latency: None,
            last_probe: None,
            probe_failures: 0,
            over_cap: false,
            data: ExitData::default(),
            last_disposition: None,
        }
    }

    pub fn exit_count(&self) -> usize {
        self.exits.lock().unwrap().len()
    }

    /// Every exit's catalog `code`, in catalog order — the worker layer spawns exactly one
    /// serving worker per code (the one-worker-per-exit binding).
    pub fn exit_codes(&self) -> Vec<String> {
        self.exits
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.rec.code.clone())
            .collect()
    }

    /// Whether this specific exit could be claimed for serving right now (ready + idle + not
    /// cooling + under the latency cap) — the non-mutating check the serving worker's pull-gate
    /// uses so it doesn't churn its exit's activity just to test it.
    pub fn is_claimable(&self, code: &str) -> bool {
        self.exits
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.rec.code == code)
            .is_some_and(|e| e.leasable() && within_cap(e, self.max_latency))
    }

    /// The pool-wide "an exit may have become (re)leasable" signal — fired when a lease returns or a
    /// probe confirms an exit ready. The headed `lease()` waiters and the background maintainer both
    /// await it (the maintainer to look for a fresh cold exit to warm).
    pub fn leasable_signal(&self) -> Arc<Notify> {
        self.ready.clone()
    }

    pub fn add_manual_exits(&self, urls: Vec<String>) {
        let mut exits = self.exits.lock().unwrap();
        exits.extend(urls.into_iter().map(manual_exit));
    }

    pub fn load_state_from_disk(&self) {
        let keys: Vec<String> = self.exits.lock().unwrap().iter().map(Exit::key).collect();
        let loaded: Vec<(String, ExitData)> = keys
            .into_iter()
            .map(|k| {
                let d = self.persistence.load_exit(&k);
                (k, d)
            })
            .collect();
        let mut exits = self.exits.lock().unwrap();
        for (k, d) in loaded {
            if let Some(e) = exits.iter_mut().find(|e| e.key() == k) {
                e.data = d;
            }
        }
    }

    pub fn availability(&self) -> Availability {
        let now = Instant::now();
        let exits = self.exits.lock().unwrap();
        // Available if any exit *can* serve once it's free — i.e. it's not wonky and not cooling,
        // regardless of whether it's idle right now or busy (`Serving`/`Solving`). A busy exit frees
        // in milliseconds, so it must NOT read as `Resting`: that's the difference between "wait a
        // moment, the pool is just busy" and "the whole pool is resting out cooldowns". Treating
        // busy-as-resting made a raw request spuriously give up `Resting` under full load (every
        // exit momentarily `Serving`). `Resting` now means *every* non-wonky exit is cooling.
        if exits
            .iter()
            .any(|e| e.health != ExitHealth::Wonky && !e.data.is_cooling())
        {
            Availability::Available
        } else {
            let soonest = exits
                .iter()
                .filter_map(|e| e.data.cooling_until())
                .map(|t| t.saturating_duration_since(now))
                .min();
            Availability::Resting(soonest)
        }
    }

    fn update_exit(&self, key: &str, persist: bool, f: impl FnOnce(&mut Exit)) {
        let now = Instant::now();
        let (data, row) = {
            let mut exits = self.exits.lock().unwrap();
            let Some(e) = exits.iter_mut().find(|e| e.key() == key) else {
                return;
            };
            f(e);
            (
                persist.then(|| e.data.clone()),
                note_row(e, now, self.max_latency),
            )
        };
        if let Some(d) = data {
            self.persistence.save_exit(key, &d);
        }
        self.emit(row);
    }

    pub fn record_request(&self, key: &str) {
        self.update_exit(key, false, |e| e.data.record_request());
    }

    pub fn record_success(&self, key: &str, latency: Duration) {
        self.update_exit(key, false, |e| e.data.record_success(latency));
    }

    pub fn record_clearance(&self, key: &str, host: &str, clearance: Clearance) {
        self.update_exit(key, true, |e| e.data.record_clearance(host, clearance));
    }

    pub fn record_rate_limit(&self, key: &str, cooldown: Duration) {
        self.update_exit(key, true, |e| e.data.record_rate_limit(cooldown));
    }

    pub fn record_block(&self, key: &str, cooldown: Duration) {
        self.update_exit(key, true, |e| e.data.record_block(cooldown));
    }

    /// A timeout means the exit stopped answering — we no longer trust it's reachable. Cool
    /// it (the backoff badge) *and* demote it to `Probing`, so it can't be leased again until
    /// a probe re-confirms it `Ready` (a cheap probe beats committing a worker to another
    /// full slim timeout). Set while the lease is still held; `return_lease` preserves it.
    pub fn record_timeout(&self, key: &str, base: Duration, max: Duration) {
        self.update_exit(key, true, |e| {
            e.data.record_timeout(base, max);
            // Inherits the worker's `exit{code}` span, so the line already carries the code.
            // INFO, not WARN: this is the *recoverable* health transition — a fast re-probe
            // (PROBE_RETRY) re-confirms `Ready`, or repeated failures escalate to `wonky`, which
            // IS logged at WARN (the durable death). It's symmetric with the INFO "probe confirmed
            // ready" transition, so a one-off slim blip stays below the default level; a healthy
            // bulk run that sheds and re-confirms exits doesn't flood WARN.
            if e.health != ExitHealth::Probing {
                tracing::info!(
                    "slim timeout · benched (health → probing) until a probe re-confirms"
                );
            }
            e.health = ExitHealth::Probing;
        });
    }

    /// A short, reason-less cooldown for a transient hiccup (e.g. a 5xx/`Unavailable`).
    pub fn record_transient(&self, key: &str, cooldown: Duration) {
        self.update_exit(key, true, |e| e.data.cool(cooldown, Cooling::Transient));
    }

    pub fn drop_clearance(&self, key: &str, host: &str) {
        self.update_exit(key, true, |e| e.data.drop_clearance(host));
    }

    /// A slim request was challenged despite a fresh clearance for `host`: drop it and bench the
    /// exit with an escalating cooldown, so a CF-flagged IP (warms fine, never serves slim) drops
    /// out of the fastest-first warming rotation instead of being re-warmed on loop.
    pub fn record_slim_challenge(&self, key: &str, host: &str, base: Duration, max: Duration) {
        self.update_exit(key, true, |e| e.data.record_challenge(host, base, max));
    }

    /// Record that this exit just served `domain`; it may not serve that domain again until `until`
    /// (the per-IP pacer). In-memory only. Keyed by catalog `code` (like `claim`/`paced_until`).
    /// Emits a delta (the exit flips to the `paced` badge).
    pub fn mark_served(&self, code: &str, domain: &str, until: Instant) {
        let now = Instant::now();
        let row = {
            let mut exits = self.exits.lock().unwrap();
            let Some(e) = exits.iter_mut().find(|e| e.rec.code == code) else {
                return;
            };
            e.data.record_served(domain, until);
            note_row(e, now, self.max_latency)
        };
        self.emit(row);
    }

    /// This exit's current pace deadline (the latest still-future `paced_until`), or `None` if it
    /// isn't pacing — what the serving worker sleeps to before its next request.
    pub fn paced_until(&self, code: &str) -> Option<Instant> {
        self.exits
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.rec.code == code)
            .and_then(|e| e.data.paced_until())
    }

    /// The warm clearance this *specific* exit holds for `host` (none if cooling/stale).
    /// The worker only ever consults its own leased exit — there is no cross-exit fan-out.
    pub fn warm(&self, key: &str, host: &str) -> Option<Clearance> {
        self.exits
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.key() == key)
            .and_then(|e| e.data.warm_clearance(host))
    }

    /// Activity transitions driven by the leasing worker as it works its exit. The lease
    /// itself already set `Serving`; these refine it for the badge and revert it.
    pub fn mark_serving(&self, key: &str) {
        self.set_activity(key, Activity::Serving);
    }

    pub fn mark_solving(&self, key: &str) {
        self.set_activity(key, Activity::Solving);
    }

    fn set_activity(&self, key: &str, activity: Activity) {
        let now = Instant::now();
        let row = {
            let mut exits = self.exits.lock().unwrap();
            let Some(e) = exits.iter_mut().find(|e| e.key() == key) else {
                return;
            };
            e.activity = activity;
            note_row(e, now, self.max_latency)
        };
        self.emit(row);
    }

    pub fn snapshot(&self) -> StoreSnapshot {
        let exits = self.exits.lock().unwrap();
        let mut clearances = Vec::new();
        let mut stats = Vec::new();
        for e in exits.iter() {
            let (mut cs, st) = e.data.inspection_rows(&e.key());
            clearances.append(&mut cs);
            stats.push(st);
        }
        StoreSnapshot { clearances, stats }
    }

    pub fn check_fingerprint(&self, user_agent: &str) {
        if !self.persistence.note_chrome_major(user_agent) {
            return;
        }
        let saved: Vec<(String, ExitData)> = {
            let mut exits = self.exits.lock().unwrap();
            for e in exits.iter_mut() {
                e.data.clear_clearances();
            }
            exits.iter().map(|e| (e.key(), e.data.clone())).collect()
        };
        for (k, d) in saved {
            self.persistence.save_exit(&k, &d);
        }
    }

    pub fn persist_all(&self) {
        if !self.persistence.is_persistent() {
            return;
        }
        let saved: Vec<(String, ExitData)> = self
            .exits
            .lock()
            .unwrap()
            .iter()
            .map(|e| (e.key(), e.data.clone()))
            .collect();
        for (k, d) in saved {
            self.persistence.save_exit(&k, &d);
        }
    }

    pub async fn lease(self: &Arc<Self>) -> Result<Lease> {
        let deadline = Instant::now() + self.lease_timeout;
        loop {
            if let Some((url, code)) = self.try_lease() {
                tracing::debug!(code = %code, "leased");
                return Ok(Lease::new(url, code, self.clone()));
            }
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                bail!(
                    "no healthy exit available within {}s",
                    self.lease_timeout.as_secs()
                );
            }
            let _ = tokio::time::timeout(wait, self.ready.notified()).await;
        }
    }

    /// Claim **this specific exit** (by `code`) for serving, if it's leasable right now
    /// (ready + idle + not cooling + under the latency cap). The one-worker-per-exit primitive:
    /// a serving worker owns one exit and only ever claims *that* one — the CAS on the `activity`
    /// facet (`Idle → Serving`) is the coordination point with the maintainer/prober. `None` if the
    /// exit is busy/cooling/unhealthy (the worker then waits on its per-exit wake).
    pub fn claim(self: &Arc<Self>, code: &str) -> Option<Lease> {
        let now = Instant::now();
        let (url, row) = {
            let mut exits = self.exits.lock().unwrap();
            let e = exits.iter_mut().find(|e| e.rec.code == code)?;
            if !(e.leasable() && within_cap(e, self.max_latency)) {
                return None;
            }
            e.activity = Activity::Serving;
            (e.rec.proxy_url(), note_row(e, now, self.max_latency))
        };
        self.emit(row);
        Some(Lease::new(url, code.to_string(), self.clone()))
    }

    /// Whether this specific exit currently holds a usable (non-stale) clearance for `host`. The
    /// serving worker's warmth gate: it only pulls work once its owned exit is warm for the
    /// solve-domains (raw hosts need no warmth). Membership test — ignores cooling, which
    /// [`claim`](Self::claim) already excludes.
    pub fn exit_warm_for(&self, code: &str, host: &str) -> bool {
        self.exits
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.rec.code == code)
            .is_some_and(|e| e.data.is_warm_for(host))
    }

    /// Lease the lowest-latency leasable exit that is **cold for at least one** of `domains` — the
    /// background maintainer's pick, so warming walks the catalog fastest-first. Returns the lease
    /// plus the host to warm (the first domain that exit is cold for). `None` when every leasable
    /// exit is already warm for every domain (nothing left to warm right now).
    pub fn lease_to_warm_any(self: &Arc<Self>, domains: &[String]) -> Option<(Lease, String)> {
        let now = Instant::now();
        let (url, code, host, row) = {
            let mut exits = self.exits.lock().unwrap();
            let idx = leasable_idx_where(&exits, self.max_latency, |e| {
                domains.iter().any(|d| !e.data.is_warm_for(d))
            })?;
            let host = domains
                .iter()
                .find(|d| !exits[idx].data.is_warm_for(d))
                .cloned()
                .unwrap_or_default();
            exits[idx].activity = Activity::Serving;
            (
                exits[idx].rec.proxy_url(),
                exits[idx].rec.code.clone(),
                host,
                note_row(&mut exits[idx], now, self.max_latency),
            )
        };
        tracing::debug!(code = %code, "leased (to warm)");
        self.emit(row);
        Some((Lease::new(url, code, self.clone()), host))
    }

    /// Returns `(proxy_url, code)` — `proxy_url` is `None` for the direct exit.
    fn try_lease(&self) -> Option<(Option<String>, String)> {
        let now = Instant::now();
        let mut exits = self.exits.lock().unwrap();
        let idx = leasable_idx(&exits, self.max_latency)?;
        exits[idx].activity = Activity::Serving;
        let result = (exits[idx].rec.proxy_url(), exits[idx].rec.code.clone());
        let row = note_row(&mut exits[idx], now, self.max_latency);
        drop(exits);
        self.emit(row);
        Some(result)
    }

    /// Hand an exit back with the worker's [`ExitStatus`] verdict. Every status frees the exit
    /// (`activity = Idle`); `Dead` additionally marks it wonky. Cooling is *not* set here — it
    /// was already recorded by the `record_*` that diagnosed the reason (see the worker's
    /// `penalize`), so `Cooled` and `Ok` touch only the facets, leaving the real
    /// rate-limited/blocked cooldown (and its badge reason) intact.
    pub fn return_lease(&self, code: &str, status: ExitStatus) {
        match status {
            ExitStatus::Ok => tracing::debug!(code = %code, "returned → ready"),
            ExitStatus::Cooled => tracing::debug!(code = %code, "returned cooling"),
            ExitStatus::Dead => tracing::warn!(code = %code, "marked wonky (dead)"),
        }
        let now = Instant::now();
        let row = {
            let mut exits = self.exits.lock().unwrap();
            match exits.iter_mut().find(|e| e.rec.code == code) {
                Some(e) => {
                    e.activity = Activity::Idle;
                    match status {
                        // Heal back to Ready — but never promote a `Probing` exit, which is how a
                        // timeout marks "unconfirmed": it must wait for a successful probe.
                        ExitStatus::Ok | ExitStatus::Cooled if e.health != ExitHealth::Probing => {
                            e.health = ExitHealth::Ready
                        }
                        ExitStatus::Ok | ExitStatus::Cooled => {}
                        ExitStatus::Dead => e.health = ExitHealth::Wonky,
                    }
                    note_row(e, now, self.max_latency)
                }
                None => None,
            }
        };
        self.ready.notify_waiters();
        self.emit(row);
    }

    /// Emit one exit's delta (if it changed). The single broadcast point — always called *after*
    /// the `exits` lock is released, with the row captured inside the lock by [`note_row`]. A
    /// disposition change is also exactly when the exit's serving worker should re-evaluate
    /// (it may now be warm/cooled-down/ready), so this is where its per-exit wake fires.
    fn emit(&self, row: Option<ExitRow>) {
        if let Some(r) = row {
            self.wake_exit(&r.code);
            self.introspect.publish_exit(r);
        }
    }

    /// The per-exit wake for `code`'s serving worker, created on first use. The worker awaits this
    /// (register-then-recheck) so a change landing between its check and its await is never lost.
    pub fn exit_notify(&self, code: &str) -> Arc<Notify> {
        self.worker_wakes
            .lock()
            .unwrap()
            .entry(code.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    fn wake_exit(&self, code: &str) {
        if let Some(n) = self.worker_wakes.lock().unwrap().get(code) {
            n.notify_waiters();
        }
    }

    /// Wake **every** serving worker at once — used on shutdown so a worker parked on a cooling
    /// exit (which won't otherwise re-check until its fallback tick) exits promptly.
    pub fn wake_all_workers(&self) {
        for n in self.worker_wakes.lock().unwrap().values() {
            n.notify_waiters();
        }
    }

    /// Sweep every exit and emit deltas for any whose disposition changed since last streamed.
    /// Used to **seed** the dashboard at startup (every exit's first row) and, each monitor cycle,
    /// to catch *time-based* transitions no mutation triggers — chiefly a cooldown expiring.
    fn sweep(&self) {
        let now = Instant::now();
        let rows: Vec<ExitRow> = {
            let mut exits = self.exits.lock().unwrap();
            exits
                .iter_mut()
                .filter_map(|e| note_row(e, now, self.max_latency))
                .collect()
        };
        for r in rows {
            self.introspect.publish_exit(r);
        }
    }

    async fn monitor(weak: Weak<Self>) {
        loop {
            let Some(this) = weak.upgrade() else { return };
            let now = Instant::now();
            let due: Vec<(String, String)> = {
                let exits = this.exits.lock().unwrap();
                exits
                    .iter()
                    .filter(|e| e.due(now))
                    .map(|e| (e.rec.code.clone(), e.rec.proxy_url().unwrap_or_default()))
                    .collect()
            };

            if !due.is_empty() {
                let probe = this.probe.clone();
                let mut stream = futures::stream::iter(due)
                    .map(|(code, url)| {
                        let probe = probe.clone();
                        let this = this.clone();
                        // A cosmetic `exit{code}` span so probe-context events read
                        // `exit{code=…}:` like the worker's — one exit greppable across
                        // both its probe and its fetch lines.
                        let span = tracing::info_span!("exit", code = %code);
                        async move {
                            let outcome = probe(url).await;
                            this.apply_one(code, outcome);
                        }
                        .instrument(span)
                    })
                    .buffer_unordered(this.probe_concurrency);
                // Each probe emits its own delta via `apply_one`; just drive the stream.
                while stream.next().await.is_some() {}
            }

            // Catch time-based disposition changes (a cooldown expiring) that no mutation fired.
            this.sweep();
            drop(this);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    fn apply_one(&self, code: String, outcome: ProbeOutcome) {
        let now = Instant::now();
        let (became_ready, row) = {
            let mut exits = self.exits.lock().unwrap();
            let Some(e) = exits.iter_mut().find(|e| e.rec.code == code) else {
                return;
            };
            let became_ready = e.observe_probe(now, outcome);
            (became_ready, note_row(e, now, self.max_latency))
        };
        // A probe-confirm is a disposition change, so `emit` already woke this exit's worker; the
        // pool-wide `ready` signal is for the headed `lease()` waiters (which pick any exit).
        self.emit(row);
        if became_ready {
            self.ready.notify_waiters();
        }
    }
}

/// A probe latency must change by at least this factor (in either direction) before
/// `observe_probe` logs it, so steady-state jitter stays quiet and only real shifts — e.g. an
/// exit going slow *while leased* — surface.
const LATENCY_SHIFT_FACTOR: u32 = 2;

fn latency_shifted(prev: Duration, new: Duration) -> bool {
    if prev.is_zero() {
        return false; // the direct exit (latency 0) — nothing to compare against
    }
    let (lo, hi) = if new >= prev {
        (prev, new)
    } else {
        (new, prev)
    };
    hi >= lo * LATENCY_SHIFT_FACTOR
}

#[cfg(test)]
impl ExitPool {
    pub(crate) fn manual_no_monitor(urls: Vec<String>) -> Arc<Self> {
        Arc::new(ExitPool {
            exits: Mutex::new(urls.into_iter().map(manual_exit).collect()),
            max_latency: None,
            lease_timeout: Duration::from_secs(60),
            probe_concurrency: 8,
            probe: connect_probe_for(None),
            ready: Arc::new(Notify::new()),
            persistence: Arc::new(Persistence::open(None, "Chrome147")),
            introspect: Introspector::new(),
            monitor: Mutex::new(None),
            worker_wakes: Mutex::new(std::collections::HashMap::new()),
        })
    }
}

fn leasable_idx(exits: &[Exit], max_latency: Option<Duration>) -> Option<usize> {
    leasable_idx_where(exits, max_latency, |_| true)
}

/// The lowest-latency leasable exit also satisfying `pred`. Leasability (ready + idle + not
/// cooling + under the latency cap) is the same rule everywhere; `pred` adds the caller's
/// refinement (warm-for-host / cold-for-host) without re-stating it.
fn leasable_idx_where(
    exits: &[Exit],
    max_latency: Option<Duration>,
    pred: impl Fn(&Exit) -> bool,
) -> Option<usize> {
    exits
        .iter()
        .enumerate()
        .filter(|(_, e)| e.leasable() && pred(e) && within_cap(e, max_latency))
        .min_by_key(|(_, e)| e.latency.unwrap_or(Duration::MAX))
        .map(|(i, _)| i)
}

/// Whether an exit's measured latency is within the cap. With a cap set, an unprobed exit
/// (latency `None`) is **not** within it — we never gamble on an unmeasured exit's speed.
/// Without a cap, latency is irrelevant and any exit qualifies.
fn within_cap(e: &Exit, max_latency: Option<Duration>) -> bool {
    match max_latency {
        Some(cap) => e.latency.is_some_and(|l| l <= cap),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool_with(
        exits: Vec<Exit>,
        max_latency: Option<Duration>,
        persistence: Arc<Persistence>,
    ) -> Arc<ExitPool> {
        Arc::new(ExitPool {
            exits: Mutex::new(exits),
            max_latency,
            lease_timeout: Duration::from_secs(60),
            probe_concurrency: 8,
            probe: connect_probe_for(None),
            ready: Arc::new(Notify::new()),
            persistence,
            introspect: Introspector::new(),
            monitor: Mutex::new(None),
            worker_wakes: Mutex::new(std::collections::HashMap::new()),
        })
    }

    fn test_pool(exits: Vec<Exit>, max_latency: Option<Duration>) -> Arc<ExitPool> {
        test_pool_with(
            exits,
            max_latency,
            Arc::new(Persistence::open(None, "Chrome147")),
        )
    }

    fn from_records(records: Vec<ExitRecord>, max_latency: Option<Duration>) -> Arc<ExitPool> {
        test_pool(
            records.into_iter().map(ExitPool::catalog_exit).collect(),
            max_latency,
        )
    }

    fn ex(
        code: &str,
        lat: Option<u64>,
        health: ExitHealth,
        activity: Activity,
        cool: Option<Duration>,
    ) -> Exit {
        let mut e = Exit {
            rec: ExitRecord {
                country: "X".into(),
                code: code.into(),
                socks: Some(format!("{code}:1080")),
            },
            health,
            activity,
            latency: lat.map(Duration::from_millis),
            last_probe: None,
            probe_failures: 0,
            over_cap: false,
            data: ExitData::default(),
            last_disposition: None,
        };
        if let Some(d) = cool {
            e.data.cool(d, Cooling::Transient);
        }
        e
    }

    fn manual_pool(codes: &[&str]) -> Arc<ExitPool> {
        let exits = codes
            .iter()
            .map(|c| ex(c, None, ExitHealth::Ready, Activity::Idle, None))
            .collect();
        test_pool(exits, None)
    }

    impl ExitPool {
        fn health_tag(&self, code: &str) -> String {
            let exits = self.exits.lock().unwrap();
            exits
                .iter()
                .find(|e| e.rec.code == code)
                .map(|e| e.row(Instant::now(), None).health)
                .unwrap()
        }
        fn cooling(&self, code: &str) -> bool {
            self.exits
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.rec.code == code)
                .is_some_and(|e| e.data.is_cooling())
        }
        fn latency_ms(&self, code: &str) -> Option<u64> {
            self.exits
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.rec.code == code)
                .and_then(|e| e.latency)
                .map(|d| d.as_millis() as u64)
        }
        fn over_latency(&self, code: &str) -> bool {
            let exits = self.exits.lock().unwrap();
            exits
                .iter()
                .find(|e| e.rec.code == code)
                .map(|e| e.row(Instant::now(), self.max_latency).over_latency)
                .unwrap()
        }
    }

    fn clr() -> Clearance {
        Clearance::new(
            vec![("cf_clearance".into(), "t".into())],
            "UA".into(),
            None,
            String::new(),
        )
    }

    #[test]
    fn leasable_picks_lowest_latency_ready_idle_under_cap() {
        let exits = vec![
            ex("a", Some(300), ExitHealth::Ready, Activity::Idle, None),
            ex("b", Some(80), ExitHealth::Ready, Activity::Idle, None),
            ex("c", Some(50), ExitHealth::Ready, Activity::Serving, None),
            ex("d", Some(40), ExitHealth::Wonky, Activity::Idle, None),
            ex(
                "cool",
                Some(10),
                ExitHealth::Ready,
                Activity::Idle,
                Some(Duration::from_secs(60)),
            ),
            ex("e", None, ExitHealth::Ready, Activity::Idle, None),
        ];
        let i = leasable_idx(&exits, None).unwrap();
        assert_eq!(
            exits[i].rec.code, "b",
            "serving, wonky and cooling exits are all unleasable"
        );
    }

    #[test]
    fn probe_timeout_tracks_the_latency_cap() {
        // No point probing longer than the slowest latency we'd lease: the timeout is ~2× the cap,
        // and under a cap a timeout is the verdict "too slow" (so the slot frees fast and the exit
        // reads `slow`, not stuck `probing`).
        assert_eq!(
            probe_timeout(Some(Duration::from_millis(500))),
            (Duration::from_secs(1), true),
            "2×cap, floored at 1s"
        );
        assert_eq!(
            probe_timeout(Some(Duration::from_secs(3))),
            (Duration::from_secs(6), true)
        );
        assert_eq!(
            probe_timeout(Some(Duration::from_secs(30))).0,
            Duration::from_secs(10),
            "capped at 10s"
        );
        assert_eq!(
            probe_timeout(None),
            (PROBE_TIMEOUT_UNCAPPED, false),
            "no cap → flat timeout, a miss just retries"
        );
    }

    #[test]
    fn delta_emits_on_disposition_change_not_on_stat_tick() {
        let mut e = ex("a", Some(50), ExitHealth::Ready, Activity::Idle, None);
        let now = Instant::now();
        assert!(
            note_row(&mut e, now, None).is_some(),
            "first row seeds the dashboard"
        );
        assert!(
            note_row(&mut e, now, None).is_none(),
            "nothing changed → no delta"
        );

        // A pure stat tick (request/success counters) is NOT a disposition change → no delta;
        // it rides along on the next real change instead of streaming a row of its own.
        e.data.record_request();
        e.data.record_request();
        assert!(
            note_row(&mut e, now, None).is_none(),
            "stat-only tick emits nothing"
        );

        // A disposition change (activity) emits — and carries the ridden-along stats.
        e.activity = Activity::Serving;
        let row = note_row(&mut e, now, None).expect("activity change emits");
        assert_eq!(row.activity, "serving");
        assert_eq!(row.stats.requests, 2, "stats ride along in the delta");

        // Cooling is a disposition change too.
        e.data.record_rate_limit(Duration::from_secs(60));
        let row = note_row(&mut e, now, None).expect("cooldown emits");
        assert!(row.cooling);
    }

    #[test]
    fn latency_cap_excludes_slow_exits() {
        let exits = vec![
            ex("slow", Some(500), ExitHealth::Ready, Activity::Idle, None),
            ex("fast", Some(120), ExitHealth::Ready, Activity::Idle, None),
        ];
        assert_eq!(
            exits[leasable_idx(&exits, Some(Duration::from_millis(200))).unwrap()]
                .rec
                .code,
            "fast"
        );
        assert!(leasable_idx(&exits, Some(Duration::from_millis(100))).is_none());
    }

    #[test]
    fn cap_excludes_unprobed_exits_so_we_never_gamble_on_latency() {
        // Ready + idle but never probed (latency None) — e.g. a manual exit pre-probe.
        let exits = vec![ex(
            "unprobed",
            None,
            ExitHealth::Ready,
            Activity::Idle,
            None,
        )];
        assert!(
            leasable_idx(&exits, Some(Duration::from_millis(800))).is_none(),
            "under a cap, an exit with no measured latency is NOT leasable until probed",
        );
        assert!(
            leasable_idx(&exits, None).is_some(),
            "without a cap, an unprobed exit is leasable"
        );
    }

    #[test]
    fn nothing_leasable_when_none_ready_idle() {
        let exits = vec![
            ex("a", Some(50), ExitHealth::Probing, Activity::Idle, None),
            ex(
                "b",
                Some(50),
                ExitHealth::Ready,
                Activity::Idle,
                Some(Duration::from_secs(60)),
            ),
        ];
        assert!(leasable_idx(&exits, None).is_none());
    }

    #[test]
    fn claim_binds_a_specific_exit_and_warming_targets_the_cold_one() {
        let p = manual_pool(&["a", "b", "c"]);
        let key = |c: &str| format!("socks5h://{c}:1080");
        // Warm `a` and `b` for host h; `c` stays cold.
        p.record_clearance(&key("a"), "h", clr());
        p.record_clearance(&key("b"), "h", clr());

        // A serving worker claims *its own* exit by code and consults its warmth for the host.
        assert!(p.exit_warm_for("a", "h") && p.exit_warm_for("b", "h"));
        assert!(!p.exit_warm_for("c", "h"), "c is cold for h");
        let la = p.claim("a").expect("claim our own exit");
        assert_eq!(la.code(), "a");
        assert!(p.claim("a").is_none(), "already claimed → not leasable");

        // Warming picks the lowest-latency exit cold for the host — here `c` (a/b are warm).
        let (cold, host) = p
            .lease_to_warm_any(&["h".into()])
            .expect("a cold-for-h exit to warm");
        assert_eq!((cold.code(), host.as_str()), ("c", "h"));
    }

    #[test]
    fn warm_for_one_host_is_cold_for_another() {
        let p = manual_pool(&["a"]);
        p.record_clearance("socks5h://a:1080", "h1", clr());
        assert!(p.exit_warm_for("a", "h1"), "warm for h1");
        assert!(
            !p.exit_warm_for("a", "h2"),
            "the same exit is cold for an unsolved host"
        );
        // Warming for the cold host still offers it, even though it is warm for h1.
        assert_eq!(
            p.lease_to_warm_any(&["h2".into()])
                .map(|(l, _)| l.code().to_string()),
            Some("a".to_string())
        );
    }

    #[test]
    fn availability_flips_to_resting_when_cooled() {
        let p = manual_pool(&["m"]);
        assert_eq!(p.availability(), Availability::Available);
        p.record_rate_limit("socks5h://m:1080", Duration::from_secs(120));
        match p.availability() {
            Availability::Resting(Some(d)) => {
                assert!(
                    d > Duration::from_secs(110) && d <= Duration::from_secs(120),
                    "retry_after ≈ cooldown"
                );
            }
            other => panic!("expected Resting(Some), got {other:?}"),
        }
    }

    #[test]
    fn a_busy_pool_is_available_not_resting() {
        // Every exit busy (Serving) but none cooling: a request should wait for one to free, NOT
        // spuriously give up `Resting`. Resting is reserved for "every non-wonky exit is cooling".
        let p = manual_pool(&["a", "b"]);
        p.mark_serving("socks5h://a:1080");
        p.mark_serving("socks5h://b:1080");
        assert_eq!(
            p.availability(),
            Availability::Available,
            "a fully-busy (but not cooling) pool is Available — the exits will free"
        );
        p.record_rate_limit("socks5h://a:1080", Duration::from_secs(60));
        p.record_rate_limit("socks5h://b:1080", Duration::from_secs(60));
        assert!(
            matches!(p.availability(), Availability::Resting(_)),
            "every exit cooling → Resting"
        );
    }

    #[test]
    fn manual_exit_leasable_before_probe_then_confirmed_by_one() {
        let p = manual_pool(&["m"]);
        assert!(
            p.try_lease().is_some(),
            "a manual exit is leasable immediately, pre-probe"
        );
        p.return_lease("m", ExitStatus::Ok);
        p.apply_one(
            "m".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(20),
            },
        );
        assert_eq!(
            p.health_tag("m"),
            "ready",
            "a reachable probe keeps it ready"
        );
    }

    #[test]
    fn catalog_exit_becomes_ready_once_a_probe_reaches_it() {
        let p = from_records(
            vec![ExitRecord {
                country: "X".into(),
                code: "a".into(),
                socks: Some("a:1080".into()),
            }],
            None,
        );
        assert_eq!(
            p.health_tag("a"),
            "probing",
            "a catalog exit starts unconfirmed"
        );
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(20),
            },
        );
        assert_eq!(p.health_tag("a"), "ready", "a reachable probe confirms it");
        p.apply_one("a".into(), ProbeOutcome::Transient);
        assert_eq!(
            p.health_tag("a"),
            "ready",
            "a transient probe failure leaves health alone"
        );
    }

    #[test]
    fn probing_exits_reprobe_faster_than_ready_ones() {
        let now = Instant::now();
        let mut probing = ex("p", Some(50), ExitHealth::Probing, Activity::Idle, None);
        probing.last_probe = Some(now - Duration::from_secs(6));
        let mut ready = ex("r", Some(50), ExitHealth::Ready, Activity::Idle, None);
        ready.last_probe = Some(now - Duration::from_secs(6));
        assert!(
            probing.due(now),
            "an unconfirmed exit retries after PROBE_RETRY (5s), not 60s"
        );
        assert!(
            !ready.due(now),
            "a confirmed exit waits the full REPROBE_AFTER (60s)"
        );
    }

    #[test]
    fn persistent_probe_failures_bench_a_probing_exit_as_wonky() {
        let p = from_records(
            vec![ExitRecord {
                country: "X".into(),
                code: "a".into(),
                socks: Some("a:1080".into()),
            }],
            None,
        );
        assert_eq!(p.health_tag("a"), "probing");
        p.apply_one("a".into(), ProbeOutcome::Transient);
        p.apply_one("a".into(), ProbeOutcome::Transient);
        assert_eq!(
            p.health_tag("a"),
            "probing",
            "a couple of failures: still actively confirming"
        );
        p.apply_one("a".into(), ProbeOutcome::Transient);
        assert_eq!(
            p.health_tag("a"),
            "wonky",
            "persistent failure → wonky, out of the probing bucket"
        );
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(20),
            },
        );
        assert_eq!(
            p.health_tag("a"),
            "ready",
            "a reachable probe recovers a benched exit"
        );
    }

    #[test]
    fn timed_out_probe_is_slow_with_no_latency_then_recovers() {
        let cap = Some(Duration::from_millis(500));
        let p = from_records(
            vec![ExitRecord {
                country: "X".into(),
                code: "a".into(),
                socks: Some("a:1080".into()),
            }],
            cap,
        );
        // A probe timeout over the cap: benched `slow`, no usable measurement (→ `n/a`), not leasable.
        p.apply_one("a".into(), ProbeOutcome::TooSlow);
        assert_eq!(
            p.latency_ms("a"),
            None,
            "a timed-out probe records no latency (shows n/a, not a fake number)"
        );
        assert!(p.over_latency("a"), "it still reads `slow` (benched)");
        assert!(p.try_lease().is_none(), "and is not leasable");
        // A later measured probe under the cap recovers it.
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(120),
            },
        );
        assert_eq!(p.latency_ms("a"), Some(120));
        assert!(
            !p.over_latency("a"),
            "measured under the cap → no longer slow"
        );
        assert!(p.try_lease().is_some(), "and leasable again");
    }

    #[test]
    fn cooling_exit_is_not_due_until_cooled() {
        let now = Instant::now();
        let cooling = ex(
            "c",
            None,
            ExitHealth::Ready,
            Activity::Idle,
            Some(Duration::from_secs(60)),
        );
        let never_probed = ex("n", None, ExitHealth::Ready, Activity::Idle, None);
        assert!(
            !cooling.due(now),
            "don't reprobe an exit while it's cooling"
        );
        assert!(
            never_probed.due(now),
            "an un-probed, un-cooling exit is due"
        );
    }

    #[test]
    fn load_hydrates_members_and_never_replays_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let persistence = Arc::new(Persistence::open(Some(dir.path().into()), "Chrome147"));
        {
            let mut m = persistence.load_exit("socks5h://m:1080");
            m.record_clearance("h", clr());
            persistence.save_exit("socks5h://m:1080", &m);
            let mut ghost = persistence.load_exit("socks5h://ghost:1080");
            ghost.record_clearance("h", clr());
            persistence.save_exit("socks5h://ghost:1080", &ghost);
        }
        let pool = test_pool_with(
            vec![manual_exit("socks5h://m:1080".into())],
            None,
            persistence,
        );
        pool.load_state_from_disk();
        assert!(
            pool.warm("socks5h://m:1080", "h").is_some(),
            "member hydrated from disk"
        );
        assert!(
            pool.warm("socks5h://ghost:1080", "h").is_none(),
            "orphan is never a member, never replayed"
        );
    }

    #[test]
    fn return_lease_lands_status_on_orthogonal_facets() {
        let p = manual_pool(&["a"]);
        // Cooling is recorded by the diagnosing record_*, never by the return verdict.
        p.record_rate_limit("socks5h://a:1080", Duration::from_secs(300));
        assert!(p.cooling("a"), "record_* cools the exit (in data)");
        p.return_lease("a", ExitStatus::Cooled);
        assert_eq!(
            p.health_tag("a"),
            "ready",
            "cooling is orthogonal: a Cooled return leaves health ready"
        );
        assert!(
            p.cooling("a"),
            "the return verdict does not disturb the recorded cooldown"
        );
        p.return_lease("a", ExitStatus::Dead);
        assert_eq!(p.health_tag("a"), "wonky");
        p.return_lease("a", ExitStatus::Ok);
        assert_eq!(p.health_tag("a"), "ready");
    }

    #[test]
    fn timeout_benches_the_exit_until_a_probe_reconfirms_it() {
        let p = manual_pool(&["a"]);
        let key = "socks5h://a:1080";
        assert_eq!(p.health_tag("a"), "ready");

        // A timeout: we no longer trust it's reachable → demote to probing (and cool it).
        p.record_timeout(key, Duration::from_secs(30), Duration::from_secs(600));
        assert_eq!(
            p.health_tag("a"),
            "probing",
            "a timeout marks the exit unconfirmed"
        );
        assert!(p.cooling("a"), "and cools it as backoff");

        // Handing the lease back must NOT heal it to ready — it stays benched.
        p.return_lease("a", ExitStatus::Cooled);
        assert_eq!(
            p.health_tag("a"),
            "probing",
            "a Cooled return preserves the timeout demotion"
        );

        // Only a successful probe re-confirms reachability → ready (leasable again).
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(20),
            },
        );
        assert_eq!(p.health_tag("a"), "ready", "a successful probe restores it");
    }

    #[test]
    fn reprobe_of_a_leased_exit_refreshes_latency_without_touching_health() {
        let p = manual_pool(&["a"]);
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(50),
            },
        );
        assert_eq!(p.latency_ms("a"), Some(50));

        // Lease it: while held, a worker owns its health, not the probe.
        assert!(p.try_lease().is_some());
        assert_eq!(p.health_tag("a"), "ready");

        // A slow re-probe while in use updates *only* the latency — the one method that writes
        // latency is also the one that (here, deliberately) leaves a leased exit's health alone.
        p.apply_one(
            "a".into(),
            ProbeOutcome::Ok {
                latency: Duration::from_millis(5000),
            },
        );
        assert_eq!(
            p.latency_ms("a"),
            Some(5000),
            "latency refreshed while in use"
        );
        assert_eq!(
            p.health_tag("a"),
            "ready",
            "a probe never changes a leased exit's health"
        );
    }

    #[test]
    fn latency_shift_gate_ignores_jitter_but_catches_real_changes() {
        let ms = Duration::from_millis;
        assert!(!latency_shifted(ms(50), ms(60)), "small jitter stays quiet");
        assert!(latency_shifted(ms(50), ms(5000)), "a big jump surfaces");
        assert!(latency_shifted(ms(5000), ms(50)), "a big drop surfaces too");
        assert!(
            !latency_shifted(Duration::ZERO, ms(50)),
            "no baseline (direct exit) → nothing"
        );
    }

    #[test]
    fn lease_times_out_when_pool_never_ready() {
        let p = from_records(
            vec![ExitRecord {
                country: "X".into(),
                code: "a".into(),
                socks: Some("a:1080".into()),
            }],
            None,
        );
        assert!(p.try_lease().is_none());
    }
}
