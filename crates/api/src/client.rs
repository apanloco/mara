use std::future::Future;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use chromiumoxide::Page;
use futures::stream::{Stream, StreamExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::classify::Reason;
use crate::introspect::{Introspector, JobProgress};
use crate::policy::Policy;
use crate::slim::{self, Method};
use crate::store::Persistence;
use crate::worker::{Job, Shared, WorkerPool};

/// The fixed-pacer interval for a `req/min` rate: even spacing of `60s / per_min` between requests.
/// More conservative than a bursty token bucket (no burst credit), which is what an app-level limit
/// wants. Shared by both the per-IP and the aggregate pacers.
pub(crate) fn per_min_interval(per_min: u32) -> Duration {
    Duration::from_secs(60) / per_min.max(1)
}

/// A configured domain, suffix-matched (longest wins), so `fragrantica.com` covers
/// `www.fragrantica.com`. Two **orthogonal** rate ceilings, both in requests-per-minute and both
/// optional (compose freely):
/// - `per_ip` — at most this many req/min **per exit** (even spacing on each IP). The defense
///   against a per-IP limit like Cloudflare's `1015`; scale aggregate throughput with more warm IPs.
/// - `aggregate` — at most this many req/min **across the whole pool**. The defense against a
///   per-account/per-key limit (e.g. an Algolia app key), where rotating IPs doesn't help. A single
///   pool-wide pacer; breadth is irrelevant to it.
#[derive(Clone, Debug)]
pub struct Domain {
    pub host: String,
    pub solve: bool,
    pub per_ip: Option<u32>,
    pub aggregate: Option<u32>,
}

impl Domain {
    /// A solve domain (warm/solve/replay path), no rate ceilings.
    pub fn solve(host: impl Into<String>) -> Domain {
        Domain {
            host: host.into(),
            solve: true,
            per_ip: None,
            aggregate: None,
        }
    }

    /// A raw domain (no Cloudflare clearance — a plain API/asset host), no rate ceilings.
    pub fn raw(host: impl Into<String>) -> Domain {
        Domain {
            host: host.into(),
            solve: false,
            per_ip: None,
            aggregate: None,
        }
    }

    /// Cap requests to this domain at `per_min` **per exit**.
    pub fn per_ip(mut self, per_min: u32) -> Domain {
        self.per_ip = Some(per_min);
        self
    }

    /// Cap requests to this domain at `per_min` **across the whole pool**.
    pub fn aggregate(mut self, per_min: u32) -> Domain {
        self.aggregate = Some(per_min);
        self
    }
}

#[derive(Clone)]
pub struct Config {
    /// B — the live-browser cap (a machine-load guard): at most this many workers hold a
    /// browser at once. Clamped down to the exit count (a browser is borrowed by a worker).
    pub browsers: usize,
    pub exits: Vec<String>,
    pub mullvad: bool,
    pub max_latency: Option<Duration>,
    pub real_display: bool,
    pub cdp_click: bool,
    pub no_click: bool,
    pub connect_grace: Duration,
    pub move_mouse: bool,
    pub timeout: Duration,
    pub width: u32,
    pub height: u32,
    pub data_dir: Option<PathBuf>,
    pub policy: Policy,
    pub capture_dir: Option<PathBuf>,
    /// Configured domains: which hosts are behind Cloudflare (`solve`) and their optional per-IP
    /// rate ceilings. A request whose host suffix-matches a `solve` domain takes the
    /// warm/solve/replay path; **every other host is fetched raw**. Register the CF hosts you scrape
    /// here (or via `Client::enable_solving_for_domain`); an unregistered CF host gives up
    /// `Challenged`, and no single challenged exit can drag a host onto the warm path — routing is
    /// fixed, not auto-detected.
    pub domains: Vec<Domain>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            browsers: 4,
            exits: Vec::new(),
            mullvad: false,
            max_latency: None,
            real_display: false,
            cdp_click: false,
            no_click: false,
            connect_grace: Duration::ZERO,
            move_mouse: true,
            timeout: Duration::from_secs(60),
            width: 1366,
            height: 768,
            data_dir: None,
            policy: Policy::default(),
            capture_dir: None,
            domains: Vec::new(),
        }
    }
}

/// One thing to fetch: a URL plus optional request customization (`method`/`headers`/`body`, for
/// non-HTML traffic like an Algolia POST) and an optional caller `key` echoed back on the result.
/// `From<&str>`/`From<String>`/`From<Url>` give a bare `GET` with no key, so `fetch_all(urls)` still
/// takes plain strings. Whether a resource is solved or fetched raw is decided by its **host**
/// (the solve-set), never per-resource.
#[derive(Clone, Debug, Default)]
pub struct Resource {
    pub url: String,
    pub method: Method,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub key: Option<String>,
}

impl Resource {
    pub(crate) fn to_request(&self) -> slim::Request {
        slim::Request {
            url: self.url.clone(),
            method: self.method,
            headers: self.headers.clone(),
            body: self.body.clone(),
        }
    }
}

impl From<String> for Resource {
    fn from(url: String) -> Resource {
        Resource {
            url,
            ..Default::default()
        }
    }
}

impl From<&str> for Resource {
    fn from(url: &str) -> Resource {
        Resource::from(url.to_string())
    }
}

impl From<url::Url> for Resource {
    fn from(url: url::Url) -> Resource {
        Resource::from(url.to_string())
    }
}

pub struct Outcome<T> {
    pub value: T,
    pub clicks: u32,
    pub elapsed: Duration,
    pub solve_required: bool,
    pub exit: Option<String>,
}

/// One settled item of a [`FetchAll`] stream: the input slot it came from, the URL, the caller's
/// `key` (echoed from the input [`Resource`], `None` if none was set), and the fetch result.
/// `index` identifies the slot because URLs can repeat (e.g. `--repeat`) — it is assigned in strict
/// input order, so an `index → your-data` map is always trivial even without a `key`. `T` is the
/// body type: `String` for [`Client::fetch_all`] (text-decoded), `Vec<u8>` for `fetch_all_bytes`.
pub struct FetchResult<T = String> {
    pub index: usize,
    pub url: String,
    pub key: Option<String>,
    pub result: Result<Outcome<T>, FetchError>,
}

/// The stream returned by [`Client::fetch_all`]: an **unordered**, completion-order stream of
/// [`FetchResult`], exactly one per input URL, that ends once every URL has settled.
///
/// Memory is O(concurrency), not O(count): a background **feeder** pulls the input lazily and
/// submits into the worker pool's bounded queue, so at most ~C URLs and ~C bodies are ever live —
/// flat whether the input is a thousand URLs or a hundred million. Termination is driven by the
/// results channel closing (every job's sender clone dropped), never by `total`; `total` is for the
/// progress display only, so a mis-sized `size_hint` degrades the ETA but never the end-of-stream.
///
/// Dropping a `FetchAll` early aborts the feeder and drops the results channel; in-flight workers
/// then find the channel closed and discard their results instead of blocking forever.
pub struct FetchAll<T = String> {
    // Boxed-pinned so `FetchAll` is `Unpin` (async_channel's `Receiver` is not), which lets callers
    // use `StreamExt` adapters like `.next()` directly. One allocation per batch — noise. The
    // channel always carries the **canonical bytes** the worker produces; `decode` turns each body
    // into `T` (identity for `Vec<u8>`, UTF-8 for `String`) as it's pulled, at the stream edge.
    rx: Pin<Box<dyn Stream<Item = FetchResult<Vec<u8>>> + Send>>,
    decode: Box<dyn Fn(Vec<u8>) -> T + Send>,
    feeder: Option<JoinHandle<()>>,
    total: Option<usize>,
    progress: Option<Arc<Progress>>,
}

/// The driver-owned progress counter for one batch — the only thing that can count completions
/// (the pool only ever sees individual jobs). The feeder bumps `submitted`; the stream bumps
/// `ok`/`err` as results are pulled; both publish a throttled [`JobProgress`] to the dashboard.
/// `in_flight` is submitted-minus-settled, so it folds in the few results buffered but not yet
/// pulled (≤ the results-channel depth) — fine for a progress readout.
struct Progress {
    intro: Arc<Introspector>,
    start: Instant,
    total: AtomicU64, // u64::MAX = unknown
    submitted: AtomicU64,
    ok: AtomicU64,
    err: AtomicU64,
    last_publish_ms: AtomicU64,
}

impl Progress {
    fn new(intro: Arc<Introspector>, total: Option<usize>) -> Arc<Self> {
        Arc::new(Progress {
            intro,
            start: Instant::now(),
            total: AtomicU64::new(total.map(|n| n as u64).unwrap_or(u64::MAX)),
            submitted: AtomicU64::new(0),
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            last_publish_ms: AtomicU64::new(0),
        })
    }

    fn set_total(&self, total: Option<usize>) {
        self.total.store(
            total.map(|n| n as u64).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.publish(true);
    }

    fn on_submit(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.publish(false);
    }

    fn on_result(&self, ok: bool) {
        if ok {
            self.ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.err.fetch_add(1, Ordering::Relaxed);
        }
        self.publish(false);
    }

    fn snapshot(&self) -> JobProgress {
        let total = match self.total.load(Ordering::Relaxed) {
            u64::MAX => None,
            v => Some(v),
        };
        let ok = self.ok.load(Ordering::Relaxed);
        let err = self.err.load(Ordering::Relaxed);
        let done = ok + err;
        let in_flight = self.submitted.load(Ordering::Relaxed).saturating_sub(done);
        let secs = self.start.elapsed().as_secs_f64().max(1e-3);
        let per_sec = done as f64 / secs;
        let eta_secs = total
            .filter(|_| per_sec > 0.0)
            .map(|t| (t.saturating_sub(done) as f64 / per_sec) as u64);
        JobProgress {
            total,
            done,
            ok,
            err,
            in_flight,
            per_sec,
            eta_secs,
        }
    }

    /// Publish, throttled to ~50 ms unless `force`d (a terminal/total update), so a million-item
    /// batch emits a steady trickle rather than a million broadcasts. ~20 Hz gives the progress bar
    /// fine, frequent targets to glide between (the messages are tiny and coalesce downstream).
    fn publish(&self, force: bool) {
        let now_ms = self.start.elapsed().as_millis() as u64;
        if !force {
            let last = self.last_publish_ms.load(Ordering::Relaxed);
            if now_ms.saturating_sub(last) < 50 {
                return;
            }
            // Claim this window; if another thread beat us to it, let it do the send.
            if self
                .last_publish_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
            {
                return;
            }
        } else {
            self.last_publish_ms.store(now_ms, Ordering::Relaxed);
        }
        self.intro.publish_progress(self.snapshot());
    }
}

impl<T> FetchAll<T> {
    /// Supply a total when the source can't size itself (a cursor, `from_fn`, an async stream).
    /// Sized inputs (ranges, vecs, maps) set it automatically from the iterator's upper bound.
    pub fn with_total(mut self, n: usize) -> Self {
        self.set_total(Some(n));
        self
    }

    fn set_total(&mut self, total: Option<usize>) {
        self.total = total;
        if let Some(p) = &self.progress {
            p.set_total(total);
        }
    }

    /// The expected number of results, if known — for progress display only (see the type docs).
    pub fn total(&self) -> Option<usize> {
        self.total
    }
}

impl<T> Stream for FetchAll<T> {
    type Item = FetchResult<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<FetchResult<T>>> {
        let this = self.get_mut(); // FetchAll is Unpin
        match this.rx.as_mut().poll_next(cx) {
            Poll::Ready(Some(fr)) => {
                if let Some(p) = &this.progress {
                    p.on_result(fr.result.is_ok());
                }
                // Decode the canonical body bytes into `T` (the only place it happens).
                let FetchResult {
                    index,
                    url,
                    key,
                    result,
                } = fr;
                let result = result.map(|o| Outcome {
                    value: (this.decode)(o.value),
                    clicks: o.clicks,
                    elapsed: o.elapsed,
                    solve_required: o.solve_required,
                    exit: o.exit,
                });
                Poll::Ready(Some(FetchResult {
                    index,
                    url,
                    key,
                    result,
                }))
            }
            Poll::Ready(None) => {
                if let Some(p) = &this.progress {
                    p.publish(true); // final, exact line (in-flight → 0)
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for FetchAll<T> {
    fn drop(&mut self) {
        // Abort the feeder so it stops pulling the (possibly infinite) input and submitting work.
        // Dropping `rx` with this struct closes the results channel, so any worker still holding a
        // sender clone gets a closed-channel error on send rather than parking forever.
        if let Some(feeder) = self.feeder.take() {
            feeder.abort();
        }
    }
}

/// The streaming core, decoupled from the worker pool by the `submit` closure so it can be driven
/// hermetically by fake workers in tests. Spawns a feeder that pulls `input` lazily and hands each
/// `(index, url, results-sender)` to `submit` — whose `.await` blocks on the *bounded* work queue,
/// which is what makes the input lazy and the memory O(C). When the input is exhausted the feeder
/// drops its template sender; once every in-flight clone is dropped too, `rx` closes and the stream
/// ends.
fn drive<St, Sub, Fut, T>(
    input: St,
    results_cap: usize,
    decode: impl Fn(Vec<u8>) -> T + Send + 'static,
    submit: Sub,
) -> FetchAll<T>
where
    St: Stream<Item = Resource> + Send + 'static,
    Sub: Fn(usize, Resource, async_channel::Sender<FetchResult<Vec<u8>>>) -> Fut + Send + 'static,
    Fut: Future<Output = ControlFlow<()>> + Send,
{
    let (tx, rx) = async_channel::bounded(results_cap.max(1));
    let feeder = tokio::spawn(async move {
        futures::pin_mut!(input);
        let mut index = 0usize;
        while let Some(resource) = input.next().await {
            // `Break` means the pool is gone (shutdown/interrupt) — stop feeding rather than turn
            // every remaining input into a "worker pool is shut down" error result.
            if submit(index, resource, tx.clone()).await.is_break() {
                break;
            }
            index += 1;
        }
        // `tx` drops here — the last sender to go once all in-flight jobs settle, closing `rx`.
    });
    FetchAll {
        rx: Box::pin(rx),
        decode: Box::new(decode),
        feeder: Some(feeder),
        total: None,
        progress: None,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("fetch gave up after exhausting attempts: {0:?}")]
    GaveUp(Reason),

    #[error(
        "fingerprint-triple mismatch on {exit}: slim replay failed with {reason:?} immediately after a \
         headed solve (check the installed Chrome major against the pinned TLS profile)"
    )]
    FingerprintMismatch { exit: String, reason: Reason },

    #[error("all exits resting; {}", match retry_after { Some(d) => format!("retry in ~{}s", d.as_secs()), None => "retry later".into() })]
    Resting {
        retry_after: Option<std::time::Duration>,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
    workers: WorkerPool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.introspect.stop();
    }
}

impl Client {
    pub async fn new(mut cfg: Config) -> Result<Self> {
        if cfg.browsers == 0 {
            bail!("browsers (--browser-concurrency) must be >= 1");
        }

        let introspect = Introspector::new();
        introspect.set_mullvad_required(cfg.mullvad);
        if std::env::var_os("MARA_NO_INTROSPECT").is_none() {
            match introspect.clone().serve().await {
                Some(url) => tracing::info!(%url, "introspect dashboard"),
                None => tracing::warn!("introspection server could not bind a port"),
            }
        }

        let data_dir = cfg
            .data_dir
            .clone()
            .or_else(|| std::env::var_os("MARA_DATA_DIR").map(PathBuf::from));
        let persistence = Arc::new(Persistence::open(data_dir, slim::PROFILE));

        let egress = if cfg.mullvad {
            #[cfg(feature = "mullvad")]
            {
                let pool = crate::mullvad::bootstrap(
                    introspect.clone(),
                    persistence.clone(),
                    cfg.max_latency,
                    cfg.policy.lease_timeout,
                    cfg.policy.probe_concurrency,
                )
                .await?;
                if !cfg.exits.is_empty() {
                    pool.add_manual_exits(cfg.exits.clone());
                }
                pool.load_state_from_disk();
                pool
            }
            #[cfg(not(feature = "mullvad"))]
            bail!("Mullvad support is not compiled in — rebuild with `--features mullvad`");
        } else if cfg.exits.is_empty() {
            let pool = crate::pool::ExitPool::direct(
                introspect.clone(),
                persistence.clone(),
                cfg.policy.lease_timeout,
            );
            pool.load_state_from_disk();
            pool
        } else {
            let pool = crate::pool::ExitPool::manual(
                cfg.exits.clone(),
                introspect.clone(),
                persistence.clone(),
                cfg.max_latency,
                cfg.policy.lease_timeout,
                cfg.policy.probe_concurrency,
            );
            pool.load_state_from_disk();
            pool
        };

        // Serving width is the exit count, by construction: one worker per exit (no separate `-c`
        // dial — breadth is how many exits you provision). B is the machine-wide live-browser guard,
        // shared between the maintainer and headed fetches; it can't exceed the catalog (no point
        // launching more browsers than exits).
        cfg.browsers = cfg.browsers.min(egress.exit_count()).max(1);
        let (browsers, mullvad) = (cfg.browsers, cfg.mullvad);
        let shared = Shared::new(cfg, egress, persistence, introspect);
        let workers = WorkerPool::spawn(shared.clone());
        let serving = workers.count();

        let client = Client {
            inner: Arc::new(Inner { shared, workers }),
        };
        fingerprint_canary().await;
        let (data_dir, persistent) = client.inner.shared.persistence.location();
        tracing::info!(browsers, serving, mullvad, persistent, data_dir = %data_dir.display(), "client ready");
        Ok(client)
    }

    /// Always launches a browser: solve `url` headed and hand the live page to `extract`.
    /// Use this when you need a live `Page` (JS, screenshots, custom extraction). Submitted
    /// to the worker pool and awaited; the page is closed once `extract` returns.
    pub async fn fetch_browser<F, Fut, T>(
        &self,
        url: &str,
        extract: F,
    ) -> Result<Outcome<T>, FetchError>
    where
        F: FnOnce(Page) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let started = Instant::now();
        let (tx, rx) = oneshot::channel();
        let exec = Box::new(
            move |res: Result<crate::worker::HeadedSession, FetchError>| {
                Box::pin(async move {
                    let out = match res {
                        Ok(s) => match extract(s.page).await {
                            Ok(value) => Ok(Outcome {
                                value,
                                clicks: s.clicks,
                                elapsed: started.elapsed(),
                                solve_required: true,
                                exit: Some(s.exit),
                            }),
                            Err(e) => Err(FetchError::Other(e)),
                        },
                        Err(e) => Err(e),
                    };
                    let _ = tx.send(out);
                }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
            },
        );
        self.inner
            .workers
            .submit(Job::Headed {
                url: url.to_string(),
                exec,
                attempts: self.inner.shared.cfg.policy.max_attempts,
            })
            .await?;
        rx.await
            .map_err(|_| FetchError::Other(anyhow!("worker dropped the job")))?
    }

    /// Fetch many URLs the cheap way (browser-free HTTP replay, escalating to a headed solve only
    /// on a challenge) as an unordered, completion-order stream. This is the **core** bulk path —
    /// memory is O(concurrency), not O(count) (see [`FetchAll`]). The input is consumed lazily.
    pub fn fetch_all<I>(&self, resources: I) -> FetchAll<String>
    where
        I: IntoIterator,
        I::Item: Into<Resource> + 'static,
        I::IntoIter: Send + 'static,
    {
        let iter = resources.into_iter().map(Into::into);
        let total = iter.size_hint().1;
        let mut all = self.fetch_all_decoded(futures::stream::iter(iter), utf8_lossy);
        all.set_total(total);
        all
    }

    /// Like [`fetch_all`](Self::fetch_all) but yields the **raw body bytes** — for non-text
    /// resources (images, binaries). Same routing, same pool; only the body decode differs.
    pub fn fetch_all_bytes<I>(&self, resources: I) -> FetchAll<Vec<u8>>
    where
        I: IntoIterator,
        I::Item: Into<Resource> + 'static,
        I::IntoIter: Send + 'static,
    {
        let iter = resources.into_iter().map(Into::into);
        let total = iter.size_hint().1;
        let mut all = self.fetch_all_decoded(futures::stream::iter(iter), |b| b);
        all.set_total(total);
        all
    }

    /// [`fetch_all`](Self::fetch_all) for an async source. The total stays unknown unless supplied
    /// via [`FetchAll::with_total`]. Map non-`Resource` items (e.g. URL strings) with `.map(Into::into)`.
    pub fn fetch_all_stream(
        &self,
        resources: impl Stream<Item = Resource> + Send + 'static,
    ) -> FetchAll<String> {
        self.fetch_all_decoded(resources, utf8_lossy)
    }

    /// The shared submit core: feed `resources` into the bounded work queue (memory O(C)) and
    /// decode each canonical-bytes result into `T` at the stream edge.
    fn fetch_all_decoded<T>(
        &self,
        resources: impl Stream<Item = Resource> + Send + 'static,
        decode: impl Fn(Vec<u8>) -> T + Send + 'static,
    ) -> FetchAll<T> {
        let client = self.clone();
        // Results channel capacity ~C: with the work queue also bounded to C, at most ~2C results
        // can buffer before a slow consumer backs pressure up to the feeder.
        let cap = self.worker_count();
        let progress = Progress::new(self.inner.shared.introspect.clone(), None);
        let p = progress.clone();
        let mut all = drive(resources, cap, decode, move |index, resource, results| {
            let client = client.clone();
            let p = p.clone();
            async move {
                p.on_submit();
                let started = Instant::now();
                let url = resource.url.clone();
                let key = resource.key.clone();
                let job = Job::Html {
                    resource,
                    index,
                    started,
                    results: results.clone(),
                    attempts: client.inner.shared.cfg.policy.max_attempts,
                };
                match client.inner.workers.submit(job).await {
                    Ok(()) => ControlFlow::Continue(()),
                    // The pool is shut down (a normal end-of-run race, or an interrupt). Surface
                    // *this* slot's failure, then stop the feeder — otherwise an aborted run emits
                    // one "worker pool is shut down" error per remaining input.
                    Err(e) => {
                        let _ = results
                            .send(FetchResult {
                                index,
                                url,
                                key,
                                result: Err(e),
                            })
                            .await;
                        ControlFlow::Break(())
                    }
                }
            }
        });
        all.progress = Some(progress);
        all
    }

    /// Fetch a single `url` as text — a thin convenience over [`fetch_all`](Self::fetch_all).
    pub async fn fetch_http(&self, url: &str) -> Result<Outcome<String>, FetchError> {
        match self
            .fetch_all(std::iter::once(url.to_string()))
            .next()
            .await
        {
            Some(r) => r.result,
            None => Err(FetchError::Other(anyhow!("worker pool produced no result"))),
        }
    }

    /// Fetch a single resource as **raw bytes** — a thin convenience over
    /// [`fetch_all_bytes`](Self::fetch_all_bytes).
    pub async fn fetch_bytes(
        &self,
        resource: impl Into<Resource>,
    ) -> Result<Outcome<Vec<u8>>, FetchError> {
        match self
            .fetch_all_bytes(std::iter::once(resource.into()))
            .next()
            .await
        {
            Some(r) => r.result,
            None => Err(FetchError::Other(anyhow!("worker pool produced no result"))),
        }
    }

    /// Register `domain` as Cloudflare-protected: requests to it (and its subdomains — suffix
    /// match) take the warm/solve/replay path instead of being fetched raw. The runtime equivalent
    /// of adding a `Domain::solve(domain)` to `Config.domains`; call it for any CF host you scrape.
    pub fn enable_solving_for_domain(&self, domain: impl Into<String>) {
        self.inner.shared.mark_solve_host(&domain.into());
    }

    pub fn snapshot(&self) -> crate::store::StoreSnapshot {
        self.inner.shared.egress.snapshot()
    }

    /// The resolved worker count C (after auto/no-limit and the direct clamp) — i.e. how many
    /// exits can be in flight at once.
    pub fn worker_count(&self) -> usize {
        self.inner.workers.count()
    }

    pub async fn shutdown(&self) {
        self.inner.workers.shutdown().await;
        self.inner.shared.egress.persist_all();
        self.inner.shared.introspect.stop();
    }

    /// Interrupt (the signal path): kill in-flight browsers and abandon in-flight work, then
    /// return — no graceful drain, no `persist_all`. Aborting the workers drops their `Browser`s so
    /// `kill_on_drop` `SIGKILL`s Chrome, so a `std::process::exit` right after this leaves no
    /// orphaned Chrome. Clearances are already persisted on solve; only in-memory stat deltas are
    /// lost, which is the right trade for an interrupt.
    pub async fn abort(&self) {
        self.inner.workers.abort().await;
        self.inner.shared.introspect.stop();
    }
}

/// Decode a body to text. UTF-8 with lossy replacement — fine for HTML/JSON (effectively always
/// UTF-8 today); charset detection from `content-type` is deliberately not done (it would mean
/// threading the header back through the bytes-canonical path for a case that doesn't arise here).
fn utf8_lossy(bytes: Vec<u8>) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn fingerprint_canary() {
    let installed = tokio::task::spawn_blocking(crate::session::installed_chrome_major)
        .await
        .ok()
        .flatten();
    match (installed, slim::profile_major()) {
        (Some(installed), Some(pinned)) if installed != pinned => tracing::error!(
            installed,
            pinned,
            profile = slim::PROFILE,
            "fingerprint drift: installed Chrome {installed} != pinned slim profile {pinned} ({}); \
             handed-down clearances will be REJECTED and clients stay stuck on the headed solver — bump \
             wreq/wreq-util AND the Chrome binary in lockstep (see the fingerprint-triple invariant)",
            slim::PROFILE,
        ),
        (Some(installed), Some(_)) => {
            tracing::info!(
                chrome_major = installed,
                profile = slim::PROFILE,
                "fingerprint canary ok"
            )
        }
        _ => tracing::debug!(
            profile = slim::PROFILE,
            "fingerprint canary skipped (Chrome version unreadable)"
        ),
    }
}

#[cfg(test)]
mod tests {
    //! The `FetchAll` streaming core, verified hermetically — no browser, no exits. These plug
    //! **fake workers** into [`drive`] over the *same* bounded-channel topology the real worker
    //! pool uses, so the load-bearing invariants (one result per input, lazy O(C) input pull,
    //! drop-cancellation) are proven here rather than only live.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ok(index: usize, url: String) -> FetchResult<Vec<u8>> {
        let value = url.clone().into_bytes();
        FetchResult {
            index,
            url,
            key: None,
            result: Ok(Outcome {
                value,
                clicks: 0,
                elapsed: Duration::ZERO,
                solve_required: false,
                exit: None,
            }),
        }
    }

    /// Drive `input` through `workers` fake workers that pull from a bounded(`work_cap`) queue —
    /// the same shape as the real pool's bounded work queue + per-job results sender. Each worker
    /// runs `handle` to settle an item, then sends it on. Returns the stream plus a counter of how
    /// many input items the feeder has pulled (the lazy-ness probe).
    fn harness<St, H, Fut>(
        input: St,
        workers: usize,
        work_cap: usize,
        results_cap: usize,
        handle: H,
    ) -> (FetchAll<Vec<u8>>, Arc<AtomicUsize>)
    where
        St: Stream<Item = String> + Send + 'static,
        H: Fn(usize, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = FetchResult<Vec<u8>>> + Send,
    {
        let pulls = Arc::new(AtomicUsize::new(0));
        let counted = {
            let pulls = pulls.clone();
            input.map(Resource::from).inspect(move |_| {
                pulls.fetch_add(1, Ordering::SeqCst);
            })
        };
        type Item = (usize, String, async_channel::Sender<FetchResult<Vec<u8>>>);
        let (work_tx, work_rx) = async_channel::bounded::<Item>(work_cap.max(1));
        let handle = Arc::new(handle);
        for _ in 0..workers.max(1) {
            let work_rx = work_rx.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                while let Ok((index, url, results)) = work_rx.recv().await {
                    let fr = handle(index, url).await;
                    let _ = results.send(fr).await;
                }
            });
        }
        let fa = drive(
            counted,
            results_cap,
            |b| b,
            move |index, resource, results| {
                let work_tx = work_tx.clone();
                async move {
                    match work_tx.send((index, resource.url, results)).await {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(_) => ControlFlow::Break(()),
                    }
                }
            },
        );
        (fa, pulls)
    }

    // Invariant: exactly one result per input, completion-order, and the stream ends after N —
    // even with scrambled completion timing and a `work_cap` far below N.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delivers_every_result_then_ends() {
        let n = 500usize;
        let input = futures::stream::iter((0..n).map(|i| format!("u{i}")));
        let (fa, pulls) = harness(input, 8, 8, 8, |index, url| async move {
            // Scramble completion order so "unordered" is actually exercised.
            for _ in 0..(index % 5) {
                tokio::task::yield_now().await;
            }
            ok(index, url)
        });
        let mut indices: Vec<usize> = fa.map(|r| r.index).collect().await;
        assert_eq!(indices.len(), n, "expected exactly one result per input");
        indices.sort_unstable();
        assert_eq!(
            indices,
            (0..n).collect::<Vec<_>>(),
            "every input slot settled exactly once"
        );
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            n,
            "pulled exactly N — no over-pull, no short-pull"
        );
    }

    // Invariant (the memory claim): with workers stalled, a million-item input is pulled only up to
    // the bounded queues' depth — never materialized. This is what makes memory O(C), not O(N).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn input_is_pulled_lazily() {
        let huge = 1_000_000usize;
        let (workers, work_cap, results_cap) = (4, 4, 4);
        let gate = Arc::new(tokio::sync::Semaphore::new(0)); // never granted → workers stall
        let input = futures::stream::iter((0..huge).map(|i| format!("u{i}")));
        let (fa, pulls) = harness(input, workers, work_cap, results_cap, {
            let gate = gate.clone();
            move |index, url| {
                let gate = gate.clone();
                async move {
                    let _ = gate.acquire().await;
                    ok(index, url)
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let pulled = pulls.load(Ordering::SeqCst);
        // Ceiling: work_cap buffered + one in each stalled worker + the feeder parked mid-submit.
        assert!(
            pulled <= work_cap + workers + 2,
            "input not lazy: pulled {pulled} of {huge}"
        );
        assert!(
            pulled >= work_cap,
            "feeder didn't fill the queue: pulled {pulled}"
        );
        drop(fa);
    }

    // Invariant: when `submit` signals the pool is gone (`Break`) — a shutdown/interrupt — the
    // feeder STOPS instead of turning every remaining input into an error result. Without this, an
    // aborted million-URL run floods the consumer with "worker pool is shut down" lines.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn feeder_stops_on_submit_break_instead_of_erroring_every_input() {
        let huge = 1_000_000usize;
        let input = futures::stream::iter((0..huge).map(|i| Resource::from(format!("u{i}"))));
        let fa: FetchAll<Vec<u8>> = drive(
            input,
            8,
            |b| b,
            |index, resource, results| async move {
                // Mimic a shut-down pool: surface this slot's failure, then break.
                let _ = results
                    .send(FetchResult {
                        index,
                        url: resource.url,
                        key: None,
                        result: Err(FetchError::Other(anyhow!("worker pool is shut down"))),
                    })
                    .await;
                ControlFlow::Break(())
            },
        );
        let results: Vec<_> = fa.collect().await;
        assert!(
            results.len() <= 2,
            "feeder must stop after the first failed submit, not error every input: got {}",
            results.len()
        );
    }

    // Invariant: dropping the stream aborts the feeder (input stops being pulled) and closes the
    // results channel, so stalled workers that later wake send into a closed channel rather than
    // blocking forever — no hang, no panic, no run-to-completion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drop_aborts_feeder_without_hanging() {
        let huge = 1_000_000usize;
        let (workers, work_cap, results_cap) = (4, 4, 4);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let input = futures::stream::iter((0..huge).map(|i| format!("u{i}")));
        let (fa, pulls) = harness(input, workers, work_cap, results_cap, {
            let gate = gate.clone();
            move |index, url| {
                let gate = gate.clone();
                async move {
                    let _ = gate.acquire().await;
                    ok(index, url)
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let before = pulls.load(Ordering::SeqCst);
        drop(fa); // abort the feeder; close the results channel
        gate.add_permits(1000); // wake the stalled workers → they send into a closed channel
        tokio::time::sleep(Duration::from_millis(150)).await;
        let after = pulls.load(Ordering::SeqCst);
        assert!(
            after < huge,
            "feeder ran to completion despite the drop ({after})"
        );
        assert!(
            after <= before + 2,
            "feeder kept pulling after drop (before {before}, after {after})"
        );
    }
}
