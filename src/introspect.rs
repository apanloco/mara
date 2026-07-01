use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use base64::Engine as _;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::{Html, Response};
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::Duration;

pub use crate::solver::observe::{BrowserId, Phase};

#[derive(Clone, Serialize)]
pub struct Snapshot {
    pub id: BrowserId,
    pub phase: Phase,
    pub url: String,
    pub clicks: u32,
    pub elapsed_ms: u64,
    pub last_click: Option<(i32, i32)>,
    pub live: bool,
    pub user_agent: Option<String>,
    /// The leased exit's catalog code (e.g. `at-vie-101`), stamped by the worker once it
    /// borrows this browser. Lets the dashboard show which exit a browser is solving on, so a
    /// FAILED card can be cross-referenced with `grep code=… run.log`.
    #[serde(default)]
    pub exit: Option<String>,
    /// Set only on the final broadcast when a browser is torn down, so a live dashboard drops
    /// its row instead of accumulating closed browsers forever.
    #[serde(default)]
    pub retired: bool,
}

struct Entry {
    snap: Snapshot,
    display: Option<String>,
    since: Instant,
}

/// The unified per-exit UI row — one projection carrying everything the dashboard shows for an
/// exit: the badge facets (health/activity/cooling/warmth/latency-cap), the latency, and the
/// cumulative stats. Streamed as a **delta** (one row per disposition change), keyed by `code`.
/// Collapses the old split between the pool's `ExitSnapshot` and the store's `ExitStatsInfo`.
#[derive(Clone, Serialize)]
pub struct ExitRow {
    pub code: String,
    pub country: String,
    pub health: String,
    pub activity: String,
    pub latency_ms: Option<u64>,
    pub last_probe_unix: Option<f64>,
    pub proxy_url: String,
    /// True when a latency cap is set and this exit's probed latency exceeds it — i.e. it's
    /// benched (not leasable), so the dashboard shows "too slow" rather than "ready".
    pub over_latency: bool,
    /// Holds a usable clearance (the warm-store membership — the header's `cleared` count).
    pub warm: bool,
    /// Spacing out requests under a per-IP rate ceiling right now (healthy + leasable, just
    /// waiting out its interval) — distinct from `cooling` (a penalty).
    pub paced: bool,
    pub cooling: bool,
    pub cooling_reason: Option<crate::store::Cooling>,
    #[serde(flatten)]
    pub stats: crate::store::Stats,
}

pub struct Introspector {
    state: Mutex<BTreeMap<BrowserId, Entry>>,
    next_id: AtomicU32,
    tx: broadcast::Sender<Snapshot>,
    /// The current exit rows, keyed by `code` — patched by every delta, so a freshly-connecting
    /// dashboard gets the current world in `init` and then follows the delta stream.
    exits: Mutex<BTreeMap<String, ExitRow>>,
    /// Per-exit change **signals**: just the changed `code`, not the row. The current row lives in
    /// `exits`; the connection pump reads the *latest* at send time, so repeated changes to one exit
    /// coalesce to a single send and the stream can never replay a stale backlog (a lag re-seeds).
    exits_tx: broadcast::Sender<String>,
    events: Mutex<VecDeque<Event>>,
    events_tx: broadcast::Sender<Event>,
    ghosts: Mutex<VecDeque<Arc<Ghost>>>,
    ghosts_tx: broadcast::Sender<GhostEvent>,
    progress: Mutex<Option<JobProgress>>,
    progress_tx: broadcast::Sender<JobProgress>,
    mullvad_required: std::sync::atomic::AtomicBool,
    server: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone, Serialize)]
pub struct Event {
    pub ts_unix: f64,
    pub msg: String,
}

/// A `fetch_all` batch's live progress, owned and published by the driver (the pool only ever
/// sees individual jobs, so nothing else can count completions). `total` is `None` for an
/// unsized source — the dashboard then shows counts + rate but no bar/percent/ETA.
#[derive(Clone, Serialize)]
pub struct JobProgress {
    pub total: Option<u64>,
    pub done: u64,
    pub ok: u64,
    pub err: u64,
    pub in_flight: u64,
    pub per_sec: f64,
    pub eta_secs: Option<u64>,
}

const MAX_EVENTS: usize = 300;
const MAX_GHOSTS: usize = 128;

/// A frozen record of a failed solve: the last frame (a data URL) plus the diagnose summary,
/// retained after the browser is torn down so the operator can click it on the dashboard and
/// see what went wrong. The ring is bounded to `MAX_GHOSTS` (oldest evicted).
#[derive(Clone)]
struct Ghost {
    snap: Snapshot,
    summary: String,
    /// `data:image/png;base64,…`, or empty if no frame could be captured.
    shot: String,
    /// The exit's probed latency at failure time — surfaced on the card so failures are scannable
    /// for patterns ("all the failures are >450ms"). Looked up from the live exits snapshot by code.
    latency_ms: Option<u64>,
}

#[derive(Clone)]
enum GhostEvent {
    Added(Arc<Ghost>),
    Retired(BrowserId),
}

impl Introspector {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(256);
        // Deltas are small and frequent; a generous buffer keeps a momentarily-busy dashboard from
        // lagging (and on the rare lag it's re-seeded from the full map, never left stale).
        let (exits_tx, _) = broadcast::channel(4096);
        Arc::new(Introspector {
            state: Mutex::new(BTreeMap::new()),
            next_id: AtomicU32::new(0),
            tx,
            exits: Mutex::new(BTreeMap::new()),
            exits_tx,
            events: Mutex::new(VecDeque::new()),
            events_tx: broadcast::channel(256).0,
            ghosts: Mutex::new(VecDeque::new()),
            ghosts_tx: broadcast::channel(64).0,
            progress: Mutex::new(None),
            progress_tx: broadcast::channel(16).0,
            mullvad_required: std::sync::atomic::AtomicBool::new(false),
            server: Mutex::new(None),
        })
    }

    pub fn event(&self, msg: impl Into<String>) {
        let msg = msg.into();
        let ts_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let ev = Event { ts_unix, msg };
        let mut q = self.events.lock().unwrap();
        if q.len() >= MAX_EVENTS {
            q.pop_front();
        }
        q.push_back(ev.clone());
        drop(q);
        let _ = self.events_tx.send(ev);
    }

    pub fn set_mullvad_required(&self, required: bool) {
        self.mullvad_required.store(required, Ordering::Relaxed);
    }

    /// Publish a `fetch_all` batch's latest progress (the driver throttles how often it calls
    /// this). Retained so a freshly-connecting dashboard sees the current line, not a blank.
    pub fn publish_progress(&self, p: JobProgress) {
        {
            let mut slot = self.progress.lock().unwrap();
            // Keep `done` monotonic. Progress is published from two tasks (the `FetchAll` feeder and
            // its consumer); a racing thread can compute a snapshot, stall, then store it *after* a
            // newer one — which would move the bar backwards. The lock serializes the check+store, so
            // we drop any snapshot that regresses `done`. A genuine new batch (done resets to 0) is
            // allowed through; mid-batch `done` only ever climbs.
            if let Some(cur) = slot.as_ref()
                && p.done < cur.done
                && p.done != 0
            {
                return;
            }
            *slot = Some(p.clone());
        }
        let _ = self.progress_tx.send(p);
    }

    pub fn set_user_agent(&self, id: BrowserId, ua: String) {
        self.update(id, |s| s.user_agent = Some(ua));
    }

    /// Tag a browser with the exit it's solving on (api-internal — the solver never sees an
    /// exit, so this isn't part of the `Observer` seam).
    pub fn set_exit(&self, id: BrowserId, code: String) {
        self.update(id, |s| s.exit = Some(code));
    }

    /// Retain a failed solve as a frozen, clickable ghost (see [`Ghost`]). Snapshots the live
    /// entry's metadata (so the exit code, clicks, and elapsed survive the teardown that
    /// follows), encodes the last frame as a data URL, and pushes onto the bounded ring.
    pub fn failed(
        &self,
        id: BrowserId,
        screenshot: Option<Vec<u8>>,
        summary: String,
        elapsed_ms: u64,
    ) {
        let mut snap = self
            .state
            .lock()
            .unwrap()
            .get(&id)
            .map(|e| e.snap.clone())
            .unwrap_or(Snapshot {
                id,
                phase: Phase::Failed,
                url: String::new(),
                clicks: 0,
                elapsed_ms: 0,
                last_click: None,
                live: false,
                user_agent: None,
                exit: None,
                retired: false,
            });
        snap.phase = Phase::Failed;
        // The live snapshot's `elapsed_ms` is time-in-current-phase (~0, since we snapshot the
        // instant the Failed phase begins). Overwrite it with the real solve duration so the ghost
        // shows how long it actually ran before giving up.
        snap.elapsed_ms = elapsed_ms;
        // Pull the exit's probed latency from the live snapshot (by code) so the ghost card shows
        // it — handy for spotting "every failure is a high-latency exit" at a glance.
        let latency_ms = snap.exit.as_ref().and_then(|code| self.exit_latency(code));
        let shot = screenshot
            .map(|b| {
                format!(
                    "data:image/png;base64,{}",
                    base64::engine::general_purpose::STANDARD.encode(b)
                )
            })
            .unwrap_or_default();
        let ghost = Arc::new(Ghost {
            snap,
            summary,
            shot,
            latency_ms,
        });
        let evicted = {
            let mut g = self.ghosts.lock().unwrap();
            g.push_back(ghost.clone());
            (g.len() > MAX_GHOSTS).then(|| g.pop_front()).flatten()
        };
        if let Some(e) = evicted {
            let _ = self.ghosts_tx.send(GhostEvent::Retired(e.snap.id));
        }
        let _ = self.ghosts_tx.send(GhostEvent::Added(ghost));
    }

    /// Drop a ghost the operator dismissed; broadcast so every dashboard removes its card.
    pub fn dismiss_ghost(&self, id: BrowserId) {
        let removed = {
            let mut g = self.ghosts.lock().unwrap();
            let before = g.len();
            g.retain(|x| x.snap.id != id);
            before != g.len()
        };
        if removed {
            let _ = self.ghosts_tx.send(GhostEvent::Retired(id));
        }
    }

    fn ghost_json(g: &Ghost) -> serde_json::Value {
        serde_json::json!({
            "id": g.snap.id,
            "exit": g.snap.exit,
            "clicks": g.snap.clicks,
            "elapsed_ms": g.snap.elapsed_ms,
            "latency_ms": g.latency_ms,
            "summary": g.summary,
            "shot": g.shot,
        })
    }

    fn ghosts_json(&self) -> Vec<serde_json::Value> {
        self.ghosts
            .lock()
            .unwrap()
            .iter()
            .map(|g| Self::ghost_json(g))
            .collect()
    }

    fn exit_rows(&self) -> Vec<ExitRow> {
        self.exits.lock().unwrap().values().cloned().collect()
    }

    fn state_json(&self) -> serde_json::Value {
        serde_json::json!({
            "mullvad_required": self.mullvad_required.load(Ordering::Relaxed),
            "tls_profile": crate::slim::PROFILE,
            "browsers": self.all(),
            "exits": self.exit_rows(),
            "events": self.events.lock().unwrap().iter().cloned().collect::<Vec<_>>(),
            "ghosts": self.ghosts_json(),
            "progress": self.progress.lock().unwrap().clone(),
        })
    }

    /// Record one exit's changed row and signal its `code`. Patches the retained map (so late
    /// joiners and the coalescing pump always read the latest) and broadcasts only the code. Called
    /// by the pool's mutation funnel — the only place exit state reaches the UI now.
    pub fn publish_exit(&self, row: ExitRow) {
        let code = row.code.clone();
        self.exits.lock().unwrap().insert(code.clone(), row);
        let _ = self.exits_tx.send(code);
    }

    fn exit_row(&self, code: &str) -> Option<ExitRow> {
        self.exits.lock().unwrap().get(code).cloned()
    }

    /// The latency an exit was last probed at, by code — for stamping a ghost card at failure time.
    pub fn exit_latency(&self, code: &str) -> Option<u64> {
        self.exits
            .lock()
            .unwrap()
            .get(code)
            .and_then(|r| r.latency_ms)
    }

    pub fn register(&self, display: Option<String>) -> BrowserId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let snap = Snapshot {
            id,
            phase: Phase::Idle,
            url: String::new(),
            clicks: 0,
            elapsed_ms: 0,
            last_click: None,
            live: display.is_some(),
            user_agent: None,
            exit: None,
            retired: false,
        };
        let entry = Entry {
            snap: snap.clone(),
            display,
            since: Instant::now(),
        };
        self.state.lock().unwrap().insert(id, entry);
        let _ = self.tx.send(snap);
        id
    }

    /// Drop the browser from state and broadcast a final `retired` snapshot so connected
    /// dashboards remove its row (a fresh page load reads `all()`, which already excludes it).
    pub fn deregister(&self, id: BrowserId) {
        let retired = self.state.lock().unwrap().remove(&id).map(|mut e| {
            e.snap.retired = true;
            e.snap
        });
        if let Some(snap) = retired {
            let _ = self.tx.send(snap);
        }
    }

    fn update(&self, id: BrowserId, f: impl FnOnce(&mut Snapshot)) {
        let snap = {
            let mut state = self.state.lock().unwrap();
            let Some(e) = state.get_mut(&id) else { return };
            f(&mut e.snap);
            e.snap.elapsed_ms = e.since.elapsed().as_millis() as u64;
            e.snap.clone()
        };
        let _ = self.tx.send(snap);
    }

    pub fn phase(&self, id: BrowserId, phase: Phase) {
        match phase {
            Phase::Cleared | Phase::Blocked | Phase::Failed => {
                tracing::info!(browser = id, phase = ?phase, "browser phase")
            }
            _ => tracing::debug!(browser = id, phase = ?phase, "browser phase"),
        }
        {
            let mut state = self.state.lock().unwrap();
            if let Some(e) = state.get_mut(&id) {
                e.since = Instant::now();
            }
        }
        self.update(id, |s| s.phase = phase);
    }

    pub fn navigating(&self, id: BrowserId, url: &str) {
        let url = url.to_string();
        self.phase(id, Phase::Navigating);
        self.update(id, |s| s.url = url);
    }

    pub fn clicked(&self, id: BrowserId, x: i32, y: i32) {
        self.phase(id, Phase::Verifying);
        self.update(id, |s| {
            s.clicks += 1;
            s.last_click = Some((x, y));
        });
    }

    fn all(&self) -> Vec<Snapshot> {
        self.state
            .lock()
            .unwrap()
            .values()
            .map(|e| e.snap.clone())
            .collect()
    }

    fn display_of(&self, id: BrowserId) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .get(&id)
            .and_then(|e| e.display.clone())
    }

    /// Every browser that has a display we can screenshot — the set the dashboard streams
    /// thumbnails for.
    fn watchable_ids(&self) -> Vec<BrowserId> {
        self.state
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.display.is_some())
            .map(|(id, _)| *id)
            .collect()
    }

    async fn frame(&self, id: BrowserId) -> Option<Vec<u8>> {
        let display = self.display_of(id)?;
        tokio::task::spawn_blocking(move || crate::solver::frame::grab(&display))
            .await
            .ok()
            .flatten()
    }

    pub async fn serve(self: Arc<Self>) -> Option<String> {
        let listener = bind_localhost(7878).await?;
        let port = listener.local_addr().ok()?.port();
        let app = Router::new()
            .route("/", get(index))
            .route("/ws", get(ws_upgrade))
            .route("/api/state", get(state_api))
            .with_state(self.clone());
        let handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!("introspection server stopped: {e}");
            }
        });
        *self.server.lock().unwrap() = Some(handle);
        Some(format!("http://localhost:{port}"))
    }

    pub fn stop(&self) {
        if let Some(handle) = self.server.lock().unwrap().take() {
            handle.abort();
        }
    }
}

async fn bind_localhost(base: u16) -> Option<tokio::net::TcpListener> {
    for port in base..base.saturating_add(64) {
        if let Ok(l) = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
            return Some(l);
        }
    }
    None
}

async fn index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

async fn state_api(State(intro): State<Arc<Introspector>>) -> axum::Json<serde_json::Value> {
    axum::Json(intro.state_json())
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(intro): State<Arc<Introspector>>) -> Response {
    ws.on_upgrade(move |socket| dashboard_socket(socket, intro))
}

/// The shared write half of one dashboard socket. Both senders — the latency-sensitive state
/// stream and the heavy frame stream — hold this only for the duration of a single `send`, so
/// neither can park the other; the slow part (the frame grab) happens *outside* the lock.
type SharedSink = Arc<tokio::sync::Mutex<futures::stream::SplitSink<WebSocket, Message>>>;

/// One dashboard connection, split into three independent halves so heavy frame I/O can never
/// delay the small JSON that drives the badges/census/table:
///
/// - `state_stream` — the four broadcast subscriptions → small JSON. Latency-sensitive.
/// - `frame_stream` — the framebuffer grabs (slow, timeout-bounded) → binary frames.
/// - the control loop here — reads client `select`/`dismiss` and fans the selection out to the
///   frame stream via a `watch`.
///
/// The grab is the slow part (up to the per-grab timeout against a stalling Xvfb); keeping it in
/// its own task means a dying display starves nothing — state JSON only ever waits behind a
/// frame's actual byte-send (sub-ms on localhost), never behind a grab.
async fn dashboard_socket(socket: WebSocket, intro: Arc<Introspector>) {
    let (mut sink, mut rx) = socket.split();

    let init = serde_json::json!({
        "type": "init",
        "browsers": intro.all(),
        "exits": intro.exit_rows(),
        "ghosts": intro.ghosts_json(),
        "progress": intro.progress.lock().unwrap().clone(),
    });
    if sink
        .send(Message::Text(init.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    let sink: SharedSink = Arc::new(tokio::sync::Mutex::new(sink));
    let (sel_tx, sel_rx) = tokio::sync::watch::channel::<Option<BrowserId>>(None);
    let mut state = tokio::spawn(state_stream(sink.clone(), intro.clone()));
    let mut frames = tokio::spawn(frame_stream(sink, intro.clone(), sel_rx));

    loop {
        tokio::select! {
            incoming = rx.next() => match incoming {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                        if let Some(id) = v.get("dismiss").and_then(|s| s.as_u64()) {
                            intro.dismiss_ghost(id as BrowserId);
                        } else {
                            let _ = sel_tx.send(v.get("select").and_then(|s| s.as_u64()).map(|n| n as BrowserId));
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            },
            // A sender task ending means the client is gone — tear the whole connection down.
            _ = &mut state => break,
            _ = &mut frames => break,
        }
    }
    state.abort();
    frames.abort();
}

/// The **coalescing** state pump. The naïve "one broadcast recv → one socket send" loop falls
/// behind under a 500-exit firehose: the broadcast buffers thousands of stale rows that drain
/// slower than they arrive, so the dashboard replays history instead of showing *now* (the
/// "UI is a second behind" symptom). Instead: block for any event, then drain everything currently
/// pending into a coalesced batch — exit changes dedup by `code` (we send each exit's *latest* row,
/// from the map, once), browser snapshots dedup by id, progress collapses to its latest — and send
/// the batch under a single lock. So repeated changes to one exit cost one send, and the stream can
/// never trail by more than one catalog's worth of current rows. A lagged exit signal re-seeds all.
async fn state_stream(sink: SharedSink, intro: Arc<Introspector>) {
    use broadcast::error::{RecvError, TryRecvError};

    let mut browser_events = intro.tx.subscribe();
    let mut exit_events = intro.exits_tx.subscribe();
    let mut ghost_events = intro.ghosts_tx.subscribe();
    let mut progress_events = intro.progress_tx.subscribe();

    // Pending, coalesced state — carried across iterations so a burst always collapses.
    let mut dirty_exits: HashSet<String> = HashSet::new();
    let mut dirty_browsers: HashMap<BrowserId, Snapshot> = HashMap::new();
    let mut ghost_msgs: Vec<serde_json::Value> = Vec::new();
    let mut progress_dirty = false;
    let mut reseed = false;

    loop {
        // 1. Block until *something* happens (no busy-spin when idle).
        tokio::select! {
            r = exit_events.recv() => match r {
                Ok(code) => { dirty_exits.insert(code); }
                Err(RecvError::Lagged(_)) => reseed = true,
                Err(RecvError::Closed) => break,
            },
            r = progress_events.recv() => match r {
                Ok(_) | Err(RecvError::Lagged(_)) => progress_dirty = true,
                Err(RecvError::Closed) => break,
            },
            r = browser_events.recv() => match r {
                Ok(s) => { dirty_browsers.insert(s.id, s); }
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break,
            },
            r = ghost_events.recv() => match r {
                Ok(GhostEvent::Added(g)) => ghost_msgs.push(serde_json::json!({ "type": "ghost", "ghost": Introspector::ghost_json(&g) })),
                Ok(GhostEvent::Retired(id)) => ghost_msgs.push(serde_json::json!({ "type": "ghost_retired", "id": id })),
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break,
            },
        }

        // 2. Drain everything else immediately available, coalescing into the pending state.
        loop {
            match exit_events.try_recv() {
                Ok(code) => {
                    dirty_exits.insert(code);
                }
                Err(TryRecvError::Lagged(_)) => reseed = true,
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
            }
        }
        while let Ok(r) = progress_events.try_recv() {
            let _ = r;
            progress_dirty = true;
        }
        while let Ok(s) = browser_events.try_recv() {
            dirty_browsers.insert(s.id, s);
        }
        while let Ok(ev) = ghost_events.try_recv() {
            match ev {
                GhostEvent::Added(g) => ghost_msgs.push(
                    serde_json::json!({ "type": "ghost", "ghost": Introspector::ghost_json(&g) }),
                ),
                GhostEvent::Retired(id) => {
                    ghost_msgs.push(serde_json::json!({ "type": "ghost_retired", "id": id }))
                }
            }
        }

        // 3. Build the coalesced batch (latest values only).
        let mut msgs: Vec<serde_json::Value> = Vec::new();
        if reseed {
            // Lost some signals — re-send every exit's current row, then resume deltas.
            reseed = false;
            dirty_exits.clear();
            msgs.push(serde_json::json!({ "type": "exits_full", "exits": intro.exit_rows() }));
        }
        for code in dirty_exits.drain() {
            if let Some(row) = intro.exit_row(&code) {
                msgs.push(serde_json::json!({ "type": "exit", "row": row }));
            }
        }
        for (_, snap) in dirty_browsers.drain() {
            msgs.push(serde_json::json!({ "type": "update", "browser": snap }));
        }
        msgs.append(&mut ghost_msgs);
        if progress_dirty {
            progress_dirty = false;
            if let Some(p) = intro.progress.lock().unwrap().clone() {
                msgs.push(serde_json::json!({ "type": "progress", "progress": p }));
            }
        }

        // 4. Send the batch under one lock. While we send, more events accrue → next loop coalesces.
        let mut s = sink.lock().await;
        for m in msgs {
            if s.send(Message::Text(m.to_string().into())).await.is_err() {
                return;
            }
        }
    }
}

/// Stream framebuffers: thumbnails for every live browser, refreshed slowly; the enlarged one (if
/// any, tracked via `sel_rx`) refreshed fast for a smooth, screenshot-quality view. Grabs run
/// outside the sink lock, so a stalling Xvfb delays only this task's next frame, never state.
async fn frame_stream(
    sink: SharedSink,
    intro: Arc<Introspector>,
    sel_rx: tokio::sync::watch::Receiver<Option<BrowserId>>,
) {
    let mut thumb_tick = tokio::time::interval(Duration::from_millis(600));
    let mut sel_tick = tokio::time::interval(Duration::from_millis(150));

    loop {
        tokio::select! {
            _ = thumb_tick.tick() => {
                // Grab every live browser concurrently with a per-grab timeout: a slow or
                // just-torn-down Xvfb can't stall the strip (it just yields no frame this tick).
                let grabs = intro.watchable_ids().into_iter().map(|id| grab_frame(&intro, id));
                for frame in futures::future::join_all(grabs).await.into_iter().flatten() {
                    if sink.lock().await.send(Message::Binary(frame.into())).await.is_err() {
                        return;
                    }
                }
            }
            _ = sel_tick.tick() => {
                let selected = *sel_rx.borrow();   // copy + drop the guard before any await
                if let Some(id) = selected
                    && let Some(frame) = grab_frame(&intro, id).await
                    && sink.lock().await.send(Message::Binary(frame.into())).await.is_err()
                {
                    return;
                }
            }
        }
    }
}

/// Grab one browser's framebuffer, id-tagged and ready to send, with a hard timeout so a
/// slow or just-torn-down Xvfb yields nothing this tick instead of stalling the strip.
async fn grab_frame(intro: &Arc<Introspector>, id: BrowserId) -> Option<Vec<u8>> {
    let png = tokio::time::timeout(Duration::from_millis(200), intro.frame(id))
        .await
        .ok()
        .flatten()?;
    Some(tag_frame(id, png))
}

/// Prefix a PNG frame with its 4-byte big-endian browser id, so the dashboard can route each
/// frame to the right thumbnail (one socket now streams every live browser, not just one).
fn tag_frame(id: BrowserId, mut png: Vec<u8>) -> Vec<u8> {
    let mut buf = id.to_be_bytes().to_vec();
    buf.append(&mut png);
    buf
}

impl crate::solver::observe::Observer for Introspector {
    fn register(&self, display: Option<String>) -> BrowserId {
        self.register(display)
    }
    fn phase(&self, id: BrowserId, phase: Phase) {
        self.phase(id, phase)
    }
    fn navigating(&self, id: BrowserId, url: &str) {
        self.navigating(id, url)
    }
    fn set_user_agent(&self, id: BrowserId, ua: String) {
        self.set_user_agent(id, ua)
    }
    fn event(&self, msg: String) {
        self.event(msg)
    }
    fn clicked(&self, id: BrowserId, x: i32, y: i32) {
        self.clicked(id, x, y)
    }
    fn failed(&self, id: BrowserId, screenshot: Option<Vec<u8>>, summary: String, elapsed_ms: u64) {
        self.failed(id, screenshot, summary, elapsed_ms)
    }
    fn deregister(&self, id: BrowserId) {
        self.deregister(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deregister_removes_the_row_so_dead_browsers_dont_accumulate() {
        let intro = Introspector::new();
        let a = intro.register(Some(":1".into()));
        let b = intro.register(Some(":2".into()));
        assert_eq!(intro.all().len(), 2);
        intro.deregister(a);
        let rows = intro.all();
        assert_eq!(rows.len(), 1, "retired browser's row must be dropped");
        assert_eq!(rows[0].id, b);
        intro.deregister(a);
        assert_eq!(intro.all().len(), 1);
    }

    #[test]
    fn deregister_broadcasts_a_retired_snapshot_so_live_dashboards_drop_the_row() {
        let intro = Introspector::new();
        let mut rx = intro.tx.subscribe();
        let a = intro.register(Some(":1".into()));
        assert!(
            !rx.try_recv().unwrap().retired,
            "register broadcasts a live snapshot"
        );
        intro.deregister(a);
        let snap = rx
            .try_recv()
            .expect("deregister must broadcast, not just mutate state");
        assert_eq!(snap.id, a);
        assert!(
            snap.retired,
            "the broadcast marks the row retired so the dashboard removes it"
        );
    }
}
