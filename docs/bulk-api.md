# Bulk streaming API

Status: **implemented.** `Client::fetch_all` → `FetchAll`, bounded work queue, shared results
channel, lazy feeder with drop-cancellation; `fetch_http` is now a shim; `main.rs` streams results;
the UI job-progress line ships (driver-owned counter → introspect broadcast → dashboard strip). The
live contract lives in CLAUDE.md; this doc keeps the design rationale. Verified hermetically (the
`client::tests` invariant suite) and live (~125 MB peak at C=8 across N=300 — memory tracks C, not N).

## Problem

The current driver spawns N tasks up front (one per URL, each awaiting its own `oneshot`), then
collects them all (main.rs:230-246): **O(N) tasks + O(N) oneshots**. And the work queue is
`async_channel::unbounded()` (worker.rs:501), so submitted jobs pile up unboundedly too. Fine at
10k, a wall at 1M. No visibility into how much work is left.

## Change

`fetch_all` is the **core**; a thin `fetch_http` shim keeps the 1-URL path ergonomic (same impl, no
stream to drive). `fetch_all` returns a **named** stream type so it can carry a total and builder
methods.

```rust
pub fn fetch_all(&self, urls: impl IntoIterator<Item = String>) -> FetchAll;

pub struct FetchAll { /* drains the shared results channel; Stream<Item = FetchResult> */ }
impl FetchAll {
    /// Supply a total when the source can't size itself (cursor, from_fn). Ranges/vecs/maps
    /// are sized automatically from the iterator's upper bound.
    pub fn with_total(self, n: usize) -> Self;
}

pub struct FetchResult {
    pub index: usize,   // input slot — URLs can repeat (e.g. --repeat); body-dominated, so url dup is noise
    pub url: String,
    pub result: Result<Outcome<String>, FetchError>,
}

// Thin convenience over fetch_all — one URL, one result, no stream.
pub async fn fetch_http(&self, url: &str) -> Result<Outcome<String>, FetchError>;

// Async-generation variant for sources that produce URLs asynchronously.
pub fn fetch_all_stream(&self, urls: impl Stream<Item = String>) -> FetchAll;
```

- **Unordered**, completion-order; exactly one result per input → the stream ends after N items.
- `fetch_browser` (live-page extraction) stays separate — it can't go browser-free.
- `fetch_http` now routes through the shared results channel instead of a dedicated `oneshot`, a
  one-extra-channel-hop cost on the 1-URL hot path; expected to be noise, confirm at benchmark time.

## Invariants (the load-bearing mechanism changes)

These are what make the headline claims true; without them the stream is still O(N). The first two
must hold together — bounding only the work queue just moves the O(N) pile from submission to
completion.

1. **Results drain over one bounded channel, never N oneshots.** `Job::Html` stops carrying a per-call
   `oneshot::Sender` (worker.rs:101) and instead carries the input **`index`**; the worker sends
   `(index, FetchResult)` into a **single shared results channel (cap ~C)** that `FetchAll` drains.
   This is a real change to `Job::Html` and `submit`. Without it, a `Stream` over
   `FuturesUnordered<submit+await>` still pins O(N) futures and N oneshots.

2. **The work queue becomes bounded (~C); submission blocks.** Replace the `unbounded()` work channel
   (worker.rs:501) with a bounded one (cap ~C), or gate `submit` behind a ~C permit. This is what
   makes "lazy, never materialized" actually true: the input iterator is pulled only as the bound
   frees, so at most ~C URLs and ~C bodies are live at once — backpressure all the way to the
   generator. The chain: consumer slow → results channel full → workers block on send → workers stop
   pulling jobs → work queue full → feeder blocks → input iterator not pulled.

3. **Feeding and draining are concurrent; dropping `FetchAll` unwedges both.** The backpressure chain
   above only holds if topping up the work queue and draining results happen on *different* paths —
   `poll_next` must never await a `submit` to completion before yielding a ready result, or it
   deadlocks (workers can't push into a full results channel; the consumer is parked on submit). So
   submission lives in a feeder concurrent with the drain (a feeder task, or a `poll_next` that
   selects over feed-and-drain, never serializing them). And because both channels are bounded,
   **dropping `FetchAll` early must close the results channel and stop the feeder** — otherwise
   in-flight workers block forever on send and C workers leak.

## Totals

`size_hint().1` is an **upper bound that may be `None`** — exact only for ranges/vecs/maps. For an
exact total "for free", bound the input (`I::IntoIter: ExactSizeIterator`); otherwise `with_total(n)`
supplies it (e.g. from a `COUNT(*)`); open-ended sources stay unknown.

The total feeds **only the progress display** — never stream termination. The stream ends when the
input is exhausted and one result has come back per URL the feeder actually pulled, so a lying or
filtered iterator (a `size_hint` that over- or under-counts) degrades the bar's accuracy but never
the end-of-stream.

## What it fixes

**O(concurrency) memory**, not O(count): only ~C bodies (~650 KB each — observed average) in flight,
flat whether N is 1k or 100M. Dynamic generation preserved (the input is a lazy tap the pool opens at
its own rate). Enables a real progress view.

Scope: this is the **slim/http bulk path**. `--loaded` (and any `fetch_browser` extraction) can't go
browser-free and stays the separate per-call path, so it keeps its current O(N) shape — `--loaded` is
not a bulk shape (a live headed page per URL is the opposite of streaming millions).

## UI

A driver-owned counter publishes `{total, ok, err, in_flight}` (with a timestamp for rate/ETA) over
the existing broadcast (introspect.rs:67-75) — **someone has to own it**, since nothing today counts
completions; the pool itself only sees individual jobs, so this lives in the `fetch_all` driver. New
**job-progress** line next to the census:

- total known → `fetched 340,201 / 1,000,000 (34%) · in-flight 312 · 1,240/s · ETA ~9m` + a bar.
- total unknown → counts + rate, no bar.

Can / can't show:

- ✅ counts done / left / in-flight, throughput, ETA (when total known); **what** is in flight *now*.
- ❌ **what** specific items are *not yet started* — generated lazily, they don't exist yet (only the count).
