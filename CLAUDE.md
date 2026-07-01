# mara

## Development philosophy

This project is **spec-driven**, and this file is the spec — an *architecture
document*: what the program is from the outside (flags, schemas, protocols, the
contracts and invariants between stages) plus the *why* behind non-obvious
decisions. The source code is the source of truth for *how*.

**The level-of-detail test.** Before adding a sentence, ask: *could an agent
recover this by reading the code?* If yes, leave it out. Function names, exact
constants, log levels, wire formats, and struct layouts live in the code. What
belongs here is what the code *can't* tell you: the external contract, the shape
of the data, the invariants a change must preserve, and the reason a non-obvious
choice was made. Describe the boundary, not the mechanism.

- **Simplicity is a hard requirement.** Prefer deleting code over adding
  abstractions. Consolidate duplicates. Simplify what you're touching, not what
  you find — surface cleanups elsewhere as their own task. When a principle
  disagrees with the simpler code, the simpler code wins.
- **Discuss before implementing.** Propose an approach and get agreement first.
  Never enter planning mode unless explicitly asked.
- **Keep this file current, in the present tense.** Update it as part of every
  task, at the architecture level. No history, no changelog; git is the record.
  Warn against a removed approach as a current rule ("don't do X — it tripped
  429s"), never as narrative.

## What mara is

A scraper that fetches over a rotating pool of egress IPs, clearing
bot-protection challenges along the way. Today that means Cloudflare: it solves
the interactive challenge in a real headed browser **once** to bank a
`cf_clearance` cookie, then serves every subsequent request to that host
**browser-free** ("slim": a plain HTTP client replaying the cookie + UA). The
browser is the fallback; slim is the hot path. "Solve once in a browser, then
replay browser-free" is the general model — Cloudflare is just the only challenge
wired up so far.

**Whether a host is solved is a property of the host, not the request**, and
**routing is fixed, not auto-detected.** Configured `Config.domains`
(longest-suffix-matched, so a domain covers its subdomains) carry
`{ host, solve, per_ip, aggregate }` (rates in req/min). A host matching a
`solve` domain takes the warm/solve/replay path; **every other host is fetched
raw** — lease any healthy exit, send as-is, no browser, but still rotate + cool
down on 429/block. Declare CF hosts in `solve_domains` (or
`Client::enable_solving_for_domain`); everything else is raw. A `raw` domain
entry is routing-identical to an unregistered host — it exists only to hang
per-host pacing (or to carve a raw exception out of a broader `solve` suffix).

The `fetch` CLI inverts this default: being a page fetcher, it registers each
target host as a `solve` domain, so `mara fetch <cf-url>` clears CF with no
flags. `--raw` opts back out for non-CF targets (an API call, an image) so they
ride the pool browser-free.

There is deliberately **no auto-promotion** of a host onto the warm path: a host
you *know* is CF should replay persisted clearances from request #1 rather than
waste a cookieless probe, and a single challenged exit must never flip a whole
host mid-run and stall a fast run.

Failure modes are local: an **unregistered CF host gives up `Challenged`** (raw
can't solve — register it), and **no single challenged exit can affect the host
or any other request**. On the raw path a challenge flags *that IP*: mara benches
it briefly and rotates to a clean exit; only when a request's own rotation is
exhausted with every tried exit challenged does that *one request* give up.
Leave non-CF traffic (an API POST, an image GET) unregistered so it rides the
pool browser-free.

The unit of work is a `Resource` (`url` + optional `method`/`headers`/`body` +
an optional caller `key` echoed on the result); `From<&str>/<String>/<Url>` give
a bare `GET`. Bodies are **bytes-canonical**: the worker produces `Vec<u8>`,
decoded to `T` once at the stream edge. `fetch_all`/`fetch_http` decode to
`String` (UTF-8 lossy — no charset detection); `fetch_all_bytes`/`fetch_bytes`
hand back raw bytes. Text-vs-bytes is orthogonal to solve-vs-raw routing.

External contract (`cli/src/main.rs`): `fetch <url…>`, `capture <url>`,
`doctor`. Key `fetch` flags:

- `-b/--browser-concurrency` — **B**, the live-browser cap (= concurrent solves),
  a machine-load guard (default 4). Clamped down to the catalog size.
- **There is no `-c` / client-concurrency dial.** Serving width **is the exit
  count** (one serving worker per exit). CF's per-IP `1015` means more warm IPs
  is the only way to scale; throttle by pacing or by provisioning fewer exits.
- `--rate <req/min>` — **per-IP** ceiling (per exit) · `--aggregate-rate` —
  **pool-wide** ceiling. Two orthogonal fixed pacers, both off by default: per-IP
  defends a per-IP limit (CF `1015`) and scales with warm-IP count; aggregate
  defends a per-account/key limit (e.g. an Algolia app key) where rotating IPs
  doesn't help.
- `--raw` — don't treat the targets as CF (see above) ·
  `--exit socks5://…` (repeatable) · `--mullvad` (live Mullvad catalog) ·
  `--max-exit-latency` · `--probe-concurrency` (default 64) · `--loaded` ·
  `--data-dir` (persist) · `--serve` (keep the dashboard alive).

**Interrupt (Ctrl-C / SIGTERM)** is an *abort*, not a graceful drain:
`Client::abort` drops the worker tasks' `Browser`s (so `kill_on_drop` SIGKILLs
Chrome) and exits immediately. This is deliberate — a signal skips Rust drops, so
a plain exit would orphan Chrome, while `shutdown()` would drain the whole batch
first. Clearances persist on solve, so only in-memory stat deltas are lost.
`shutdown()` (with `persist_all`) is the normal end-of-run path; a second signal
hard-exits.

## Module decomposition

One published library crate **`mara`** (`src/`) plus an unpublished CLI binary
**`mara-cli`** (`cli/`). The library has three layers with a one-way dependency
arrow **`orchestration → solver → vocabulary`** — once compiler-enforced as
crates, now a convention. Keep it one-way.

**Public API surface is deliberately small** (the rest is `mod`, not `pub mod`,
reachable in-crate via `crate::…`): the entry points [`Client`]/`Config`/`Domain`/
`Resource`/`Policy` + the result types (`FetchAll`/`FetchResult`/`Outcome`/
`FetchError`/`Reason`/`Method`) + `host_of` + `wait_full_load`, and two report
modules `doctor` and `store` (the read-model behind `Client::snapshot`). The crate
is `#![deny(missing_docs)]`, so **every new `pub` item needs a doc comment** — if an
internal type doesn't belong on docs.rs, keep its module private and re-export only
the specific type. Don't widen the surface to satisfy a test: white-box tests of an
internal module live as `#[cfg(test)]` units *inside* that module (they see private
items), not in `tests/` (which sees only the public API).

- **vocabulary** — `Reason` (why a fetch failed:
  Challenged/RateLimited/Blocked/Timeout/Unreachable/Unavailable) and
  `Clearance` (lifted cookies + UA + host + expiry).
- **`crate::solver`** — the browser. `Browser::solve(cfg, url) -> Result<Cleared,
  Reason>`. **Firewall (must hold):** `solver` has zero references to the
  lease/pool/worker/store/client — the browser code cannot see an exit. Its only
  outward seam is `solver::observe::Observer`.
- **orchestration** — `Client`, the one-worker-per-exit serving pool + background
  maintainer, egress/pool, slim, store, ladder, introspection, doctor, mullvad.

Two fetch shapes: **browser-free** (slim — warm path on a solve host, escalating
to a headed solve on challenge; or the raw path elsewhere) and **headed**
(`fetch_browser` — always launches a browser, hands the caller a live `Page`).
The browser-free core is `Client::fetch_all(resources) -> FetchAll<String>`, an
**unordered, completion-order `Stream`** of one `FetchResult` per input;
`fetch_all_bytes` is the raw-bytes twin, `fetch_http`/`fetch_bytes` one-shot
shims.

`FetchAll`'s contract is **O(exit count) memory, not O(count)**: a background
feeder pulls input lazily into a work queue bounded to the exit count, so at most
~one resource + one body per exit is ever live. Termination is driven by the
results channel closing, never by a total (`with_total`/`size_hint` feed only the
progress display). Dropping a `FetchAll` aborts the feeder and closes the
channel.

## The protagonists: one worker per exit + the maintainer

**Serving is one-worker-per-exit; warming is a separate background task.** Exactly
one serving worker per exit (bound to its `code` for life) and a small pool of
**B** maintainer tasks. They coordinate only through the exit's `activity` facet —
the CAS `Idle→Serving`/`Idle→Solving` is the single coordination point.

- **Serving worker** (bound to exit *X*): waits until *X* is claimable (ready +
  idle + not cooling + under the latency cap) **and**, for a solve workload, warm
  for **all** solve domains (raw needs no warmth). Only then pulls a `Job`,
  claims *X*, and serves slim. It **never touches a browser**. This gate is the
  **tail-latency guarantee**: a cold/warming worker never claims a resource, so
  warm idle workers finish the stragglers — a slow-to-warm or failing exit can
  never hostage a request another exit could serve *now*.
- **The maintainer** (B tasks): walks the catalog **fastest-first**, solves once
  under a B permit to bank a `(exit, domain)` clearance, and releases the exit
  warm — persistently, keeping the catalog warm as exits free/cool/recover. It
  warms host-wide clearances keyed by the registered **solve-domain** (so a
  suffix registration covers subdomains). A warm solve gets a short warm timeout
  (≪ `--timeout`) so a stuck exit frees its scarce B slot fast; a headed
  `fetch_browser` gets the full timeout. Empty solve-set → the maintainer idles.

This makes pacing **local**: a worker owns its exit, so per-IP spacing is a local
sleep with no shared coordinator.

- **Pacing — two orthogonal fixed pacers, both req/min, no burst.**
  - **per-IP**: the worker stamps a next-serve deadline on its own exit
    (in-memory) and sleeps precisely to it before pulling. No cross-worker
    coordination. Aggregate throughput = warm-exits × per_ip → **breadth is the
    scale dial**. Defends CF's per-IP `1015`.
  - **aggregate**: a lock-free GCRA pacer (one atomic clock per domain) holds the
    whole pool to the rate regardless of exit count. Taken **before leasing an
    exit** and **per attempt** (retries re-claim a slot, so no un-paced burst);
    cancellable on shutdown. Defends a per-account/key limit where rotating IPs
    is useless — breadth is irrelevant to it.

  Neither pacer cools the exit or tips the pool into `Resting` — a paced exit is
  healthy, just spacing. Both apply to solve *and* raw hosts. Still deferred: a
  knee-driven decrease-only ratchet, and aggregate-429 backoff.

- **Two queues, a rotation budget.** Callers submit a `Job` to an MPMC
  fresh-work queue bounded to the exit count (the backpressure behind
  `FetchAll`'s O(exits) memory); a serve **failure re-queues** onto an unbounded
  retry queue (drained first) so another exit tries it. Live jobs stay ≤ exits
  since a job only re-queues after being pulled. `Job::Headed` carries a
  type-erased executor; the worker solves headed on its own exit, keeps the page
  alive across extraction, then tears it down.

- **A resource never fails on a *winnable* obstacle — only on the origin's answer
  or an unwinnable config error.** Any real origin response (2xx, 404, most 4xx)
  is delivered as success. A bot-protection/transport obstacle (rate-limit,
  block, timeout, unreachable, a stale-clearance challenge on a registered host)
  is mara's job to overcome, so the resource **retries forever** across exits.
  Give-up is reserved for three unwinnable-by-retry cases:
  1. a **raw challenge** on every tried exit → an unregistered CF host
     (`GaveUp(Challenged)` — register it);
  2. a **broken fingerprint triple** — a solve-host challenge while slim has
     *never once* served (`FingerprintMismatch`);
  3. a **persistently dead pool** — when `Resting` (every non-wonky exit cooling)
     *and* a resource has made no progress for `lease_timeout` (`Resting`). A
     *transient* resting wave is winnable, so the resource waits. Pacing never
     triggers this reap (a paced exit isn't cooling).

- **Failure routing.** A winnable exit-quality failure (5xx/timeout/rate-limit/
  block) cools or kills *this* exit and re-queues **without touching
  `Job.attempts`** (retry forever). Because the exit is now cooled/cold, its own
  worker won't re-pull the job, so the retry lands on a *different* exit — one bad
  IP can't fail a resource. Only a **raw challenge** runs down `Job.attempts`. A
  **solve-host challenge** drops the stale clearance and benches the exit with an
  **escalating** cooldown (reset by the next successful serve): this is the
  key anti-waste rule — a CF-flagged IP the browser can warm but whose slim
  replay is always challenged climbs to a long bench and drops out of the
  fastest-first warming rotation, instead of soaking up the B budget being
  re-warmed on loop. A healthy exit whose cookie occasionally expires challenges
  rarely and resets its streak → only a brief cool.

- **Wakes are per-exit.** Each exit has one interested worker, so a disposition
  change (a banked clearance, a lapsed cooldown, a re-confirming probe) fires just
  *that* exit's `Notify` — never a broadcast across a 500-exit catalog. A worker
  parks with register-then-recheck plus a ~1s fallback bounding the reap check.
  Shutdown wakes every worker.

- **B** is a shared semaphore acquired around a solve. Slim HTTP clients are
  pooled per proxy (one reusable client per exit — bounds FDs).

**Hermetic coverage.** The warming **solve** and the **slim** request are seams
(`SolveFn`/`SlimFn`, `None` in production). Tests inject fakes and drive the real
workers + maintainer with no browser or HTTP, proving: the catalog warms and
serves, multi-domain independence, a raw host never solves, one challenged exit
can't block the scrape, the tail-latency guarantee, per-IP spacing, and that the
three give-ups above are the *only* failures. Only real CF/Mullvad behaviour is
left to the live tests.

**Diagnostics.** Tracing is a flat, greppable log dump keyed by exit (`code=`)
and resource (`req=`); the link from a stalled serving request to the maintainer
warm that unblocks it is the **host/domain**. `-v <level>` sets the `mara`
crate's level; `RUST_LOG` overrides. On a solve give-up, `solver::diagnose` saves
a screenshot, framebuffer, DOM, widget probe, and summary under
`<artifacts>/fail-<id>`, and hands the last frame + summary to the
`Observer::failed` ghost seam — so a failed browser is inspectable from the
dashboard and traceable to its give-up line by `code=`.

## Egress, the pool, and the unified `Exit`

There is **one** egress: `ExitPool`. *Direct* (no-proxy) egress is a pool of one
always-ready synthetic exit, so there is no separate direct code path.
`api::egress` is the thin leasing vocabulary (`Lease`, `ExitStatus`,
`Availability`). The pool is **source-agnostic**: fed exits + an injected liveness
`Probe`. The probe is a plain **TCP-connect** to the SOCKS endpoint for both
manual and Mullvad exits (reaching a `*.relays.mullvad.net` SOCKS port already
proves it's a Mullvad relay), run at high concurrency with continuous refill so a
500-exit catalog warms in seconds. The connect timeout is **tied to the latency
cap**: with `--max-exit-latency` set, a timeout *is* the verdict "slower than the
cap" (recorded as an over-cap latency → reads `slow`); without it, a miss just
retries. `mullvad::bootstrap` builds the pool from the live catalog after a
one-time `am.i.mullvad` tunnel check.

Each `Exit` holds all per-exit state as three **orthogonal** facets mutated by the
single leasing worker:

- **health** — `Probing | Ready | Wonky` (durable). A slim **timeout** demotes
  `Ready → Probing` (it stopped answering; a cheap probe beats another full slim
  timeout) — the one non-probe event that writes health. All probe effects go
  through **one** writer (`observe_probe`), so a latency refresh can never split
  from the health transition it implies. While leased, a re-probe refreshes *only*
  latency; on an idle exit a reachable probe confirms `Ready`. Re-probe cadence is
  health-aware (unconfirmed `Probing` retries fast; settled exits re-confirm
  slowly). Enough consecutive failures demote `Probing → Wonky`; a later reachable
  probe recovers it.
- **activity** — `Idle | Serving | Solving`. `Idle` = free to lease; the others
  mean held by a worker.
- **warmth + cooldown + pacing** (all in `store::ExitData`) — per-`(exit, host)`
  clearance presence, the **single** cooldown field, and the per-`(exit, domain)`
  next-serve deadline. All three are orthogonal: an exit can be warm *and* cooling
  *and* healthy-but-paced.

Leasable = `Ready` + `Idle` + not cooling. There is exactly one cooldown per exit.
The pool is the single source of truth for routing: a serving worker claims its
own exit by `code` and gates on warmth-for-domain; the maintainer picks the
lowest-latency exit cold for *some* domain. Warm *membership* (non-stale
clearance, ignoring cooling) is distinct from warm-*now* (also excludes cooling).

## Persistence & the fingerprint triple (`api::store`)

`store` persists per-exit clearances + stats, keyed by proxy URL, under
`--data-dir` (ephemeral otherwise). Clearances persist **only on solve** and are
seeded on load; everything else is in-memory.

**Fingerprint-triple invariant:** a handed-down clearance is only accepted by CF
if the *installed Chrome major* ↔ the *pinned slim TLS profile* ↔ the *replayed
UA* all agree. On drift the stored clearances are discarded (so a stale cookie
never wedges every client onto the headed solver) and a startup canary warns.
Bumping the Chrome binary and `wreq`/`wreq-util` must happen in lockstep. The
first slim failure after a solve is surfaced as `FingerprintMismatch`, not a
generic give-up.

## Introspection (`api::introspect`)

A single-page live dashboard over WebSocket: a header **census** (a per-state
head-count of the catalog that sums to the total), a **job-progress** strip
(owned by the `FetchAll` driver, since only it can count completions), then
collapsible carousels of live browser thumbnails and failed-run **ghosts** over
the exits table.

**Exit state streams as per-exit deltas, not catalog snapshots** — load-bearing at
500+ exits: a single-exit change must cost one small row, not a re-serialize of
the whole catalog on the serving thread. The unit is one **`ExitRow`** keyed by
`code`. Every pool mutation funnels through one decision point that diffs the
exit's *disposition* (badge facets + bucketed latency, excluding monotonic
counters) and emits a delta only on a real change; raw counters ride along in
whatever delta fires next. Time-based transitions with no mutation (a cooldown
expiring) are caught by a per-cycle sweep. Delivery **coalesces**: a burst of
thousands of changes collapses to ≤ one catalog of current rows, so the socket
can never trail behind a growing backlog of stale rows.

Each connection runs **three** independent tasks sharing the socket write half
behind a per-`send` mutex: a state stream (exit deltas, ghosts, job-progress), a
frame stream (framebuffer grabs), and a control loop (select/dismiss). The split
is load-bearing — frame grabs are slow and timeout-bounded, so a stalling Xvfb
must never park state delivery. **Ghosts** are the *only* retained state in
introspect: a bounded ring of frozen (frame + summary) records making a failed
solve inspectable after the browser is gone.

The dashboard projects an exit's three facets to one badge by a single priority
(`BADGE_ORDER`, also the census + sort order): activity first, then every reason
it can't be leased, then the two leasable states —
`solving > serving > rate-limited/blocked/cooldown > wonky > slow > probing >
paced > warm > cold`. The ordering is **honest about leasability**: `warm`/`cold`
mean "servable *right now*", so a cleared-but-still-`probing` exit reads `probing`,
and `paced` (warm + healthy but spacing) sits just above `warm`. The header's
**`cleared`** count is the orthogonal warm-store size (every exit holding a usable
clearance, servable now or not); it can't be a census pill or it would
double-count and break the sums-to-total invariant.

**Warmth-display boundary.** Warmth is per-`(exit, domain)` — routing keys on that,
so routing is always correct. The per-exit `warm` badge (warm-for-≥1 host) is a
single-solve-domain projection: honest with one CF host in play (the common case),
but it overclaims with two. Display-only. The documented upgrade, once a real
multi-CF-host consumer exists, is a `warm k/n` badge; the per-host truth is always
the clearances table. Don't build `k/n` speculatively.
