# mara

## Development philosophy

This project is **spec-driven**, and this file is the spec — an *architecture
document*: what the program is from the outside (flags, schemas, protocols, file
layout, the contracts and invariants between stages) plus the *why* behind
decisions that aren't obvious from the code. The source code is the source of
truth for *how* each piece is built.

**The level-of-detail test.** Before adding a paragraph, ask: *could an agent
recover this by reading the code?* If yes, leave it out. Function names, library
choices, CSS selectors, struct layouts, exact constants, and parser quirks live
in the code. What belongs here is what the code *can't* tell you: the external
contract, the shape of the data, the invariants a change must preserve, and the
reason a non-obvious choice was made. When in doubt, describe the boundary, not
the mechanism.

- **Simplicity is a hard requirement.** Prefer deleting code over adding
  abstractions. Consolidate duplicates rather than adding a near-twin beside an
  old one. Simplify what you're touching, not what you find — cleanups elsewhere
  are their own task; surface them. When a principle disagrees with the simpler
  code, the simpler code wins.
- **Discuss before implementing.** Propose an approach and get agreement first.
  Never enter planning mode unless explicitly asked.
- **Keep this file current, in the present tense.** Update it as part of every
  task, at the architecture level — not as a transcript of the diff. No history,
  no changelog; git is the record. Warn against a removed approach as a current
  rule, never a narrative: "don't do X — it tripped 429s," not "we tried X."

## What mara is

A scraper that fetches over a rotating pool of egress IPs, with support for
clearing bot-protection challenges along the way. Today that means Cloudflare
(Turnstile/managed): it solves the interactive challenge in a real headed browser
**once** to bank a `cf_clearance` cookie, then serves every subsequent request to
that host **browser-free** ("slim": a plain HTTP client replaying the cookie + UA).
The browser is the fallback; slim is the hot path. The "solve once in a browser,
then replay browser-free" shape is the general model — Cloudflare is the only
challenge wired up so far, not the limit of what could be.

**Whether a host is solved is a property of the host, not the request.** The
configured **domains** (`Config.domains: Vec<Domain>`, resolved in `api::worker::Shared`,
longest-suffix-matched so a domain covers its subdomains) decide per host — a `Domain` carries
`{ host, solve, per_ip: Option<u32>, aggregate: Option<u32> }` (rates in req/min). A host matching a `solve` domain
takes the warm/solve/replay path; **every other host is fetched raw** — lease any
healthy exit, send the request as-is, no browser, but still rotate + cool down on
429/block via the same ladder. **Routing is fixed, not auto-detected:** you declare
the CF hosts in `solve_domains` (or `Client::enable_solving_for_domain`); everything
else is raw. There is deliberately **no auto-promotion** — that was removed because
it was both worse than registering (a host you *know* is CF should use the warm path
from request #1, replaying persisted clearances, not waste a cookieless probe) and a
footgun: a single challenged exit could flip the whole host onto the warm path
mid-run and stall a fast catalog-wide run.

So the failure modes are simple and local: an **unregistered CF host gives up
`Challenged`** (raw can't solve — the fix is to register it), and crucially **no
single challenged exit can ever affect the host or any other request**. On the raw
path a challenge means *that IP* is flagged: mara benches that one exit briefly (a
transient cooldown, so the next lease skips it) and **rotates to a clean exit**; only
when a request's own rotation is exhausted with every tried exit challenged does that
*one request* give up `Challenged`. Register the CF hosts you scrape; leave non-CF
traffic (an API POST, an image GET) unregistered so it rides the exit pool
browser-free. The `fetch` CLI, being for HTML pages, **registers each target URL's
host as a solve domain by default** (`--raw` opts out), so `mara fetch <cf-url>` still
clears CF without extra flags.

The unit of work is a `Resource` (`url` + optional `method`/`headers`/`body` for
non-HTML traffic + an optional caller `key` echoed back on the result);
`From<&str>/<String>/<Url>` give a bare `GET`, so URL strings still work directly.
Bodies are **bytes-canonical** in the slim path: `fetch_all`/`fetch_http` decode to
`String` for you (UTF-8 lossy — no charset detection, fine for HTML/JSON), while
`fetch_all_bytes`/`fetch_bytes` hand back raw `Vec<u8>` for images/binaries. The
text/bytes choice is orthogonal to the solve/raw routing.

External contract (`src/main.rs`): `fetch <url…>`, `capture <url>`, `doctor`.
Key `fetch` flags:

- `-b/--browser-concurrency` — **B**, the live-browser cap (= concurrent solves); a
  **machine-load guard**, default **4**. Drawn by the background *maintainer* and by headed
  `fetch_browser`. Clamped down to the catalog size (no point launching more browsers than exits).
- **There is no `-c` / client-concurrency dial.** Serving width **is the exit count**, by
  construction: one serving worker per exit (see the protagonists section). Breadth is how many
  exits you provision — CF's `1015` is per-IP, so more warm IPs is the only way to scale. Throttle
  a run by pacing per-IP (a later phase) or by provisioning fewer exits, not by a worker count.
- `--rate <req/min>` — **per-IP** rate ceiling (per exit) · `--aggregate-rate <req/min>` —
  **pool-wide** rate ceiling (across all exits). Two orthogonal fixed pacers (see the pacing bullet):
  per-IP defends a per-IP limit (CF `1015`) and scales with warm-IP count; aggregate defends a
  per-account/key limit (e.g. an Algolia app key) where rotating IPs doesn't help. Both off by default.
- `--exit socks5://…` (repeatable) manual exits · `--mullvad` the live Mullvad
  catalog · `--max-exit-latency` (skip exits slower than N ms; off by default) ·
  `--probe-concurrency` (exits health-probed at once; default 64) ·
  `--loaded` (wait full load via a headed page) ·
  `--data-dir` (persist) · `--serve` (keep the dashboard alive).

**Interrupt (Ctrl-C / SIGTERM):** an *abort*, not a graceful drain — `Client::abort` aborts the
worker tasks (dropping their `Browser`s, so `kill_on_drop` `SIGKILL`s Chrome) and abandons
in-flight work, then the process exits immediately. This is deliberate: a signal skips Rust drops,
so a plain exit would orphan Chrome (chromiumoxide sets no `PR_SET_PDEATHSIG`), but the graceful
`shutdown()` would "keep working" by draining the whole batch first. Aborting kills the browsers
without the drain. Clearances persist on solve (`record_clearance`), so only in-memory stat deltas
are lost on interrupt; `shutdown()` (with `persist_all`) stays the normal end-of-run path. A second
signal hard-exits in case `abort` wedges.

## Module decomposition

Three crates, dependency arrow **`api → solver → core`** (compiler-enforced, no
cycles):

- **`core` (`mara-core`)** — the vocabulary: `Reason` (why a fetch failed:
  Challenged/RateLimited/Blocked/Timeout/Unreachable/Unavailable) and
  `Clearance` (the lifted cookies + UA + host + expiry).
- **`solver` (`mara-solver`)** — the browser. `Browser::solve(cfg, url) ->
  Result<Cleared, Reason>`, where `Cleared { page, clearance, clicks }`. The
  cookie-lift is a private detail of `solve`. **Firewall (must hold):** `solver`
  has zero references to `Lease`/`ExitPool`/the worker/`store`/the client — the
  browser crate cannot see an exit or a lease. Its only outward seam is
  `observe::Observer`.
- **`api` (`mara`)** — orchestration: `Client`, the one-worker-per-exit serving pool + the
  background maintainer, egress/pool, slim, store, ladder, introspection, doctor, mullvad. Two fetch shapes:
  **browser-free** (slim — the warm path on a solve host, escalating to a headed solve on a
  challenge; or the **raw** path on any other host, never touching a browser) and
  **headed** (`fetch_browser` — always launches a browser, hands the caller a live
  `Page`; can't go browser-free). The browser-free path's core is
  `Client::fetch_all(resources) -> FetchAll<String>`, an **unordered, completion-order `Stream`**
  of one `FetchResult` per input; `fetch_all_bytes -> FetchAll<Vec<u8>>` is the raw-bytes twin and
  `fetch_http`/`fetch_bytes` are one-shot shims. Input is `impl Into<Resource>` (a URL string or a
  full `Resource`); the canonical body the worker produces is `Vec<u8>`, decoded to `T` once at the
  stream edge (`FetchAll<T>` carries the decoder), so the slim path is no longer hardwired to
  `String`. `Client::enable_solving_for_domain()` pre-seeds the solve-set (a bare exit lease is
  deliberately **not** exposed — raw `fetch_all` already routes non-CF traffic through the pool with
  rotation, which is the same unlock with no second API to drive).
  `FetchAll`'s contract is **O(exit count) memory, not O(count)**: a background
  feeder pulls the input lazily and submits into the bounded work queue (bounded to the
  exit count), so at most ~one resource + one body per exit is ever live — flat whether N
  is a thousand or a hundred million. Termination is driven by
  the results channel closing (every job's sender clone dropped), never by the
  total; `with_total`/`size_hint` feed the progress display only. Dropping a
  `FetchAll` aborts the feeder and closes the results channel, so in-flight workers
  discard their results instead of blocking.

## The protagonists: one worker per exit + the maintainer (`api::worker`)

**Serving is one-worker-per-exit; warming is a separate background task.** There is exactly
one **serving worker per exit** (bound to its `code` for life) and a small pool of **B
maintainer** tasks. They share `Shared` + the pool and coordinate only through the exit's
`activity` facet (`Idle` is the up-for-grabs state — the CAS `Idle→Serving`/`Idle→Solving`
is the single coordination point; the old `warmset` demand-coordination and round-robin
`lease_warm` are gone).

- **Serving worker** (bound to exit *X*): waits until *X* can serve — `pool.is_claimable(X)`
  (ready + idle + not cooling + under the latency cap) **and**, for a solve workload, *X* is
  warm for **all** solve domains (a raw workload needs no warmth). *Only then* does it pull a
  `Job` (retries first, then fresh work), `pool.claim(X)`, and serve slim on *X*. It **never
  touches a browser**. This gate is the **tail-latency guarantee**: a cold/warming worker
  never claims a resource, so warm idle workers finish the stragglers — a slow-to-warm or
  failing exit can never hostage a request that another exit could serve *now*.
- **The maintainer** (B tasks): walks the catalog **fastest-first** (`lease_to_warm_any`
  leases the lowest-latency exit cold for some solve domain), solves once under a B permit to
  bank a `(exit, domain)` clearance, and releases the exit warm — persistently, keeping the
  whole catalog warm for the solve domains as exits free/cool/recover. It warms
  `https://{domain}/` (host-wide clearance keyed by the registered **solve-domain**, not the
  request host — so a suffix registration covers its subdomains; serving resolves host→domain
  via `solve_domain_for`). A warm solve gets the short `policy.warm_timeout` (≪ `--timeout`):
  warming is speculative bulk work, so a stuck exit frees its scarce B slot fast (a headed
  `fetch_browser` still gets the full `--timeout`). Empty solve-set → the maintainer idles.

This is the model that makes **pacing local**: a worker owns its exit, so per-IP spacing is a
local sleep, no shared coordinator. Breadth is the catalog size — you scale with more warm IPs
(CF `1015` is per-IP), never more depth on one IP.

- **Pacing — two orthogonal fixed pacers, both `req/min`, no burst.** A `Domain` may set either or
  both:
  - **`per_ip`** (per-exit): the worker stamps `paced_until = now + 60s/per_ip` on **its own exit**
    (`pool.mark_served`, in-memory); `may_pull` refuses to pull while that's in the future, and it
    **sleeps precisely to the deadline** (`pace_wait`, no poll). No cross-worker coordination (the
    worker owns its exit) — `paced_until` lives in `ExitData` only so the dashboard can show the
    `paced` badge. Aggregate throughput a domain sees = warm-exits × per_ip → **breadth is the scale
    dial**. Defends CF's per-IP `1015`.
  - **`aggregate`** (pool-wide): a lock-free GCRA pacer (one monotonic-`Instant` atomic per domain,
    `Shared::aggregate_wait`) whose CAS-advance of a shared "next-allowed" clock holds the **whole
    pool** to `60s/aggregate` regardless of exit count. Taken **before leasing an exit** (a worker
    awaiting its slot holds nothing) and **per attempt** — retries are requeues that re-enter
    `handle`, so each send re-claims a slot (no un-paced burst); the CAS-bump is at claim time so
    concurrent workers can't share a slot; a wasted slot undershoots (safe). Cancellable on shutdown
    (a `Shared` `shutdown` `Notify`) so a far-future slot can't hang the drain. Defends a
    per-account/key limit (e.g. an Algolia app key), where rotating IPs is useless — so breadth is
    irrelevant to it.
  Neither pacer cools the exit or tips the pool into `Resting` (a paced exit is healthy, just
  spacing). Both apply to solve *and* raw hosts. Still deferred: the knee-driven decrease-only
  ratchet, and the aggregate-429 backoff (bump the shared clock rather than bench an IP).

- **Two queues, a rotation budget.** Callers submit a `Job` to an MPMC **fresh-work queue
  bounded to the exit count** (backpressure that makes `FetchAll` O(exits), not O(N)); a
  serve **failure re-queues** the job — carrying its decremented `attempts` (the rotation
  budget) — onto an **unbounded retry queue** (drained first) so another exit tries it. A
  re-queue never blocks (a full fresh queue can't deadlock it), and live jobs stay ≤ exits
  since a job only re-queues after being pulled. `Job::Html` carries its input `index` + a
  clone of its batch's bounded results channel (never a per-call oneshot). `Job::Headed`
  carries a type-erased executor; the worker solves headed on its own exit, keeps the page
  alive across the caller's extraction, then tears it down.
- **A resource never fails on a *winnable* obstacle — only on the origin's answer or an unwinnable
  config error.** The contract: any real origin response (2xx, **404**, most 4xx — `classify` passes
  them through) is delivered as success. A bot-protection/transport obstacle (rate-limit, block,
  timeout, unreachable, a stale-clearance challenge on a registered host) is mara's job to overcome,
  so the resource **retries forever** across exits — it does *not* fail. Give-up is reserved for the
  three genuinely-unwinnable-by-retry cases: (1) a **raw challenge** on every exit → an unregistered
  CF host (`GaveUp(Challenged)` — register it), (2) a **broken fingerprint triple** — a solve-host
  challenge while slim has *never* once served (`FingerprintMismatch`), and (3) a **persistently dead
  pool** — the reap: when `Resting` (every non-wonky exit cooling) *and* a resource has been unable to
  make progress for `lease_timeout`, fail it (`Resting`); a *transient* resting wave (cooldowns that
  will lift) is winnable, so the resource waits, not fails. Pacing never triggers the reap (a paced
  exit isn't cooling → the pool reads `Available`).
- **Wakes are per-exit.** Each exit has one interested worker, so a disposition change (the
  maintainer banks a clearance, a cooldown lapses, a probe re-confirms it) fires just *that*
  exit's `Notify` — never a broadcast across a 500-exit catalog. A worker parks on its exit's
  signal with **register-then-recheck** (enable the `Notified`, re-check, then await), plus a
  ~1s fallback that bounds the reap check. Shutdown sets a `closing` flag and wakes every
  worker (a worker parked on a cooling exit would otherwise wait out its cooldown).
- **B** is a shared semaphore the maintainer (and headed fetch) acquire around a solve. Slim
  HTTP clients are pooled per proxy and shared (one reusable client per exit — bounds FDs /
  fixed the EMFILE blowup).
- **Failure routing** (`worker::apply_failure` + `worker::penalize`): a winnable exit-quality
  failure (5xx/timeout/rate-limit/block) cools or kills *this* exit (`penalize` applies the side
  effect; `ExitStatus` is `Ok | Cooled | Dead`) and **re-queues without touching `Job.attempts`** —
  retry forever on another exit. Because the failing exit is cooled/cold, its own worker fails
  `may_pull` and won't re-pull the job, so the retry always lands on a *different* exit (one bad IP
  can't fail a resource). Only a **raw challenge** runs down `Job.attempts` (the unregistered-CF
  give-up). A **solve-host challenge** is handled separately (`ExitData::record_challenge`): drop the
  stale clearance and **bench the exit with an *escalating* cooldown** (`base × streak`, reset by the
  next successful serve). This is the key anti-waste rule: an exit the browser can warm but whose
  slim replay is always challenged — a **CF-flagged IP** — climbs the streak to a long bench and so
  drops out of the fastest-first warming rotation, instead of being re-warmed on loop (without it,
  the fastest flagged exits soak up ~all the B browser budget warming exits that then serve *zero*).
  A healthy exit whose cookie occasionally expires challenges rarely and serves in between → its
  streak resets → only ever a brief cool. The *resource* is winnable, so it re-queues onto a good
  exit — free (no budget) once slim has proven the fingerprint by serving at least once; if slim has
  *never* served, it runs the budget down to a `FingerprintMismatch` give-up (`replay_giveup`). The
  old `ladder`/`decide_headed` still routes the headed (`fetch_browser`) path's solve failures.

The loop **has hermetic coverage**: the warming **solve** and the **slim** request are seams
(`SolveFn`/`SlimFn` on `Shared`, `None` in production). Tests inject fakes (a warm exit
serves, a cold one challenges) and drive the real serving workers + maintainer with no
browser or HTTP — proving the catalog warms and everything serves, multi-domain independence,
a raw host never solves, one challenged exit can't block the scrape, the tail-latency guarantee
(a cold/failing exit never blocks a warm one), per-IP pacing spacing, and the give-up boundary:
an unregistered CF host, a broken fingerprint (`FingerprintMismatch`), and a persistently dead
pool (after `lease_timeout`) are the *only* failures — winnable obstacles retry. `penalize`, the
ladder, and the pool's claim/warm-leasing are also unit-tested; only the real CF/Mullvad
behaviour is left to the live tests.

**Tracing (`tracing` crate).** A flat log dump is greppable by exit and by resource.
A **serving** request runs as `fetch{req,url}` (one resource, `req` a monotonic id)
wrapping `exit{code}` (the serve episode on the worker's exit). A **warm** solve runs under
its own `warm{code,host}` span (not tied to a `req` — the maintainer's solve that warms an
exit isn't any one request's solve). The link from "serving stalled on host H" to "maintainer
banked H on exit X" is therefore the **host/domain**: `grep req=` follows a request's serving,
`grep code=`/`host=` follows an exit's warms. The background probe monitor wraps each probe in
the *same* `exit{code}` span (sans `req`), so an exit's health transitions land under
`grep code=…` alongside its fetches. Events shed
fields the spans already carry. Levels are deliberate so `-v` is a real filter:
INFO = the happy path (slim served / solved) **and durable health transitions**
(probe confirmed ready, a re-probe latency shift past `LATENCY_SHIFT_FACTOR` — which
is how a slow-while-in-use exit is caught, and the **recoverable** slim-timeout bench →
probing — symmetric with "confirmed ready", a one-off blip the next probe heals), WARN =
the **durable** bad health transition to **wonky** (an exit that fails repeated probes / is
marked dead), the stale-clearance re-warm, and a warm solve that fails for **reputation**
(Blocked / RateLimited), is **Unreachable** (a burning or dead IP), or ends still
**Challenged** (the solver couldn't clear CF), ERROR = give-up / fingerprint mismatch,
DEBUG = lease churn (leased / returned), per-attempt exit **rotations**, and a transient
warm-solve failure (**Timeout** or **Unavailable** — a slow exit or a 5xx/3xx blip mid-solve). The rule: an exit's **durable death** (→ wonky) and reputation
burns are visible at the default level; the **expected, self-healing** transients —
a recoverable rotation (retries left — one exit returned 5xx/429/odd-transport, try another),
a warm-solve timeout (the short warm budget timing out a slow exit), and a slim-timeout
bench → probing (the next probe re-confirms it) — sit *below* WARN even though they cool or
re-probe the exit, because at bulk scale they're routine, not signal. The give-up that follows
*exhausted* retries is ERROR. So a healthy bulk run is quiet at the default level even as it
sheds and re-confirms flaky exits; `-v debug`/`-v info` surface the transients. `-v <level>` sets mara's own crates (deps stay at warn,
chromiumoxide muted); `RUST_LOG` overrides.

When a solve gives up — timeout, stuck challenge (no checkbox), **or a hard
block/rate-limit** classified from the page — `solver::Browser::diagnose`
saves the screenshot, Xvfb framebuffer, live DOM, widget probe, and a `summary.txt`
under `<artifacts>/fail-<browser-id>`, calls the `Observer::failed` ghost seam, and logs
one WARN line *inside* the active `exit{code}` span. (Pure **transport** failures — a
`goto`/`new_page` connect error — are the exception: blank frame, often transient, so they
stay WARN logs rather than flooding the bounded ghost ring.) So a FAILED browser is
traceable end-to-end: the dashboard card
shows its exit code (the worker stamps it via `Introspector::set_exit` — the solver
still never sees an exit), and `grep code=… run.log` lands on the give-up line with
the `fail-<id>` path. A loaded framebuffer with an empty logged `title` is the
stale-CDP-context signature (the page rendered but `document.title`/`content()`
read empty, so the loop never classified it cleared).

## Egress, the pool, and the unified `Exit` (`api::egress`, `api::pool`)

There is **one** egress: `ExitPool`. *Direct* (no-proxy) egress is just a pool of
one always-ready synthetic exit (`socks: None` → no proxy URL → store key `""`,
lease `url: None`), so there is no separate direct code path to keep in sync —
`can_rotate()` falls out of `exit_count() > 1`. `api::egress` is now only the thin
leasing vocabulary over the pool: `Lease`, `ExitStatus`, `Availability`. The pool
is **source-agnostic**: fed exits + an injected liveness `Probe`
(`Fn(proxy) -> ProbeOutcome`, built by `connect_probe_for`). The probe is a plain
**TCP-connect** to the SOCKS endpoint for *both* manual and Mullvad exits — reaching a
`*.relays.mullvad.net` SOCKS port already proves it's a Mullvad relay, and a connect is
cheap enough to run at high concurrency (`--probe-concurrency`, default 64) so a 500-exit
catalog warms in seconds. The monitor runs probes with `buffer_unordered` — the
concurrency is **continuously refilled** (a finishing probe starts the next immediately),
never batch-and-wait. Its drain rate is therefore `concurrency / per-probe-time`, so the
**connect timeout is tied to the latency cap** (`probe_timeout`): there's no point waiting
longer than the slowest latency we'd ever lease. With `--max-exit-latency` set the timeout
is ~2× the cap, and a timeout *is* the verdict "slower than the cap" → recorded as an
over-cap `latency` (so the exit reads **`slow`**, not stuck **`probing`**) rather than a
`Transient` retry. Without it, the timeout is a flat `PROBE_TIMEOUT_UNCAPPED` and a miss
just retries. This is what stops a handful of slow/far relays from holding the probe slots
for the old fixed 10 s and starving the rest. A connection *error* (refused/reset/DNS) is
always `Transient` (the relay is down). A probe yields just a connect-RTT `latency` (or
`Transient`); no per-probe `am.i.mullvad` round-trip, no IP lifted. `mullvad::bootstrap`
builds the pool from the live catalog after a **one-time** `am.i.mullvad` check that the
local egress is on a Mullvad tunnel (the SOCKS hostnames only resolve inside Mullvad's
network).

Each `Exit` holds **all** its per-exit state in one place, as three **orthogonal**
facets mutated by the single leasing worker:

- **health** — `Probing | Ready | Wonky` (durable disposition). A slim **timeout**
  demotes a `Ready` exit back to `Probing` — it stopped answering, so it's
  unconfirmed and can't be leased again until a probe re-confirms it `Ready` (a
  cheap probe beats committing a worker to another full slim timeout). A timeout
  is the one non-probe event that writes health; `return_lease` preserves it. The
  probe's effect on an exit goes through **one** method, `Exit::observe_probe` — the
  sole writer of `latency`, so a latency refresh can never be split from the health
  transition it implies. While an exit is leased its health is the worker's, not the
  probe's, so a re-probe then refreshes *only* latency (a slow exit can go slow
  mid-lease); on an `Idle` exit a reachable probe (re-)confirms `Ready`. **Re-probe
  cadence is health-aware** (`due`): an unconfirmed `Probing` exit retries fast
  (`PROBE_RETRY`, ~5 s) so a relay whose first probe blipped under load isn't stuck
  `probing` for a minute, while a settled `Ready`/`Wonky` exit only re-confirms every
  `REPROBE_AFTER` (60 s). A `Probing` exit that fails `PROBE_FAILS_TO_WONKY` consecutive
  probes is demoted to **`Wonky`** — it's unreachable, not "being checked", so it leaves
  the probing bucket (and re-confirms slowly); a later reachable probe recovers it.
- **activity** — `Idle | Serving | Solving`. `Idle` = free to lease; the other two mean
  "held by a worker" (so not leasable) — `Serving` a slim request, `Solving` a headed browser.
- **warmth + cooldown + pacing** — all in `store::ExitData`: per-`(exit, host)` clearance
  presence, the **single** cooldown field (with a `Cooling` reason), and the per-`(exit, domain)`
  **`paced_until`** (in-memory, the fixed pacer's next-serve deadline). Warmth is orthogonal to
  cooling — an exit can be warm *and* rate-limited; pacing is orthogonal to both — a paced exit is
  healthy and leasable, just spacing out (it never reads `Resting`).

Leasable = `Ready` + `Idle` + not cooling. There is exactly one cooldown per exit
(no separate pool-side vs store-side bookkeeping to hand-sync). The pool is the single
source of truth for routing. A serving worker claims *its own* exit by code —
`claim(code)` (CAS `Idle→Serving` if leasable) — and gates on `exit_warm_for(code, domain)`;
the maintainer picks work with `lease_to_warm_any(domains)` (the lowest-latency leasable exit
cold for *some* domain, returned with that domain). `ExitData::is_warm_for(host)` (non-stale
clearance, ignoring cooling) is the warm *membership* test, distinct from `warm_clearance`
which also excludes cooling (usable *now*).

## Persistence & the fingerprint triple (`api::store`)

`store` persists per-exit `clearances` + `stats`, keyed by proxy URL, under
`--data-dir` (ephemeral otherwise). Clearances persist **only on solve** and are
seeded on load; everything else is in-memory.

**Fingerprint-triple invariant:** a handed-down clearance is only accepted by CF
if the *installed Chrome major* ↔ the *pinned slim TLS profile* (`slim::PROFILE`)
↔ the *replayed UA* all agree. On drift the stored clearances are discarded (so a
stale cookie never wedges every client onto the headed solver), and a startup
canary warns. Bumping the Chrome binary and `wreq`/`wreq-util` must happen in
lockstep. The first-ever-slim-failure-after-solve is surfaced as
`FetchError::FingerprintMismatch`, not a generic give-up.

## Introspection (`api::introspect`)

A single-page live dashboard (`dashboard.html`) over WebSocket: a header **census**
(a per-state head-count of the exit catalog — solving/serving/…/idle —
that sums to the total), a **job-progress** strip below it (a `fetch_all` batch's
`fetched X / N (P%) · in-flight · rate · ETA` + bar; counts + rate only, no bar, when
the total is unknown — owned and published by the `FetchAll` driver, since the pool
only sees individual jobs and nothing else can count completions), then two
collapsible carousels (live browser thumbnails,
expanded; **failed-run ghosts**, collapsed) over the full-width exits table (no
tabs — the tracing log is the event record). Each live thumbnail carries its exit
code (stamped by the worker via `Introspector::set_exit`, so a card cross-refs with
`grep code=… run.log`). Frames are *pulled* server-side by grabbing each browser's
Xvfb framebuffer (via the display name the solver reports at `register` — the
`Observer` seam pushes phase/click events, never frames). The socket streams a
frame for **every** live browser (slow, for thumbnails) plus the **enlarged** one
faster; each binary frame is prefixed with its 4-byte browser id so the dashboard
routes it to the right thumbnail. Clicking a thumbnail opens a full-resolution
overlay (good for screenshots).

**Exit state streams as per-exit deltas, not catalog snapshots.** This is load-bearing at scale
(a 500+ exit Mullvad catalog): a single-exit change must cost one small row, not a re-serialize +
broadcast of the whole catalog on the serving thread. The unit is one **`ExitRow`** (the badge
facets ∪ the cumulative stats — collapsing the old `ExitSnapshot`/`ExitStatsInfo` split), keyed by
`code`. Every pool mutation funnels through **one** decision point, `pool::note_row`: it diffs the
exit's *disposition* (badge-relevant facets, latency bucketed to 25 ms, **excluding** monotonic
counters) against the last streamed one and emits a delta only on a real change — so a stat-only
tick sends nothing and the raw counters **ride along** in whatever delta fires next (serving flips
activity twice per request, so stats stay near-live). The pool never pushes the whole catalog: the
13 scattered `publish()` calls are gone. Time-based transitions no mutation triggers (a cooldown
expiring) are caught by the monitor's per-cycle `sweep`. `introspect` keeps the current rows in a
`code`-keyed map (patched by every delta) so a fresh connection's `init` carries the world and then
follows the stream. **Delivery coalesces** (`state_stream`): the broadcast carries only the changed
`code`, and the per-connection pump blocks for any event, drains everything currently pending, then
sends each dirty exit's *latest* row (from the map) **once** under a single lock — so a burst of
thousands of changes collapses to ≤ one catalog of current rows, and the socket can never trail by a
growing backlog of stale rows (the "UI is a second behind" failure of a naïve one-recv-one-send
loop). Browser snapshots coalesce by id and progress to its latest the same way; a **lagged** exit
signal re-seeds the full map (`exits_full`). The dashboard patches the **one** changed `<tr>`
surgically (a `code → <tr>` map) and re-sorts only when a badge changed — never rebuilding 500 rows
per frame.

Each connection runs as **three** independent tasks sharing the socket's write half
behind a mutex held only per-`send`: a *state* stream (the broadcast subscriptions →
small JSON: exit deltas, ghosts, job-progress), a *frame* stream (the framebuffer grabs),
and a control loop (client `select`/`dismiss`, fanned to the frame stream via a
`watch`). The split is load-bearing: frame grabs are slow and timeout-bounded, so they
must never share a task with state delivery — a stalling/dying Xvfb would otherwise park
the whole select loop and freeze the badges for seconds. The grab happens *outside* the
sink lock, so state JSON only ever waits behind a frame's actual byte-send (sub-ms on
localhost), never behind a grab.

**Ghosts** make a failed solve inspectable after the browser is gone: on a give-up
the solver hands its last frame + the diagnose summary over the `Observer::failed`
seam, and the introspector retains a bounded ring (`MAX_GHOSTS`) of frozen records
— the frame as an inline data URL (not the live binary-frame channel, since the
browser has torn down) plus the summary text. A ghost card is clickable (overlay
shows the frozen frame + summary) and dismissable (the client sends `{dismiss}`).
This is the *only* retained-state in introspect; everything else is live.

The dashboard projects an exit's three facets to one badge by a single priority
(`BADGE_ORDER`, also the census + row-sort order so they can't drift): activity first, then
**every reason it can't be leased**, then the two leasable states —
`solving > serving > rate-limited/blocked/cooldown > wonky > slow > probing >
paced > warm > cold(ready)`. The ordering is **honest about leasability**: `warm` / `ready` mean
"servable *right now*", so a cleared-but-still-`probing` exit reads `probing` (not a
misleading `warm`) — `probing` outranks `warm` precisely because it isn't leasable yet.
**`paced`** sits just above `warm`: it's warm + healthy but *spacing out* under a per-IP rate
ceiling, so it isn't servable this instant (distinct from a `cooldown` penalty). Because it's a
badge state, the census counts it — and since worker↔exit is 1:1, that count *is* "how many
workers are sleeping on pacing" (no separate gauge). The
`solving` activity reads **"warming"** and `serving` reads **"serving"** — never a vague "in
use"; **`slow`** = benched by `--max-exit-latency`. The badge is the *live* state; the
header's **`cleared`** count is the orthogonal **warm-store size** (every exit holding a
usable clearance, servable now or not), so a cold start shows the store is intact even while
most of it is still `probing`. It can't be a census pill — an in-use exit is also cleared, so
it would double-count and break the census-sums-to-total invariant.

**Warmth-display boundary.** Warmth is per-`(exit, domain)` — that's what `exit_warm_for`
and `lease_to_warm_any` key on, so routing is always correct. The per-exit `warm` badge (`has_warm()` =
warm-for-**≥1** host) is therefore a *single-solve-domain projection*: honest when one CF host
is in play (the common case — and raw hosts hold no clearance, so they never read warm at all),
but it overclaims with two ("warm for A, cold for B" reads the same as "warm for both"). This is
display-only, not a bug. The documented upgrade, once a real multi-CF-host consumer exists, is a
`warm k/n` badge over the solve-set (`n=1` renders as today's `warm`); the per-host truth is
always the clearances table. Don't build `k/n` speculatively.
