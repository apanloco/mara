# Decouple solve from serve (proposal)

Status: **plan, not implemented.** The throughput fix. An internal worker-pool restructure — no
public API change, but it **rewrites the worker model in CLAUDE.md** (the "one exclusive exit lease
for its working life" section), so it's a spec change, not just a tuning knob.

## Problem

Serving is **fused** to solving: a request that hits a challenge waits out the solve on its own exit,
blocked on the browser cap **B** (`serve_html` commits the request to one exit, worker.rs:217-243),
instead of being routed to a warm exit. A cold exit can't serve anyway — so it should warm in the
background while the request goes elsewhere. Today browsers gate *everything*, not just warm-up.

Observed (run2.log, 342 exits, B=4): a cold-start challenge wave froze ~145 workers in the browser
queue — req=190 sat **123 s** between "challenged" and the actual solve — while warm capacity sat
idle. Throughput collapsed to ~0.2 solves/s and the run went choppy with multi-second dead air.

The deeper cause is **worker↔exit binding for life.** Because a worker owns one exit, it can only
solve *its own* cold exit; it can't serve from a peer's freshly-warmed one. So a run pays roughly
`min(exits, requests)` solves — up to one per exit (342 here) — before the catalog is fully warm,
and every challenged request is hostage to a B-paced browser queue. The win below is **not** faster
cold-start solving (that stays B-paced); it's (a) capping total solves at the warm-set size instead
of the catalog size, and (b) letting requests flood across already-warm exits the instant they exist.

## Why not the smaller fix first

The cheap version is: *on a challenge, rotate to an already-warm exit for the host if one exists, and
solve inline only when none does.* It reuses the existing lease/rotate/ladder machinery with one
routing rule, and it would unfreeze the queue. We reject it as the endpoint — but it's the honest
baseline to beat:

- It does **not** cap solves. With worker-for-life binding, every worker still eventually solves its
  own cold exit, so you still pay ~`min(exits, requests)` solves. Capping at `Σ_host W_host` is the
  whole point, and it *requires* unbinding the worker from the exit — a worker that can't be reassigned
  to a peer's warm exit has no way to *avoid* warming its own.
- It re-tangles serving and solving in the same loop, which is exactly the fusion this proposal
  removes; the honest badges (§UI) and the separate warm span (§Tracing) fall out of the split, not
  the routing rule.

So the restructure earns its complexity specifically through the cap-at-W invariant. If that
invariant were dropped, the smaller fix would be the right call.

## The model — and the headline decision

**Serving is one request per exit at a time. Exclusivity holds for *both* warming and serving.** A
warm exit serves one slim request at a time; the **flood is aggregate across the warm set** (many
exits, each ~one request per slim-RTT), never N concurrent requests on one IP. We deliberately do
**not** relax serving to per-exit concurrency: CF 1015 is rate-based, and concurrent slim on one IP
reconcentrates exactly the pressure CLAUDE.md's breadth model exists to avoid. This is what makes
"flood through slim" and "one writer per IP" consistent — the flood is breadth, not depth.

**The `Lease` stays the enforcement mechanism — it just gets short.** Exclusivity is not re-invented
with new coordination; it's still a `Lease` (egress.rs:30) held while an exit is worked, returned the
instant the work ends. The change is *who holds it and for how long*: today one worker holds one lease
for its whole life; now a serving worker takes a lease per request (borrow → serve → return) and the
warmer takes a lease per solve. The existing activity facet already names which kind of work holds it
(`Serving` vs `Solving`, plus `Challenged`), so "one writer per IP at a time" remains a structural
property of leasing, not a new invariant to police by hand.

- **Warmer**: bounded task pool (size B), demand-driven; the only browser user *on the fetch path*.
  Drains a host-keyed "needs-warming" queue, **leases** an exit, solves, and banks a clearance per
  **(exit, host)**. Warm jobs are **deduped per (exit, host)** and capped to a **warm-set target W per
  active host** — so 145 requests on one host enqueue at most W warm jobs, never 145. The producer of
  warm jobs is the parking serving worker (below). B and W set the cold-start ramp.
  - **Which exit it warms:** the lowest-latency `Ready`, `Idle`, not-cooling exit — *regardless of
    which other hosts it is already warm for.* Clearances are (exit, host)-keyed and an exit holds
    many (store.rs), so the warmer never "runs out" of exits to warm while ≥1 is leasable: it can
    re-solve an exit already warm for other hosts. This is what makes W independent of catalog size
    (§Workloads) and removes any hosts-≫-exits starvation cliff.
- **Serving worker**: pull work → **lease** an idle warm exit for the host (lowest-latency, spread
  least-recently-served so the fastest isn't hammered) → one slim request → release the exit.
  Exclusivity fans the serving pool across the best idle exits — no stampede onto a single one.
- **Park, don't spin.** No warm exit for the host → the request **parks on a per-host availability
  signal**. It does **not** re-enter the work queue (that would busy-loop until the first solve lands,
  and would couple parking to the work-queue size). On parking, the worker also enqueues warm jobs for
  the host (deduped, up to W). Two events wake a parked waiter, both first-class on the per-host
  signal: (a) the **warmer banks** a clearance for the host (a new warm exit appears), and (b) a
  serving worker **releases** a warm exit for the host (an existing one frees). Parked requests are
  **FIFO per host** — whichever event fires, the freed/new exit goes to the oldest waiter first, so
  continuous fresh input can't starve them. Parking is bounded by the fetch timeout; on expiry the
  request fails like any other exhausted attempt.
- **Challenge while serving** = this exit's clearance is stale → release the exit and enqueue it for
  warming (drop its stale clearance, as `penalize` does today), re-park the request.

Invariants:

- **one writer per IP at a time — for both warming and serving**, enforced by the `Lease`. An exit is
  being warmed, or serving one request, never two things at once.
- **warming is deduped + capped**: cold-start solves ≈ `Σ_host W_host`, B-paced. Never
  `requests × exits`, never `exits × hosts`.

## Concurrency dials: B, C, W

The restructure repartitions what the three dials mean. **B** is unchanged: the machine-wide
live-browser guard, now shared between the warmer and `fetch_browser` (§fetch_browser) rather than
borrowed by per-exit workers.

**C stops meaning "exclusive leases for life" and becomes "the size of the serving-worker pool."** A
serving worker no longer owns an exit; it borrows one per request and parks when none is warm. Because
a parked worker is just an idle task (no exit held, no IP pressure), **over-provisioning C is cheap** —
the old "more workers = more pressure per IP" hazard is gone, since serving is still exclusive per
exit and the warm set, not C, bounds how many IPs are hit at once.

- **Default C = the exit count** (parking is cheap, so this can't over-pressure any IP), preserving
  "use all available capacity" without the for-life binding. Still clamped to **1** for a single-exit
  pool (`can_rotate()` false): with one IP there's nothing to fan across.
- **Recommended C ≈ `Σ_host W_host`** when the host set is known — enough serving workers to drain the
  warm set, no more. The single-host case is just `C ≈ W`.

**W** (warm-set target per active host) is the new throughput dial and the one genuinely-empirical
number; see §Remaining tuning.

## What it fixes

Serving throughput is set by the **warm-set size**, not B: once W exits are warm for a host, requests
flood across them. B and W only pace how fast the warm set grows/recovers — including the cold-start
ramp, which is unavoidably B-paced. So **"independent of B" means in steady state**, not during the
ramp. Steady-state per-exit behavior is otherwise unchanged from today; the entire win is in the
cold-start and stale-wave transients — which is precisely what dominated run2.log.

## fetch_browser

Stays a browser user — it hands a live `Page` to the caller's executor (worker.rs:167-181), not a
banked cookie, so it is never served from the warm pool. It draws from the **same B cap** as the
warmer: B is the machine-wide live-browser guard, now shared between warming and headed fetches.
Headed fetches are explicit and rare; heavy contention with warming is a B-sizing call, not a new
mechanism.

## Sequencing with the bulk API

This proposal and `bulk-api.md` **both rewrite `Job` and the worker loop**, so they must be sequenced,
not landed in parallel. Order: **bulk API first, decouple second.** The bulk API replaces the
per-call `oneshot` with one shared results channel and bounds the work queue (bulk-api.md §Invariants);
this proposal then splits the loop that drains that queue into serving workers + warmer.

The interaction to preserve: bulk-api's claim that **in-flight work is bounded** survives the split,
but for a slightly different reason. A parked serving worker is "in flight" yet holds no exit and pulls
no new job, so the bounded work queue still backpressures the input generator — the bound is the
serving-pool size + queue cap, and parked workers simply don't pull. Whichever lands second must
restate this; the two docs' line references into worker.rs will also need a refresh after the first.

## Tracing

The current contract (`fetch{req}` wraps `exit{code}`, fresh span per loop turn) breaks: with
borrow-per-request a `req` spans multiple borrow episodes, and the solve runs in the warmer under no
`req` span. New model: the `req` span covers a request's serving attempts (across borrows); **warming
gets its own `warm{code,host}` span**, not tied to a `req`. The link from "req parked on host H" to
"warmer banked H on exit X" is the **host**, not `req` — `grep req=` follows a request's serving,
`grep code=`/`host=` follows an exit's warms. CLAUDE.md's tracing section updates accordingly.

This is a real debuggability cost, not just a relabel: answering "why did this request take 123 s"
becomes a two-step inference — `grep req=` shows it parked on host H, then `grep host=H` shows the
warm-set's progress — rather than one grep landing on the solve that served it. It's inherent to
decoupling (the solve that unblocks a request is no longer *that request's* solve), and it's the price
of the cap-at-W win. The per-host FIFO and the `warm{code,host}` span keep the inference mechanical.

## Workloads

Mixed multi-domain is supported, no caller grouping — the pool derives `host_of` on each request and
routes/warms per host. Single-domain is the degenerate one-host fast path. Clearances are already
host-keyed per exit (store.rs), so one exit serves every host it has solved, and the warmer can warm
any leasable exit for any host (§the model). Inherent costs:

- cold-start warm-up multiplies per distinct host (`Σ_host W_host` solves);
- per-host throughput tracks how many exits (≤ W) are warm for that host.

There is **no** hosts-≫-exits starvation cliff: because any leasable exit can be warmed for the needed
host (even one already warm for others), W is independent of catalog size as long as ≥1 exit is
leasable. The genuine floor is the existing one — when *no* exit is leasable at all (all cooling), the
pool reports `Availability::Resting` and parked requests time out, exactly as today.

## UI

Badges get more honest: an exit reads `warming` (browser) or `serving` (slim) — never "in use" while
actually blocked on a browser permit. These project directly from the activity facet
(`Solving`→warming, `Serving`→serving), so the dashboard's existing badge-priority projection needs
only the relabel, not new state.

## Remaining tuning

These are empirical knobs to set against live runs, not unresolved design questions.

1. **Warm-set target W per host** — how many exits to keep warm per active host. Sets steady-state
   throughput *and* the cold-start ramp. The one number that must be found by measurement (start small,
   raise until per-host throughput plateaus or 1015 appears).
2. **Per-exit pacing** — optional minimum inter-request spacing on a warm exit to bound per-IP request
   *rate* (1015 is rate-based). Default none — under exclusive serving the slim RTT is the natural
   pacer, and the least-recently-served spread keeps any one exit from being hit back-to-back. Add only
   if 1015 reappears with a small warm set.
3. **Standing warm-buffer** — keep a small, capped buffer of warm exits ahead of demand for the
   *dominant* active host, to hide warm latency on a stale wave — never the eager `exits × hosts` warm.
   A later optimization; the demand-driven warmer is correct without it.
