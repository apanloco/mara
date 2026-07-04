mod metrics;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::stream::{Stream, StreamExt};
use mara::{Client, Config, FetchResult};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "mara",
    about = "A scraper that clears challenges over a rotating pool of egress IPs"
)]
struct Cli {
    /// Log verbosity for mara's own crates (dependencies stay quiet). `RUST_LOG`, if set,
    /// overrides this entirely.
    #[arg(short = 'v', long, global = true, value_enum, default_value_t = Verbosity::Info)]
    verbose: Verbosity,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Verbosity {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Verbosity {
    fn as_level(self) -> &'static str {
        match self {
            Verbosity::Error => "error",
            Verbosity::Warn => "warn",
            Verbosity::Info => "info",
            Verbosity::Debug => "debug",
            Verbosity::Trace => "trace",
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    Fetch(FetchArgs),
    Warm(WarmArgs),
    Capture(CaptureArgs),
    Doctor,
}

/// Pre-warm the exit pool for one or more Cloudflare hosts: solve once per exit to bank a
/// `cf_clearance`, so a later `fetch` (sharing the same `--data-dir`/`MARA_DATA_DIR`) replays
/// browser-free from request #1. Best-effort — an exit that can't clear (unreachable, or a
/// CF-flagged IP whose challenge never settles) is left cold; warming reports how many made it.
#[derive(clap::Args)]
struct WarmArgs {
    /// Hosts/URLs to warm. Each host is registered as a solve domain (warmth is per-host, so a
    /// bare host works — no path needed). Matched exactly — warm `www.example.com` and
    /// `example.com` separately if you fetch both.
    urls: Vec<String>,
    /// Concurrent live browsers = concurrent solves (a machine-load guard). Clamped to catalog size.
    #[arg(short = 'b', long, default_value_t = 4)]
    browser_concurrency: usize,
    #[arg(long = "exit")]
    exits: Vec<String>,
    #[arg(long)]
    mullvad: bool,
    /// Skip exits whose probed latency exceeds this (ms). Off by default.
    #[arg(long, value_name = "MS")]
    max_exit_latency: Option<u64>,
    /// How many exits to health-probe concurrently (default 64).
    #[arg(long, value_name = "N")]
    probe_concurrency: Option<usize>,
    /// Per-solve timeout in seconds.
    #[arg(long, default_value_t = 60)]
    timeout: u64,
    /// Persist banked clearances here. Falls back to `MARA_DATA_DIR`; without either, warming is
    /// in-memory only (pointless once the process exits — set one).
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,
    /// Stop once the warm count hasn't grown for this many seconds — the maintainer has warmed
    /// every reachable exit and is only retrying benched ones.
    #[arg(long, value_name = "SECS", default_value_t = 20)]
    settle: u64,
    /// Hard cap: stop after this many seconds regardless (safety net; 0 = no cap).
    #[arg(long, value_name = "SECS", default_value_t = 600)]
    max_wait: u64,
    /// After warming, keep the engine + dashboard alive until interrupted (inspect the pool).
    #[arg(long)]
    serve: bool,
}

#[derive(clap::Args)]
struct CaptureArgs {
    url: String,
    #[arg(long, value_name = "DIR", default_value = "captures")]
    out: PathBuf,
    #[arg(long = "exit")]
    exits: Vec<String>,
    #[arg(long)]
    mullvad: bool,
}

#[derive(clap::Args)]
struct FetchArgs {
    urls: Vec<String>,
    #[arg(short = 'b', long, default_value_t = 4)]
    browser_concurrency: usize,
    #[arg(long, default_value_t = 1)]
    repeat: usize,
    #[arg(long = "exit")]
    exits: Vec<String>,
    #[arg(long)]
    mullvad: bool,
    /// Skip exits whose probed latency exceeds this (ms). Off by default.
    #[arg(long, value_name = "MS")]
    max_exit_latency: Option<u64>,
    /// How many exits to health-probe concurrently (default 64). Probes are cheap TCP connects.
    #[arg(long, value_name = "N")]
    probe_concurrency: Option<usize>,
    #[arg(long)]
    loaded: bool,
    #[arg(long, default_value_t = 60)]
    timeout: u64,
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, value_name = "SECS", default_value_t = 0)]
    wait: u64,
    #[arg(long, default_value_t = 0)]
    hold_secs: u64,
    #[arg(long)]
    serve: bool,
    #[arg(long)]
    real_display: bool,
    #[arg(long)]
    cdp_click: bool,
    #[arg(long)]
    no_click: bool,
    #[arg(long)]
    no_move: bool,
    /// Fetch raw — don't treat the target hosts as Cloudflare-protected. By default the CLI
    /// registers each URL's host as a solve domain (warm/solve/replay), since `fetch` is for HTML
    /// pages; pass `--raw` for non-CF targets (an API endpoint, an image) so they ride the exit
    /// pool browser-free with no solve.
    #[arg(long)]
    raw: bool,
    /// Per-IP request-rate ceiling in **requests per minute, per exit** — the defense against a
    /// per-IP limit (Cloudflare's 1015). Aggregate throughput scales with the warm-IP count. Off by default.
    #[arg(long, value_name = "REQ_PER_MIN")]
    rate: Option<u32>,
    /// Pool-wide request-rate ceiling in **requests per minute across all exits** — the defense
    /// against a per-account/key limit (e.g. an Algolia app key), where rotating IPs doesn't help.
    /// Off by default.
    #[arg(long, value_name = "REQ_PER_MIN")]
    aggregate_rate: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    match cli.cmd {
        Cmd::Fetch(a) => fetch(a).await,
        Cmd::Warm(a) => warm(a).await,
        Cmd::Capture(a) => capture(a).await,
        Cmd::Doctor => doctor().await,
    }
}

fn init_tracing(verbose: Verbosity) {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;
    let lvl = verbose.as_level();
    // Our crate at the chosen level; deps (wreq, hyper, …) stay at warn and chromiumoxide is
    // muted, so `-v debug` doesn't drown the log. `RUST_LOG`, if set, takes over completely.
    let default = format!("warn,mara={lvl},chromiumoxide=off");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        // Colorize only an interactive terminal; piping to a file stays plain text so the
        // span fields (`req=8`, `code=…`) are greppable without stripping escapes.
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .init();
}

/// Interrupt handler for Ctrl-C / SIGTERM. Orphaned Chrome is *already* prevented at the kernel
/// level — every browser runs under `PR_SET_PDEATHSIG(SIGKILL)` (see `ChromeExec`), so even a
/// SIGKILL of `mara` reaps them. This handler is the *tidy* path on top: rather than the graceful
/// `shutdown()` (which drains the whole in-flight batch — a Ctrl-C that "keeps working"), `abort()`
/// kills the browsers now (aborting the workers drops their `Browser`s → `kill_on_drop`) and
/// abandons everything else, for a near-instant interrupt. A second signal hard-exits in case
/// `abort` itself wedges — and PDEATHSIG still cleans up the browsers on that exit.
fn shutdown_on_signal(client: Client) {
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).ok();
        let on_term = async {
            match term.as_mut() {
                Some(s) => {
                    s.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = on_term => {}
        }
        eprintln!("\n⏹  interrupted — killing browsers…");
        tokio::spawn(async {
            let _ = tokio::signal::ctrl_c().await;
            std::process::exit(130);
        });
        let _ = tokio::time::timeout(Duration::from_secs(3), client.abort()).await;
        std::process::exit(130);
    });
}

async fn doctor() -> Result<()> {
    let report = tokio::task::spawn_blocking(mara::doctor::run).await?;
    for c in &report.checks {
        let sym = match c.status {
            mara::doctor::Status::Ok => "✓",
            mara::doctor::Status::Warn => "⚠",
            mara::doctor::Status::Fail => "✗",
        };
        println!("{sym} {}: {}", c.name, c.detail);
        if let Some(hint) = &c.hint {
            println!("    ↳ {hint}");
        }
    }
    if report.ok() {
        println!("\nAll required dependencies present.");
        Ok(())
    } else {
        eprintln!("\nMissing required dependencies — see above.");
        std::process::exit(1);
    }
}

/// Surface any domain confirmed structurally misconfigured (solve keeps redirecting to a host that
/// never validates) — the same spirit as the "N could not clear" summary, but actionable without
/// needing `-v debug` to have caught the underlying warning.
fn warn_misconfigured(client: &Client) {
    for (host, landed) in client.misconfigured_domains() {
        eprintln!(
            "⚠ {host} looks misconfigured — solve keeps redirecting to {landed}, whose clearance \
             never validates against {host}; register {landed} as its own host instead"
        );
    }
}

/// Bare hosts (no `://`) are assumed `https` — lets `mara fetch example.com` / `mara warm
/// example.com` work without a full URL, as the help text advertises.
fn normalize_url(u: &str) -> String {
    if u.contains("://") {
        u.to_string()
    } else {
        format!("https://{u}")
    }
}

async fn warm(args: WarmArgs) -> Result<()> {
    let mut hosts: Vec<String> = args
        .urls
        .iter()
        .filter_map(|u| mara::host_of(&normalize_url(u)))
        .collect();
    hosts.sort();
    hosts.dedup();
    if hosts.is_empty() {
        eprintln!("no hosts given — warm needs at least one Cloudflare host to warm exits for.");
        return Ok(());
    }

    let browsers = args.browser_concurrency.max(1);
    let mut policy = mara::Policy::default();
    if let Some(pc) = args.probe_concurrency {
        policy.probe_concurrency = pc.max(1);
    }
    let domains: Vec<mara::Domain> = hosts.iter().cloned().map(mara::Domain::solve).collect();
    let client = Client::new(Config {
        browsers,
        exits: args.exits.clone(),
        mullvad: args.mullvad,
        domains,
        policy,
        max_latency: args.max_exit_latency.map(Duration::from_millis),
        timeout: Duration::from_secs(args.timeout),
        data_dir: args.data_dir.clone(),
        ..Default::default()
    })
    .await
    .context("building client")?;
    shutdown_on_signal(client.clone());

    let total = client.worker_count();
    eprintln!(
        "warming {} host(s) [{}] over {total} exit(s) | browsers(b)={browsers} | egress={}",
        hosts.len(),
        hosts.join(", "),
        if args.mullvad {
            "mullvad pool".to_string()
        } else if args.exits.is_empty() {
            "direct".to_string()
        } else {
            format!("{} manual exit(s)", args.exits.len())
        },
    );

    // The maintainer warms the catalog fastest-first with no jobs submitted; we only watch. Warming
    // is "done" once the warm count *and* the total solve count both stop growing for `settle`s —
    // every reachable exit has been solved, and the rest are benched/unreachable (tried, can't
    // clear). A warmup floor keeps an early poll from calling it done before the first solves land;
    // `max_wait` is the hard safety net.
    let settle = Duration::from_secs(args.settle.max(1));
    let warmup_floor = Duration::from_secs(15);
    let max_wait = (args.max_wait > 0).then(|| Duration::from_secs(args.max_wait));

    // An exit counts as "warm" only once it holds a non-stale clearance for *every* requested host.
    // The per-exit `ExitStatsInfo::warm` flag is coarser ("warm for ≥1 host") — it overclaims the
    // moment more than one host is being warmed, and can even read warm off an unrelated host's
    // leftover clearance in a persistent `--data-dir`. The clearances table is the per-host truth.
    let count_warm = |s: &mara::store::StoreSnapshot| -> usize {
        let fresh: std::collections::HashSet<(&str, &str)> = s
            .clearances
            .iter()
            .filter(|c| !c.stale)
            .map(|c| (c.exit_key.as_str(), c.host.as_str()))
            .collect();
        s.stats
            .iter()
            .filter(|e| {
                hosts
                    .iter()
                    .all(|h| fresh.contains(&(e.exit_key.as_str(), h.as_str())))
            })
            .count()
    };
    let count_solves =
        |s: &mara::store::StoreSnapshot| -> u64 { s.stats.iter().map(|e| e.stats.solves).sum() };

    let start = tokio::time::Instant::now();
    let mut last_progress = start;
    let (mut best_warm, mut best_solves) = (0usize, 0u64);
    let mut last_shown = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let snap = client.snapshot();
        let (warm, solves) = (count_warm(&snap), count_solves(&snap));
        if warm > best_warm || solves > best_solves {
            best_warm = best_warm.max(warm);
            best_solves = best_solves.max(solves);
            last_progress = tokio::time::Instant::now();
        }
        if warm != last_shown {
            let cooling = snap.stats.iter().filter(|e| e.cooling).count();
            eprintln!("  {warm}/{total} warm  (cooling {cooling})");
            last_shown = warm;
        }
        let idle = last_progress.elapsed();
        if (start.elapsed() >= warmup_floor && idle >= settle)
            || max_wait.is_some_and(|cap| start.elapsed() >= cap)
        {
            break;
        }
    }

    let snap = client.snapshot();
    let warm = count_warm(&snap);
    let cold = total.saturating_sub(warm);
    eprintln!(
        "\n═══ warmed {warm}/{total} · {cold} could not clear · solves {} ═══",
        count_solves(&snap),
    );
    if hosts.len() > 1 {
        for h in &hosts {
            let n = snap
                .clearances
                .iter()
                .filter(|c| &c.host == h && !c.stale)
                .count();
            eprintln!("  {h}: {n} exit(s)");
        }
    }
    warn_misconfigured(&client);

    if args.serve {
        eprintln!(
            "\nengine + dashboard alive — open the `introspect dashboard` URL logged above; Ctrl-C to exit."
        );
        std::future::pending::<()>().await;
    }

    if tokio::time::timeout(Duration::from_secs(30), client.shutdown())
        .await
        .is_err()
    {
        eprintln!("⚠ shutdown timed out after 30s; exiting anyway");
    }
    Ok(())
}

async fn capture(args: CaptureArgs) -> Result<()> {
    let url = normalize_url(&args.url);
    // `fetch_browser` is a headed fetch, gated by the same exact-match `Domain` routing as the
    // browser-free path — register the target so the call doesn't fail `Unconfigured`. `raw`, not
    // `solve`: `Job::Headed` only needs a matching `Domain` to exist, and a `solve` entry would
    // also enter the maintainer's warm-list, spending a browser permit and writing frames into
    // `args.out` for a background solve capture never asked for.
    let domains = mara::host_of(&url)
        .into_iter()
        .map(mara::Domain::raw)
        .collect();
    let client = Client::new(Config {
        browsers: 1,
        exits: args.exits,
        mullvad: args.mullvad,
        capture_dir: Some(args.out.clone()),
        domains,
        ..Default::default()
    })
    .await?;
    shutdown_on_signal(client.clone());

    match client.fetch_browser(&url, |_page| async { Ok(()) }).await {
        Ok(out) => eprintln!(
            "✓ {url}  ({}, {:.1}s, {} click(s))",
            if out.solve_required {
                "solved"
            } else {
                "direct"
            },
            out.elapsed.as_secs_f64(),
            out.clicks
        ),
        Err(e) => eprintln!("✗ {url}  {e:#}"),
    }
    eprintln!("frames written to {}", args.out.display());
    let _ = tokio::time::timeout(Duration::from_secs(30), client.shutdown()).await;
    Ok(())
}

async fn fetch(args: FetchArgs) -> Result<()> {
    let urls: Vec<String> = args.urls.iter().map(|u| normalize_url(u)).collect();
    let work: Vec<String> = (0..args.repeat.max(1))
        .flat_map(|_| urls.iter().cloned())
        .collect();
    let single = work.len() == 1;
    let hold_secs = if args.real_display && args.hold_secs == 0 {
        300
    } else {
        args.hold_secs
    };

    if work.is_empty() && !args.serve {
        eprintln!("no URLs given (and --serve not set) — nothing to do.");
        return Ok(());
    }

    let browsers = args.browser_concurrency.max(1);
    let mut policy = mara::Policy::default();
    if let Some(pc) = args.probe_concurrency {
        policy.probe_concurrency = pc.max(1);
    }
    // `fetch` is for HTML pages, so by default treat each target host as Cloudflare-protected
    // (warm/solve/replay). `--raw` opts out (non-CF targets: an API call, an image) so they ride
    // the exit pool browser-free. Unknown CF hosts that aren't registered just give up Challenged.
    let (per_ip, aggregate) = (args.rate, args.aggregate_rate);
    let mut hosts: Vec<String> = urls.iter().filter_map(|u| mara::host_of(u)).collect();
    hosts.sort();
    hosts.dedup();
    let domains: Vec<mara::Domain> = hosts
        .into_iter()
        .map(|h| {
            let mut d = if args.raw {
                mara::Domain::raw(h)
            } else {
                mara::Domain::solve(h)
            };
            if let Some(n) = per_ip {
                d = d.per_ip(n);
            }
            if let Some(n) = aggregate {
                d = d.aggregate(n);
            }
            d
        })
        .collect();
    let client = Client::new(Config {
        browsers,
        exits: args.exits.clone(),
        mullvad: args.mullvad,
        domains,
        policy,
        max_latency: args.max_exit_latency.map(Duration::from_millis),
        real_display: args.real_display,
        cdp_click: args.cdp_click,
        no_click: args.no_click,
        move_mouse: !args.no_move,
        timeout: Duration::from_secs(args.timeout),
        connect_grace: Duration::from_secs(args.wait),
        data_dir: args.data_dir.clone(),
        ..Default::default()
    })
    .await
    .context("building client")?;
    shutdown_on_signal(client.clone());

    let serving = client.worker_count();
    eprintln!(
        "config: {} | serving={serving} browsers(b)={browsers} fetches={} | egress={}",
        if args.real_display {
            "headed on REAL display"
        } else {
            "headed on Xvfb"
        },
        work.len(),
        if args.mullvad {
            "mullvad pool".to_string()
        } else if args.exits.is_empty() {
            "direct".to_string()
        } else {
            format!("{} manual exit(s)", args.exits.len())
        },
    );

    let stop = Arc::new(AtomicBool::new(false));
    let peak_kb = Arc::new(AtomicUsize::new(0));
    let sampler = tokio::spawn(metrics::sample_ram(stop.clone(), peak_kb.clone()));

    // One unordered, completion-order stream of results, consumed lazily — the input is never
    // materialized beyond ~C in flight, so memory is flat whatever the URL count. The browser-free
    // bulk path is `fetch_all`; `--loaded` needs a live page per URL so it can't go browser-free —
    // it streams `fetch_browser` calls at the same C-wide concurrency via `buffer_unordered`.
    let total = work.len();
    let mut stream: Pin<Box<dyn Stream<Item = FetchResult<String>>>> = if args.loaded {
        let client = client.clone();
        Box::pin(
            futures::stream::iter(work.into_iter().enumerate())
                .map(move |(index, url)| {
                    let client = client.clone();
                    async move {
                        let result = client
                            .fetch_browser(&url, |page| async move {
                                Ok(mara::wait_full_load(&page, Duration::from_secs(15)).await)
                            })
                            .await;
                        FetchResult {
                            index,
                            url,
                            key: None,
                            result,
                        }
                    }
                })
                .buffer_unordered(serving),
        )
    } else {
        Box::pin(client.fetch_all(work))
    };

    let (mut succeeded, mut total_clicks) = (0u32, 0u32);
    let mut times: Vec<Duration> = Vec::new();
    let mut html_out: Option<String> = None;
    while let Some(FetchResult {
        index: i,
        url,
        result,
        ..
    }) = stream.next().await
    {
        match result {
            Ok(out) => {
                succeeded += 1;
                total_clicks += out.clicks;
                times.push(out.elapsed);
                eprintln!(
                    "  ✓ {} {} {:.1}s {} click(s) exit={} {}B  {url}",
                    i,
                    if out.solve_required {
                        "solved"
                    } else {
                        "direct"
                    },
                    out.elapsed.as_secs_f64(),
                    out.clicks,
                    out.exit.as_deref().unwrap_or("?"),
                    out.value.len(),
                );
                if single {
                    html_out = Some(out.value);
                }
            }
            Err(e) => {
                eprintln!("  ✗ {i}  {url}  {e:#}");
                if e.is_config_error() {
                    eprintln!(
                        "✗ config error — aborting the batch (every remaining request would fail the same way)"
                    );
                    break;
                }
            }
        }
    }

    if hold_secs > 0 {
        eprintln!("holding browsers alive for {hold_secs}s for inspection…");
        tokio::time::sleep(Duration::from_secs(hold_secs)).await;
    }

    let snapshot = client.snapshot();
    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    if total > 0 {
        times.sort();
        let median = match times.len() {
            0 => Duration::ZERO,
            n if n % 2 == 1 => times[n / 2],
            n => (times[n / 2 - 1] + times[n / 2]) / 2,
        };
        eprintln!(
            "\n═══ ok {succeeded}/{total} | clicks {total_clicks} | median {:.1}s | peak RAM {} MB ═══",
            median.as_secs_f64(),
            peak_kb.load(Ordering::Relaxed) / 1024,
        );
        // One aggregate line, not a per-exit dump (a Mullvad run has hundreds of exits — the
        // dashboard / `--data-dir` state hold the per-exit detail). Surface only the totals plus
        // how many exits saw trouble.
        let used = snapshot
            .stats
            .iter()
            .filter(|e| e.stats.requests > 0)
            .count();
        if used > 0 {
            let sum = |f: fn(&mara::store::Stats) -> u64| -> u64 {
                snapshot.stats.iter().map(|e| f(&e.stats)).sum()
            };
            let troubled = snapshot
                .stats
                .iter()
                .filter(|e| e.stats.rate_limits + e.stats.blocks + e.stats.timeouts > 0)
                .count();
            eprintln!(
                "exits: {used} used · solves {} · rate_limits {} · blocks {} · timeouts {} · {troubled} troubled",
                sum(|s| s.solves),
                sum(|s| s.rate_limits),
                sum(|s| s.blocks),
                sum(|s| s.timeouts),
            );
        }
        warn_misconfigured(&client);
    }

    if let Some(html) = html_out {
        println!("{html}");
    }

    if args.serve {
        eprintln!(
            "\nengine + dashboard alive — open the `introspect dashboard` URL logged above; Ctrl-C to exit."
        );
        // Stay alive until a signal; `shutdown_on_signal` then closes browsers and exits.
        std::future::pending::<()>().await;
    }

    if tokio::time::timeout(Duration::from_secs(30), client.shutdown())
        .await
        .is_err()
    {
        eprintln!("⚠ shutdown timed out after 30s; exiting anyway");
    }
    Ok(())
}
