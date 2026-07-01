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
#[command(name = "mara", about = "Cloudflare-clearing scraper")]
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
    Capture(CaptureArgs),
    Doctor,
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

/// Interrupt handler for Ctrl-C / SIGTERM. Chrome is spawned by chromiumoxide with no
/// `PR_SET_PDEATHSIG`, and Rust destructors don't run on a signal, so a plain signal-killed `mara`
/// orphans every in-flight browser (→ runaway Chrome processes). So we can't just exit — but we
/// also don't want the *graceful* `shutdown()`, which drains the whole in-flight batch first
/// (a Ctrl-C that "keeps working"). Instead `abort()` kills the browsers (aborting the workers
/// drops their `Browser`s → `kill_on_drop` `SIGKILL`s Chrome) and abandons everything else, then we
/// exit — a normal-feeling, near-instant interrupt with no orphans. A second signal hard-exits in
/// case `abort` itself wedges. SIGKILL is uncatchable and remains the one case that can leak.
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

async fn capture(args: CaptureArgs) -> Result<()> {
    let url = if args.url.contains("://") {
        args.url
    } else {
        format!("https://{}", args.url)
    };
    let client = Client::new(Config {
        browsers: 1,
        exits: args.exits,
        mullvad: args.mullvad,
        capture_dir: Some(args.out.clone()),
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
    let urls: Vec<String> = args
        .urls
        .iter()
        .map(|u| {
            if u.contains("://") {
                u.clone()
            } else {
                format!("https://{u}")
            }
        })
        .collect();
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
    let domains: Vec<mara::Domain> = if args.raw && per_ip.is_none() && aggregate.is_none() {
        Vec::new() // pure raw, no pacing → nothing to configure
    } else {
        let mut hosts: Vec<String> = urls.iter().filter_map(|u| mara::host_of(u)).collect();
        hosts.sort();
        hosts.dedup();
        hosts
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
            .collect()
    };
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
            Err(e) => eprintln!("  ✗ {i}  {url}  {e:#}"),
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
