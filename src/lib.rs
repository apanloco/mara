//! **mara** is a scraper that fetches over a rotating pool of egress IPs, clearing
//! bot-protection challenges along the way.
//!
//! Today that means Cloudflare: mara solves the interactive challenge in a real headed browser
//! **once** to bank a `cf_clearance` cookie, then serves every subsequent request to that host
//! **browser-free** — a plain HTTP client replaying the cookie and user-agent. The browser is the
//! fallback; the slim replay is the hot path. Hosts you don't register are fetched raw over the
//! same rotating pool, never touching a browser.
//!
//! # Quick start
//!
//! ```no_run
//! use futures::StreamExt;
//! use mara::{Client, Config};
//!
//! # async fn run() -> anyhow::Result<()> {
//! let client = Client::new(Config::default()).await?;
//!
//! // One result per input URL, in completion order. Bare URL strings work directly.
//! let mut results = client.fetch_all(["https://example.com/a", "https://example.com/b"]);
//! while let Some(item) = results.next().await {
//!     match item.result {
//!         Ok(page) => println!("{} → {} bytes", item.url, page.value.len()),
//!         Err(err) => eprintln!("{} failed: {err}", item.url),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`Client`] is the entry point; [`Client::fetch_all`] is the browser-free bulk path.
//! See [`Config`] and [`Domain`] for routing (which hosts take the solve path) and [`Policy`]
//! for timeouts, pacing, and probe tuning. [`Client::fetch_browser`] hands you a live headed
//! [`chromiumoxide::Page`] when you need one; pair it with [`wait_full_load`].
//!
//! The public surface is deliberately small — the entry points above plus the [`doctor`] and
//! [`store`] read-models. The orchestration internals (the exit pool, the per-exit workers, the
//! introspection dashboard, the browser solver) are private: they're the *how*, not the contract.
#![deny(missing_docs)]

// Vocabulary and internals are private modules; the public types they define are re-exported
// below. Same-crate code still reaches everything via `crate::<module>::…` paths.
mod classify;
mod clearance;
mod client;
mod egress;
mod introspect;
mod ladder;
mod mullvad;
mod policy;
mod pool;
mod slim;
mod worker;

// The browser solver (a one-way dependency — see the module source). Private except for the one
// consumer-facing helper re-exported below.
mod solver;

/// Startup diagnostics: environment checks (Xvfb, Chrome, the fingerprint triple, GPU) with a
/// pass/warn/fail verdict. Backs the `mara doctor` command.
pub mod doctor;

/// The persisted, read-only view: per-exit [`store::Stats`] and the clearance/cooldown state
/// surfaced by [`Client::snapshot`]. These are report types — the store itself is internal.
pub mod store;

pub use classify::Reason;
pub use client::{Client, Config, Domain, FetchAll, FetchError, FetchResult, Outcome, Resource};
pub use policy::Policy;
pub use slim::Method;
pub use solver::settle::wait_full_load;

/// The host component of a URL, or `None` if it doesn't parse or has no host. The same host
/// derivation the router uses to match a URL against the configured [`Domain`]s.
pub fn host_of(url: &str) -> Option<String> {
    url::Url::parse(url).ok()?.host_str().map(str::to_string)
}
