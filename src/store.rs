use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::clearance::{Clearance, now_unix};

const SCHEMA: u32 = 1;

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Fingerprint {
    pub tls_profile: String,
    pub chrome_major: Option<u32>,
}

/// Per-exit counters for the **current run**, surfaced via [`Client::snapshot`](crate::Client::snapshot).
/// Deliberately **not** persisted: each run starts from zero so the dashboard reflects *this* run,
/// not a lifetime total accumulated across every past scrape. Only clearances survive across runs.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Stats {
    /// Total requests served over this exit.
    pub requests: u64,
    /// Requests that reached the origin successfully.
    #[serde(default)]
    pub successes: u64,
    /// Headed challenge solves performed on this exit.
    pub solves: u64,
    /// Rate-limits (CF 1015 / HTTP 429) hit on this exit.
    pub rate_limits: u64,
    /// Reputation blocks (CF 1020) hit on this exit.
    pub blocks: u64,
    /// Request timeouts on this exit.
    #[serde(default)]
    pub timeouts: u64,
    /// Unix time of the last rate-limit, if any.
    pub last_rate_limit_unix: Option<f64>,
    /// Estimated sustainable request rate before rate-limiting kicks in, if known.
    pub knee_estimate: Option<u64>,
    /// Unix time of the last successful request, if any.
    pub last_ok_unix: Option<f64>,
    /// Rolling average request latency in milliseconds, if measured.
    #[serde(default)]
    pub avg_latency_ms: Option<u64>,
    /// Latency of the most recent request in milliseconds, if measured.
    #[serde(default)]
    pub last_latency_ms: Option<u64>,
}

/// Why an exit is currently cooling. Orthogonal to warmth — a warm exit can still be
/// cooling. Serialized to the dashboard so the badge can say *why* (kebab-case strings).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Cooling {
    /// Cooling after a rate-limit (CF 1015 / HTTP 429).
    RateLimited,
    /// Cooling after a reputation block (CF 1020).
    Blocked,
    /// Cooling after a transient failure (5xx / timeout / odd-transport).
    Transient,
}

#[derive(Default, Clone)]
pub(crate) struct ExitData {
    clearances: HashMap<String, Clearance>,
    stats: Stats,
    cooldown_until: Option<Instant>,
    cooling_reason: Option<Cooling>,
    requests_since_clear: u64,
    consecutive_timeouts: u32,
    /// Consecutive slim challenges with no successful serve **of that same host** in between (reset
    /// by `record_success`), keyed **per host**. Drives the **escalating challenge cooldown**: an
    /// exit the browser can warm but whose slim replay keeps getting challenged (a CF-flagged IP) is
    /// benched for longer and longer, so the fastest-first maintainer stops pouring browser solves
    /// into it. A healthy exit whose cookie occasionally expires serves in between → the streak
    /// resets → it's only ever briefly cooled. Keyed per host (not exit-global) so a healthy second
    /// domain sharing this exit can't reset a different, genuinely-broken domain's streak — an exit
    /// serving both must let each host's escalation run independently.
    consecutive_challenges: HashMap<String, u32>,
    /// Per-domain **pace deadline**: the earliest instant this exit may serve that domain again
    /// (last-served + the domain's rate `interval`). In-memory only — pacing is a live throttle, not
    /// durable state. Distinct from `cooldown` (a penalty that makes the exit unleasable and can
    /// tip the pool into `Resting`); a paced exit is healthy and leasable, just spacing its requests.
    paced_until: HashMap<String, Instant>,
}

impl ExitData {
    pub fn record_request(&mut self) {
        self.stats.requests += 1;
        self.requests_since_clear += 1;
    }

    pub fn record_success(&mut self, host: &str, latency: Duration) {
        self.stats.successes += 1;
        self.stats.last_ok_unix = Some(now_unix());
        self.consecutive_timeouts = 0;
        self.consecutive_challenges.remove(host);
        let ms = latency.as_millis() as u64;
        self.stats.last_latency_ms = Some(ms);
        self.stats.avg_latency_ms =
            Some(self.stats.avg_latency_ms.map_or(ms, |a| (a * 7 + ms) / 8));
    }

    pub fn record_clearance(&mut self, host: &str, clearance: Clearance) {
        self.clearances.insert(host.to_string(), clearance);
        self.stats.solves += 1;
        self.stats.last_ok_unix = Some(now_unix());
        self.requests_since_clear = 0;
    }

    pub fn record_rate_limit(&mut self, cooldown: Duration) {
        self.stats.rate_limits += 1;
        self.stats.last_rate_limit_unix = Some(now_unix());
        self.stats.knee_estimate = Some(self.requests_since_clear);
        self.cool(cooldown, Cooling::RateLimited);
    }

    pub fn record_block(&mut self, cooldown: Duration) {
        self.stats.blocks += 1;
        self.clearances.clear();
        self.cool(cooldown, Cooling::Blocked);
    }

    pub fn record_timeout(&mut self, base: Duration, max: Duration) {
        self.stats.timeouts += 1;
        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);
        self.cool(
            timeout_cooldown(self.consecutive_timeouts, base, max),
            Cooling::Transient,
        );
    }

    /// A slim request to `host` was challenged despite this exit holding a (freshly-warmed)
    /// clearance — drop that clearance and bench the exit with an **escalating** cooldown
    /// (`base × streak`, capped at `max`). The escalation is what tells a one-off stale cookie
    /// (streak resets on the next successful serve) from a CF-flagged IP that never serves (streak
    /// climbs → long bench → the maintainer stops re-warming it).
    pub fn record_challenge(&mut self, host: &str, base: Duration, max: Duration) {
        self.clearances.remove(host);
        let entry = self
            .consecutive_challenges
            .entry(host.to_string())
            .or_insert(0);
        *entry = entry.saturating_add(1);
        let streak = *entry;
        self.cool(timeout_cooldown(streak, base, max), Cooling::Transient);
    }

    pub(crate) fn cool(&mut self, cooldown: Duration, reason: Cooling) {
        self.cooldown_until = Some(Instant::now() + cooldown);
        self.cooling_reason = Some(reason);
    }

    pub fn drop_clearance(&mut self, host: &str) {
        self.clearances.remove(host);
    }

    /// Record that this exit just served `domain` and may not serve it again until `until`
    /// (last-served + the rate interval). The serving worker writes this per request.
    pub fn record_served(&mut self, domain: &str, until: Instant) {
        self.paced_until.insert(domain.to_string(), until);
    }

    /// The exit's current pace deadline — the latest still-future `paced_until` across its domains,
    /// or `None` if it isn't pacing right now. Drives both the worker's precise sleep and the
    /// `paced` badge.
    pub fn paced_until(&self) -> Option<Instant> {
        let now = Instant::now();
        self.paced_until
            .values()
            .filter(|&&t| t > now)
            .max()
            .copied()
    }

    /// Whether the exit is spacing out requests right now (for the badge; orthogonal to cooling).
    pub fn is_paced(&self) -> bool {
        self.paced_until().is_some()
    }

    pub fn clear_clearances(&mut self) {
        self.clearances.clear();
    }

    pub fn warm_clearance(&self, host: &str) -> Option<Clearance> {
        if self.is_cooling() {
            return None;
        }
        self.clearances.get(host).filter(|c| !c.is_stale()).cloned()
    }

    /// Warmth is *orthogonal* to cooling: an exit holds a usable clearance for some host
    /// even while it sits in a rate-limit cooldown. The badge priority decides precedence.
    pub fn has_warm(&self) -> bool {
        self.clearances.values().any(|c| !c.is_stale())
    }

    /// A clone of the cumulative stats — for the unified exit row sent to the dashboard.
    pub fn stats(&self) -> Stats {
        self.stats.clone()
    }

    /// Whether this exit holds a non-stale clearance for `host` — *ignoring* cooling, unlike
    /// [`warm_clearance`](Self::warm_clearance). This is the warm-set *membership* test (does
    /// the exit count toward host's warm target W?); a transiently-cooling-but-cleared exit is
    /// still a warm-set member, so it isn't re-solved just because it's briefly resting.
    pub fn is_warm_for(&self, host: &str) -> bool {
        self.clearances.get(host).is_some_and(|c| !c.is_stale())
    }

    pub fn is_cooling(&self) -> bool {
        self.cooldown_until.is_some_and(|t| Instant::now() < t)
    }

    pub fn cooling(&self) -> Option<Cooling> {
        self.is_cooling().then_some(self.cooling_reason).flatten()
    }

    pub fn cooling_until(&self) -> Option<Instant> {
        self.cooldown_until.filter(|&t| Instant::now() < t)
    }

    pub fn inspection_rows(&self, key: &str) -> (Vec<ClearanceInfo>, ExitStatsInfo) {
        let now = now_unix();
        let clearances = self
            .clearances
            .iter()
            .map(|(host, c)| ClearanceInfo {
                exit_key: key.to_string(),
                host: host.clone(),
                user_agent: c.user_agent.clone(),
                age_secs: c.age_secs(),
                expires_unix: c.expires_unix,
                expires_in_secs: c.expires_unix.map(|x| (x - now) as i64),
                stale: c.is_stale(),
            })
            .collect();
        let stats = ExitStatsInfo {
            exit_key: key.to_string(),
            stats: self.stats.clone(),
            cooling: self.is_cooling(),
            cooling_reason: self.cooling(),
            warm: self.has_warm(),
        };
        (clearances, stats)
    }
}

/// On-disk per-exit record. Only **clearances** persist across runs — stats are per-run
/// (see [`Stats`]) and never written. A legacy file's `stats` key deserializes into nothing
/// (serde ignores unknown fields) and is dropped on the next save.
#[derive(Serialize, Deserialize)]
struct PersistedExit {
    key: String,
    #[serde(default)]
    clearances: HashMap<String, Clearance>,
}

#[derive(Serialize, Deserialize)]
struct Meta {
    schema: u32,
    fingerprint: Fingerprint,
}

/// One banked clearance: which exit holds it, for which host, and its age/expiry.
#[derive(Clone, Serialize)]
pub struct ClearanceInfo {
    /// The exit (SOCKS URL) holding the clearance; `""` for direct egress.
    pub exit_key: String,
    /// The host the clearance is for.
    pub host: String,
    /// The user-agent replayed alongside the cookie.
    pub user_agent: String,
    /// How long ago the clearance was banked, in seconds.
    pub age_secs: u64,
    /// Cookie expiry as a Unix timestamp, if known.
    pub expires_unix: Option<f64>,
    /// Seconds until expiry (negative if already expired), if known.
    pub expires_in_secs: Option<i64>,
    /// Whether this clearance is too old to use (mirrors the crate's internal `Clearance::is_stale`).
    /// The per-host truth: a consumer that needs "is *this* host actually warm on *this* exit" (not
    /// the coarser per-exit [`ExitStatsInfo::warm`]) should filter on this rather than re-derive
    /// staleness from `expires_in_secs`/`age_secs` (the fallback-TTL leg when expiry is unknown isn't
    /// otherwise reconstructable outside this crate).
    pub stale: bool,
}

/// Per-exit stats plus its current warmth/cooldown disposition.
#[derive(Clone, Serialize)]
pub struct ExitStatsInfo {
    /// The exit (SOCKS URL); `""` for direct egress.
    pub exit_key: String,
    /// The cumulative counters for this exit.
    #[serde(flatten)]
    pub stats: Stats,
    /// Whether the exit is currently cooling (unleasable).
    pub cooling: bool,
    /// Why it's cooling, if it is.
    pub cooling_reason: Option<Cooling>,
    /// Whether the exit holds a usable clearance for at least one host.
    pub warm: bool,
}

/// A point-in-time view of the store: every banked clearance and every exit's stats. Returned by
/// [`Client::snapshot`](crate::Client::snapshot).
#[derive(Clone, Serialize)]
pub struct StoreSnapshot {
    /// Every banked clearance across all exits.
    pub clearances: Vec<ClearanceInfo>,
    /// Per-exit stats and disposition.
    pub stats: Vec<ExitStatsInfo>,
}

pub(crate) struct Persistence {
    dir: Option<PathBuf>,
    profile_base: PathBuf,
    fingerprint: Mutex<Fingerprint>,
    profile_drift: bool,
}

impl Persistence {
    pub fn open(dir: Option<PathBuf>, tls_profile: &str) -> Persistence {
        let profile_base = dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("mara-ephemeral"));
        let mut fingerprint = Fingerprint {
            tls_profile: tls_profile.to_string(),
            chrome_major: None,
        };
        let mut profile_drift = false;

        if let Some(d) = &dir {
            let prior: Option<Meta> = read_json(&d.join("meta.json"));
            profile_drift = prior
                .as_ref()
                .map(|m| m.schema != SCHEMA || m.fingerprint.tls_profile != tls_profile)
                .unwrap_or(false);
            if let Some(m) = &prior
                && !profile_drift
            {
                fingerprint.chrome_major = m.fingerprint.chrome_major;
            }
            if profile_drift {
                tracing::warn!(
                    "fingerprint pin changed since last run — discarding stored clearances"
                );
            }
            let _ = std::fs::create_dir_all(d.join("exits"));
            write_json(
                &d.join("meta.json"),
                &Meta {
                    schema: SCHEMA,
                    fingerprint: fingerprint.clone(),
                },
            );
        }

        Persistence {
            dir,
            profile_base,
            fingerprint: Mutex::new(fingerprint),
            profile_drift,
        }
    }

    pub fn is_persistent(&self) -> bool {
        self.dir.is_some()
    }

    pub fn location(&self) -> (PathBuf, bool) {
        (self.profile_base.clone(), self.dir.is_some())
    }

    pub fn profile_dir(&self, exit_key: &str) -> PathBuf {
        self.profile_base
            .join("exits")
            .join(sanitize(exit_key))
            .join("profile")
    }

    pub fn artifact_dir(&self) -> PathBuf {
        self.profile_base.join("artifacts")
    }

    pub fn load_exit(&self, exit_key: &str) -> ExitData {
        let Some(dir) = &self.dir else {
            return ExitData::default();
        };
        let path = dir
            .join("exits")
            .join(sanitize(exit_key))
            .join("state.json");
        let Some(mut pe) = read_json::<PersistedExit>(&path) else {
            return ExitData::default();
        };
        if self.profile_drift {
            pe.clearances.clear();
        }
        ExitData {
            clearances: pe.clearances,
            ..Default::default()
        }
    }

    pub fn save_exit(&self, exit_key: &str, data: &ExitData) {
        let Some(dir) = &self.dir else { return };
        let pe = PersistedExit {
            key: exit_key.to_string(),
            clearances: data.clearances.clone(),
        };
        let exit_dir = dir.join("exits").join(sanitize(exit_key));
        let _ = std::fs::create_dir_all(&exit_dir);
        write_json(&exit_dir.join("state.json"), &pe);
    }

    pub fn note_chrome_major(&self, user_agent: &str) -> bool {
        let major = user_agent
            .split("Chrome/")
            .nth(1)
            .and_then(|s| s.split('.').next()?.parse::<u32>().ok());
        let Some(major) = major else { return false };
        let mut fp = self.fingerprint.lock().unwrap();
        let drifted = match fp.chrome_major {
            Some(prev) if prev == major => return false,
            Some(prev) => {
                tracing::warn!(
                    "Chrome major changed {prev}→{major}; discarding stored clearances (fingerprint drift)"
                );
                true
            }
            None => false,
        };
        fp.chrome_major = Some(major);
        let fp = fp.clone();
        if let Some(d) = &self.dir {
            write_json(
                &d.join("meta.json"),
                &Meta {
                    schema: SCHEMA,
                    fingerprint: fp,
                },
            );
        }
        drifted
    }
}

fn sanitize(key: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    let clean: String = key
        .chars()
        .take(48)
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let clean = if clean.is_empty() {
        "direct".to_string()
    } else {
        clean
    };
    format!("{clean}-{:016x}", h.finish())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &std::path::Path) -> Option<T> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_json<T: Serialize>(path: &std::path::Path, value: &T) {
    let Ok(bytes) = serde_json::to_vec_pretty(value) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, &bytes).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// Linear-capped escalation shared by every "streak of bad events → longer bench" cooldown in the
/// crate (a per-host slim-challenge streak here; a per-domain confirmed-misconfiguration streak in
/// `worker`): `base × streak`, capped at `max`.
pub(crate) fn timeout_cooldown(consecutive: u32, base: Duration, max: Duration) -> Duration {
    (base * consecutive).min(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    const COOL: Duration = Duration::from_secs(600);

    #[test]
    fn sanitize_is_safe_and_unique() {
        let a = sanitize("socks5h://nl-ams-001.relays.mullvad.net:1080");
        let b = sanitize("socks5h://nl-ams-002.relays.mullvad.net:1080");
        assert!(!a.contains('/') && !a.contains(':'));
        assert_ne!(a, b);
        assert_eq!(sanitize(""), sanitize(""));
    }

    fn clr() -> Clearance {
        Clearance::new(
            vec![("cf_clearance".into(), "tok".into())],
            "UA/147".into(),
            None,
            String::new(),
        )
    }

    #[test]
    fn timeout_cooldown_escalates_linearly_and_caps() {
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        assert_eq!(timeout_cooldown(1, base, max), Duration::from_secs(30));
        assert_eq!(timeout_cooldown(3, base, max), Duration::from_secs(90));
        assert_eq!(
            timeout_cooldown(100, base, max),
            max,
            "capped, never unbounded"
        );
    }

    #[test]
    fn challenge_cooldown_drops_the_clearance_escalates_and_resets_on_serve() {
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        let remaining = |d: &ExitData| {
            d.cooling_until()
                .unwrap()
                .saturating_duration_since(Instant::now())
        };

        let mut d = ExitData::default();
        d.record_clearance("h", clr());
        d.record_challenge("h", base, max);
        assert!(
            d.warm_clearance("h").is_none(),
            "a challenge drops the clearance and cools"
        );
        let first = remaining(&d);
        d.record_challenge("h", base, max);
        assert!(
            remaining(&d) > first,
            "consecutive challenges escalate the cooldown (bench a flagged IP)"
        );

        // A successful serve resets the streak, so the next challenge starts from the base again —
        // a healthy exit whose cookie occasionally expires is never benched for long.
        d.record_success("h", Duration::from_millis(5));
        d.record_clearance("h", clr());
        d.record_challenge("h", base, max);
        assert!(
            remaining(&d) <= first + Duration::from_secs(1),
            "a successful serve resets the challenge streak"
        );
    }

    #[test]
    fn challenge_streak_is_per_host_not_exit_wide() {
        // An exit serving two domains: "good" keeps succeeding, "bad" keeps getting challenged.
        // "good"'s successes must not reset "bad"'s streak — otherwise a healthy second domain
        // sharing the exit would mask a genuinely broken one and it would never escalate past the
        // base cooldown.
        let base = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        let mut d = ExitData::default();
        d.record_clearance("bad", clr());
        d.record_challenge("bad", base, max);
        let first = d.cooling_until().unwrap();

        d.record_success("good", Duration::from_millis(5));
        d.record_clearance("bad", clr());
        d.record_challenge("bad", base, max);
        let second = d.cooling_until().unwrap();

        assert!(
            second > first,
            "an unrelated host's success must not reset this host's challenge streak"
        );
    }

    #[test]
    fn rate_limit_records_knee_and_cools_out_of_the_warm_set() {
        let mut d = ExitData::default();
        for _ in 0..7 {
            d.record_request();
        }
        d.record_clearance("h", clr());
        for _ in 0..7 {
            d.record_request();
        }
        assert!(d.warm_clearance("h").is_some());
        d.record_rate_limit(COOL);
        assert_eq!(d.stats.knee_estimate, Some(7));
        assert_eq!(d.stats.rate_limits, 1);
        assert!(d.is_cooling());
        assert!(
            d.warm_clearance("h").is_none(),
            "a cooling exit is out of the warm set"
        );
    }

    #[test]
    fn timeouts_count_and_cool_then_a_clear_resets_the_streak() {
        let mut d = ExitData::default();
        d.record_timeout(COOL, COOL);
        d.record_timeout(COOL, COOL);
        assert_eq!(d.consecutive_timeouts, 2, "streak builds");
        assert_eq!(d.stats.timeouts, 2, "lifetime count");
        assert!(
            d.is_cooling(),
            "a timed-out exit is cooled out of the warm set"
        );

        d.record_success("h", Duration::from_millis(500));
        assert_eq!(d.consecutive_timeouts, 0, "a clear resets the streak");
        assert_eq!(
            d.stats.timeouts, 2,
            "lifetime count is not reset by a clear"
        );
    }

    #[test]
    fn block_clears_clearances_and_cools() {
        let mut d = ExitData::default();
        d.record_clearance("h", clr());
        assert!(d.warm_clearance("h").is_some());
        d.record_block(COOL);
        assert!(
            d.warm_clearance("h").is_none(),
            "block drops clearances + cools the exit"
        );
        assert_eq!(d.stats.blocks, 1);
    }

    #[test]
    fn warmth_is_orthogonal_to_cooling_and_carries_a_reason() {
        let mut d = ExitData::default();
        d.record_clearance("h", clr());
        assert!(d.has_warm());
        d.record_rate_limit(COOL);
        assert!(
            d.has_warm(),
            "still warm while cooling — the facets are orthogonal"
        );
        assert!(
            d.warm_clearance("h").is_none(),
            "but a cooling exit is not selectable for use"
        );
        assert_eq!(
            d.cooling(),
            Some(Cooling::RateLimited),
            "the badge can say *why* it's cooling"
        );
    }

    #[test]
    fn challenge_drop_forgets_the_clearance_only() {
        let mut d = ExitData::default();
        d.record_clearance("h", clr());
        d.drop_clearance("h");
        assert!(d.warm_clearance("h").is_none());
        assert!(!d.is_cooling(), "a drop is not a cooldown");
    }

    #[test]
    fn warm_clearance_excludes_stale() {
        let mut d = ExitData::default();
        d.record_clearance(
            "h",
            Clearance::new(vec![], "UA".into(), Some(now_unix() - 10.0), String::new()),
        );
        assert!(
            d.warm_clearance("h").is_none(),
            "a stale clearance is not warm"
        );
    }

    #[test]
    fn inspection_rows_reports_staleness_per_clearance() {
        let mut d = ExitData::default();
        d.record_clearance("fresh.example", clr());
        d.record_clearance(
            "stale.example",
            Clearance::new(vec![], "UA".into(), Some(now_unix() - 10.0), String::new()),
        );
        let (clearances, _) = d.inspection_rows("exit1");
        let stale_of = |host: &str| {
            clearances
                .iter()
                .find(|c| c.host == host)
                .unwrap_or_else(|| panic!("no row for {host}"))
                .stale
        };
        assert!(!stale_of("fresh.example"));
        assert!(stale_of("stale.example"));
    }

    #[test]
    fn ephemeral_persistence_is_noop_but_usable() {
        let p = Persistence::open(None, "Chrome147");
        assert!(!p.is_persistent());
        let mut d = p.load_exit("e1");
        d.record_rate_limit(COOL);
        p.save_exit("e1", &d);
        assert_eq!(
            p.load_exit("e1").stats.rate_limits,
            0,
            "ephemeral never persists"
        );
    }

    #[test]
    fn clearances_survive_reload_but_stats_do_not() {
        let dir = tempfile::tempdir().unwrap();
        {
            let p = Persistence::open(Some(dir.path().into()), "Chrome147");
            let mut d = p.load_exit("nl-001");
            d.record_clearance("example.com", clr());
            d.record_request();
            d.record_rate_limit(COOL);
            p.save_exit("nl-001", &d);
        }
        let p = Persistence::open(Some(dir.path().into()), "Chrome147");
        let d = p.load_exit("nl-001");
        assert!(
            d.warm_clearance("example.com").is_some(),
            "clearance should persist across reload"
        );
        assert_eq!(d.stats.solves, 0, "stats are per-run, never persisted");
        assert_eq!(d.stats.rate_limits, 0, "stats are per-run, never persisted");
    }

    #[test]
    fn tls_profile_drift_discards_clearances() {
        let dir = tempfile::tempdir().unwrap();
        {
            let p = Persistence::open(Some(dir.path().into()), "Chrome147");
            let mut d = p.load_exit("nl-001");
            d.record_clearance("example.com", clr());
            p.save_exit("nl-001", &d);
        }
        let p = Persistence::open(Some(dir.path().into()), "Chrome148");
        let d = p.load_exit("nl-001");
        assert!(
            d.warm_clearance("example.com").is_none(),
            "drift must discard clearances"
        );
    }

    #[test]
    fn chrome_major_drift_is_signalled_once() {
        let p = Persistence::open(None, "Chrome147");
        assert!(
            !p.note_chrome_major("... Chrome/147.0 ..."),
            "first sighting is not drift"
        );
        assert!(
            !p.note_chrome_major("... Chrome/147.0 ..."),
            "same major is not drift"
        );
        assert!(
            p.note_chrome_major("... Chrome/148.0 ..."),
            "a bumped major drifts"
        );
        assert!(
            !p.note_chrome_major("no version here"),
            "an unparseable UA is ignored"
        );
    }

    #[test]
    fn corrupt_state_is_treated_as_cold_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let exit_dir = dir.path().join("exits").join(sanitize("nl-001"));
        std::fs::create_dir_all(&exit_dir).unwrap();
        std::fs::write(exit_dir.join("state.json"), b"{ this is not json").unwrap();
        let p = Persistence::open(Some(dir.path().into()), "Chrome147");
        assert!(
            p.load_exit("nl-001")
                .warm_clearance("example.com")
                .is_none()
        );
    }
}
