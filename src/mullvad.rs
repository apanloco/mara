use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::introspect::Introspector;
use crate::pool::{ExitPool, ExitRecord, connect_probe_for};
use crate::store::Persistence;

const API_URL: &str = "https://api.mullvad.net/public/relays/wireguard/v2/";
const PROBE_URL: &str = "https://am.i.mullvad.net/json";
const PROBE_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Debug, Deserialize)]
struct MullvadProbe {
    ip: String,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    mullvad_exit_ip: bool,
}

async fn probe(proxy: Option<&str>) -> Result<MullvadProbe> {
    let mut builder = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .user_agent("mara/0.1 (egress check)");
    if let Some(p) = proxy {
        builder = builder.proxy(reqwest::Proxy::all(p)?);
    }
    Ok(builder
        .build()?
        .get(PROBE_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

/// Build an [`ExitPool`] from the live Mullvad catalog. Verifies the local egress is on a
/// Mullvad tunnel first (the SOCKS exit hostnames only resolve inside Mullvad's network),
/// then injects the plain TCP-connect probe as the pool's liveness check — reaching a relay's
/// SOCKS port already proves it's a Mullvad exit, so no per-probe `am.i.mullvad` round-trip
/// is needed.
pub async fn bootstrap(
    introspect: Arc<Introspector>,
    persistence: Arc<Persistence>,
    max_latency: Option<Duration>,
    probe_concurrency: usize,
) -> Result<Arc<ExitPool>> {
    let local = probe(None).await.context("probing local egress")?;
    if !local.mullvad_exit_ip {
        bail!(
            "egress {} ({}) is NOT a Mullvad exit — connect a Mullvad tunnel \
             (the SOCKS5 exit hostnames only resolve inside Mullvad's network)",
            local.ip,
            local.country.as_deref().unwrap_or("?"),
        );
    }

    let records = fetch_all().await.context("fetching Mullvad exit catalog")?;
    if records.is_empty() {
        bail!("Mullvad catalog returned no usable exits");
    }
    tracing::info!(count = records.len(), "mullvad exits fetched, verifying");

    let exits = records.into_iter().map(ExitPool::catalog_exit).collect();
    Ok(ExitPool::spawn(
        exits,
        connect_probe_for(max_latency),
        max_latency,
        probe_concurrency,
        persistence,
        introspect,
    ))
}

pub async fn fetch_all() -> Result<Vec<ExitRecord>> {
    let client = reqwest::Client::builder()
        .user_agent("mara/0.1 (fetch-exits)")
        .timeout(Duration::from_secs(30))
        .build()
        .context("building fetch-exits client")?;
    let body = client
        .get(API_URL)
        .send()
        .await
        .context("GET mullvad relay catalog")?
        .error_for_status()?
        .text()
        .await
        .context("reading mullvad relay catalog")?;
    parse_catalog(&body).context("parsing mullvad relay catalog")
}

fn parse_catalog(json: &str) -> Result<Vec<ExitRecord>> {
    #[derive(Deserialize)]
    struct ApiResponse {
        locations: std::collections::HashMap<String, ApiLocation>,
        wireguard: ApiWireguard,
    }
    #[derive(Deserialize)]
    struct ApiLocation {
        country: String,
    }
    #[derive(Deserialize)]
    struct ApiWireguard {
        relays: Vec<ApiRelay>,
    }
    #[derive(Deserialize)]
    struct ApiRelay {
        hostname: String,
        location: String,
        #[serde(default)]
        active: bool,
    }

    let resp: ApiResponse = serde_json::from_str(json)?;
    let mut out = Vec::new();
    for r in resp.wireguard.relays {
        if !r.active || !r.hostname.contains("-wg-") {
            continue;
        }
        let socks_host = r.hostname.replacen("-wg-", "-wg-socks5-", 1);
        let socks = format!("{socks_host}.relays.mullvad.net:1080");
        let code = r.hostname.replacen("-wg-", "-", 1);
        let country = resp
            .locations
            .get(&r.location)
            .map(|l| l.country.clone())
            .unwrap_or_else(|| r.location.clone());
        out.push(ExitRecord {
            country,
            code,
            socks: Some(socks),
        });
    }
    out.sort_by(|a, b| a.code.cmp(&b.code));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "locations": {
        "nl-ams": { "country": "Netherlands" },
        "de-fra": { "country": "Germany" }
      },
      "wireguard": {
        "relays": [
          { "hostname": "nl-ams-wg-001", "location": "nl-ams", "active": true },
          { "hostname": "de-fra-wg-007", "location": "de-fra", "active": true },
          { "hostname": "de-fra-wg-008", "location": "de-fra", "active": false },
          { "hostname": "weird-relay",   "location": "nl-ams", "active": true }
        ]
      }
    }"#;

    #[test]
    fn parse_rewrites_socks_host_and_skips_inactive() {
        let exits = parse_catalog(SAMPLE).unwrap();
        assert_eq!(exits.len(), 2);
        assert_eq!(exits[0].code, "de-fra-007");
        assert_eq!(exits[0].country, "Germany");
        assert_eq!(
            exits[0].socks.as_deref(),
            Some("de-fra-wg-socks5-007.relays.mullvad.net:1080")
        );
        assert_eq!(exits[1].code, "nl-ams-001");
        assert_eq!(
            exits[1].proxy_url().as_deref(),
            Some("socks5h://nl-ams-wg-socks5-001.relays.mullvad.net:1080")
        );
    }
}
