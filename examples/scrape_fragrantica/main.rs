mod decrypt;
mod parse;

use anyhow::{Context, Result};
use mara::{Client, Config, Domain};
use parse::{ProsCon, SimilarPerfume};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,chromiumoxide=off".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let mut url = None;
    let mut exits = Vec::new();
    let mut mullvad = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--mullvad" => mullvad = true,
            "--exit" => exits.extend(args.next()),
            _ => url = Some(a),
        }
    }
    let url = url.context("usage: scrape_fragrantica <perfume-url> [--exit ..] [--mullvad]")?;
    let url = if url.contains("://") {
        url
    } else {
        format!("https://{url}")
    };

    let path = url::Url::parse(&url)
        .ok()
        .map(|u| u.path().to_string())
        .unwrap_or_default();
    let (id, slug) = parse::parse_perfume_href(&path)
        .context("URL is not a /perfume/<Brand>/<Name>-<id>.html link")?;

    let host = mara::host_of(&url).context("URL has no host")?;

    let client = Client::new(Config {
        exits,
        mullvad,
        domains: vec![Domain::solve(host)],
        ..Default::default()
    })
    .await?;

    let out = client.fetch_http(&url).await?;
    let html = &out.value;

    let blob = decrypt::decrypt_inline(html, "status").ok();

    let mut fields = parse::parse(html).context("parse detail fields")?;

    match decrypt::decrypt_inline(html, "ai_opinions") {
        Ok(ai) => {
            fields.pros = opinions(&ai, "pros");
            fields.cons = opinions(&ai, "cons");
        }
        Err(e) => eprintln!("⚠ ai_opinions (pros/cons): {e}"),
    }
    match decrypt::decrypt_inline(html, "similar_perfumes") {
        Ok(sim) => fields.reminds_me_of = similars(&sim),
        Err(e) => eprintln!("⚠ similar_perfumes (reminds_me_of): {e}"),
    }

    let record = serde_json::json!({
        "blob": blob,
        "html": fields,
        "id": id,
        "slug": slug,
        "perfume_url": url,
        "blob_decrypted": blob.is_some(),
    });

    eprintln!(
        "✓ scraped via mara ({}, {:.1}s, exit={})",
        if out.solve_required {
            "solved"
        } else {
            "direct"
        },
        out.elapsed.as_secs_f64(),
        out.exit.as_deref().unwrap_or("?"),
    );
    println!("{}", serde_json::to_string_pretty(&record)?);

    let _ = tokio::time::timeout(Duration::from_secs(30), client.shutdown()).await;
    Ok(())
}

fn opinions(ai: &serde_json::Value, section: &str) -> Vec<ProsCon> {
    ai.get(section)
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|o| {
                    let text = o.get("opinion")?.as_str()?.to_string();
                    Some(ProsCon {
                        text,
                        up_votes: o.get("vote_yes").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                        down_votes: o.get("vote_no").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn similars(sim: &serde_json::Value) -> Vec<SimilarPerfume> {
    let mut out: Vec<SimilarPerfume> = Vec::new();
    let Some(items) = sim.get("similar_perfumes").and_then(|v| v.as_array()) else {
        return out;
    };
    for it in items {
        let Some(p) = it.get("perfume") else { continue };
        let Some(id) = p.get("id").and_then(|v| v.as_u64()) else {
            continue;
        };
        let Some(slug) = p.get("slug").and_then(|v| v.as_str()) else {
            continue;
        };
        if out.iter().any(|e| e.id == id) {
            continue;
        }
        out.push(SimilarPerfume {
            id,
            slug: slug.to_string(),
            up_votes: it.get("vote_yes").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
            down_votes: it.get("vote_no").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
        });
    }
    out
}
