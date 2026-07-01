#![allow(
    clippy::collapsible_if,
    clippy::doc_lazy_continuation,
    clippy::unnecessary_sort_by
)]

use anyhow::Result;
use scraper::{Html, Selector};
use serde::Serialize;
use url::Url;

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct ProsCon {
    pub text: String,
    pub up_votes: u32,
    pub down_votes: u32,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct SimilarPerfume {
    pub id: u64,
    pub slug: String,
    pub up_votes: u32,
    pub down_votes: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AlsoLiked {
    pub id: u64,
    pub slug: String,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct Perfumer {
    pub name: String,
    pub slug: String,
    pub image_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct Note {
    pub name: String,
    pub slug: String,
    pub image_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DetailFields {
    pub rating_count: Option<u64>,
    pub accords: Vec<(String, u8)>,
    pub notes_top: Vec<Note>,
    pub notes_middle: Vec<Note>,
    pub notes_base: Vec<Note>,
    pub notes_flat: Vec<Note>,
    pub perfumers: Vec<Perfumer>,
    pub description: Option<String>,
    pub pros: Vec<ProsCon>,
    pub cons: Vec<ProsCon>,
    pub reminds_me_of: Vec<SimilarPerfume>,
    pub also_liked: Vec<AlsoLiked>,
}

pub fn parse(html: &str) -> Result<DetailFields> {
    let doc = Html::parse_document(html);
    let (notes_top, notes_middle, notes_base, notes_flat) = extract_notes(&doc);
    let (pros, cons) = extract_pros_cons(&doc);
    let (reminds_me_of, also_liked) = extract_similars(&doc);
    Ok(DetailFields {
        rating_count: extract_rating_count(&doc),
        accords: extract_accords(&doc),
        notes_top,
        notes_middle,
        notes_base,
        notes_flat,
        perfumers: extract_perfumers(&doc),
        description: extract_itemprop(&doc, "description"),
        pros,
        cons,
        reminds_me_of,
        also_liked,
    })
}

fn extract_itemprop(doc: &Html, prop: &str) -> Option<String> {
    let sel = Selector::parse(&format!("[itemprop=\"{prop}\"]")).ok()?;
    let el = doc.select(&sel).next()?;
    if let Some(c) = el.value().attr("content") {
        return Some(c.to_string());
    }
    let txt = el.text().collect::<String>().trim().to_string();
    if txt.is_empty() { None } else { Some(txt) }
}

fn extract_rating_count(doc: &Html) -> Option<u64> {
    let sel = Selector::parse("[itemprop=\"ratingCount\"]").ok()?;
    let el = doc.select(&sel).next()?;
    if let Some(c) = el.value().attr("content") {
        if let Ok(n) = c.parse() {
            return Some(n);
        }
    }
    el.text()
        .collect::<String>()
        .trim()
        .replace(',', "")
        .parse()
        .ok()
}

fn extract_accords(doc: &Html) -> Vec<(String, u8)> {
    let sel = match Selector::parse("a[href*=\"/accords-search/\"]") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    for a in doc.select(&sel) {
        let href = match a.value().attr("href") {
            Some(h) => h,
            None => continue,
        };
        let resolved = match Url::parse(href) {
            Ok(u) => u,
            Err(_) => match Url::parse("https://www.fragrantica.com").and_then(|b| b.join(href)) {
                Ok(u) => u,
                Err(_) => continue,
            },
        };
        let mut accords: Vec<(String, u8)> = Vec::new();
        for (k, v) in resolved.query_pairs() {
            let key = k.to_string();
            if key.starts_with("f_") || key == "from" {
                continue;
            }
            if let Ok(pct) = v.parse::<u8>() {
                accords.push((key.replace('+', " "), pct));
            }
        }
        if !accords.is_empty() {
            accords.sort_by(|a, b| b.1.cmp(&a.1));
            return accords;
        }
    }
    Vec::new()
}

fn extract_notes(doc: &Html) -> (Vec<Note>, Vec<Note>, Vec<Note>, Vec<Note>) {
    let combined = match Selector::parse("h4, div.pyramid-level-container") {
        Ok(s) => s,
        Err(_) => return (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
    };
    let label_sel = Selector::parse(".pyramid-note-label").unwrap();

    let mut top = Vec::new();
    let mut middle = Vec::new();
    let mut base = Vec::new();
    let mut unclaimed_first: Vec<Note> = Vec::new();
    let mut pending: Option<&str> = None;

    for elem in doc.select(&combined) {
        if elem.value().name() == "h4" {
            let raw = elem.text().collect::<String>();
            let normalised = raw.split_whitespace().collect::<Vec<_>>().join(" ");
            pending = match normalised.as_str() {
                "Top Notes" => Some("top"),
                "Middle Notes" => Some("middle"),
                "Base Notes" => Some("base"),
                _ => None,
            };
        } else {
            let notes: Vec<Note> = elem
                .select(&label_sel)
                .filter_map(|l| {
                    let name = l.text().collect::<String>().trim().to_string();
                    if name.is_empty() {
                        return None;
                    }
                    let (slug, image_id) = note_link_of(l);
                    Some(Note {
                        name,
                        slug,
                        image_id,
                    })
                })
                .collect();
            if let Some(level) = pending.take() {
                match level {
                    "top" => top = notes,
                    "middle" => middle = notes,
                    "base" => base = notes,
                    _ => {}
                }
            } else if unclaimed_first.is_empty() && !notes.is_empty() {
                unclaimed_first = notes;
            }
        }
    }

    let flat = if top.is_empty() && middle.is_empty() && base.is_empty() {
        unclaimed_first
    } else {
        Vec::new()
    };
    (top, middle, base, flat)
}

fn note_link_of(label: scraper::ElementRef) -> (String, Option<u64>) {
    for anc in label.ancestors() {
        let Some(el) = anc.value().as_element() else {
            continue;
        };
        if el.name() != "a" {
            continue;
        }
        if let Some((slug, id)) = note_slug_id(el.attr("href").unwrap_or("")) {
            return (slug, Some(id));
        }
    }
    (String::new(), None)
}

/// Split a `<slug>-<id>.html` path tail into its slug and numeric id.
fn split_slug_id(tail: &str) -> Option<(String, u64)> {
    let path = tail.strip_suffix(".html")?;
    let dash = path.rfind('-')?;
    let id: u64 = path[dash + 1..].parse().ok()?;
    let slug = path[..dash].to_string();
    (!slug.is_empty()).then_some((slug, id))
}

fn note_slug_id(href: &str) -> Option<(String, u64)> {
    split_slug_id(href.split("/notes/").nth(1)?)
}

fn extract_pros_cons(doc: &Html) -> (Vec<ProsCon>, Vec<ProsCon>) {
    let combined = match Selector::parse(
        r#"span[class~="uppercase"], div[class*="group/item"], p[class*="text-zinc-400"]"#,
    ) {
        Ok(s) => s,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let p_sel = Selector::parse("p").unwrap();
    let count_sel = Selector::parse(r#"button span[class*="text-["]"#).unwrap();

    let mut pros = Vec::new();
    let mut cons = Vec::new();
    let mut section: Option<&'static str> = None;

    for el in doc.select(&combined) {
        match el.value().name() {
            "span" => {
                let raw = el.text().collect::<String>();
                let trimmed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
                section = match trimmed.as_str() {
                    "Pros" => Some("pros"),
                    "Cons" => Some("cons"),
                    _ => section,
                };
            }
            "p" => {
                let raw = el.text().collect::<String>();
                if raw.contains("These pros and cons are AI-generated") {
                    break;
                }
            }
            "div" => {
                if let Some(sec) = section {
                    if let Some(item) = parse_pros_con_item(el, &p_sel, &count_sel) {
                        if sec == "pros" {
                            pros.push(item);
                        } else {
                            cons.push(item);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    (pros, cons)
}

fn parse_pros_con_item(
    el: scraper::ElementRef,
    p_sel: &Selector,
    count_sel: &Selector,
) -> Option<ProsCon> {
    let text = el
        .select(p_sel)
        .next()?
        .text()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        return None;
    }
    let counts: Vec<u32> = el
        .select(count_sel)
        .filter_map(|s| parse_compact_number(s.text().collect::<String>().trim()))
        .collect();
    Some(ProsCon {
        text,
        up_votes: counts.first().copied().unwrap_or(0),
        down_votes: counts.get(1).copied().unwrap_or(0),
    })
}

fn parse_compact_number(s: &str) -> Option<u32> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u32>() {
        return Some(n);
    }
    let lower = s.to_lowercase();
    let (num_str, mult) = if let Some(r) = lower.strip_suffix('k') {
        (r, 1_000.0)
    } else if let Some(r) = lower.strip_suffix('m') {
        (r, 1_000_000.0)
    } else if let Some(r) = lower.strip_suffix('b') {
        (r, 1_000_000_000.0)
    } else {
        return None;
    };
    let val: f64 = num_str.parse().ok()?;
    Some((val * mult).round() as u32)
}

fn extract_similars(doc: &Html) -> (Vec<SimilarPerfume>, Vec<AlsoLiked>) {
    const REMINDS: &str = "This perfume reminds me of";
    const ALSO: &str = "People who like this also like";

    let combined = match Selector::parse(r#"h3, div[class~="tw-carousel-perfume-card"]"#) {
        Ok(s) => s,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let anchor_sel = Selector::parse(r#"a[href*="/perfume/"]"#).unwrap();
    let vote_sel = Selector::parse(r#"span[class~="text-xs"]"#).unwrap();
    let mut reminds: Vec<SimilarPerfume> = Vec::new();
    let mut also: Vec<AlsoLiked> = Vec::new();
    let mut section: Option<&'static str> = None;

    for el in doc.select(&combined) {
        if el.value().name() == "h3" {
            let raw = el.text().collect::<String>();
            let trimmed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
            section = if trimmed == REMINDS {
                Some("reminds")
            } else if trimmed == ALSO {
                Some("also")
            } else {
                None
            };
            continue;
        }
        let Some(sec) = section else { continue };
        let Some(anchor) = el.select(&anchor_sel).next() else {
            continue;
        };
        let Some((id, slug)) = parse_perfume_href(anchor.value().attr("href").unwrap_or("")) else {
            continue;
        };
        if sec == "reminds" {
            if reminds.iter().any(|e| e.id == id) {
                continue;
            }
            let counts: Vec<u32> = el
                .select(&vote_sel)
                .filter_map(|s| parse_compact_number(s.text().collect::<String>().trim()))
                .collect();
            reminds.push(SimilarPerfume {
                id,
                slug,
                up_votes: counts.first().copied().unwrap_or(0),
                down_votes: counts.get(1).copied().unwrap_or(0),
            });
        } else if !also.iter().any(|e| e.id == id) {
            also.push(AlsoLiked { id, slug });
        }
    }

    (reminds, also)
}

pub fn parse_perfume_href(href: &str) -> Option<(u64, String)> {
    let (slug, id) = split_slug_id(href.strip_prefix("/perfume/")?)?;
    Some((id, slug))
}

fn extract_perfumers(doc: &Html) -> Vec<Perfumer> {
    let sel = match Selector::parse("a[href*=\"/noses/\"]") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let img_sel = Selector::parse("img").unwrap();
    let mut out: Vec<Perfumer> = Vec::new();
    for a in doc.select(&sel) {
        let href = a.value().attr("href").unwrap_or("");
        let Some(slug) = noses_slug(href) else {
            continue;
        };
        let name = a.text().collect::<String>().trim().to_string();
        if name.is_empty() || name.eq_ignore_ascii_case("perfumers") {
            continue;
        }
        let image_id = a
            .select(&img_sel)
            .find_map(|img| nosevi_id(img.value().attr("src").unwrap_or("")));
        if let Some(existing) = out.iter_mut().find(|p| p.name == name) {
            if existing.image_id.is_none() {
                existing.image_id = image_id;
            }
        } else {
            out.push(Perfumer {
                name,
                slug,
                image_id,
            });
        }
    }
    out
}

fn noses_slug(href: &str) -> Option<String> {
    let tail = href.rsplit("/noses/").next()?;
    let slug = tail.strip_suffix(".html").unwrap_or(tail);
    if slug.is_empty() {
        return None;
    }
    Some(slug.to_string())
}

fn nosevi_id(src: &str) -> Option<u64> {
    src.split("/nosevi/fit.")
        .nth(1)?
        .split('.')
        .next()?
        .parse()
        .ok()
}
