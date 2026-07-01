use crate::solver::frame::Image;
use anyhow::Result;

pub struct Target {
    pub x: i32,
    pub y: i32,
    pub precise: bool,
}

pub fn target(display: &str) -> Result<Option<Target>> {
    Ok(find_target(&crate::solver::frame::grab_image(display)?))
}

const LOGO_LEFT_TO_CHECKBOX_DX: i32 = 236;
const LOGO_TO_CHECKBOX_DY: i32 = 12;

pub fn find_target(img: &Image) -> Option<Target> {
    let (logo_cy, logo_left) = orange_anchor(img)?;
    if let Some((cx, cy)) = checkbox_in_band(img, logo_cy, logo_left) {
        return Some(Target {
            x: cx,
            y: cy,
            precise: true,
        });
    }
    Some(Target {
        x: (logo_left as i32 - LOGO_LEFT_TO_CHECKBOX_DX).max(0),
        y: logo_cy as i32 + LOGO_TO_CHECKBOX_DY,
        precise: false,
    })
}

fn is_orange(r: u8, g: u8, b: u8) -> bool {
    (235..=255).contains(&r) && (110..=185).contains(&g) && (20..=80).contains(&b)
}

fn is_dark(r: u8, g: u8, b: u8) -> bool {
    let mx = r.max(g).max(b);
    let mn = r.min(g).min(b);
    mn >= 50 && mx <= 110 && mx - mn < 18
}

fn orange_anchor(img: &Image) -> Option<(usize, usize)> {
    let mut orange: Vec<(usize, usize)> = Vec::new();
    for y in 0..img.h {
        for x in 0..img.w {
            let (r, g, b) = img.at(x, y);
            if is_orange(r, g, b) {
                orange.push((y, x));
            }
        }
    }
    if orange.len() < 30 {
        return None;
    }
    orange.sort_unstable();
    let logo_cy = orange[orange.len() / 2].0;
    let logo_left = orange
        .iter()
        .filter(|&&(y, _)| y.abs_diff(logo_cy) <= 20)
        .map(|&(_, x)| x)
        .min()?;
    Some((logo_cy, logo_left))
}

fn checkbox_in_band(img: &Image, logo_cy: usize, logo_left: usize) -> Option<(i32, i32)> {
    let band_top = logo_cy.saturating_sub(30);
    let band_bot = (logo_cy + 30).min(img.h);
    let col_dark = |x: usize| {
        (band_top..band_bot)
            .filter(|&y| is_dark_at(img, x, y))
            .count()
    };

    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut cur: Option<(usize, usize)> = None;
    for x in 0..logo_left {
        if col_dark(x) >= 2 {
            cur = Some(match cur {
                Some((s, _)) => (s, x),
                None => (x, x),
            });
        } else if let Some((s, e)) = cur
            && x - e > 5
        {
            runs.push((s, e));
            cur = None;
        }
    }
    if let Some(r) = cur {
        runs.push(r);
    }

    for (xl, xr) in runs {
        let w = xr - xl + 1;
        if !(18..=40).contains(&w) {
            continue;
        }
        let Some((top, bot)) = dark_run_extent(img, xl, logo_cy) else {
            continue;
        };
        if !(18..=40).contains(&(bot - top + 1)) {
            continue;
        }
        let (cx, cy) = ((xl + xr) / 2, (top + bot) / 2);
        if is_light(img.at(cx, cy)) {
            return Some((cx as i32, cy as i32));
        }
    }
    None
}

#[inline]
fn is_dark_at(img: &Image, x: usize, y: usize) -> bool {
    let (r, g, b) = img.at(x, y);
    is_dark(r, g, b)
}

fn is_light((r, g, b): (u8, u8, u8)) -> bool {
    r > 180 && g > 180 && b > 180
}

fn dark_run_extent(img: &Image, x: usize, cy: usize) -> Option<(usize, usize)> {
    let top = cy.saturating_sub(45);
    let bot = (cy + 45).min(img.h);
    let mut best: Option<(usize, usize)> = None;
    let mut start: Option<usize> = None;
    for y in top..bot {
        if is_dark_at(img, x, y) {
            let s = *start.get_or_insert(y);
            if best.is_none_or(|(bs, be)| be - bs < y - s) {
                best = Some((s, y));
            }
        } else {
            start = None;
        }
    }
    best
}

#[cfg(test)]
pub(crate) mod scenes {
    use crate::solver::frame::Image;

    pub const BORDER: (u8, u8, u8) = (74, 74, 74);
    pub const ORANGE: (u8, u8, u8) = (243, 128, 32);

    pub fn solid(w: usize, h: usize) -> Image {
        Image {
            w,
            h,
            px: vec![255u8; w * h * 3],
        }
    }
    pub fn set(img: &mut Image, x: usize, y: usize, c: (u8, u8, u8)) {
        let o = (y * img.w + x) * 3;
        img.px[o] = c.0;
        img.px[o + 1] = c.1;
        img.px[o + 2] = c.2;
    }
    pub fn hollow_square(img: &mut Image, sx: usize, sy: usize, side: usize) {
        for d in 0..side {
            set(img, sx + d, sy, BORDER);
            set(img, sx + d, sy + side - 1, BORDER);
            set(img, sx, sy + d, BORDER);
            set(img, sx + side - 1, sy + d, BORDER);
        }
    }
    pub fn fill(img: &mut Image, x0: usize, x1: usize, y0: usize, y1: usize, c: (u8, u8, u8)) {
        for y in y0..y1 {
            for x in x0..x1 {
                set(img, x, y, c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::scenes::*;
    use super::*;

    #[test]
    fn precise_target_on_the_checkbox() {
        let mut img = solid(400, 200);
        hollow_square(&mut img, 50, 80, 28);
        fill(&mut img, 300, 330, 84, 104, ORANGE);
        let t = find_target(&img).expect("target");
        assert!(t.precise, "should be a precise checkbox target");
        assert!(
            (t.x - 64).abs() <= 2 && (t.y - 93).abs() <= 3,
            "({},{})",
            t.x,
            t.y
        );
    }

    #[test]
    fn no_orange_means_no_target() {
        let mut img = solid(400, 200);
        hollow_square(&mut img, 50, 80, 28);
        assert!(find_target(&img).is_none());
    }

    #[test]
    fn estimate_when_box_absent() {
        let mut img = solid(400, 200);
        fill(&mut img, 50, 78, 80, 108, BORDER);
        fill(&mut img, 300, 330, 84, 104, ORANGE);
        let t = find_target(&img).expect("estimate target from the logo");
        assert!(
            !t.precise,
            "must be an estimate, not a clickable precise target"
        );
        assert!(t.x < 300, "estimate sits left of the logo: x={}", t.x);
    }

    #[test]
    fn stray_orange_does_not_shift_band() {
        let mut img = solid(400, 200);
        hollow_square(&mut img, 50, 80, 28);
        fill(&mut img, 300, 330, 84, 104, ORANGE);
        set(&mut img, 5, 5, ORANGE);
        set(&mut img, 6, 5, ORANGE);
        let t = find_target(&img).expect("target despite strays");
        assert!(
            t.precise && (t.x - 64).abs() <= 2 && (t.y - 93).abs() <= 3,
            "({},{})",
            t.x,
            t.y
        );
    }

    const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/locate");
    const COORD_TOL: i32 = 6;

    struct Expectation {
        file: String,
        verdict: String,
        coords: Option<(i32, i32)>,
    }

    fn read_manifest() -> Vec<Expectation> {
        let path = std::path::Path::new(FIXTURE_DIR).join("expected.txt");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        text.lines()
            .map(|l| l.split('#').next().unwrap().trim())
            .filter(|l| !l.is_empty())
            .map(|l| {
                let f: Vec<&str> = l.split_whitespace().collect();
                let coords = f
                    .get(2)
                    .zip(f.get(3))
                    .map(|(x, y)| (x.parse().unwrap(), y.parse().unwrap()));
                Expectation {
                    verdict: f[0].to_string(),
                    file: f[1].to_string(),
                    coords,
                }
            })
            .collect()
    }

    #[test]
    #[ignore = "triage aid for filling expected.txt; run with --ignored --nocapture"]
    fn classify_corpus() {
        let mut paths: Vec<_> = std::fs::read_dir(FIXTURE_DIR)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "png"))
            .collect();
        paths.sort();
        for p in paths {
            let img = Image::decode_png(&std::fs::read(&p).unwrap()).unwrap();
            let v = match find_target(&img) {
                Some(t) if t.precise => format!("precise   {} {}", t.x, t.y),
                Some(t) => format!("estimate  ({},{})", t.x, t.y),
                None => "none".into(),
            };
            println!(
                "{:<44} {}x{}  -> {v}",
                p.file_name().unwrap().to_string_lossy(),
                img.w,
                img.h
            );
        }
    }

    #[test]
    fn fixture_corpus_matches_expected_verdicts() {
        let dir = std::path::Path::new(FIXTURE_DIR);
        let manifest = read_manifest();
        assert!(
            !manifest.is_empty(),
            "expected.txt is empty — capture real frames with `mara capture`"
        );

        let listed: std::collections::HashSet<&str> =
            manifest.iter().map(|e| e.file.as_str()).collect();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".png") {
                assert!(
                    listed.contains(name.as_str()),
                    "{name}: not in expected.txt (add a line classifying it)"
                );
            }
        }

        for e in &manifest {
            let bytes = std::fs::read(dir.join(&e.file))
                .unwrap_or_else(|_| panic!("{}: listed but missing", e.file));
            let target = find_target(&Image::decode_png(&bytes).expect("decode fixture PNG"));
            let got = match &target {
                Some(t) if t.precise => "precise",
                Some(_) => "estimate",
                None => "none",
            };
            assert_eq!(
                got, e.verdict,
                "{}: expected {}, got {got}",
                e.file, e.verdict
            );
            if let (Some((ex, ey)), Some(t)) = (e.coords, &target) {
                assert!(
                    (t.x - ex).abs() <= COORD_TOL && (t.y - ey).abs() <= COORD_TOL,
                    "{}: target ({},{}) not within {COORD_TOL}px of ({ex},{ey})",
                    e.file,
                    t.x,
                    t.y,
                );
            }
        }
    }
}
