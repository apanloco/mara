use anyhow::{Result, anyhow};
use chromiumoxide::Page;
use chromiumoxide::layout::Point;
use std::time::{Duration, Instant};
use x11rb::connection::Connection;
use x11rb::protocol::xproto::{
    BUTTON_PRESS_EVENT, BUTTON_RELEASE_EVENT, ConnectionExt as _, MOTION_NOTIFY_EVENT,
};
use x11rb::protocol::xtest::ConnectionExt as _;

use crate::challenge::ChallengeWidget;

const CHECKBOX_DX: f64 = 24.0;

pub fn checkbox_viewport(b: &ChallengeWidget) -> (f64, f64) {
    (b.x + CHECKBOX_DX, b.y + b.h / 2.0)
}

pub async fn move_to_humanlike(page: &Page, x: f64, y: f64) -> Result<()> {
    let (sx, sy) = (x - 130.0, y - 90.0);
    const STEPS: u32 = 16;
    for i in 1..=STEPS {
        let t = i as f64 / STEPS as f64;
        let e = if t < 0.5 {
            2.0 * t * t
        } else {
            -1.0 + (4.0 - 2.0 * t) * t
        };
        let px = sx + (x - sx) * e;
        let py = sy + (y - sy) * e;
        let _ = page.move_mouse(Point::new(px, py)).await;
        tokio::time::sleep(Duration::from_millis(22)).await;
    }
    Ok(())
}

const LOCATE_INTERVAL: Duration = Duration::from_millis(100);
const MOVE_INTERVAL: Duration = Duration::from_millis(25);
const MAX_STEP_PX: f64 = 18.0;
const HOMING_GAIN: f64 = 0.25;
const ON_TARGET_PX: f64 = 4.0;
const REACTION_WINDOW: Duration = Duration::from_millis(120);
const MAX_CLICKS: u32 = 25;
const BLANK_BAIL: u32 = 3;

#[derive(Debug, PartialEq, Eq)]
enum Step {
    Aim { x: i32, y: i32, precise: bool },
    Hold,
    Reacted,
    Bail,
}

#[derive(Default)]
struct Tracker {
    clicks: u32,
    blank: u32,
}

impl Tracker {
    fn observe(&mut self, located: Option<&Target>) -> Step {
        match located {
            Some(t) if t.precise => {
                self.blank = 0;
                Step::Aim {
                    x: t.x,
                    y: t.y,
                    precise: true,
                }
            }
            Some(t) => {
                self.blank = 0;
                if self.clicks > 0 {
                    Step::Reacted
                } else {
                    Step::Aim {
                        x: t.x,
                        y: t.y,
                        precise: false,
                    }
                }
            }
            None => {
                if self.clicks > 0 {
                    return Step::Reacted;
                }
                self.blank += 1;
                if self.blank >= BLANK_BAIL {
                    Step::Bail
                } else {
                    Step::Hold
                }
            }
        }
    }
}

use crate::locate::Target;

/// Drive the OS mouse cursor toward the located checkbox and click it (one or more times)
/// until the widget reacts or `deadline` passes. Returns the click point, if any.
pub fn click_challenge(display: &str, deadline: Instant) -> Result<Option<(i32, i32)>> {
    let (conn, screen_num) =
        x11rb::connect(Some(display)).map_err(|e| anyhow!("x11 connect: {e}"))?;
    let screen = &conn.setup().roots[screen_num];
    let root = screen.root;
    let (w, h) = (
        screen.width_in_pixels as f64,
        screen.height_in_pixels as f64,
    );

    let (mut cx, mut cy) = conn
        .query_pointer(root)
        .ok()
        .and_then(|c| c.reply().ok())
        .map(|p| (p.root_x as f64, p.root_y as f64))
        .unwrap_or((w / 2.0, h / 2.0));

    let mut rng = Rng::new();
    let mut target: Option<(f64, f64, bool)> = None;
    let mut tracker = Tracker::default();
    let mut last_press: Option<Instant> = None;
    let mut last_pos = (0i32, 0i32);
    let mut next_locate = Instant::now();

    loop {
        if Instant::now() >= deadline {
            return Ok((tracker.clicks > 0).then_some(last_pos));
        }
        if Instant::now() >= next_locate {
            next_locate = Instant::now() + LOCATE_INTERVAL;
            match tracker.observe(crate::locate::target(display)?.as_ref()) {
                Step::Aim { x, y, precise } => target = Some((x as f64, y as f64, precise)),
                Step::Hold => {}
                Step::Reacted => return Ok(Some(last_pos)),
                Step::Bail => return Ok(None),
            }
        }

        if let Some((tx, ty, precise)) = target {
            let (dx, dy) = (tx - cx, ty - cy);
            let dist = dx.hypot(dy);
            if dist > 1.0 {
                let step = (dist * HOMING_GAIN).min(MAX_STEP_PX).min(dist);
                let tremor = (dist / 200.0).min(1.0) * 2.0;
                cx = (cx + dx / dist * step + rng.signed(0.0, tremor.max(0.1))).clamp(0.0, w - 1.0);
                cy = (cy + dy / dist * step + rng.signed(0.0, tremor.max(0.1))).clamp(0.0, h - 1.0);
                conn.xtest_fake_input(
                    MOTION_NOTIFY_EVENT,
                    0,
                    0,
                    root,
                    cx.round() as i16,
                    cy.round() as i16,
                    0,
                )?;
                conn.flush()?;
            }
            let due = last_press.is_none_or(|t| t.elapsed() >= REACTION_WINDOW);
            if precise && dist < ON_TARGET_PX && due {
                if tracker.clicks >= MAX_CLICKS {
                    return Ok(Some(last_pos));
                }
                conn.xtest_fake_input(
                    MOTION_NOTIFY_EVENT,
                    0,
                    0,
                    root,
                    tx.round() as i16,
                    ty.round() as i16,
                    0,
                )?;
                conn.flush()?;
                std::thread::sleep(Duration::from_secs_f64(rng.range(0.05, 0.12)));
                conn.xtest_fake_input(BUTTON_PRESS_EVENT, 1, 0, root, 0, 0, 0)?;
                conn.flush()?;
                std::thread::sleep(Duration::from_secs_f64(rng.range(0.05, 0.12)));
                conn.xtest_fake_input(BUTTON_RELEASE_EVENT, 1, 0, root, 0, 0, 0)?;
                conn.flush()?;
                tracker.clicks += 1;
                last_press = Some(Instant::now());
                last_pos = (tx as i32, ty as i32);
            }
        }
        std::thread::sleep(MOVE_INTERVAL);
    }
}

struct Rng(u64);

impl Rng {
    fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }
    fn signed(&mut self, lo: f64, hi: f64) -> f64 {
        let m = self.range(lo, hi);
        if self.unit() < 0.5 { -m } else { m }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn precise(x: i32, y: i32) -> Target {
        Target {
            x,
            y,
            precise: true,
        }
    }
    fn estimate(x: i32, y: i32) -> Target {
        Target {
            x,
            y,
            precise: false,
        }
    }

    #[test]
    fn precise_square_aims_and_presses() {
        let mut t = Tracker::default();
        assert_eq!(
            t.observe(Some(&precise(64, 93))),
            Step::Aim {
                x: 64,
                y: 93,
                precise: true
            }
        );
    }

    #[test]
    fn estimate_before_a_press_pre_hovers() {
        let mut t = Tracker::default();
        assert_eq!(
            t.observe(Some(&estimate(265, 485))),
            Step::Aim {
                x: 265,
                y: 485,
                precise: false
            }
        );
    }

    #[test]
    fn square_then_vanish_after_press_is_reaction() {
        let mut t = Tracker::default();
        assert!(matches!(
            t.observe(Some(&precise(64, 93))),
            Step::Aim { precise: true, .. }
        ));
        t.clicks += 1;
        assert_eq!(t.observe(None), Step::Reacted);
    }

    #[test]
    fn estimate_after_a_press_is_also_reaction() {
        let mut t = Tracker::default();
        t.clicks += 1;
        assert_eq!(t.observe(Some(&estimate(265, 485))), Step::Reacted);
    }

    #[test]
    fn blank_flicker_holds_until_bail_threshold() {
        let mut t = Tracker::default();
        for _ in 0..(BLANK_BAIL - 1) {
            assert_eq!(t.observe(None), Step::Hold);
        }
        assert_eq!(t.observe(None), Step::Bail);
    }

    #[test]
    fn a_visible_widget_resets_the_blank_run() {
        let mut t = Tracker::default();
        assert_eq!(t.observe(None), Step::Hold);
        assert!(matches!(
            t.observe(Some(&estimate(10, 10))),
            Step::Aim { .. }
        ));
        assert_eq!(t.observe(None), Step::Hold);
        assert_eq!(t.observe(None), Step::Hold);
    }

    #[test]
    fn reel_drives_a_real_solve_through_find_target_and_tracker() {
        use crate::frame::Image;
        use crate::locate::find_target;
        let dir = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/locate"));
        let load = |name: &str| {
            Image::decode_png(&std::fs::read(dir.join(name)).expect(name)).expect("decode")
        };
        let observe = |t: &mut Tracker, name: &str| t.observe(find_target(&load(name)).as_ref());

        let mut t = Tracker::default();
        assert_eq!(observe(&mut t, "01_spinner_fragrantica.png"), Step::Hold);
        assert!(matches!(
            observe(&mut t, "02_verifying_fragrantica.png"),
            Step::Aim { precise: false, .. }
        ));
        assert!(matches!(
            observe(&mut t, "03_checkbox_fragrantica.png"),
            Step::Aim { precise: true, .. }
        ));
        t.clicks += 1;
        for frame in [
            "04_checkbox_clicked_fragrantica.png",
            "05_verifying_again_fragrantica.png",
            "06_cleared_fragrantica.png",
            "07_page_loaded_fragrantica.png",
        ] {
            assert_eq!(observe(&mut t, frame), Step::Reacted, "{frame}");
        }
    }
}
