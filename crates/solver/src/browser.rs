use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chromiumoxide::Page;
use chromiumoxide::cdp::browser_protocol::page::CaptureScreenshotFormat;
use chromiumoxide::layout::Point;
use chromiumoxide::page::ScreenshotParams;

use mara_core::classify::{self, Reason};
use mara_core::clearance::Clearance;
use mara_core::host_of;

use crate::challenge;
use crate::click;
use crate::display::VirtualDisplay;
use crate::observe::{BrowserId, Observer, Phase};
use crate::session::{LaunchOpts, Session};

const POLL: Duration = Duration::from_millis(500);
const GRACE_BEFORE_CLICK: Duration = Duration::from_secs(3);
const CLICK_COOLDOWN: Duration = Duration::from_secs(6);
const APPROACH_DEADLINE: Duration = Duration::from_secs(12);

#[derive(Clone)]
pub struct SolveConfig {
    pub real_display: bool,
    pub cdp_click: bool,
    pub no_click: bool,
    pub move_mouse: bool,
    pub connect_grace: Duration,
    pub timeout: Duration,
    pub no_checkbox_deadline: Duration,
    pub width: u32,
    pub height: u32,
    pub capture_dir: Option<PathBuf>,
    pub artifact_dir: PathBuf,
}

pub struct Browser {
    id: BrowserId,
    user_agent: Option<String>,
    session: Session,
    display: Option<VirtualDisplay>,
    click_display: String,
    observer: Arc<dyn Observer>,
    artifact_dir: PathBuf,
}

pub struct Cleared {
    pub page: Page,
    pub clearance: Clearance,
    pub clicks: u32,
}

#[derive(Default)]
struct Clicks {
    count: u32,
    last: Option<Instant>,
    moved: bool,
}

impl Browser {
    pub async fn launch(
        proxy: Option<&str>,
        profile: &Path,
        cfg: &SolveConfig,
        observer: Arc<dyn Observer>,
    ) -> Result<Browser> {
        let data_dir = profile.to_path_buf();
        let artifact_dir = cfg.artifact_dir.clone();

        if cfg.real_display {
            let name = std::env::var("DISPLAY").unwrap_or_else(|_| ":0".to_string());
            let session = launch_session(cfg, &data_dir, proxy, Some(&name), true).await?;
            let id = observer.register(Some(name.clone()));
            return Ok(Browser {
                id,
                user_agent: None,
                session,
                display: None,
                click_display: name,
                observer,
                artifact_dir,
            });
        }

        let (w, h) = (cfg.width, cfg.height);
        let display = tokio::task::spawn_blocking(move || VirtualDisplay::start(w, h)).await??;
        let name = display.name();
        // Register only once the session is up: a failed launch must not leave a dashboard row
        // that never deregisters.
        let session = launch_session(cfg, &data_dir, proxy, Some(&name), false).await?;
        let id = observer.register(Some(name.clone()));
        Ok(Browser {
            id,
            user_agent: None,
            session,
            display: Some(display),
            click_display: name,
            observer,
            artifact_dir,
        })
    }

    pub fn id(&self) -> BrowserId {
        self.id
    }

    pub async fn solve(&mut self, cfg: &SolveConfig, url: &str) -> Result<Cleared, Reason> {
        let id = self.id;
        let page = match self.session.browser.new_page("about:blank").await {
            Ok(p) => p,
            Err(e) => {
                self.observer.phase(id, Phase::Failed);
                return Err(classify::from_transport(&e.to_string()));
            }
        };
        let _ = page.bring_to_front().await;
        self.observer.navigating(id, url);
        if let Err(e) = page.goto(url).await {
            self.observer.phase(id, Phase::Failed);
            return Err(classify::from_transport(&e.to_string()));
        }
        if self.user_agent.is_none()
            && let Ok(v) = page.evaluate("navigator.userAgent").await
            && let Ok(ua) = v.into_value::<String>()
        {
            self.observer.set_user_agent(id, ua.clone());
            self.user_agent = Some(ua);
        }

        let _grabber = cfg
            .capture_dir
            .clone()
            .map(|dir| crate::capture::Grabber::start(self.click_display.clone(), id, dir));

        if !cfg.connect_grace.is_zero() {
            tracing::info!(
                browser = id,
                secs = cfg.connect_grace.as_secs(),
                "paused before solving (watch on the dashboard)"
            );
            tokio::time::sleep(cfg.connect_grace).await;
        }

        let start = Instant::now();
        let mut clicks = Clicks::default();
        let mut challenge_since: Option<Instant> = None;

        loop {
            if start.elapsed() > cfg.timeout {
                self.observer.phase(id, Phase::Failed);
                self.diagnose(&page, "timeout", clicks.count, start.elapsed())
                    .await;
                let _ = page.close().await;
                return Err(Reason::Timeout);
            }
            if clicks.count == 0
                && challenge_since.is_some_and(|t| t.elapsed() > cfg.no_checkbox_deadline)
            {
                self.observer.event(format!(
                    "#{id}: no checkbox after {}s — abandoning exit (challenge stuck / non-interactive)",
                    cfg.no_checkbox_deadline.as_secs()
                ));
                self.observer.phase(id, Phase::Failed);
                self.diagnose(
                    &page,
                    "no checkbox (challenge stuck / non-interactive)",
                    clicks.count,
                    start.elapsed(),
                )
                .await;
                let _ = page.close().await;
                return Err(Reason::Timeout);
            }

            // Classify the live page once, via core's single source of truth. An interactive
            // challenge ("just a moment") surfaces as `Challenged` — that's the state we drive,
            // not a terminal failure; any other reason is terminal; `None` with a non-empty title
            // means the origin is through; an empty title means still loading, so keep polling.
            let title = page_title(&page).await;
            let html = page.content().await.unwrap_or_default();
            match classify::from_page(&title, &html) {
                Some(Reason::Challenged) => {
                    if challenge_since.is_none() {
                        self.observer.phase(id, Phase::Challenge);
                        challenge_since = Some(Instant::now());
                    }
                    let cooled = clicks.last.is_none_or(|t| t.elapsed() > CLICK_COOLDOWN);
                    if !cfg.no_click && cooled {
                        self.try_click(cfg, &page, start.elapsed(), &mut clicks)
                            .await;
                    }
                }
                Some(reason) => {
                    // A hard block / rate-limit: the page itself is the evidence (e.g. CF 1020),
                    // so diagnose it → a ghost retains the block screen + which exit hit it,
                    // instead of the card silently vanishing.
                    self.observer.phase(id, Phase::Blocked);
                    self.diagnose(&page, &format!("{reason:?}"), clicks.count, start.elapsed())
                        .await;
                    let _ = page.close().await;
                    return Err(reason);
                }
                None if title.is_empty() => {}
                None => {
                    self.observer.phase(id, Phase::Cleared);
                    let clearance = self.lift_clearance(&page).await;
                    return Ok(Cleared {
                        page,
                        clearance,
                        clicks: clicks.count,
                    });
                }
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn try_click(
        &self,
        cfg: &SolveConfig,
        page: &Page,
        since_start: Duration,
        clicks: &mut Clicks,
    ) {
        let id = self.id;
        if cfg.cdp_click {
            if since_start > GRACE_BEFORE_CLICK
                && let Some(widget) = challenge::locate_widget(page).await
            {
                let (x, y) = click::checkbox_viewport(&widget);
                if cfg.move_mouse && !clicks.moved {
                    let _ = click::move_to_humanlike(page, x, y).await;
                    clicks.moved = true;
                }
                let _ = page.click(Point::new(x, y)).await;
                clicks.count += 1;
                clicks.last = Some(Instant::now());
                self.observer.clicked(id, x as i32, y as i32);
            }
        } else {
            let display = self.click_display.clone();
            let deadline = Instant::now() + APPROACH_DEADLINE;
            let hit =
                tokio::task::spawn_blocking(move || click::click_challenge(&display, deadline))
                    .await;
            if let Ok(Ok(Some((x, y)))) = hit {
                tracing::info!("OS-mouse click at screen ({x},{y})");
                clicks.count += 1;
                clicks.last = Some(Instant::now());
                self.observer.clicked(id, x, y);
            }
        }
    }

    async fn lift_clearance(&self, page: &Page) -> Clearance {
        let user_agent = match page
            .evaluate("navigator.userAgent")
            .await
            .ok()
            .and_then(|v| v.into_value::<String>().ok())
        {
            Some(ua) => ua,
            None => self.user_agent.clone().unwrap_or_default(),
        };
        let host = page
            .url()
            .await
            .ok()
            .flatten()
            .and_then(|u| host_of(&u))
            .unwrap_or_default();
        let raw = page.get_cookies().await.unwrap_or_default();
        let expires_unix = raw
            .iter()
            .find(|c| c.name == "cf_clearance")
            .map(|c| c.expires)
            .filter(|e| *e > 0.0);
        let cookies = raw
            .into_iter()
            .filter(|c| c.name == "cf_clearance" || c.name == "__cf_bm")
            .map(|c| (c.name, c.value))
            .collect();
        Clearance::new(cookies, user_agent, expires_unix, host)
    }

    /// On a give-up, persist everything needed to answer "why wasn't this counted as solved?":
    /// the CDP screenshot, the Xvfb framebuffer, the live DOM, the widget probe, and a
    /// `summary.txt`. The one-line `tracing::warn!` is emitted while the worker's
    /// `exit{code=…}` span is still active, so `grep code=… run.log` leads straight here — and
    /// reveals when the framebuffer shows a loaded page the loop never classified as cleared
    /// (typically an empty `document.title`/`content()` from a CDP context that went stale).
    async fn diagnose(&self, page: &Page, reason: &str, clicks: u32, elapsed: Duration) {
        let dir = self.artifact_dir.join(format!("fail-{}", self.id));
        let _ = std::fs::create_dir_all(&dir);
        let url = page.url().await.ok().flatten().unwrap_or_default();
        let title = page_title(page).await;
        let html = page.content().await.unwrap_or_default();

        // Capture both the CDP screenshot and (when headless) the Xvfb framebuffer. The frame
        // handed to the observer prefers the framebuffer — that's the same surface the live
        // thumbnails grab, so the ghost freezes exactly what the operator saw on screen.
        let params = ScreenshotParams::builder()
            .format(CaptureScreenshotFormat::Png)
            .build();
        let cdp = page.screenshot(params).await.ok();
        if let Some(b) = &cdp {
            let _ = std::fs::write(dir.join("screenshot.png"), b);
        }
        let framebuffer = self.display.as_ref().and_then(|d| {
            let p = dir.join("framebuffer.png");
            d.screenshot(&p).ok().and_then(|_| std::fs::read(&p).ok())
        });

        let _ = std::fs::write(dir.join("page.html"), &html);
        if let Ok(v) = page.evaluate(challenge::PROBE_JS).await
            && let Ok(probe) = v.into_value::<String>()
        {
            let _ = std::fs::write(dir.join("widget-probe.json"), probe);
        }
        let summary = format!(
            "reason: {reason}\nurl: {url}\ntitle: {title}\nhtml_bytes: {}\nclicks: {clicks}\nelapsed_ms: {}\nartifacts: {}",
            html.len(),
            elapsed.as_millis(),
            dir.display(),
        );
        let _ = std::fs::write(dir.join("summary.txt"), &summary);
        tracing::warn!(
            browser = self.id, %url, title = %title, html_bytes = html.len(), clicks,
            artifacts = %dir.display(),
            "solve abandoned — diagnostics saved (framebuffer may show a loaded page the loop never classified cleared)"
        );
        self.observer.failed(
            self.id,
            framebuffer.or(cdp),
            summary,
            elapsed.as_millis() as u64,
        );
    }

    pub async fn close(self) {
        self.observer.deregister(self.id);
        self.session.close().await;
    }
}

async fn page_title(page: &Page) -> String {
    page.evaluate("document.title")
        .await
        .ok()
        .and_then(|v| v.into_value::<String>().ok())
        .unwrap_or_default()
}

async fn launch_session(
    cfg: &SolveConfig,
    data_dir: &Path,
    proxy: Option<&str>,
    display: Option<&str>,
    native_display: bool,
) -> Result<Session> {
    Session::launch(LaunchOpts {
        display,
        native_display,
        data_dir,
        proxy,
        width: cfg.width,
        height: cfg.height,
    })
    .await
}
