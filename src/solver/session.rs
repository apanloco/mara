use anyhow::{Context, Result, anyhow};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use futures::StreamExt;
use std::path::Path;
use tokio::task::JoinHandle;

pub struct LaunchOpts<'a> {
    pub display: Option<&'a str>,
    pub native_display: bool,
    pub data_dir: &'a Path,
    pub proxy: Option<&'a str>,
    pub width: u32,
    pub height: u32,
}

pub struct Session {
    pub browser: Browser,
    handler_task: JoinHandle<()>,
}

impl Session {
    pub async fn launch(opts: LaunchOpts<'_>) -> Result<Self> {
        std::fs::create_dir_all(opts.data_dir)
            .with_context(|| format!("creating chromium data dir {}", opts.data_dir.display()))?;

        let mut builder = BrowserConfig::builder()
            .viewport(Viewport {
                width: opts.width,
                height: opts.height,
                ..Default::default()
            })
            .window_size(opts.width, opts.height)
            .arg("disable-blink-features=AutomationControlled")
            .arg("no-first-run")
            .arg("no-default-browser-check")
            .arg("disable-crash-reporter")
            .user_data_dir(opts.data_dir);

        if let Ok(path) = std::env::var("CHROME") {
            builder = builder.chrome_executable(path);
        }

        if opts.native_display {
            builder = builder.with_head();
        } else {
            let display = opts.display.context("headed Xvfb launch needs a display")?;
            builder = builder
                .with_head()
                .arg("ozone-platform=x11")
                .arg("use-gl=angle")
                .arg("use-angle=gl-egl")
                .env("DISPLAY", display)
                .env("WAYLAND_DISPLAY", "")
                .env("XDG_SESSION_TYPE", "x11");
        }

        if let Some(proxy) = opts.proxy {
            let arg = proxy
                .strip_prefix("socks5h://")
                .map(|rest| format!("socks5://{rest}"))
                .unwrap_or_else(|| proxy.to_string());
            builder = builder.arg(format!("proxy-server={arg}"));
        }

        let config = builder.build().map_err(|e| {
            anyhow!(
                "could not find Chrome ({e}); install Google Chrome or set CHROME=/path/to/chrome"
            )
        })?;
        let (browser, mut handler) = Browser::launch(config).await.context("launching Chrome")?;

        let handler_task = tokio::spawn(async move {
            while let Some(ev) = handler.next().await {
                if let Err(e) = ev {
                    tracing::debug!("chromiumoxide handler error: {e}");
                }
            }
        });

        Ok(Session {
            browser,
            handler_task,
        })
    }

    pub async fn close(mut self) {
        let _ = self.browser.close().await;
        let _ = self.browser.wait().await;
        self.handler_task.abort();
    }
}

pub fn locate_chrome() -> Option<(String, String)> {
    let from_env = std::env::var("CHROME").ok();
    let candidates = from_env.iter().map(String::as_str).chain([
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ]);
    for bin in candidates {
        if let Ok(out) = std::process::Command::new(bin).arg("--version").output()
            && out.status.success()
        {
            let line = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !line.is_empty() {
                return Some((bin.to_string(), line));
            }
        }
    }
    None
}

pub fn installed_chrome_major() -> Option<u32> {
    parse_chrome_major(&locate_chrome()?.1)
}

fn parse_chrome_major(version_line: &str) -> Option<u32> {
    version_line
        .split_whitespace()
        .find_map(|tok| tok.split('.').next()?.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::parse_chrome_major;

    #[test]
    fn parses_chrome_and_chromium_version_lines() {
        assert_eq!(parse_chrome_major("Google Chrome 147.0.7300.0"), Some(147));
        assert_eq!(parse_chrome_major("Chromium 148.0.1.2\n"), Some(148));
        assert_eq!(parse_chrome_major("no version here"), None);
    }
}
