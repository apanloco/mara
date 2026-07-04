use anyhow::{Context, Result, anyhow};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::handler::viewport::Viewport;
use futures::StreamExt;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

pub struct LaunchOpts<'a> {
    pub display: Option<&'a str>,
    pub native_display: bool,
    pub data_dir: &'a Path,
    pub proxy: Option<&'a str>,
    pub width: u32,
    pub height: u32,
    /// The executable chromiumoxide launches — normally the `setpriv --pdeathsig` wrapper (see
    /// [`ChromeExec`]) so Chrome dies with us; the real Chrome binary when `setpriv` is absent.
    pub chrome: &'a Path,
}

/// The Chrome executable to launch, resolved once per client. When `setpriv` (util-linux) is present
/// we launch Chrome through a tiny `sh` wrapper that sets **`PR_SET_PDEATHSIG(SIGKILL)`** before
/// exec'ing it — so the kernel kills Chrome the instant our process dies, for *any* reason (SIGKILL,
/// panic, or a library consumer that never runs `shutdown`/`abort` on a signal). That's the one
/// guarantee `kill_on_drop` and signal handlers can't give (both need our code to run). The wrapper
/// is a per-instance temp file (PID plus a counter, since one process can hold more than one
/// `ChromeExec` — e.g. two `Client`s — and each must own a file the others can't delete out from
/// under it) removed on a clean exit by this guard's `Drop`; a leaked ~60-byte script on `kill -9`
/// is harmless. With no `setpriv`, we launch Chrome directly (no guarantee) and warn. chromiumoxide
/// 0.9 exposes no `pre_exec` hook, so wrapping the executable is how we get it.
pub struct ChromeExec {
    path: PathBuf,
    wrapper: Option<PathBuf>,
}

impl ChromeExec {
    /// Resolve the executable to launch from an already-run [`locate_chrome`] probe (`None` if it
    /// found nothing). Takes the probe rather than running its own, so a caller that also needs it
    /// elsewhere (the startup fingerprint canary) can share one `locate_chrome` instead of shelling
    /// out twice.
    pub fn resolve(located: Option<(String, String)>) -> ChromeExec {
        let chrome = std::env::var("CHROME")
            .ok()
            .or_else(|| located.map(|(bin, _)| bin))
            .unwrap_or_else(|| "google-chrome".to_string());
        match write_pdeathsig_wrapper(&chrome) {
            Some(wrapper) => ChromeExec {
                path: wrapper.clone(),
                wrapper: Some(wrapper),
            },
            None => {
                tracing::warn!(
                    "setpriv (util-linux) not found — launching Chrome without PR_SET_PDEATHSIG; a \
                     hard-killed process can orphan Chrome. Install util-linux to guarantee cleanup."
                );
                ChromeExec {
                    path: PathBuf::from(chrome),
                    wrapper: None,
                }
            }
        }
    }

    /// A direct, unwrapped executable — for tests, which drive the loop without a real browser.
    #[cfg(test)]
    pub fn direct(path: impl Into<PathBuf>) -> ChromeExec {
        ChromeExec {
            path: path.into(),
            wrapper: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ChromeExec {
    fn drop(&mut self) {
        if let Some(w) = &self.wrapper {
            let _ = std::fs::remove_file(w);
        }
    }
}

fn setpriv_available() -> bool {
    std::process::Command::new("setpriv")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Write the `sh` wrapper that exec's Chrome under `setpriv --pdeathsig SIGKILL`. `None` if `setpriv`
/// is unavailable or the file can't be written (caller falls back to launching Chrome directly).
fn write_pdeathsig_wrapper(chrome: &str) -> Option<PathBuf> {
    if !setpriv_available() {
        return None;
    }
    // PID alone isn't unique within a process: two `ChromeExec`s here (e.g. two `Client`s) would
    // resolve to the same path, and whichever drops first deletes the file the other still needs.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "mara-chrome-pdeathsig-{}-{n}.sh",
        std::process::id()
    ));
    // `exec` chains sh → setpriv → chrome in one PID, so chromiumoxide's child *is* Chrome (its
    // `kill_on_drop` still targets Chrome) and setpriv's PDEATHSIG binds Chrome to our process.
    let script = format!(
        "#!/bin/sh\nexec setpriv --pdeathsig SIGKILL {} \"$@\"\n",
        shell_single_quote(chrome)
    );
    create_executable(&path, &script).ok()?;
    Some(path)
}

/// Create `path` fresh and write `script` into it as an executable file. `create_new` is
/// `O_CREAT|O_EXCL`: it fails if `path` already exists as anything, including a symlink, so a
/// symlink planted at a predictable temp path can never be followed. Permissions are set on the
/// open fd (`fchmod`), not the path, so a symlink swapped in after `open` succeeded can't be
/// followed by the chmod either.
fn create_executable(path: &Path, script: &str) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(script.as_bytes())?;
    file.set_permissions(std::fs::Permissions::from_mode(0o755))
}

/// Single-quote a path for safe inclusion in the `sh` wrapper (handles spaces; escapes any quote).
fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
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

        // Always launch the resolved executable (the PDEATHSIG wrapper, or Chrome directly) rather
        // than chromiumoxide's own PATH search — this both binds Chrome's lifetime to ours and pins
        // exactly which binary runs (no silent fallback to a different Chrome/Chromium on PATH).
        builder = builder.chrome_executable(opts.chrome);

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
    chrome_major_from(&locate_chrome())
}

/// Like [`installed_chrome_major`], but from an already-run `locate_chrome` probe — for a caller
/// (the startup fingerprint canary) that needs to reuse the probe rather than repeat it.
pub(crate) fn chrome_major_from(located: &Option<(String, String)>) -> Option<u32> {
    parse_chrome_major(&located.as_ref()?.1)
}

fn parse_chrome_major(version_line: &str) -> Option<u32> {
    version_line
        .split_whitespace()
        .find_map(|tok| tok.split('.').next()?.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::{ChromeExec, create_executable, parse_chrome_major};

    #[test]
    fn parses_chrome_and_chromium_version_lines() {
        assert_eq!(parse_chrome_major("Google Chrome 147.0.7300.0"), Some(147));
        assert_eq!(parse_chrome_major("Chromium 148.0.1.2\n"), Some(148));
        assert_eq!(parse_chrome_major("no version here"), None);
    }

    #[test]
    fn two_resolves_in_one_process_dont_share_a_wrapper_file() {
        let a = ChromeExec::resolve(None);
        let b = ChromeExec::resolve(None);
        let Some(b_wrapper) = b.wrapper.clone() else {
            return; // no setpriv on this machine — nothing to disambiguate
        };
        assert_ne!(a.path(), b.path(), "same PID must not yield the same path");
        drop(a);
        assert!(
            b_wrapper.exists(),
            "dropping the first ChromeExec deleted the second's wrapper file"
        );
    }

    #[test]
    fn create_executable_refuses_to_follow_a_planted_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "untouched").unwrap();
        let planted = dir.path().join("planted.sh");
        std::os::unix::fs::symlink(&victim, &planted).unwrap();

        let result = create_executable(&planted, "#!/bin/sh\necho pwned\n");

        assert!(
            result.is_err(),
            "must refuse a path that already exists, symlink or not"
        );
        assert_eq!(
            std::fs::read_to_string(&victim).unwrap(),
            "untouched",
            "the symlink's target must never be written through"
        );
    }
}
