use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::slim;
use crate::solver::session;

/// The verdict of a single [`Check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Passed.
    Ok,
    /// Passed with a caveat worth noting.
    Warn,
    /// Failed — mara likely won't work until it's fixed.
    Fail,
}

/// One environment check with its verdict and (on trouble) a fix hint.
#[derive(Debug, Clone)]
pub struct Check {
    /// Short name of the check (e.g. `"chrome"`).
    pub name: &'static str,
    /// The verdict.
    pub status: Status,
    /// Human-readable detail — what was found.
    pub detail: String,
    /// A suggested fix, when the check didn't pass.
    pub hint: Option<String>,
}

/// The full set of startup checks from [`run`].
#[derive(Debug, Clone)]
pub struct Report {
    /// Every check performed, in order.
    pub checks: Vec<Check>,
}

impl Report {
    /// `true` unless any check [`Fail`](Status::Fail)ed (warnings don't count).
    pub fn ok(&self) -> bool {
        !self.checks.iter().any(|c| c.status == Status::Fail)
    }
}

/// Run all startup checks (Xvfb, Chrome, the fingerprint triple, GPU) and collect a [`Report`].
pub fn run() -> Report {
    Report {
        checks: vec![
            xvfb_check(),
            chrome_check(),
            fingerprint_check(),
            gpu_check(),
        ],
    }
}

fn xvfb_check() -> Check {
    match on_path("Xvfb") {
        Some(p) => ok("Xvfb", format!("found at {}", p.display())),
        None => fail(
            "Xvfb",
            "not found on PATH".into(),
            "install Xvfb — e.g. `apt install xvfb` (Debian/Ubuntu) or \
             `dnf install xorg-x11-server-Xvfb` (Fedora). The off-screen display is required.",
        ),
    }
}

fn chrome_check() -> Check {
    match session::locate_chrome() {
        Some((bin, version)) => ok("Chrome", format!("{version} (via `{bin}`)")),
        None => fail(
            "Chrome",
            "no google-chrome / chromium found".into(),
            "install Google Chrome, or point mara at it with CHROME=/path/to/chrome. \
             Don't use Chrome-for-Testing — it's a more detectable build.",
        ),
    }
}

fn fingerprint_check() -> Check {
    match (session::installed_chrome_major(), slim::profile_major()) {
        (Some(installed), Some(pinned)) if installed == pinned => ok(
            "Fingerprint pin",
            format!(
                "Chrome {installed} ↔ pinned slim profile {} (matched)",
                slim::PROFILE
            ),
        ),
        (Some(installed), Some(pinned)) => Check {
            name: "Fingerprint pin",
            status: Status::Fail,
            detail: format!(
                "installed Chrome {installed} ≠ pinned slim profile {pinned} ({})",
                slim::PROFILE
            ),
            hint: Some(format!(
                "slim clearance replay will be rejected on Cloudflare-challenged pages until you bump \
                 wreq/wreq-util AND slim::EMULATION/PROFILE to Chrome{installed}, in lockstep with the \
                 binary. The headed solver and the `fetch` lease are unaffected."
            )),
        },
        _ => Check {
            name: "Fingerprint pin",
            status: Status::Warn,
            detail: "skipped — Chrome version unreadable".into(),
            hint: Some(
                "install/repair Chrome so its `--version` is readable to verify the slim pin."
                    .into(),
            ),
        },
    }
}

fn gpu_check() -> Check {
    match render_node() {
        Some(node) => ok("GPU render node", format!("found at {}", node.display())),
        None => Check {
            name: "GPU render node",
            status: Status::Warn,
            detail: "no /dev/dri/renderD* found".into(),
            hint: Some(
                "the Xvfb path needs a GPU DRM render node for GPU-backed WebGL; without it WebGL is \
                 null — a bot tell that can stop pages clearing. Ensure /dev/dri/renderD* exists and \
                 this user is in the `render`/`video` group. (Not needed for --real-display.)"
                    .into(),
            ),
        },
    }
}

fn render_node() -> Option<PathBuf> {
    std::fs::read_dir("/dev/dri")
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|n| n.starts_with("renderD"))
        })
}

fn on_path(bin: &str) -> Option<PathBuf> {
    search_path(&std::env::var_os("PATH")?, bin)
}

fn search_path(path: &OsStr, bin: &str) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(bin))
        .find(|p| is_executable(p))
}

fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn ok(name: &'static str, detail: String) -> Check {
    Check {
        name,
        status: Status::Ok,
        detail,
        hint: None,
    }
}

fn fail(name: &'static str, detail: String, hint: &str) -> Check {
    Check {
        name,
        status: Status::Fail,
        detail,
        hint: Some(hint.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn report_ok_tolerates_warn_but_not_fail() {
        let warn = Check {
            name: "w",
            status: Status::Warn,
            detail: String::new(),
            hint: None,
        };
        let okc = Check {
            name: "o",
            status: Status::Ok,
            detail: String::new(),
            hint: None,
        };
        let failc = Check {
            name: "f",
            status: Status::Fail,
            detail: String::new(),
            hint: None,
        };
        assert!(
            Report {
                checks: vec![okc.clone(), warn.clone()]
            }
            .ok()
        );
        assert!(
            !Report {
                checks: vec![okc, warn, failc]
            }
            .ok()
        );
    }

    #[test]
    fn search_path_finds_executable_and_misses_absent() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("mara-doctor-fixture");
        fs::write(&bin, b"#!/bin/sh\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();

        let path = std::env::join_paths([dir.path()]).unwrap();
        assert_eq!(
            search_path(&path, "mara-doctor-fixture").as_deref(),
            Some(bin.as_path())
        );
        assert_eq!(search_path(&path, "nope-not-here"), None);
    }

    #[test]
    fn non_executable_is_not_on_path() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("data.txt");
        fs::write(&f, b"x").unwrap();
        let path = std::env::join_paths([dir.path()]).unwrap();
        assert_eq!(search_path(&path, "data.txt"), None);
    }

    #[test]
    fn run_is_total_and_covers_every_dependency() {
        let report = run();
        let names: Vec<_> = report.checks.iter().map(|c| c.name).collect();
        assert_eq!(
            names,
            ["Xvfb", "Chrome", "Fingerprint pin", "GPU render node"]
        );
    }
}
