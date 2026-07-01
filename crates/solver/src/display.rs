use anyhow::{Context, Result, anyhow, bail};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};

pub struct VirtualDisplay {
    num: u32,
    child: Child,
}

impl VirtualDisplay {
    pub fn start(width: u32, height: u32) -> Result<Self> {
        let mut child = Command::new("Xvfb")
            .arg("-displayfd")
            .arg("1")
            .arg("-screen")
            .arg("0")
            .arg(format!("{width}x{height}x24"))
            .arg("-nolisten")
            .arg("tcp")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    anyhow!("Xvfb not found — install it: `sudo apt install -y xvfb`")
                } else {
                    anyhow!("spawning Xvfb: {e}")
                }
            })?;

        let stdout = child.stdout.take().expect("piped stdout");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .context("reading Xvfb display number")?;
        let Ok(num) = line.trim().parse::<u32>() else {
            let mut err = String::new();
            if let Some(mut e) = child.stderr.take() {
                use std::io::Read;
                let _ = e.read_to_string(&mut err);
            }
            let _ = child.kill();
            bail!("Xvfb failed to start: {}", err.trim());
        };
        Ok(VirtualDisplay { num, child })
    }

    pub fn name(&self) -> String {
        format!(":{}", self.num)
    }

    pub fn screenshot(&self, path: &Path) -> Result<()> {
        crate::frame::screenshot(&self.name(), path)
    }
}

impl Drop for VirtualDisplay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
