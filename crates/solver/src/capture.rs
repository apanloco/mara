use crate::frame;
use crate::locate;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const INTERVAL: Duration = Duration::from_millis(200);

pub struct Grabber {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Grabber {
    pub fn start(display: String, browser: u32, dir: PathBuf) -> Grabber {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let _ = std::fs::create_dir_all(&dir);
            let mut seq = 0u32;
            while !flag.load(Ordering::Relaxed) {
                if let Ok(img) = frame::grab_image(&display) {
                    let verdict = match locate::find_target(&img) {
                        Some(t) if t.precise => format!("precise_x{}_y{}", t.x, t.y),
                        Some(_) => "estimate".into(),
                        None => "none".into(),
                    };
                    if let Ok(png) = img.to_png() {
                        let _ = std::fs::write(
                            dir.join(format!("b{browser}-{seq:04}-{verdict}.png")),
                            png,
                        );
                    }
                    seq += 1;
                }
                std::thread::sleep(INTERVAL);
            }
        });
        Grabber {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Grabber {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
