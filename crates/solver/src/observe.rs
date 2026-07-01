pub type BrowserId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Idle,
    Navigating,
    Challenge,
    Verifying,
    Cleared,
    Blocked,
    Failed,
}

pub trait Observer: Send + Sync {
    fn register(&self, display: Option<String>) -> BrowserId;
    fn phase(&self, id: BrowserId, phase: Phase);
    fn navigating(&self, id: BrowserId, url: &str);
    fn set_user_agent(&self, id: BrowserId, ua: String);
    fn event(&self, msg: String);
    fn clicked(&self, id: BrowserId, x: i32, y: i32);
    /// A solve gave up. Hands over the last frame (CDP screenshot or Xvfb framebuffer), a text
    /// summary, and how long the solve actually ran (`elapsed_ms`) so the observer can retain a
    /// frozen, inspectable record of the failure — the browser itself is torn down right after.
    /// `screenshot` is `None` only if every capture failed.
    fn failed(&self, id: BrowserId, screenshot: Option<Vec<u8>>, summary: String, elapsed_ms: u64);
    fn deregister(&self, id: BrowserId);
}
