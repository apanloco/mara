pub mod classify;
pub mod clearance;

/// The host component of a URL, or `None` if it doesn't parse or has no host. Shared by the
/// browser-free actor and the solver so both derive a clearance's host the same way.
pub fn host_of(url: &str) -> Option<String> {
    url::Url::parse(url).ok()?.host_str().map(str::to_string)
}
