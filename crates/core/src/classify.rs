#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Challenged,
    RateLimited,
    Blocked,
    Unreachable,
    Timeout,
    Unavailable,
}

const BLOCK_MARKERS: [&str; 3] = ["error 1020", "you have been blocked", "attention required"];
const RATE_MARKERS: [&str; 2] = ["error 1015", "rate limited"];
const CHALLENGE_TITLE: &str = "just a moment";

pub fn from_http(status: u16, cf_mitigated: bool, body: &str) -> Option<Reason> {
    let lower = body.to_lowercase();
    if status == 429 || RATE_MARKERS.iter().any(|m| lower.contains(m)) {
        return Some(Reason::RateLimited);
    }
    if BLOCK_MARKERS.iter().any(|m| lower.contains(m)) {
        return Some(Reason::Blocked);
    }
    if cf_mitigated
        || status == 403
        || status == 503
        || lower.contains(&format!("<title>{CHALLENGE_TITLE}"))
    {
        return Some(Reason::Challenged);
    }
    if status >= 500 || (300..400).contains(&status) {
        return Some(Reason::Unavailable);
    }
    None
}

pub fn from_page(title: &str, html: &str) -> Option<Reason> {
    let t = title.to_lowercase();
    let h = html.to_lowercase();
    if BLOCK_MARKERS.iter().any(|m| t.contains(m) || h.contains(m)) {
        return Some(Reason::Blocked);
    }
    if RATE_MARKERS.iter().any(|m| t.contains(m) || h.contains(m)) {
        return Some(Reason::RateLimited);
    }
    if t.contains(CHALLENGE_TITLE) {
        return Some(Reason::Challenged);
    }
    None
}

pub fn from_transport(err: &str) -> Reason {
    let e = err.to_lowercase();
    if e.contains("timed out") || e.contains("timeout") {
        return Reason::Timeout;
    }
    const DEAD: [&str; 5] = [
        "connection refused",
        "connection reset",
        "dns error",
        "unreachable",
        "broken pipe",
    ];
    if DEAD.iter().any(|m| e.contains(m)) {
        Reason::Unreachable
    } else {
        Reason::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_origin_reached_on_200() {
        assert_eq!(from_http(200, false, "<title>Perfumes</title>"), None);
    }

    #[test]
    fn http_challenge_on_mitigated_and_403() {
        assert_eq!(from_http(403, true, ""), Some(Reason::Challenged));
        assert_eq!(from_http(503, false, ""), Some(Reason::Challenged));
        assert_eq!(
            from_http(200, false, "<title>Just a moment...</title>"),
            Some(Reason::Challenged)
        );
    }

    #[test]
    fn http_rate_limit_beats_challenge() {
        assert_eq!(
            from_http(429, true, "Error 1015"),
            Some(Reason::RateLimited)
        );
    }

    #[test]
    fn http_block() {
        assert_eq!(
            from_http(403, false, "Sorry, you have been blocked. Error 1020"),
            Some(Reason::Blocked)
        );
    }

    #[test]
    fn page_origin_reached_with_preloaded_challenge_js() {
        assert_eq!(
            from_page("Fragrantica", "<script src=/challenge-platform/x.js>"),
            None
        );
    }

    #[test]
    fn page_block_and_challenge() {
        assert_eq!(
            from_page("Attention Required! | Cloudflare", ""),
            Some(Reason::Blocked)
        );
        assert_eq!(from_page("Just a moment...", ""), Some(Reason::Challenged));
    }

    #[test]
    fn transport_timeout_dead_and_blip() {
        assert_eq!(
            from_transport("error sending request: operation timed out"),
            Reason::Timeout
        );
        assert_eq!(
            from_transport("error sending request: connection refused"),
            Reason::Unreachable
        );
        assert_eq!(
            from_transport("socks: connection reset by peer"),
            Reason::Unreachable
        );
        assert_eq!(from_transport("unexpected eof"), Reason::Unavailable);
    }
}
