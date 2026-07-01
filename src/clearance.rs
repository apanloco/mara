use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const FALLBACK_TTL_SECS: f64 = 25.0 * 60.0;

#[derive(Clone, Serialize, Deserialize)]
pub struct Clearance {
    cookies: Vec<(String, String)>,
    pub user_agent: String,
    pub captured_unix: f64,
    pub expires_unix: Option<f64>,
    pub host: String,
}

impl Clearance {
    pub fn new(
        cookies: Vec<(String, String)>,
        user_agent: String,
        expires_unix: Option<f64>,
        host: String,
    ) -> Clearance {
        Clearance {
            cookies,
            user_agent,
            captured_unix: now_unix(),
            expires_unix,
            host,
        }
    }

    pub fn cookie_header(&self) -> String {
        self.cookies
            .iter()
            .map(|(n, v)| format!("{n}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub fn is_stale(&self) -> bool {
        let now = now_unix();
        match self.expires_unix {
            Some(exp) => now >= exp,
            None => now - self.captured_unix > FALLBACK_TTL_SECS,
        }
    }

    pub fn age_secs(&self) -> u64 {
        (now_unix() - self.captured_unix).max(0.0) as u64
    }
}

pub fn now_unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed(cookies: &[(&str, &str)]) -> Clearance {
        Clearance {
            cookies: cookies
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
            user_agent: "Mozilla/5.0 … Chrome/147.0.0.0 Safari/537.36".into(),
            captured_unix: now_unix(),
            expires_unix: None,
            host: String::new(),
        }
    }

    #[test]
    fn cookie_header_joins_with_semicolons() {
        let c = fixed(&[("cf_clearance", "abc"), ("__cf_bm", "xyz")]);
        assert_eq!(c.cookie_header(), "cf_clearance=abc; __cf_bm=xyz");
    }

    #[test]
    fn single_cookie_has_no_separator() {
        assert_eq!(
            fixed(&[("cf_clearance", "abc")]).cookie_header(),
            "cf_clearance=abc"
        );
    }

    #[test]
    fn expiry_drives_staleness() {
        let mut c = fixed(&[("cf_clearance", "x")]);
        c.expires_unix = Some(now_unix() - 1.0);
        assert!(c.is_stale());
        c.expires_unix = Some(now_unix() + 3600.0);
        assert!(!c.is_stale());
    }
}
