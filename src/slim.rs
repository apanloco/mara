use std::time::Duration;

use wreq::Client;
use wreq_util::{Emulation, Profile};

use crate::classify::{self, Reason};
use crate::clearance::Clearance;

const EMULATION: Profile = Emulation::Chrome147;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub const PROFILE: &str = "Chrome147";

pub fn profile_major() -> Option<u32> {
    PROFILE.strip_prefix("Chrome")?.parse().ok()
}

/// The HTTP method a [`Resource`](crate::Resource) uses. A dependency-free mirror of the handful we
/// need, so the public fetch API doesn't leak `wreq`/`http` types. Defaults to `Get` (the HTML bulk path).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Method {
    /// `GET` — the default, used by the HTML bulk path.
    #[default]
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `PATCH`.
    Patch,
    /// `DELETE`.
    Delete,
    /// `HEAD`.
    Head,
}

impl Method {
    fn to_wreq(self) -> wreq::Method {
        match self {
            Method::Get => wreq::Method::GET,
            Method::Post => wreq::Method::POST,
            Method::Put => wreq::Method::PUT,
            Method::Patch => wreq::Method::PATCH,
            Method::Delete => wreq::Method::DELETE,
            Method::Head => wreq::Method::HEAD,
        }
    }
}

/// One slim HTTP request: a URL plus optional method/headers/body. A bare `GET` (the default) is
/// what the HTML bulk path uses; method/headers/body let non-HTML traffic — e.g. an Algolia POST —
/// ride the same exit pool. When a clearance is replayed, its UA + cookie headers are added on top
/// (and the host is rewritten to the clearance's host); custom `headers` never overwrite those.
#[derive(Clone, Debug, Default)]
pub struct Request {
    pub url: String,
    pub method: Method,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl Request {
    #[cfg(test)]
    pub fn get(url: impl Into<String>) -> Request {
        Request {
            url: url.into(),
            ..Default::default()
        }
    }
}

pub fn build_client(
    proxy: Option<&str>,
    timeout: Duration,
    connect_timeout: Duration,
) -> Result<Client, Reason> {
    let mut builder = Client::builder()
        .emulation(EMULATION)
        .redirect(wreq::redirect::Policy::limited(5))
        .timeout(timeout)
        .connect_timeout(connect_timeout);
    if let Some(p) = proxy {
        match wreq::Proxy::all(p) {
            Ok(px) => builder = builder.proxy(px),
            Err(e) => {
                tracing::warn!(proxy = %p, error = %e, "slim: unusable proxy — exit treated as unreachable");
                return Err(Reason::Unreachable);
            }
        }
    }
    builder.build().map_err(|e| {
        tracing::warn!(error = %e, "slim: failed to build client");
        Reason::Unavailable
    })
}

/// Send `req` (replaying `clearance` if present) and return the **raw body bytes**. Bytes are the
/// canonical wire form — text is an edge decode (see the client). Classification is unaffected: it
/// keys off the status, the `cf-mitigated` header, and a lossy-UTF-8 marker scan, and a CF
/// challenge/block page is always text — so a 200 binary body (an image) matches nothing and
/// passes through untouched.
pub async fn fetch(
    client: &Client,
    req: &Request,
    clearance: Option<&Clearance>,
) -> Result<Vec<u8>, Reason> {
    let target = match clearance {
        Some(c) if !c.host.is_empty() => rewrite_host(&req.url, &c.host),
        _ => req.url.clone(),
    };

    let mut r = client.request(req.method.to_wreq(), &target);
    if let Some(c) = clearance {
        r = r.header("user-agent", c.user_agent.clone());
        let cookies = c.cookie_header();
        if !cookies.is_empty() {
            r = r.header("cookie", cookies);
        }
    }
    for (k, v) in &req.headers {
        r = r.header(k.as_str(), v.clone());
    }
    if let Some(body) = &req.body {
        r = r.body(body.clone());
    }
    let resp = match r.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "slim transport error");
            return Err(classify::from_transport(&e.to_string()));
        }
    };

    let status = resp.status().as_u16();
    let mitigated = resp.headers().contains_key("cf-mitigated");
    let bytes = resp.bytes().await.unwrap_or_default();

    let sniff = String::from_utf8_lossy(&bytes);
    match classify::from_http(status, mitigated, &sniff) {
        None => Ok(bytes.to_vec()),
        Some(reason) => Err(reason),
    }
}

fn rewrite_host(url: &str, host: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            if u.set_host(Some(host)).is_ok() {
                u.to_string()
            } else {
                url.to_string()
            }
        }
        Err(_) => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    //! White-box tests of the slim client against a stdlib TCP mock: cookie/UA replay, redirect
    //! following, prompt timeout, client reuse, and that each obstacle classifies to the right
    //! [`Reason`](crate::classify::Reason). Lives here (not in `tests/`) because it drives the
    //! crate-internal `slim`/`classify`/`clearance` surface directly.
    use std::io::{Read, Write};
    use std::net::TcpListener;

    use super::{DEFAULT_CONNECT_TIMEOUT, DEFAULT_TIMEOUT, Request, build_client, fetch};
    use crate::classify::Reason;
    use crate::clearance::Clearance;

    fn spawn_mock() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                let mut buf = [0u8; 8192];
                let n = s.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");
                let resp = match path {
                    "/ratelimit" => http(
                        429,
                        &[],
                        "<title>Just a moment</title>Error 1015: you are being rate limited",
                    ),
                    "/block" => http(403, &[], "Sorry, you have been blocked — Error 1020"),
                    "/challenge" => http(
                        403,
                        &[("cf-mitigated", "challenge")],
                        "<title>Just a moment...</title>",
                    ),
                    "/redirect" => http(
                        301,
                        &[("location", "/")],
                        "<title>301 Moved Permanently</title>",
                    ),
                    "/slow" => {
                        std::thread::sleep(std::time::Duration::from_secs(3));
                        http(200, &[], "too late")
                    }
                    _ => {
                        let ua = header(&req, "user-agent");
                        let cookie = header(&req, "cookie");
                        http(200, &[], &format!("ok\nua={ua}\ncookie={cookie}"))
                    }
                };
                let _ = s.write_all(resp.as_bytes());
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    fn http(code: u16, headers: &[(&str, &str)], body: &str) -> String {
        let mut h = format!(
            "HTTP/1.1 {code} X\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (k, v) in headers {
            h.push_str(&format!("{k}: {v}\r\n"));
        }
        format!("{h}\r\n{body}")
    }

    fn header(req: &str, name: &str) -> String {
        req.lines()
            .find(|l| l.to_lowercase().starts_with(&format!("{name}:")))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v.trim().to_string())
            .unwrap_or_default()
    }

    async fn get(url: &str, clearance: Option<&Clearance>) -> Result<String, Reason> {
        let client = build_client(None, DEFAULT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT).unwrap();
        fetch(&client, &Request::get(url), clearance)
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    fn clearance() -> Clearance {
        Clearance::new(
            vec![("cf_clearance".into(), "TOKEN123".into())],
            "Mozilla/5.0 TEST Chrome/147.0.0.0".into(),
            None,
            String::new(),
        )
    }

    #[tokio::test]
    async fn success_replays_cookie_and_ua() {
        let base = spawn_mock();
        let c = clearance();
        match get(&base, Some(&c)).await {
            Ok(body) => {
                assert!(
                    body.contains("ua=Mozilla/5.0 TEST Chrome/147.0.0.0"),
                    "pinned UA must be sent: {body}"
                );
                assert!(
                    body.contains("cookie=cf_clearance=TOKEN123"),
                    "clearance cookie must be sent: {body}"
                );
            }
            Err(r) => panic!("expected the origin, got {r:?}"),
        }
    }

    #[tokio::test]
    async fn rate_limit_classified() {
        let base = spawn_mock();
        let r = get(&format!("{base}/ratelimit"), Some(&clearance())).await;
        assert!(matches!(r, Err(Reason::RateLimited)));
    }

    #[tokio::test]
    async fn block_classified() {
        let base = spawn_mock();
        let r = get(&format!("{base}/block"), Some(&clearance())).await;
        assert!(matches!(r, Err(Reason::Blocked)));
    }

    #[tokio::test]
    async fn challenge_classified() {
        let base = spawn_mock();
        let r = get(&format!("{base}/challenge"), Some(&clearance())).await;
        assert!(matches!(r, Err(Reason::Challenged)));
    }

    #[tokio::test]
    async fn follows_redirect_to_the_real_page() {
        let base = spawn_mock();
        match get(&format!("{base}/redirect"), Some(&clearance())).await {
            Ok(body) => assert!(
                body.starts_with("ok"),
                "should follow to the origin, got: {body}"
            ),
            Err(r) => panic!("expected the origin after following the redirect, got {r:?}"),
        }
    }

    #[tokio::test]
    async fn cold_attempt_sends_no_cookie() {
        let base = spawn_mock();
        match get(&base, None).await {
            Ok(body) => assert!(
                body.contains("cookie=\n") || body.ends_with("cookie="),
                "no cookie when cold: {body}"
            ),
            Err(r) => panic!("expected the origin, got {r:?}"),
        }
    }

    #[tokio::test]
    async fn one_client_serves_many_requests() {
        let base = spawn_mock();
        let client = build_client(None, DEFAULT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT).unwrap();
        for _ in 0..2 {
            assert!(
                fetch(&client, &Request::get(&base), None).await.is_ok(),
                "a reused client must keep serving"
            );
        }
    }

    #[tokio::test]
    async fn slow_exit_times_out_promptly() {
        let base = spawn_mock();
        let started = std::time::Instant::now();
        let client = build_client(
            None,
            std::time::Duration::from_millis(150),
            DEFAULT_CONNECT_TIMEOUT,
        )
        .unwrap();
        let r = fetch(&client, &Request::get(format!("{base}/slow")), None).await;
        assert!(
            matches!(r, Err(Reason::Timeout)),
            "a slow response must surface as Timeout, got {r:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "must time out, not wait out the slow server"
        );
    }
}
