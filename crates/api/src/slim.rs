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

/// The HTTP method a [`Request`] uses. A dependency-free mirror of the handful we need, so the
/// public fetch API doesn't leak `wreq`/`http` types. Defaults to `Get` (the HTML bulk path).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Method {
    #[default]
    Get,
    Post,
    Put,
    Patch,
    Delete,
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
