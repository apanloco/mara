use std::io::{Read, Write};
use std::net::TcpListener;

use mara::classify::Reason;
use mara::clearance::Clearance;
use mara::slim;

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

async fn fetch(url: &str, clearance: Option<&Clearance>) -> Result<String, Reason> {
    let client =
        slim::build_client(None, slim::DEFAULT_TIMEOUT, slim::DEFAULT_CONNECT_TIMEOUT).unwrap();
    slim::fetch(&client, &slim::Request::get(url), clearance)
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
    match fetch(&base, Some(&c)).await {
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
    let r = fetch(&format!("{base}/ratelimit"), Some(&clearance())).await;
    assert!(matches!(r, Err(Reason::RateLimited)));
}

#[tokio::test]
async fn block_classified() {
    let base = spawn_mock();
    let r = fetch(&format!("{base}/block"), Some(&clearance())).await;
    assert!(matches!(r, Err(Reason::Blocked)));
}

#[tokio::test]
async fn challenge_classified() {
    let base = spawn_mock();
    let r = fetch(&format!("{base}/challenge"), Some(&clearance())).await;
    assert!(matches!(r, Err(Reason::Challenged)));
}

#[tokio::test]
async fn follows_redirect_to_the_real_page() {
    let base = spawn_mock();
    match fetch(&format!("{base}/redirect"), Some(&clearance())).await {
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
    match fetch(&base, None).await {
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
    let client =
        slim::build_client(None, slim::DEFAULT_TIMEOUT, slim::DEFAULT_CONNECT_TIMEOUT).unwrap();
    for _ in 0..2 {
        assert!(
            slim::fetch(&client, &slim::Request::get(&base), None)
                .await
                .is_ok(),
            "a reused client must keep serving"
        );
    }
}

#[tokio::test]
async fn slow_exit_times_out_promptly() {
    let base = spawn_mock();
    let started = std::time::Instant::now();
    let client = slim::build_client(
        None,
        std::time::Duration::from_millis(150),
        slim::DEFAULT_CONNECT_TIMEOUT,
    )
    .unwrap();
    let r = slim::fetch(&client, &slim::Request::get(format!("{base}/slow")), None).await;
    assert!(
        matches!(r, Err(Reason::Timeout)),
        "a slow response must surface as Timeout, got {r:?}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "must time out, not wait out the slow server"
    );
}
