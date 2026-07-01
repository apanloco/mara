use mara::{Client, Config};

const TARGET: &str = "https://fragrantica.com/search/";

fn init_logs() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,mara=debug,chromiumoxide=off,wreq=off"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: needs Chrome+Xvfb; run with `cargo test-live`"]
async fn earn_headed_then_spend_slim() {
    init_logs();
    let client = Client::new(Config::default()).await.expect("client");

    let first = client.fetch_http(TARGET).await.expect("first fetch");
    assert!(!first.value.is_empty(), "first fetch returned a page");

    let second = client.fetch_http(TARGET).await.expect("second fetch");
    let preview: String = second.value.chars().take(300).collect();

    let _ = &first;
    assert!(
        !second.solve_required,
        "second fetch should be served browser-free once the exit is warm"
    );

    assert!(
        second.value.len() > 2000,
        "body is only {} bytes — looks like a redirect/stub, not the page:\n{preview}",
        second.value.len(),
    );

    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: needs Chrome+Xvfb+Mullvad; run with `cargo test-live`"]
async fn mullvad_burst_spreads_and_clears() {
    init_logs();
    let client = Client::new(Config {
        browsers: 4,
        mullvad: true,
        max_latency: Some(std::time::Duration::from_millis(800)),
        ..Default::default()
    })
    .await
    .expect("client (is a Mullvad tunnel up?)");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            c.fetch_http(TARGET)
                .await
                .map(|o| (o.solve_required, o.value.len()))
        }));
    }
    let mut ok = 0;
    for t in tasks {
        if let Ok(Ok((_solved, bytes))) = t.await
            && bytes > 2000
        {
            ok += 1;
        }
    }
    eprintln!("burst: {ok}/8 cleared with a real page");
    assert!(ok > 0, "at least one of the burst should clear");
    client.shutdown().await;
}
