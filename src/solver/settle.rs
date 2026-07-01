use anyhow::Result;
use chromiumoxide::Page;
use std::time::Duration;

const COUNTER_PRESCRIPT: &str = r#"
window.__maraPending = 0;
(function() {
    const origFetch = window.fetch;
    if (origFetch) {
        window.fetch = function(...args) {
            window.__maraPending++;
            return origFetch.apply(this, args).finally(() => {
                window.__maraPending = Math.max(0, window.__maraPending - 1);
            });
        };
    }
    const origSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function(...args) {
        window.__maraPending++;
        this.addEventListener('loadend', () => {
            window.__maraPending = Math.max(0, window.__maraPending - 1);
        }, { once: true });
        return origSend.apply(this, args);
    };
})();
"#;

const SETTLE_JS: &str = r#"new Promise((resolve) => {
    const DEADLINE_MS = 12000;
    const start = performance.now();
    const idle = () => (window.__maraPending || 0) === 0;

    const finish = (reason) => {
        document.documentElement.setAttribute('data-mara-settle', reason);
        resolve(document.documentElement.outerHTML);
    };

    const steps = 5;
    let step = 0;
    const scroll = () => {
        if (step <= steps) {
            window.scrollTo(0, (document.body.scrollHeight * step) / steps);
            step++;
            setTimeout(scroll, 120);
        } else {
            settle();
        }
    };

    const settle = async () => {
        const deadline = performance.now() + DEADLINE_MS;
        while (performance.now() < deadline) {
            await new Promise(r => requestAnimationFrame(r));
            if (!idle()) continue;
            await new Promise(r => requestAnimationFrame(r));
            if (idle()) { finish('idle'); return; }
        }
        finish('deadline');
    };

    scroll();
})"#;

async fn install_counter(page: &Page) -> Result<()> {
    page.evaluate(COUNTER_PRESCRIPT).await?;
    Ok(())
}

/// Wait for a headed [`Page`] to finish loading — network to settle plus lazy content triggered by
/// scrolling — bounded by `budget`, then return its HTML. Pair with
/// [`Client::fetch_browser`](crate::Client::fetch_browser) when you need the fully-rendered page.
pub async fn wait_full_load(page: &Page, budget: Duration) -> String {
    let work = async {
        let _ = install_counter(page).await;
        page.evaluate(SETTLE_JS).await
    };
    match tokio::time::timeout(budget, work).await {
        Ok(Ok(v)) => v.into_value::<String>().unwrap_or_default(),
        _ => page.content().await.unwrap_or_default(),
    }
}
