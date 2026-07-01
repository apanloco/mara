use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

pub async fn sample_ram(stop: Arc<AtomicBool>, peak_kb: Arc<AtomicUsize>) {
    while !stop.load(Ordering::Relaxed) {
        if let Some(kb) = read_rss_kb().await {
            peak_kb.fetch_max(kb, Ordering::Relaxed);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn read_rss_kb() -> Option<usize> {
    let out = tokio::process::Command::new("ps")
        .args(["-eo", "pid=,ppid=,rss="])
        .output()
        .await
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut rss = std::collections::HashMap::new();
    let mut children: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(pid), Some(ppid), Some(r)) = (it.next(), it.next(), it.next()) else {
            continue;
        };
        let (Ok(pid), Ok(ppid), Ok(r)) =
            (pid.parse::<u32>(), ppid.parse::<u32>(), r.parse::<usize>())
        else {
            continue;
        };
        rss.insert(pid, r);
        children.entry(ppid).or_default().push(pid);
    }

    let mut total = 0;
    let mut stack = vec![std::process::id()];
    while let Some(pid) = stack.pop() {
        total += rss.get(&pid).copied().unwrap_or(0);
        if let Some(kids) = children.get(&pid) {
            stack.extend(kids);
        }
    }
    Some(total)
}
