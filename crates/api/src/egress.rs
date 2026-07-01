//! The leasing surface over the exit pool. "Egress" is the way out to the origin; there is
//! exactly one implementation — [`ExitPool`](crate::pool::ExitPool) — and *direct* (no
//! proxy) is just a pool of one always-ready exit. An worker takes an exclusive [`Lease`],
//! works it, and hands it back with an [`ExitStatus`].

use std::sync::Arc;
use std::time::Duration;

use crate::pool::ExitPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Available,
    Resting(Option<Duration>),
}

/// The verdict a worker hands back when it returns a lease. `Ok` = the exit is clean and
/// immediately reusable; `Cooled` = it was just penalized and a cooldown is already recorded
/// in its data (so it stays unleasable until the cooldown expires); `Dead` = it's broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitStatus {
    Ok,
    Cooled,
    Dead,
}

/// An exclusive hold on one exit. Dropping it without an explicit [`Lease::release`] returns
/// the exit healthy (`ExitStatus::Ok`).
pub struct Lease {
    url: Option<String>,
    code: String,
    pool: Arc<ExitPool>,
    returned: bool,
}

impl Lease {
    pub(crate) fn new(url: Option<String>, code: String, pool: Arc<ExitPool>) -> Lease {
        Lease {
            url,
            code,
            pool,
            returned: false,
        }
    }

    /// The proxy URL, or `None` for the direct exit.
    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    /// The store/exit key: the proxy URL, or `""` for direct.
    pub fn key(&self) -> String {
        self.url.clone().unwrap_or_default()
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn release(mut self, status: ExitStatus) {
        self.pool.return_lease(&self.code, status);
        self.returned = true;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if !self.returned {
            self.pool.return_lease(&self.code, ExitStatus::Ok);
        }
    }
}
