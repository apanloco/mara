use crate::classify::Reason;
use crate::egress::ExitStatus;

/// The fetch step a failure happened at. Used by the worker's `penalize` to tell a slim-stage
/// challenge (a stale cookie → drop it) from a headed-stage one (a burn).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Slim,
    Headed,
}

/// What to do after a failure on the **slim** step. Only the slim step can `Escalate` (a
/// challenge lifts to a headed solve); the other two mirror [`HeadedAction`]. Test-only: the live
/// slim path routes failures through the worker's `penalize`/`record_challenge`, not this enum,
/// which survives to pin the routing table in the ladder's unit tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlimAction {
    Escalate,
    Rotate(ExitStatus),
    Fail(ExitStatus),
}

/// What to do after a failure on the **headed** step. There is no escalation past a headed
/// solve, so this is total with just rotate/fail — the worker never has to handle (and
/// `unreachable!`-away) an impossible `Escalate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadedAction {
    Rotate(ExitStatus),
    Fail(ExitStatus),
}

#[cfg(test)]
impl From<HeadedAction> for SlimAction {
    fn from(action: HeadedAction) -> SlimAction {
        match action {
            HeadedAction::Rotate(status) => SlimAction::Rotate(status),
            HeadedAction::Fail(status) => SlimAction::Fail(status),
        }
    }
}

/// Route a slim-step failure. A challenge escalates to a headed solve; every other reason
/// routes exactly as it would on the headed step. This is *pure routing*: the exit side
/// effect (drop a stale clearance, start the right cooldown) is applied separately by the
/// worker's `penalize`, so cooldown durations live in one place (`Policy`) — never here.
#[cfg(test)]
pub fn decide_slim(reason: Reason, attempts_left: u32) -> SlimAction {
    match reason {
        Reason::Challenged => SlimAction::Escalate,
        _ => decide_headed(reason, attempts_left).into(),
    }
}

/// The four ways a **solve-host slim challenge** resolves. The worker applies the side effects
/// (drop the stale cookie, bench the exit); this is the pure routing that pins the outcome — and,
/// crucially, guarantees `fetch_all` terminates: every path ends in a give-up or `Escalate`, and the
/// only looping path (`RetrySlim`) decrements the rotation budget each time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChallengeAction {
    /// slim has *never once* served → the fingerprint triple is the prime suspect (not a stray
    /// page). Give up so the misconfiguration surfaces rather than browsering every request.
    GiveUp,
    /// A stale/loaded cookie — the winnable case. Re-warm and retry slim on a fresh clearance.
    RetrySlim,
    /// Either a **fresh** cookie was rejected (a per-URL CF challenge a cookie can't satisfy) or the
    /// retry budget is spent — fetch the URL in a real browser instead.
    Escalate,
    /// The **domain itself** — not this one exit — is confirmed structurally misconfigured (solve
    /// keeps landing on a different host whose clearance never validates against the configured
    /// one). Retrying just repeats the same failure on every exit, and escalating would "succeed" by
    /// browsering every request forever without ever telling anyone. Give up immediately, ahead of
    /// the rotation budget — continuing to spend it here can't change the outcome.
    GiveUpMisconfigured,
}

/// Route a solve-host slim challenge. A domain confirmed structurally misconfigured (see
/// [`ChallengeAction::GiveUpMisconfigured`]) short-circuits everything else — no amount of
/// retrying or escalating fixes a redirect to the wrong host. Otherwise, re-warm and retry slim
/// while the rotation budget lasts — a *transient* challenge (a stale cookie, a one-off CF hiccup)
/// clears on the next fresh clearance and never escalates. Only a challenge that survives the
/// *whole* budget is genuinely per-URL-hard: then either **escalate** to a headed fetch (when the
/// fingerprint triple is trustworthy — the Chrome↔TLS pin matches, or slim has already served) or
/// **give up** `FingerprintMismatch` (a broken triple: pin mismatched *and* slim never served, so
/// browsering every request would only mask the misconfig). Retrying before escalating is what
/// keeps escalations rare — only the truly-unclearable URLs draw a browser, so they never flood the
/// scarce solve budget the maintainer needs for warming.
pub fn decide_challenge(
    escalate_allowed: bool,
    attempts_left: u32,
    domain_misconfigured: bool,
) -> ChallengeAction {
    if domain_misconfigured {
        ChallengeAction::GiveUpMisconfigured
    } else if attempts_left > 0 {
        ChallengeAction::RetrySlim
    } else if escalate_allowed {
        ChallengeAction::Escalate
    } else {
        ChallengeAction::GiveUp
    }
}

/// Route a headed-step failure: an unreachable exit is dead, everything else cools. Rotate
/// while attempts remain, otherwise fail — keeping the status either way.
pub fn decide_headed(reason: Reason, attempts_left: u32) -> HeadedAction {
    let status = match reason {
        Reason::Unreachable => ExitStatus::Dead,
        _ => ExitStatus::Cooled,
    };
    if attempts_left > 0 {
        HeadedAction::Rotate(status)
    } else {
        HeadedAction::Fail(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slim_challenge_escalates_headed_challenge_rotates() {
        assert_eq!(decide_slim(Reason::Challenged, 3), SlimAction::Escalate);
        assert_eq!(
            decide_headed(Reason::Challenged, 3),
            HeadedAction::Rotate(ExitStatus::Cooled)
        );
    }

    #[test]
    fn unreachable_marks_exit_dead() {
        assert_eq!(
            decide_slim(Reason::Unreachable, 3),
            SlimAction::Rotate(ExitStatus::Dead)
        );
        assert_eq!(
            decide_headed(Reason::Unreachable, 3),
            HeadedAction::Rotate(ExitStatus::Dead)
        );
    }

    #[test]
    fn cooling_reasons_rotate_while_attempts_remain() {
        for r in [
            Reason::RateLimited,
            Reason::Blocked,
            Reason::Timeout,
            Reason::Unavailable,
        ] {
            assert_eq!(
                decide_slim(r, 3),
                SlimAction::Rotate(ExitStatus::Cooled),
                "{r:?}"
            );
            assert_eq!(
                decide_headed(r, 3),
                HeadedAction::Rotate(ExitStatus::Cooled),
                "{r:?}"
            );
        }
    }

    #[test]
    fn challenge_retries_slim_while_budget_lasts() {
        // A transient challenge clears on a slim retry, so we always retry before escalating —
        // regardless of whether escalation would eventually be allowed.
        assert_eq!(decide_challenge(true, 3, false), ChallengeAction::RetrySlim);
        assert_eq!(
            decide_challenge(false, 3, false),
            ChallengeAction::RetrySlim
        );
    }

    #[test]
    fn budget_exhausted_escalates_when_allowed_else_gives_up() {
        // Survived the whole budget → genuinely per-URL-hard: escalate to a browser when the
        // fingerprint's trustworthy…
        assert_eq!(decide_challenge(true, 0, false), ChallengeAction::Escalate);
        // …else give up FingerprintMismatch (broken triple — don't browser every request).
        assert_eq!(decide_challenge(false, 0, false), ChallengeAction::GiveUp);
    }

    #[test]
    fn misconfigured_domain_gives_up_immediately_regardless_of_budget_or_fingerprint() {
        // A confirmed-misconfigured domain short-circuits everything else — plenty of budget left
        // and a trustworthy fingerprint don't matter, because retrying/escalating can't fix a
        // redirect to the wrong host.
        assert_eq!(
            decide_challenge(true, 3, true),
            ChallengeAction::GiveUpMisconfigured
        );
        assert_eq!(
            decide_challenge(false, 0, true),
            ChallengeAction::GiveUpMisconfigured
        );
    }

    #[test]
    fn no_attempts_left_fails_but_keeps_the_status() {
        assert_eq!(
            decide_slim(Reason::Blocked, 0),
            SlimAction::Fail(ExitStatus::Cooled)
        );
        assert_eq!(
            decide_slim(Reason::Unreachable, 0),
            SlimAction::Fail(ExitStatus::Dead)
        );
        assert_eq!(
            decide_headed(Reason::Blocked, 0),
            HeadedAction::Fail(ExitStatus::Cooled)
        );
    }
}
