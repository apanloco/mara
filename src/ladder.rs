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
