//! Thin production-style gates over generated decision modules.
//! Call generated pure/actions — do not re-inline formulas.

use crate::gha_lifecycle;
use crate::log_groups;
use crate::rate_limit;
use crate::span_tree;
use crate::sync_bounds;
use crate::timing_clamp;
use crate::tui_reload;

/// Encode GitHub "completed with timestamp" (Go `status==completed && completedAt!=""`).
fn gha_has_completed_at(status: &str, completed_at: &str) -> bool {
    status == "completed" && !completed_at.is_empty()
}

/// gha-lifecycle: job still pending (→ `can_classify_pending`).
/// Matches Go `isJobPending`.
pub fn is_job_pending(status: &str, completed_at: &str) -> bool {
    gha_lifecycle::State {
        has_completed_at: gha_has_completed_at(status, completed_at),
        conclusion: String::from("failure"),
        counted_pending: false,
        counted_failed: false,
        queue_counted: false,
    }
    .can_classify_pending()
}

/// gha-lifecycle: count as failed (→ `can_classify_failed`).
/// Matches Go `countsFailed` (not pending AND failure|timed_out).
pub fn counts_failed(status: &str, completed_at: &str, conclusion: &str) -> bool {
    // Go: HasCompletedAt: !isJobPending(job)
    let has = !is_job_pending(status, completed_at);
    gha_lifecycle::State {
        has_completed_at: has,
        conclusion: String::from(conclusion),
        counted_pending: false,
        counted_failed: false,
        queue_counted: false,
    }
    .can_classify_failed()
}

/// gha-lifecycle: count for queue sample (→ `can_classify_queue`).
/// Matches Go `countsQueue` (skip/cancel stay outside the decision core).
pub fn counts_queue(status: &str, completed_at: &str, conclusion: &str) -> bool {
    if conclusion == "skipped" || conclusion == "cancelled" {
        return false;
    }
    let has = !is_job_pending(status, completed_at);
    gha_lifecycle::State {
        has_completed_at: has,
        conclusion: String::from(conclusion),
        counted_pending: false,
        counted_failed: false,
        queue_counted: false,
    }
    .can_classify_queue()
}

/// log-groups: may close stack (→ `log_groups::State::can_close`).
pub fn can_close_group(depth: i64) -> bool {
    log_groups::State { depth }.can_close()
}

/// log-groups: may open when depth is within decision MaxDepth=3.
pub fn can_open_group(depth: i64, max_depth: i64) -> bool {
    if depth < 0 {
        return false;
    }
    if max_depth <= 0 {
        return true; // unbounded (matches Go splitGroups)
    }
    if max_depth == 3 {
        return log_groups::State { depth }.can_open();
    }
    depth < max_depth
}

/// rate-limit: wait needed (scalar encoding of duration, like Go).
pub fn rate_limit_wait_needed(remaining: i64, reset_known: bool, until_reset_positive: bool) -> bool {
    let reset_at = if reset_known { 1 } else { 0 };
    let clock = if until_reset_positive { 0 } else { reset_at };
    rate_limit::State {
        remaining,
        sleeping: false,
        clock,
        reset_at,
        sent_while_exhausted: false,
    }
    .wait_needed()
}

/// sync-bounds: accept jobs attempt.
pub fn accept_jobs_attempt(stored: i64, incoming: i64) -> bool {
    sync_bounds::State {
        phase: String::from("stored"),
        stored_attempt: stored,
        incoming_attempt: incoming,
        accepted: false,
    }
    .accept_allowed()
}

/// tui-reload: fresh log-fetch (job match + CanFetchAccept).
pub fn log_fetch_result_fresh(msg_job: i64, fetching_job: i64, msg_gen: i64, reload_gen: i64) -> bool {
    if msg_job == 0 || msg_job != fetching_job {
        return false;
    }
    tui_reload::State {
        is_loading: false,
        reload_gen,
        fetch_job: fetching_job,
        fetch_gen: msg_gen,
        stale_accepted: false,
    }
    .can_fetch_accept()
}

/// span-tree: drop API side of 1+1 twin.
pub fn drop_api_for_runner_twin(api_count: i64, runner_count: i64, this_is_runner: bool) -> bool {
    if api_count != 1 || runner_count != 1 {
        return false;
    }
    let kept = span_tree::State::init()
        .see_api()
        .see_runner()
        .dedup_choose()
        .kept;
    kept == "runner" && !this_is_runner
}

/// timing-clamp: DoClamp on hostile child.
pub fn clamp_span_to_parent(start: i64, end: i64, parent_start: i64, parent_end: i64) -> (i64, i64) {
    let s = timing_clamp::State {
        phase: String::from("init"),
        start,
        end,
        parent_start,
        parent_end,
        out_start: 0,
        out_end: 0,
    }
    .do_clamp();
    (s.out_start, s.out_end)
}
