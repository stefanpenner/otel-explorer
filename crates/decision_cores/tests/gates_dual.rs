//! Dual tables: thin `gates::*` wrappers ↔ generated decision modules.
//! Mirrors Go production gate tables (rate-limit, sync-bounds, tui-reload, …).

use decision_cores::gates;

#[test]
fn gha_lifecycle_table() {
    // Mirror pkg/analyzer/gha_lifecycle_decision_dual_test.go table.
    let cases: &[(&str, &str, &str, bool, bool, bool)] = &[
        // status, completed_at, conclusion, pending, failed, queue
        ("completed", "", "failure", true, false, false),
        ("completed", "", "timed_out", true, false, false),
        ("completed", "2026-01-01T00:00:00Z", "failure", false, true, true),
        ("completed", "2026-01-01T00:00:00Z", "timed_out", false, true, true),
        ("completed", "2026-01-01T00:00:00Z", "success", false, false, true),
        ("in_progress", "", "", true, false, false),
        ("completed", "2026-01-01T00:00:00Z", "skipped", false, false, false),
        ("completed", "2026-01-01T00:00:00Z", "cancelled", false, false, false),
    ];
    for (status, completed_at, conclusion, want_p, want_f, want_q) in cases {
        assert_eq!(
            gates::is_job_pending(status, completed_at),
            *want_p,
            "pending {status:?} {completed_at:?}"
        );
        assert_eq!(
            gates::counts_failed(status, completed_at, conclusion),
            *want_f,
            "failed {status:?} {conclusion:?}"
        );
        assert_eq!(
            gates::counts_queue(status, completed_at, conclusion),
            *want_q,
            "queue {status:?} {conclusion:?}"
        );
    }
}

#[test]
fn rate_limit_wait_needed_table() {
    assert!(!gates::rate_limit_wait_needed(1, true, true));
    assert!(!gates::rate_limit_wait_needed(0, false, true));
    assert!(!gates::rate_limit_wait_needed(0, true, false));
    assert!(gates::rate_limit_wait_needed(0, true, true));
}

#[test]
fn accept_jobs_attempt_table() {
    assert!(gates::accept_jobs_attempt(2, 0)); // unknown attempt
    assert!(gates::accept_jobs_attempt(2, 2));
    assert!(!gates::accept_jobs_attempt(2, 1)); // stale
    assert!(!gates::accept_jobs_attempt(3, 2));
}

#[test]
fn log_fetch_result_fresh_table() {
    assert!(gates::log_fetch_result_fresh(1, 1, 0, 0));
    assert!(!gates::log_fetch_result_fresh(0, 1, 0, 0)); // zero job
    assert!(!gates::log_fetch_result_fresh(1, 2, 0, 0)); // wrong job
    assert!(!gates::log_fetch_result_fresh(1, 1, 0, 1)); // stale gen
}

#[test]
fn drop_api_for_runner_twin_table() {
    assert!(gates::drop_api_for_runner_twin(1, 1, false)); // drop API
    assert!(!gates::drop_api_for_runner_twin(1, 1, true)); // keep runner
    assert!(!gates::drop_api_for_runner_twin(1, 0, false));
    assert!(!gates::drop_api_for_runner_twin(2, 1, false));
}

#[test]
fn clamp_span_to_parent_contains() {
    // child end past parent → clamp end into parent
    let (s, e) = gates::clamp_span_to_parent(2, 4, 1, 3);
    assert!(s < e);
    assert!(s >= 1);
    assert!(e <= 3);
}
