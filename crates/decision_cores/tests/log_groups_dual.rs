//! Dual: Rust gates ↔ generated log_groups decision core (same tables as Go duals).

use decision_cores::gates;
use decision_cores::log_groups;

#[test]
fn close_forbidden_at_zero() {
    let s = log_groups::State::init();
    assert_eq!(s.depth, 0);
    assert!(!s.can_close());
    assert!(!gates::can_close_group(0));
}

#[test]
fn open_close_roundtrip() {
    let s = log_groups::State::init();
    assert!(s.can_open());
    let s = s.open();
    assert_eq!(s.depth, 1);
    assert!(gates::can_close_group(s.depth));
    assert!(s.can_close());
    let s = s.close();
    assert_eq!(s.depth, 0);
}

#[test]
fn max_depth_three_matches_can_open() {
    assert!(gates::can_open_group(0, 3));
    assert!(gates::can_open_group(2, 3));
    assert!(!gates::can_open_group(3, 3));
    assert!(gates::can_open_group(3, 0)); // unbounded
}

#[test]
fn pure_inv_depth_non_neg() {
    let s = log_groups::State::init();
    assert!(s.inv_depth_non_neg());
}
