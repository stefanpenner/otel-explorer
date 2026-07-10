//! Decision cores for otel-explorer (Rust peer of Go `*spec` packages).
//!
//! **SSOT:** `specs/<core>/decision/Decision.tla`  
//! **Regen:** `bazel run //tools/decision:update`  
//! **Check:** `cargo test -p decision_cores` and `bazel test //tools/decision:up_to_date`
//!
//! Generated modules are pure state machines. Production-style gates live in
//! [`gates`] — thin wrappers (same idea as Go `canCloseGroup` → `CanClose`).

#![allow(dead_code)]

pub mod gha_lifecycle;
pub mod log_groups;
pub mod rate_limit;
pub mod span_tree;
pub mod sync_bounds;
pub mod timing_clamp;
pub mod tui_reload;

pub mod gates;
