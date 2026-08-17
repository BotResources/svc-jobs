#!/usr/bin/env bash
# Workspace checks: fmt, clippy, audit, helm lint.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/check.sh
#   run_crate_checks

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_checks() {
    cd "$REPO_ROOT" || exit 1

    info "Running workspace checks"

    info "  cargo fmt --check"
    cargo fmt --all --check

    info "  cargo clippy --workspace --all-targets"
    cargo clippy --workspace --all-targets --locked -- -D warnings

    info "Running cargo audit"
    if command -v cargo-audit >/dev/null 2>&1; then
        cargo audit --ignore RUSTSEC-2023-0071
    else
        warn "cargo-audit not installed — skipping (install with: cargo install cargo-audit)"
    fi

    if command -v helm >/dev/null 2>&1; then
        info "  helm lint charts/br-svc-jobs"
        helm lint "$REPO_ROOT/charts/br-svc-jobs"
    else
        warn "helm not installed — skipping chart lint"
    fi
}
