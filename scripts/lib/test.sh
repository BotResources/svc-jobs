#!/usr/bin/env bash
# Unit tests. The e2e suites need a real Postgres and a real NATS and live in
# crates/svc-jobs/tests/ — they are gated by the `e2e jobs (reality chains)`
# job in ci.yml, not by publish.sh.
#
# Usage (sourced by publish.sh):
#   source scripts/lib/test.sh
#   run_crate_tests

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

run_crate_tests() {
    cd "$REPO_ROOT" || exit 1

    info "[${CRATE_NAME}] Running unit tests"
    cargo test --workspace --locked --lib --bins
}
