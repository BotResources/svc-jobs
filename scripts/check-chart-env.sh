#!/usr/bin/env bash
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/charts/br-svc-jobs"
workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

cat >"${workdir}/large.yaml" <<'VALUES'
port: 1000008
runnerTypes:
  retirementQuietPeriodSeconds: 1209600
  impactSweepIntervalSeconds: 2000000
  catalogReconcileIntervalSeconds: 3000000
priorities:
  defaultMaxPriority: 1000004
claims:
  maxWaitSeconds: 5000000
  reservationSeconds: 6000000
  maxHeld: 7000000
savedState:
  maxBytes: 123456789
retention:
  days: 1000395
  cancelledDays: 1000010
VALUES

failures=0

assert_plain_integers() {
    local label="$1"
    shift
    local rendered pairs
    rendered="$(helm template ci "${chart}" "$@")"
    pairs="$(awk '
        /^ +- name: (PORT|JOBS_[A-Z0-9_]+)$/ { name = $3; next }
        name != "" && /^ +value: / { value = $2; gsub(/"/, "", value); print name "=" value; name = ""; next }
        { name = "" }
    ' <<<"${rendered}")"

    if [[ -z "${pairs}" ]]; then
        echo "::error::${label}: no PORT or JOBS_* env entry rendered"
        failures=$((failures + 1))
        return
    fi
    while IFS='=' read -r name value; do
        if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
            echo "::error::${label}: ${name} renders as '${value}', not a plain integer"
            failures=$((failures + 1))
        fi
    done <<<"${pairs}"
    echo "${label}: $(wc -l <<<"${pairs}" | tr -d ' ') integer env entries checked"
}

assert_refused() {
    local label="$1"
    shift
    if helm template ci "${chart}" "$@" >/dev/null 2>&1; then
        echo "::error::${label}: the chart rendered a value that is not a whole number"
        failures=$((failures + 1))
        return
    fi
    echo "${label}: refused"
}

assert_plain_integers "default values"
assert_plain_integers "values of one million and above" --values "${workdir}/large.yaml"
assert_refused "cancelled retention of 7d" --set-string retention.cancelledDays=7d
assert_refused "saved state of 1MB" --set-string savedState.maxBytes=1MB

exit "$((failures > 0))"
