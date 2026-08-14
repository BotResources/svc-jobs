#!/usr/bin/env bash
# setup-branch-protection.sh
#
# Applies the branch-protection posture for `main`: pull-request only, no admin
# bypass, required status checks, branch up to date, linear history, zero
# approvals. Idempotent — re-running with the same inputs changes no state.
#
# Requirements:
#   - gh CLI installed and authenticated, with admin rights on the repository
#   - jq
#
# Usage:
#   scripts/setup-branch-protection.sh              # apply
#   scripts/setup-branch-protection.sh --dry-run    # preview the payload
#   REPO=BotResources/foo scripts/setup-branch-protection.sh
#
# The required checks are READ from .github/required-checks.json. That is the
# whole point of this version: a second, hand-written copy of the list is a copy
# that drifts, and the drift is invisible until a required context nobody
# reports hangs every pull request. One list, in the file the workflow is gated
# against.
#
# Written by runkit when it bootstrapped this repository's CI, and rewritten by
# it if a pre-scaffold copy carried its own list.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DECL="$ROOT/.github/required-checks.json"
BRANCH="${BRANCH:-main}"
DRY_RUN=false

# Default to the repository this checkout actually pushes to, rather than a name
# baked into the script: a copied script that protects somebody else's branch is
# a worse outcome than one that stops and asks.
if [[ -z "${REPO:-}" ]]; then
    origin="$(git -C "$ROOT" remote get-url origin 2>/dev/null || true)"
    REPO="$(sed -E 's#^git@github\.com:##; s#^https://github\.com/##; s#\.git$##' <<<"$origin")"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)    DRY_RUN=true;      shift ;;
        -h|--help)
            sed -n '2,/^set -euo pipefail$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "${REPO}" || "${REPO}" != */* ]]; then
    echo "ERROR: could not read the repository from the 'origin' remote. Pass it: REPO=BotResources/<repo> $0" >&2
    exit 1
fi

if [[ ! -f "$DECL" ]]; then
    echo "ERROR: $DECL not found — the declared required-check list is missing." >&2
    exit 1
fi

# No mapfile: operators run this from macOS, whose /bin/bash is stuck at 3.2.
REQUIRED_CHECKS=()
while IFS= read -r check; do
    [[ -n "$check" ]] && REQUIRED_CHECKS+=("$check")
done < <(jq -r '.required[]' "$DECL")
if [[ "${#REQUIRED_CHECKS[@]}" -eq 0 ]]; then
    echo "ERROR: $DECL declares no required check. Refusing to unprotect ${BRANCH}." >&2
    exit 1
fi

# Posture:
#   - pull-request only, enforced through enforce_admins (no bypass)
#   - 0 approvals: the review requirement is on, the count is simply zero
#     (solo posture; raise it when the team grows)
#   - branch up to date: required_status_checks.strict
#   - linear history, no force pushes, no deletions
PAYLOAD="$(jq -n \
    --argjson contexts "$(printf '%s\n' "${REQUIRED_CHECKS[@]}" | jq -R . | jq -s .)" \
    '{
        required_status_checks: {
            strict: true,
            contexts: $contexts
        },
        enforce_admins: true,
        required_pull_request_reviews: {
            dismiss_stale_reviews: true,
            require_code_owner_reviews: false,
            required_approving_review_count: 0,
            require_last_push_approval: false
        },
        restrictions: null,
        required_linear_history: true,
        allow_force_pushes: false,
        allow_deletions: false,
        block_creations: false,
        required_conversation_resolution: true,
        lock_branch: false,
        allow_fork_syncing: false
    }')"

echo "── Branch protection plan ────────────────────────────────────"
echo "  repo            : ${REPO}"
echo "  branch          : ${BRANCH}"
echo "  approvals       : 0 (solo posture; raise to 1 when the team grows)"
echo "  status checks   : strict (branch must be up to date)"
for c in "${REQUIRED_CHECKS[@]}"; do
    echo "                    - ${c}"
done
echo "  linear history  : enforced (rebase or squash, no merge commits)"
echo "  enforce admins  : true (no admin bypass)"
echo "  force pushes    : forbidden"
echo "  deletions       : forbidden"
echo "  conversation res: required"
echo "─────────────────────────────────────────────────────────────"

if [[ "${DRY_RUN}" == "true" ]]; then
    echo
    echo "DRY-RUN: payload that would be sent to gh api:"
    echo "${PAYLOAD}" | jq .
    exit 0
fi

if ! gh auth status >/dev/null 2>&1; then
    echo "ERROR: gh CLI is not authenticated. Run 'gh auth login' first." >&2
    exit 1
fi

echo
echo "Applying via: gh api -X PUT repos/${REPO}/branches/${BRANCH}/protection"
echo "${PAYLOAD}" \
    | gh api \
        -X PUT \
        --input - \
        "repos/${REPO}/branches/${BRANCH}/protection" \
    | jq -r '
        "Applied. Current protection summary:",
        "  url:                        \(.url)",
        "  enforce_admins:             \(.enforce_admins.enabled)",
        "  required_linear_history:    \(.required_linear_history.enabled)",
        "  allow_force_pushes:         \(.allow_force_pushes.enabled)",
        "  required_status_checks.strict: \(.required_status_checks.strict)",
        "  required_status_checks.contexts:",
        (.required_status_checks.contexts[] | "    - \(.)")
    '

echo
echo "Done. From now on every change to ${BRANCH} must go through a pull request"
echo "with all of the above status checks reporting success."
