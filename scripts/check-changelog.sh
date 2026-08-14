#!/usr/bin/env bash
# check-changelog.sh — every crate's version must have release notes.
#
# Each crate under `crates/` must carry a `## [${version}]` heading in its own
# CHANGELOG.md matching the version in its Cargo.toml. `0.0.0-dev` maps to
# `## [Unreleased]`: a scaffolded crate accumulates changes there until the
# cutover to its first real release promotes the heading.
#
# It catches the "bumped Cargo.toml without writing release notes" mistake at
# pull-request time, which is the only time it is cheap to fix — a version with
# nothing written down is a version nobody can audit later.
#
# Run locally:    scripts/check-changelog.sh
# Wired in CI:    .github/workflows/ci.yml job `changelog`.
#
# Reports every failure before exiting, so one CI run shows the whole picture.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

read_version() {
    # `[package]` … `version = "x.y.z"`. Crate manifests pin their own version:
    # it is deliberately NOT inherited from [workspace.package], because a
    # version is per crate and a shared one would make every release a lie about
    # the crates that did not change.
    awk '
        /^\[package\]/      { in_pkg = 1; next }
        /^\[/ && !/^\[package\]/ { in_pkg = 0 }
        in_pkg && /^version *=/ {
            gsub(/[" ]/, "", $3); print $3; exit
        }' "$1"
}

failures=0
checked=0

for cargo in crates/*/Cargo.toml; do
    crate_dir="$(dirname "$cargo")"
    crate="$(basename "$crate_dir")"
    changelog="$crate_dir/CHANGELOG.md"
    checked=$((checked + 1))

    if [[ ! -f "$changelog" ]]; then
        echo "FAIL ${crate}: $changelog not found"
        failures=$((failures + 1))
        continue
    fi

    version="$(read_version "$cargo")"
    if [[ -z "${version}" ]]; then
        echo "FAIL ${crate}: could not parse package.version from $cargo"
        failures=$((failures + 1))
        continue
    fi

    if [[ "${version}" == "0.0.0-dev" ]]; then
        expected='^## \[Unreleased\]'
        description='## [Unreleased]'
    else
        expected="^## \[${version}\](\$| )"
        description="## [${version}]"
    fi

    if grep -qE "${expected}" "$changelog"; then
        echo "OK   ${crate}: version ${version} <-> '${description}'"
    else
        cat <<EOF
FAIL ${crate}: CHANGELOG.md is missing the entry for the current version.

  Cargo.toml version: ${version}
  Expected heading:   ${description}
  Changelog:          ${changelog}

Add a section near the top of ${changelog}:

  ## [${version}]

  - <what changed, for whoever reads this in six months>
EOF
        failures=$((failures + 1))
    fi
done

echo
if [[ "${checked}" -eq 0 ]]; then
    echo "No crate found under crates/ — nothing to check, and an empty workspace is not a pass."
    exit 1
fi
if [[ "${failures}" -gt 0 ]]; then
    echo "${failures}/${checked} crate(s) failed the changelog check."
    exit 1
fi

echo "All ${checked} crate(s) have a matching CHANGELOG entry."
