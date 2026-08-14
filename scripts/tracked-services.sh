#!/usr/bin/env bash
# tracked-services.sh — the single source of truth for which `crates/svc-*` get
# auto-published by the release pipeline and registry-gated by ci.yml. Source it:
#
#   source scripts/tracked-services.sh   # exports TRACKED_SERVICES
#
# Bootstrapped by runkit, empty: the first scaffolded service adds itself, with
# a note above saying why the entry is dormant.
#
# A service belongs here once it has:
#   - a CHANGELOG.md with at least one `## [VERSION]` entry,
#   - a version that is NOT 0.0.0-dev/-rc/-pre (the release pipeline skips
#     those outright, which is what makes a scaffolded entry harmless),
#   - a consumer (a chart in dp-botresources.ai),
#   - a Services-registry entry named after the crate minus its `svc-` prefix.
#
# Adding a service here REQUIRES adding it to scripts/service-meta.sh: the
# release documentation cannot be photographed for a crate that file does not
# know, and the pair is written meta-first for exactly that reason.
# svc-jobs: scaffolded by runkit and DORMANT — cd.yml skips any
# version matching 0.0.0-dev/-rc/-pre, which is what the generated
# crate declares, so this entry publishes nothing until the first
# real version is cut. It needs a consumer chart in dp by then.

export TRACKED_SERVICES="svc-jobs"
