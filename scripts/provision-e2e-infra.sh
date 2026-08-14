#!/usr/bin/env bash
# Provision the infrastructure the e2e suites need, bootstrapped by runkit for a
# GitHub-hosted `ubuntu-latest` runner (passwordless sudo, apt, dpkg).
#
# Generic by design: it provisions only the two host prerequisites the shared
# harness (br-test-harness) needs and exports `E2E_PG_ADMIN_URL` — nothing
# service-specific.
#
# The harness needs exactly two things from the host:
#   1. an ADMIN PostgreSQL connection (E2E_PG_ADMIN_URL) able to CREATE
#      DATABASE / CREATE ROLE — the harness mints a per-scenario database and
#      owner itself;
#   2. a `nats-server` binary in PATH — each scenario spawns its OWN ephemeral
#      JetStream server, so scenarios never observe each other's subjects.
#
# No docker and no job `services:`: the runner's own PostgreSQL and a static
# nats-server binary are faster to stand up and work identically on a laptop
# running Linux. On macOS, point E2E_PG_ADMIN_URL at your own PostgreSQL and
# `brew install nats-server` instead of running this script.
set -euo pipefail

# ── PostgreSQL ───────────────────────────────────────────────────────
# ubuntu-latest images pre-install a PostgreSQL server but do not start it. The
# guard targets the `postgres` server binary + pg_ctlcluster, NOT the mere
# /usr/lib/postgresql tree: postgresql-client alone also populates that tree,
# and a tree-based guard would skip the install on an image with no server at
# all. On a bare image we install it — self-sufficient either way.
if ! command -v pg_ctlcluster >/dev/null 2>&1 \
    || [[ -z "$(find /usr/lib/postgresql -maxdepth 3 -type f -name postgres 2>/dev/null)" ]]; then
    sudo env DEBIAN_FRONTEND=noninteractive apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends postgresql >/dev/null
fi

# Resolve the version from the server binary's path (/usr/lib/postgresql/
# <ver>/bin/postgres), never from the bare version dirs — a client-only dir of
# a different major would otherwise win and point pg_ctlcluster at a
# nonexistent cluster.
PGVER="$(find /usr/lib/postgresql -maxdepth 3 -type f -name postgres -printf '%h\n' | awk -F/ '{print $(NF-1)}' | sort -V | tail -1)"
sudo pg_ctlcluster "${PGVER}" main start

# Password for TCP auth (debian defaults to peer on the unix socket only).
sudo -u postgres psql -qtAc "ALTER USER postgres PASSWORD 'e2e-ci'"

for _ in $(seq 1 30); do
    pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1 && break
    sleep 1
done
pg_isready -h 127.0.0.1 -p 5432 -U postgres

ADMIN_URL="postgresql://postgres:e2e-ci@127.0.0.1:5432/postgres"
echo "PostgreSQL ${PGVER} ready."

# ── nats-server (static binary, pinned) ──────────────────────────────
# Pinned, not latest: the platform pins nats-server across every environment
# (per-key KV TTLs need ≥2.11), and a floating `latest` here would let CI drift
# from what production runs. A binary of any other version already in PATH is
# ignored — the pin wins and we fetch it.
NATS_VERSION="${NATS_VERSION:-v2.14.1}"
if ! command -v nats-server >/dev/null 2>&1 || ! nats-server --version 2>/dev/null | grep -qF "${NATS_VERSION}"; then
    ARCH="$(dpkg --print-architecture)" # amd64 | arm64
    curl -fsSL \
        "https://github.com/nats-io/nats-server/releases/download/${NATS_VERSION}/nats-server-${NATS_VERSION}-linux-${ARCH}.tar.gz" \
        | tar -xz -C /tmp
    sudo install -m 0755 /tmp/nats-server-*/nats-server /usr/local/bin/nats-server
fi
nats-server --version # trace the resolved binary regardless of install path

# ── export for subsequent steps ──────────────────────────────────────
if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "E2E_PG_ADMIN_URL=${ADMIN_URL}" >>"${GITHUB_ENV}"
else
    echo "export E2E_PG_ADMIN_URL=${ADMIN_URL}"
fi
