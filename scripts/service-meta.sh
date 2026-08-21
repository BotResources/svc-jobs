#!/usr/bin/env bash
# service-meta.sh — the per-service facts the release pipeline cannot infer.
#
#   source scripts/service-meta.sh          # defines service_meta()
#   service_meta svc-<name>                 # populates the SVC_META_* globals
#
# Companion of tracked-services.sh (WHICH crates ship) — this file answers WHAT
# each one is made of: does it serve GraphQL, does it own a database, which
# migrations compose that database, which registry Service it realizes.
#
# WHY EVERY FACT IS DECLARED RATHER THAN DETECTED. The release documentation is
# photographed from a built service and posed on the production Services
# registry as the record of what shipped. Inferring "no GraphQL" from a binary
# that crashed, or "no database" from an empty migrations directory, would pose
# an empty document and let a broken build masquerade as a service with no
# surface. Declared facts fail loudly when reality contradicts them; inferred
# ones fail silently.
#
# Adding a service to TRACKED_SERVICES REQUIRES adding it here.
#
# Bootstrapped by runkit with no branch but the refusal: the first scaffolded
# service files its own `svc-<name>)` branch above `*)`, alphabetically.

# shellcheck disable=SC2034
service_meta() {
    SVC_META_GRAPHQL=""
    SVC_META_REGISTRY_ID=""
    SVC_META_DB_NAME=""
    SVC_META_MIGRATIONS=""
    SVC_META_DB_ROLES=""

    case "$1" in
    svc-jobs)
        # Scaffolded by runkit from major 0. The binary answers
        # `svc-jobs schema` with its SDL — crates/svc-jobs/src/main.rs.
        SVC_META_GRAPHQL="yes"
        SVC_META_REGISTRY_ID="019f8137-e784-7320-87f3-13074aacc4d4"
        SVC_META_DB_NAME="jobs"
        SVC_META_MIGRATIONS="public=crates/svc-jobs/migrations"
        # The scaffolded migration creates no role and references none, so
        # none has to pre-exist for the dump to describe production:
        # jobs_app is provisioned and granted at boot by the composition
        # root, which is runtime provisioning and not schema. Same posture
        # as svc-projects and svc-services. Add a role here the day a
        # jobs migration GRANTs to one under an `IF EXISTS` guard.
        SVC_META_DB_ROLES=""
        ;;
    *)
        echo "::error::service-meta: no metadata declared for '$1'." >&2
        return 1
        ;;
    esac
}

registry_ids() {
    local crate="$1" want_version="${2:-}" file="crates/$1/registry.toml"
    local uuid_re='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

    service_meta "${crate}" || return 1

    [[ -f "${file}" ]] || {
        echo "::error::registry ids: ${file} not found. Every tracked service commits its registry coordinate; create it alongside the crate (see another crate's registry.toml for the shape)." >&2
        return 1
    }

    if [[ -n "${want_version}" ]]; then
        local declared
        declared=$(awk '
            /^\[package\]/           { in_pkg = 1; next }
            /^\[/ && !/^\[package\]/  { in_pkg = 0 }
            in_pkg && /^version *=/   { gsub(/[" ]/, "", $3); print $3; exit }
        ' "crates/${crate}/Cargo.toml")
        [[ "${declared}" == "${want_version}" ]] || {
            echo "::error::registry ids: asked to act on ${crate} ${want_version}, but this checkout declares ${declared} in Cargo.toml. The release documents are photographed from THIS tree, so proceeding would describe ${declared} inside ${want_version}'s registry patch. Publish from a checkout of the commit that released ${want_version} (its tag), or bump to ${want_version} properly." >&2
            return 1
        }
    fi

    SVC_REG_SERVICE_ID=$(sed -n 's/^[[:space:]]*service-id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "${file}" | head -1)

    [[ "${SVC_REG_SERVICE_ID}" =~ ${uuid_re} ]] || {
        echo "::error::registry ids: ${file} has no well-formed service-id (got '${SVC_REG_SERVICE_ID}')." >&2
        return 1
    }
    [[ "${SVC_REG_SERVICE_ID}" == "${SVC_META_REGISTRY_ID}" ]] || {
        echo "::error::registry ids: ${file} declares service-id ${SVC_REG_SERVICE_ID}, but scripts/service-meta.sh has ${SVC_META_REGISTRY_ID} for ${crate}. One of the two is wrong — refusing to touch the registry until they agree." >&2
        return 1
    }
}
