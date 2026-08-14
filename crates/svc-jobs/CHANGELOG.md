# Changelog — svc-jobs

All notable changes to this service are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the service adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The version in [`Cargo.toml`](Cargo.toml) is the source of truth. CI gates every
PR on `scripts/check-changelog.sh`: whatever version sits in `Cargo.toml` must
have a matching `## [${version}]` heading here. The `0.0.0-dev` placeholder this
crate ships with is the one exception — it maps to `## [Unreleased]`, which the
first real release promotes to a numbered heading.

## [Unreleased]

- Scaffolded by `runkit`: the composition root, the GraphQL edge with a single
  placeholder query, and the event-store migration. No behaviour yet.
