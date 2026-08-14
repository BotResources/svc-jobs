# svc-jobs

Standalone generic jobs service for the BotResources platform.

**Status: pre-scaffold.** The functional specification is being written; no code
has landed yet. This repository currently holds only the project governance
files. The service scaffold, its contract crate, and the full README (the public
API contract) will land once the specification is sealed.

Like the other BotResources generic services (`svc-auth`, `svc-notifier`), this
service will ship as a portable container image, versioned per-repo with a
keepachangelog `CHANGELOG.md`, and built on the shared
[`br-rust-common`](https://github.com/BotResources/br-rust-common) library.

## License

Apache-2.0 — see [LICENSE](LICENSE). This repository is published read-only and
does not accept external contributions; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [SUPPORT.md](SUPPORT.md).
