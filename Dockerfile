# Runtime-only image for svc-jobs.
#
# The binary is compiled OUTSIDE Docker (by scripts/publish.sh or CI) and
# copied in. This image contains only the runtime dependencies — no Rust
# toolchain, no source code, no build artifacts. Migrations are embedded in
# the binary at compile time (sqlx::migrate!), so nothing else ships.
#
# Build args:
#   BIN_PATH — path to the pre-compiled Linux binary, REQUIRED.
#              Must point to an ELF binary matching the target --platform;
#              a host (e.g. Mach-O) binary will build silently and crash
#              at runtime with `exec format error`.
#
# Local usage: prefer `./scripts/publish.sh --local-image`, which cross-
# compiles for the host arch, invokes this Dockerfile with the correct
# BIN_PATH, and tags {image}:{version}-local. Reach for a direct
# `docker build` only when you already have a Linux binary on disk:
#
#   cargo zigbuild --release -p svc-jobs --target x86_64-unknown-linux-musl
#   docker build \
#     --platform linux/amd64 \
#     --build-arg BIN_PATH=target/x86_64-unknown-linux-musl/release/svc-jobs \
#     -t br-svc-jobs .
#
# Multi-arch release goes through scripts/publish.sh.

FROM debian:bookworm-slim

# No `RUN` instructions: keeps the Dockerfile cross-arch buildable without
# QEMU emulation. The binary is built static-musl (see scripts/lib/
# build-cross-{amd64,arm64}.sh), so it has no runtime libc dep on this
# base image. ca-certificates is intentionally NOT installed — outbound
# TLS goes through rustls with roots bundled into the binary at compile
# time; production k8s probes are native httpGet.

ARG BIN_PATH

COPY ${BIN_PATH} /usr/local/bin/svc-jobs

WORKDIR /app
EXPOSE 8006
ENTRYPOINT ["/usr/local/bin/svc-jobs"]
