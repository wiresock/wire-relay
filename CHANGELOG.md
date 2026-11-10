# Changelog

WireRelay uses this file for curated release milestones and compatibility
changes. Patch-by-patch history is generated from merged pull requests on the
[GitHub Releases](https://github.com/wiresock/wire-relay/releases) page.
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Client-to-backend (upload) bursts are no longer silently dropped by the
  eight-datagram per-session queue. The listener now sends directly on the
  session's connected upstream socket and only queues when that socket is
  busy.

### Added

- `service.session_queue_depth` (default 512) with a runtime-enforced queue
  memory budget based on actual datagram sizes.
- Per-reason drop counters in `/metrics`, `wire-relay stats`, and
  `wire-relay listeners`, plus a rate-limited warning when a session queue is
  full. `wire_relay_datagrams_dropped_total` is unchanged.

### Changed

- Replaced the repository-only management wrapper with the self-contained
  `wire-relay-install.sh`, which supports reviewed downloads and direct
  `curl | sudo bash` installation.
- The installer leaves the full Rust test suite to CI by default. Set
  `WIRE_RELAY_RUN_TESTS=1` to opt in when validating a custom source checkout.
- Published the WireSock contact for commercial licensing inquiries.

## [0.1.1] - 2026-07-28

### Added

- Initial WireRelay release: bounded opaque UDP forwarding, per-client
  connected upstream sockets, strict TOML configuration, cached DNS,
  transactional reload, a versioned local control plane, operator CLI,
  Prometheus metrics, systemd packaging, bootstrap management, tests, and
  Linux release automation.
- Automatic patch-version commits and release tags after every merged pull
  request, with explicit manual minor/major version support.
- A version-aware source-checkout management command that avoids same-version
  reinstalls and rejects accidental downgrades.

[Unreleased]: https://github.com/wiresock/wire-relay/commits/main
[0.1.1]: https://github.com/wiresock/wire-relay/releases/tag/v0.1.1
