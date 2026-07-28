# Changelog

WireRelay uses this file for curated release milestones and compatibility
changes. Patch-by-patch history is generated from merged pull requests on the
[GitHub Releases](https://github.com/wiresock/wire-relay/releases) page.
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

No curated milestone is currently pending.

## [0.1.1] - 2026-07-28

### Added

- Initial WireRelay release: bounded opaque UDP forwarding, per-client
  connected upstream sockets, strict TOML configuration, cached DNS,
  transactional reload, a versioned local control plane, operator CLI,
  Prometheus metrics, systemd packaging, bootstrap management, tests, and
  Linux release automation.
- Automatic patch-version commits and release tags after every merged pull
  request, with explicit manual minor/major version support.
- A version-aware `bootstrap.sh upgrade` command that avoids same-version
  reinstalls and rejects accidental downgrades.

[Unreleased]: https://github.com/wiresock/wire-relay/commits/main
[0.1.1]: https://github.com/wiresock/wire-relay/releases/tag/v0.1.1
