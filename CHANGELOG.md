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
- A closed session no longer keeps its route or its client's
  `max_sessions_per_ip` slot while it stops, so the next datagram from the
  same client opens a new session instead of being dropped as
  `session_closed` or `rejected`, even with `max_sessions_per_ip = 1`. The
  closed session still counts toward `max_sessions`, the queue budget and the
  session gauges until its task ends, so `max_sessions` bounds the upstream
  sockets that exist. At a relay that is at `max_sessions`, a returning
  client can therefore still be dropped as `rejected` for the moment the
  closed sessions' tasks take to end, for example right after a backend
  change closes many sessions. A closed task no longer waits for a pending
  client-direction send. A reload or listener stop no longer counts sessions that were already
  closed, and a close that coincides with idle expiry is counted as a close.
  A session task dropped before it first runs or in mid-run still removes its
  table entry and gauges and counts the datagrams it never sent as
  `session_closed`.
- Datagrams the relay drops itself (`queue_full`, `budget_exhausted`,
  `session_closed`) no longer refresh a session's idle timer, so a client
  sending only into a full queue or exhausted budget still idles out. Backend
  send errors still count as client activity, and so does delivering a queued
  datagram, so a session is not expired with datagrams still queued when a
  stalled upstream socket recovers.
- Sessions of a stopping listener reject datagrams immediately instead of
  sending or queueing them until their tasks wake up.
- Idle expiry no longer races the direct upload path: a session is not
  expired under a datagram it has just accepted, and an expired session
  sends nothing further on its upstream socket.

### Added

- `service.session_queue_depth` (default 512) and
  `service.max_queue_memory_bytes` (default 256 MiB): a shared,
  runtime-enforced queue memory limit charged with each datagram's size plus
  a fixed 64-byte overhead. The limit must hold one full queue of
  maximum-sized datagrams and is further reduced to what the per-session
  receive buffers leave of the defensive 4 GiB. A few back-pressured sessions
  can exhaust it so that others drop as `budget_exhausted`. After a
  reload lowers `max_sessions` below the live session count, the budget
  recovers to its full size as the surplus sessions end. A reload that leaves
  less than one maximum-sized datagram of budget beside the live sessions'
  receive buffers (for example by raising `max_datagram_size` under many live
  sessions) logs a warning. Budget validation errors now name the field to
  change, including `service.session_queue_depth`.
- Per-reason drop counters in `/metrics`, `wire-relay stats`, and
  `wire-relay listeners`, including
  `wire_relay_datagrams_dropped_budget_exhausted_total` for shared queue
  memory, separate from a full per-session queue (`queue_full`), plus
  rate-limited warnings for each. `wire_relay_datagrams_dropped_total` is unchanged; it is now the sum
  of the per-reason counters, and datagrams still queued when a session stops
  are counted as `session_closed`.

### Changed

- Rate-limited log sites no longer take a process-wide lock or search by
  name; each site has an enum-indexed atomic timestamp, declared from one
  list.
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
