# Contributing to WireRelay

Thanks for helping improve WireRelay. Correct routing, bounded resource use,
and predictable lifecycle behavior matter more than cleverness in this
project.

## Before starting

- Search existing issues and pull requests before opening a duplicate.
- Use a private security report rather than a public issue for a suspected
  vulnerability; see [`SECURITY.md`](SECURITY.md).
- For a behavior or architecture change, open an issue first so its operational
  and compatibility effects can be discussed.
- Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). It records invariants
  that implementations and tests must preserve.

## Development environment

WireRelay uses stable Rust, edition 2024, with a minimum supported Rust version
of 1.85. Linux is required for full daemon and Unix-domain-socket integration
testing.

```bash
git clone https://github.com/wiresock/wire-relay.git
cd wire-relay
rustup toolchain install stable --profile minimal \
  --component rustfmt --component clippy
cargo build --all-features
cargo test --all-features
```

Keep `Cargo.lock` in changes that intentionally update dependency resolution.
Do not update unrelated dependencies in the same pull request.

## Required checks

Run the same checks as CI before requesting review:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --lib --bins --benches --all-features --locked
cargo test --doc --all-features --locked
cargo test --test "*" --all-features --locked
cargo build --release --all-features --locked
python3 scripts/versioning/version.py validate
python3 -m unittest discover -s scripts/versioning -p "test_*.py"
bash -n wire-relay.sh scripts/bootstrap.sh tests/bootstrap_version_tests.sh
shellcheck wire-relay.sh scripts/bootstrap.sh tests/bootstrap_version_tests.sh
bash tests/bootstrap_version_tests.sh
```

If a check cannot be run locally, state which one and why in the pull request.
Do not weaken a lint, test, hardening directive, or resource bound merely to
make a check pass; explain and justify a deliberate policy change.

## Implementation expectations

- Treat every UDP datagram and control message as untrusted input.
- Never parse or log UDP payloads.
- Preserve payload bytes and one-to-one datagram boundaries.
- Keep one fixed configured backend per listener. A client must never be able
  to select an arbitrary destination.
- Keep one connected upstream UDP socket per session unless a replacement has
  a written correctness proof that does not depend on parsing WireGuard.
- Do not spawn a task per packet, create an unbounded channel, hold a lock
  across `.await`, or detach background tasks.
- Avoid `unsafe`. The crate forbids it; any proposal to change that policy
  requires a focused design review and documented safety argument.
- Return typed errors from reusable modules. Add contextual application errors
  at process boundaries.
- Do not use `unwrap()` or `expect()` in a runtime path unless a documented
  invariant makes failure impossible.
- Rate-limit repetitive logs and never use client endpoints as metrics labels.
- Keep code paths suitable for both IPv4 and IPv6.

Rust source files use:

```rust
// SPDX-License-Identifier: AGPL-3.0-or-later
```

## Tests

Add the smallest useful unit tests and the end-to-end test needed to protect
the behavior. Relay integration tests should use local UDP echo backends and
arbitrary byte sequences; do not make them understand WireGuard internals.

Routing or lifecycle changes should cover relevant cases such as:

- two clients receiving only their own backend responses;
- two listeners routing to different fixed backends;
- exact payload and datagram-boundary preservation;
- oversized-datagram drops;
- global, per-IP, and rate admission limits;
- idle expiry and explicit session closure;
- DNS changes affecting new sessions only;
- failed reloads leaving runtime state untouched;
- graceful shutdown terminating tasks and removing owned sockets.

Tests must not depend on public DNS, public UDP services, timing margins too
narrow for CI, or privileged ports. When timing is intrinsic, use Tokio's
paused time where practical.

## Documentation and operational changes

Configuration fields must include validation, defaults when appropriate,
normalized control output, tests, and updates to
`config/config.example.toml` and the README.

Changes to the systemd unit or bootstrap script need testing on a supported
Linux distribution. Explain why each added hardening directive remains
compatible with UDP sockets, resolver access, journald, configuration reads,
and `/run/wire-relay` socket creation.

Breaking, security-sensitive, and operationally significant changes belong in
`CHANGELOG.md`; routine patch history is generated from pull request metadata
in GitHub Releases. CLI JSON and control-protocol changes must be treated as
compatibility changes and documented explicitly.

## Versioning

`Cargo.toml` is the application-version source of truth, and the matching
`wire-relay` package entry in `Cargo.lock` must stay synchronized. CI validates
that invariant. After each pull request merges, automation normally increments
the patch version, creates an annotated `vMAJOR.MINOR.PATCH` tag, and publishes
the release.

For an intentional minor or major release, update both files to a strictly
higher stable semantic version in the pull request and update the curated
changelog milestone. Automation detects the manual increase and does not add a
second bump. Do not change the independent control-protocol version unless the
local protocol itself changes. The complete policy and recovery behavior are
documented in
[`docs/VERSIONING.md`](docs/VERSIONING.md).

## Pull requests

Keep pull requests focused and include:

- the problem and chosen approach;
- security and resource-bound implications;
- user-visible behavior or compatibility changes;
- tests performed and their results;
- deployment, reload, and rollback considerations when relevant.

Reviewers may ask for benchmarks when a hot-path optimization adds complexity.
Include the workload, host details, commands, and both baseline and candidate
results so a benchmark can be reproduced.

## Licensing

Contributions to the AGPL-licensed code must be compatible with
`AGPL-3.0-or-later`. Do not add code, fixtures, or assets unless you have the
right to contribute them and can identify their license when it differs from
the project default.

The project also offers separate commercial licensing. This document does not
create a contributor agreement or assign contribution rights. Maintainers must
resolve any additional relicensing permission needed for a contribution before
accepting it into a commercial offering.
