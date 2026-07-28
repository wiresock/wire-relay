# Versioning and releases

WireRelay uses stable semantic versions in the form `MAJOR.MINOR.PATCH`.
The package version in [`Cargo.toml`](../Cargo.toml) is the source of truth.
The `wire-relay` package entry in [`Cargo.lock`](../Cargo.lock) must always
contain the same version.

The control protocol has its own integer version in `src/lib.rs`. Application
and control-protocol versions are intentionally independent: a release can
change without changing the protocol, while an incompatible protocol change
must increment the protocol version explicitly.

## Automatic patch versions

Every pull request merged into `main` is assigned a release version:

1. If the pull request did not change the package version, the
   `Version and Release` workflow increments the current patch version on
   `main`.
2. If the pull request deliberately increased the package version, the
   workflow keeps that version instead of adding another patch increment.
3. The workflow creates an annotated `vMAJOR.MINOR.PATCH` tag on the exact
   versioned commit and runs the release workflow for that tag.

GitHub's generated release notes are the patch-by-patch history. `CHANGELOG.md`
is curated for initial, minor, major, security, and compatibility milestones;
the automatic patch commit therefore does not rewrite it on every merge.

The post-merge version commit records the pull request number. This makes a
rerun idempotent and lets an interrupted run repair a missing tag or release
without advancing the version again.

The workflow uses GitHub Actions' expanded concurrency queue so merge events
wait instead of replacing an already-pending version run. Optimistic push
retries remain as a second guard: if `main` advances unexpectedly, the run
fetches it, computes the next patch version, and retries.

## Manual minor or major versions

For a planned minor or major release, update both `Cargo.toml` and the root
`wire-relay` entry in `Cargo.lock`, and add the curated changelog milestone in
the pull request. The new version must be a strict semantic-version increase.
The post-merge workflow will tag that version without adding an automatic
patch increment.

Validate version metadata locally:

```bash
python3 scripts/versioning/version.py validate
python3 -m unittest discover -s scripts/versioning -p "test_*.py"
```

Pre-release/build suffixes and leading-zero components are not accepted by the
automatic release policy.

## Checking and upgrading an installation

The local executable and the running daemon are deliberately reported
separately:

```bash
wire-relay --version
wire-relay version
```

`--version` reports the installed executable without contacting the daemon.
The `version` subcommand queries the running daemon and also reports its local
control-protocol version. This distinction exposes a stale process after a
binary replacement.

The bootstrap upgrade follows the configured source checkout's remote `main`
branch, builds and tests the candidate, and compares its semantic version with
the installed binary. The checkout must be clean and on a branch that tracks
remote `main`; detached/tag and feature-branch checkouts are rejected:

```bash
sudo ./bootstrap.sh upgrade
```

For an equal version, the bootstrap still validates the configuration but does
not replace the binary or restart a daemon that reports the same application
version. If the daemon is stopped or stale, the bootstrap repairs and restarts
the same-version installation. A lower candidate is rejected to prevent an
accidental downgrade. The legacy `update` spelling remains an alias for
compatibility.
