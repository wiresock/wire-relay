#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"

# shellcheck source=scripts/bootstrap.sh
source "$REPOSITORY_ROOT/scripts/bootstrap.sh"

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

assert_equal() {
    local expected="$1"
    local actual="$2"
    local description="$3"

    [[ "$actual" == "$expected" ]] ||
        fail "$description (expected '$expected', got '$actual')"
}

assert_valid_semantic_version() {
    local version="$1"
    local actual

    actual="$(parse_semantic_version "$version")" ||
        fail "expected '$version' to be a valid semantic version"
    assert_equal "$version" "$actual" "semantic version should round-trip"
}

assert_invalid_semantic_version() {
    local version="$1"

    if parse_semantic_version "$version" >/dev/null; then
        fail "expected '$version' to be rejected"
    fi
}

assert_version_comparison() {
    local expected="$1"
    local left="$2"
    local right="$3"
    local actual

    actual="$(compare_semantic_versions "$left" "$right")" ||
        fail "comparison failed for '$left' and '$right'"
    assert_equal "$expected" "$actual" "comparison of '$left' and '$right'"
}

for version in \
    "0.0.0" \
    "0.1.0" \
    "1.0.0" \
    "10.20.30" \
    "999999999999999999999999.2.3"; do
    assert_valid_semantic_version "$version"
done

for version in \
    "" \
    "1" \
    "1.2" \
    "1.2.3.4" \
    "v1.2.3" \
    "01.2.3" \
    "1.02.3" \
    "1.2.03" \
    "1.2.3-alpha" \
    "1.2.3+build" \
    " 1.2.3" \
    "1.2.3 "; do
    assert_invalid_semantic_version "$version"
done

assert_equal \
    "1.2.3" \
    "$(parse_wire_relay_version "wire-relay 1.2.3")" \
    "binary version output should yield its semantic version"

for output in \
    "wire-relay" \
    "wire-relay v1.2.3" \
    "wire-relay 1.2.3 extra" \
    "other-program 1.2.3"; do
    if parse_wire_relay_version "$output" >/dev/null; then
        fail "expected binary version output '$output' to be rejected"
    fi
done

if ! daemon_version_matches_binary \
    "wire-relay 1.2.3 (control protocol 7)" \
    "wire-relay 1.2.3"; then
    fail "matching daemon and binary versions should be accepted"
fi
for daemon_output in \
    "wire-relay 1.2.2 (control protocol 7)" \
    "wire-relay 1.2.3" \
    "wire-relay 1.2.3 (protocol 7)"; do
    if daemon_version_matches_binary "$daemon_output" "wire-relay 1.2.3"; then
        fail "stale or malformed daemon output '$daemon_output' should be rejected"
    fi
done

assert_version_comparison "0" "0.0.0" "0.0.0"
assert_version_comparison "0" "10.20.30" "10.20.30"
assert_version_comparison "-1" "0.9.9" "1.0.0"
assert_version_comparison "1" "2.0.0" "1.999.999"
assert_version_comparison "-1" "1.2.9" "1.3.0"
assert_version_comparison "1" "1.10.0" "1.2.999"
assert_version_comparison "-1" "1.2.3" "1.2.4"
assert_version_comparison "1" "1.2.10" "1.2.9"
assert_version_comparison \
    "1" \
    "999999999999999999999999.0.0" \
    "999999999999999999999998.999.999"

if compare_semantic_versions "1.2" "1.2.3" >/dev/null; then
    fail "comparison should reject an invalid left version"
fi
if compare_semantic_versions "1.2.3" "01.2.3" >/dev/null; then
    fail "comparison should reject an invalid right version"
fi

lock_test_path="$(mktemp)"
acquire_operation_lock_at "$lock_test_path"
if flock --nonblock "$lock_test_path" true; then
    fail "a concurrent bootstrap operation should not acquire the lock"
fi
exec {OPERATION_LOCK_FD}>&-
if ! flock --nonblock "$lock_test_path" true; then
    fail "the bootstrap lock should be released when its descriptor closes"
fi
rm -f -- "$lock_test_path"

printf 'bootstrap version tests passed\n'
