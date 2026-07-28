#!/usr/bin/env bash
#
# Install, upgrade, configure, and remove WireRelay on supported systemd Linux
# distributions.

set -Eeuo pipefail
IFS=$'\n\t'
umask 027

readonly PROGRAM_NAME="WireRelay bootstrap"
readonly SERVICE_NAME="wire-relay"
readonly SERVICE_USER="wire-relay"
readonly SERVICE_GROUP="wire-relay"
readonly MINIMUM_RUST_MAJOR=1
readonly MINIMUM_RUST_MINOR=85
readonly MINIMUM_RUST_PATCH=0
readonly MINIMUM_RUST_VERSION="1.85.0"
readonly MAX_SESSION_LIMIT=100000
readonly MAX_SESSION_RATE=100000
readonly MAX_SESSION_DATAGRAM_MEMORY_BYTES=4294967296
readonly SESSION_DATAGRAM_SLOTS=10
readonly SEMANTIC_VERSION_PATTERN='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
readonly DEFAULT_REPOSITORY_URL="https://github.com/wiresock/wire-relay.git"
readonly REPOSITORY_URL="${WIRE_RELAY_REPOSITORY_URL:-$DEFAULT_REPOSITORY_URL}"
readonly BINARY_PATH="/usr/local/bin/wire-relay"
readonly STATE_DIR="/usr/local/lib/wire-relay"
readonly ROLLBACK_BINARY_PATH="$STATE_DIR/wire-relay.rollback"
readonly UNIT_PATH="/etc/systemd/system/wire-relay.service"
readonly ROLLBACK_UNIT_PATH="$STATE_DIR/wire-relay.service.rollback"
readonly CONFIG_DIR="/etc/wire-relay"
readonly CONFIG_PATH="$CONFIG_DIR/config.toml"
readonly OPERATION_LOCK_PATH="/run/wire-relay-bootstrap.lock"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
WORK_DIR=""
SOURCE_DIR=""
BUILD_TARGET_DIR=""
BUILDER_USER=""
BUILDER_HOME=""
PLATFORM_ARCH=""
DISTRO_FAMILY=""
PACKAGE_MANAGER=""
LAST_CONFIG_BACKUP=""
CONFIG_CHANGED=0
CONFIG_EXISTED_BEFORE=0
CONFIG_METADATA_CHANGED=0
CONFIG_ORIGINAL_UID=""
CONFIG_ORIGINAL_GID=""
CONFIG_ORIGINAL_MODE=""
UNIT_EXISTED_BEFORE=0
SELECTED_COMMAND=""
INSTALL_TRANSACTION_ACTIVE=0
INSTALL_HAD_OLD_BINARY=0
INSTALL_CONFIG_EXISTED_BEFORE=0
INSTALL_SERVICE_WAS_ACTIVE=0
INSTALL_SERVICE_WAS_ENABLED=0
INSTALL_OLD_VERSION=""
INSTALL_SERVICE_USER_EXISTED_BEFORE=0
INSTALL_SERVICE_GROUP_EXISTED_BEFORE=0
INSTALL_CONFIG_DIR_EXISTED_BEFORE=0
INSTALL_CONFIG_DIR_ORIGINAL_UID=""
INSTALL_CONFIG_DIR_ORIGINAL_GID=""
INSTALL_CONFIG_DIR_ORIGINAL_MODE=""
OPERATION_LOCK_FD=""
declare -a WIZARD_LISTENER_NAMES=()
declare -a WIZARD_LISTENER_BINDS=()
declare -a WIZARD_LISTENER_BACKENDS=()
declare -a WIZARD_LISTENER_BIND_IPS=()
declare -a WIZARD_LISTENER_BIND_PORTS=()
declare -a CLEANUP_FILES=()

if (( BASH_VERSINFO[0] < 4 )); then
    printf '[wire-relay] ERROR: Bash 4.0 or newer is required.\n' >&2
    exit 1
fi

log() {
    printf '[wire-relay] %s\n' "$*"
}

warn() {
    printf '[wire-relay] WARNING: %s\n' "$*" >&2
}

die() {
    printf '[wire-relay] ERROR: %s\n' "$*" >&2
    exit 1
}

parse_semantic_version() {
    local version="$1"

    [[ "$version" =~ $SEMANTIC_VERSION_PATTERN ]] || return 1
    printf '%s\n' "$version"
}

parse_wire_relay_version() {
    local output="$1"
    local version

    [[ "$output" =~ ^wire-relay[[:space:]]+([^[:space:]]+)$ ]] || return 1
    version="${BASH_REMATCH[1]}"
    parse_semantic_version "$version"
}

compare_version_component() {
    local left="$1"
    local right="$2"
    local LC_ALL=C

    if (( ${#left} < ${#right} )); then
        printf '%s\n' "-1"
    elif (( ${#left} > ${#right} )); then
        printf '%s\n' "1"
    elif [[ "$left" == "$right" ]]; then
        printf '%s\n' "0"
    elif [[ "$left" < "$right" ]]; then
        printf '%s\n' "-1"
    else
        printf '%s\n' "1"
    fi
}

compare_semantic_versions() {
    local left="$1"
    local right="$2"
    local left_major
    local left_minor
    local left_patch
    local right_major
    local right_minor
    local right_patch
    local comparison
    local index
    local -a left_components
    local -a right_components

    parse_semantic_version "$left" >/dev/null || return 2
    left_major="${BASH_REMATCH[1]}"
    left_minor="${BASH_REMATCH[2]}"
    left_patch="${BASH_REMATCH[3]}"
    parse_semantic_version "$right" >/dev/null || return 2
    right_major="${BASH_REMATCH[1]}"
    right_minor="${BASH_REMATCH[2]}"
    right_patch="${BASH_REMATCH[3]}"

    left_components=("$left_major" "$left_minor" "$left_patch")
    right_components=("$right_major" "$right_minor" "$right_patch")
    for index in 0 1 2; do
        comparison="$(
            compare_version_component \
                "${left_components[$index]}" \
                "${right_components[$index]}"
        )"
        if [[ "$comparison" != "0" ]]; then
            printf '%s\n' "$comparison"
            return
        fi
    done
    printf '%s\n' "0"
}

handle_error() {
    local status="$1"
    local line="$2"

    trap - ERR
    printf '[wire-relay] ERROR: command failed at line %s (exit status %s)\n' \
        "$line" "$status" >&2
    exit "$status"
}

cleanup() {
    local status=$?
    local temporary_file

    trap - EXIT
    for temporary_file in "${CLEANUP_FILES[@]}"; do
        if [[ -n "$temporary_file" ]]; then
            if ! rm -f -- "$temporary_file"; then
                warn "Could not remove temporary file $temporary_file."
            fi
        fi
    done
    if (( INSTALL_TRANSACTION_ACTIVE == 1 )); then
        if ! rollback_install_transaction; then
            warn "Automatic rollback was incomplete; inspect the warnings above."
        fi
    fi
    if [[ -n "${WORK_DIR:-}" && -d "$WORK_DIR" ]]; then
        if ! rm -rf -- "$WORK_DIR"; then
            warn "Could not remove temporary directory $WORK_DIR."
        fi
    fi
    exit "$status"
}

register_cleanup_file() {
    CLEANUP_FILES+=("$1")
}

trap 'handle_error "$?" "$LINENO"' ERR
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

usage() {
    cat <<'EOF'
Usage:
  sudo ./bootstrap.sh install
  sudo ./bootstrap.sh upgrade
  sudo ./bootstrap.sh configure
  sudo ./bootstrap.sh uninstall
  sudo ./bootstrap.sh status

The legacy "update" command remains an alias for "upgrade".

Running without a command opens an interactive menu.

Optional environment variables:
  WIRE_RELAY_SOURCE_DIR       Existing checkout or clone destination
  WIRE_RELAY_REPOSITORY_URL   Repository cloned when no checkout is available
EOF
}

require_root() {
    if (( EUID != 0 )); then
        die "This operation changes system files. Run it with sudo or as root."
    fi
}

acquire_operation_lock() {
    acquire_operation_lock_at "$OPERATION_LOCK_PATH"
}

acquire_operation_lock_at() {
    local lock_path="$1"

    command -v flock >/dev/null 2>&1 ||
        die "flock is required to serialize bootstrap operations."
    if ! exec {OPERATION_LOCK_FD}>"$lock_path"; then
        die "Could not open the bootstrap operation lock: $lock_path"
    fi
    if ! flock --nonblock "$OPERATION_LOCK_FD"; then
        exec {OPERATION_LOCK_FD}>&-
        die "Another WireRelay bootstrap operation is already running."
    fi
}

require_systemd() {
    command -v systemctl >/dev/null 2>&1 ||
        die "systemctl is required; WireRelay's bootstrap supports systemd hosts."
    [[ -d /run/systemd/system ]] ||
        die "systemd is not running as the system manager on this host."
}

create_work_dir() {
    local temporary_base

    if [[ -n "$WORK_DIR" ]]; then
        return
    fi

    temporary_base="${TMPDIR:-/tmp}"
    [[ -d "$temporary_base" && ! -L "$temporary_base" ]] ||
        die "Temporary directory is unavailable or unsafe: $temporary_base"
    temporary_base="$(cd -- "$temporary_base" && pwd -P)"
    WORK_DIR="$(mktemp -d "$temporary_base/wire-relay-bootstrap.XXXXXXXX")"
    chmod 0700 "$WORK_DIR"
}

detect_platform() {
    local os_id=""
    local os_like=""

    case "$(uname -m)" in
        x86_64 | amd64)
            PLATFORM_ARCH="x86_64"
            ;;
        aarch64 | arm64)
            PLATFORM_ARCH="aarch64"
            ;;
        *)
            die "Unsupported architecture '$(uname -m)'; supported: x86_64 and aarch64."
            ;;
    esac

    [[ -r /etc/os-release ]] ||
        die "Cannot identify this Linux distribution (/etc/os-release is missing)."
    # shellcheck source=/dev/null
    . /etc/os-release
    os_id="${ID:-}"
    os_like=" ${ID_LIKE:-} "

    case "$os_id" in
        debian | ubuntu | linuxmint | pop)
            DISTRO_FAMILY="debian"
            ;;
        fedora | rhel | centos | rocky | almalinux | ol)
            DISTRO_FAMILY="redhat"
            ;;
        arch | manjaro | endeavouros)
            DISTRO_FAMILY="arch"
            ;;
        opensuse* | sles)
            DISTRO_FAMILY="suse"
            ;;
        *)
            case "$os_like" in
                *" debian "* | *" ubuntu "*)
                    DISTRO_FAMILY="debian"
                    ;;
                *" fedora "* | *" rhel "* | *" centos "*)
                    DISTRO_FAMILY="redhat"
                    ;;
                *" arch "*)
                    DISTRO_FAMILY="arch"
                    ;;
                *" suse "*)
                    DISTRO_FAMILY="suse"
                    ;;
                *)
                    die "Unsupported Linux distribution '${PRETTY_NAME:-$os_id}'."
                    ;;
            esac
            ;;
    esac

    case "$DISTRO_FAMILY" in
        debian)
            command -v apt-get >/dev/null 2>&1 ||
                die "This Debian-family host does not provide apt-get."
            PACKAGE_MANAGER="apt-get"
            ;;
        redhat)
            if command -v dnf >/dev/null 2>&1; then
                PACKAGE_MANAGER="dnf"
            elif command -v yum >/dev/null 2>&1; then
                PACKAGE_MANAGER="yum"
            else
                die "This Red Hat-family host provides neither dnf nor yum."
            fi
            ;;
        arch)
            command -v pacman >/dev/null 2>&1 ||
                die "This Arch-family host does not provide pacman."
            PACKAGE_MANAGER="pacman"
            ;;
        suse)
            command -v zypper >/dev/null 2>&1 ||
                die "This SUSE-family host does not provide zypper."
            PACKAGE_MANAGER="zypper"
            ;;
    esac

    log "Detected ${PRETTY_NAME:-$os_id}, architecture $PLATFORM_ARCH, package manager $PACKAGE_MANAGER."
}

install_system_packages() {
    log "Installing required compiler, Git, TLS, and download packages."

    case "$PACKAGE_MANAGER" in
        apt-get)
            apt-get update
            env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
                build-essential pkg-config git curl ca-certificates
            ;;
        dnf)
            dnf install -y gcc gcc-c++ make pkgconf-pkg-config git curl ca-certificates
            ;;
        yum)
            yum install -y gcc gcc-c++ make pkgconfig git curl ca-certificates
            ;;
        pacman)
            pacman -Syu --needed --noconfirm base-devel pkgconf git curl ca-certificates
            ;;
        zypper)
            zypper --non-interactive refresh
            zypper --non-interactive install --no-recommends \
                gcc gcc-c++ make pkg-config git curl ca-certificates
            ;;
        *)
            die "Internal error: unsupported package manager '$PACKAGE_MANAGER'."
            ;;
    esac
}

select_builder() {
    local passwd_entry

    if (( EUID == 0 )) &&
        [[ -n "${SUDO_USER:-}" && "${SUDO_USER:-}" != "root" ]] &&
        id "$SUDO_USER" >/dev/null 2>&1; then
        BUILDER_USER="$SUDO_USER"
    else
        BUILDER_USER="$(id -un)"
    fi

    passwd_entry="$(getent passwd "$BUILDER_USER")" ||
        die "Cannot resolve account information for build user '$BUILDER_USER'."
    IFS=: read -r _ _ _ _ _ BUILDER_HOME _ <<<"$passwd_entry"
    [[ -n "$BUILDER_HOME" && -d "$BUILDER_HOME" ]] ||
        die "Build user '$BUILDER_USER' has no usable home directory."

    log "Release builds and tests will run as '$BUILDER_USER'."
}

run_as_builder() {
    local builder_path

    builder_path="$BUILDER_HOME/.cargo/bin:$BUILDER_HOME/.local/bin:$BUILDER_HOME/.nix-profile/bin"
    builder_path="$builder_path:$BUILDER_HOME/.asdf/shims:$BUILDER_HOME/.local/share/mise/shims"
    builder_path="$builder_path:/opt/rust/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    if (( EUID == 0 )) && [[ "$BUILDER_USER" != "root" ]]; then
        command -v runuser >/dev/null 2>&1 ||
            die "runuser is required to build without root privileges."
        runuser -u "$BUILDER_USER" -- env \
            HOME="$BUILDER_HOME" \
            USER="$BUILDER_USER" \
            LOGNAME="$BUILDER_USER" \
            CARGO_HOME="$BUILDER_HOME/.cargo" \
            RUSTUP_HOME="$BUILDER_HOME/.rustup" \
            PATH="$builder_path" \
            "$@"
    else
        env \
            HOME="$BUILDER_HOME" \
            CARGO_HOME="$BUILDER_HOME/.cargo" \
            RUSTUP_HOME="$BUILDER_HOME/.rustup" \
            PATH="$builder_path" \
            "$@"
    fi
}

tool_version_meets_rust_minimum() {
    local version_output="$1"
    local major
    local minor
    local patch

    [[ "$version_output" =~ ^[^[:space:]]+[[:space:]]+([0-9]+)\.([0-9]+)\.([0-9]+) ]] ||
        return 1
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    patch="${BASH_REMATCH[3]}"

    if (( 10#$major != MINIMUM_RUST_MAJOR )); then
        (( 10#$major > MINIMUM_RUST_MAJOR ))
        return
    fi
    if (( 10#$minor != MINIMUM_RUST_MINOR )); then
        (( 10#$minor > MINIMUM_RUST_MINOR ))
        return
    fi
    (( 10#$patch >= MINIMUM_RUST_PATCH ))
}

ensure_rust() {
    local actual_checksum
    local cargo_version
    local expected_checksum
    local rustup_target
    local rustup_init
    local rustup_checksum
    local rustc_version
    local has_cargo=0
    local has_rustc=0

    if run_as_builder bash -c 'command -v cargo >/dev/null 2>&1'; then
        has_cargo=1
    fi
    if run_as_builder bash -c 'command -v rustc >/dev/null 2>&1'; then
        has_rustc=1
    fi

    if (( has_cargo == 1 && has_rustc == 1 )); then
        cargo_version="$(run_as_builder cargo --version)"
        rustc_version="$(run_as_builder rustc --version)"
        if ! tool_version_meets_rust_minimum "$cargo_version" ||
            ! tool_version_meets_rust_minimum "$rustc_version"; then
            die "WireRelay requires Cargo and rustc $MINIMUM_RUST_VERSION or newer. \
Upgrade the existing Rust installation for '$BUILDER_USER' and retry; it was not modified."
        fi
        log "Using existing Rust installation: $rustc_version."
        return
    fi
    if (( has_cargo != has_rustc )); then
        die "The existing Rust installation is incomplete; install both cargo and rustc, then retry."
    fi

    create_work_dir
    if [[ "$BUILDER_USER" != "root" ]]; then
        chown "$BUILDER_USER" "$WORK_DIR"
    fi

    case "$PLATFORM_ARCH" in
        x86_64)
            rustup_target="x86_64-unknown-linux-gnu"
            ;;
        aarch64)
            rustup_target="aarch64-unknown-linux-gnu"
            ;;
        *)
            die "Internal error: no rustup target for '$PLATFORM_ARCH'."
            ;;
    esac

    rustup_init="$WORK_DIR/rustup-init"
    rustup_checksum="$WORK_DIR/rustup-init.sha256"
    log "Rust is absent; downloading the official rustup-init executable over TLS."
    run_as_builder curl \
        --proto '=https' \
        --tlsv1.2 \
        --fail \
        --silent \
        --show-error \
        --location \
        "https://static.rust-lang.org/rustup/dist/$rustup_target/rustup-init" \
        --output "$rustup_init"
    run_as_builder curl \
        --proto '=https' \
        --tlsv1.2 \
        --fail \
        --silent \
        --show-error \
        --location \
        "https://static.rust-lang.org/rustup/dist/$rustup_target/rustup-init.sha256" \
        --output "$rustup_checksum"
    expected_checksum="$(run_as_builder cut -c 1-64 "$rustup_checksum")"
    actual_checksum="$(run_as_builder sha256sum "$rustup_init")"
    actual_checksum="${actual_checksum%% *}"
    [[ "$expected_checksum" =~ ^[0-9a-fA-F]{64}$ &&
        "${actual_checksum,,}" == "${expected_checksum,,}" ]] ||
        die "The downloaded rustup-init checksum does not match the official checksum."
    run_as_builder chmod 0700 "$rustup_init"
    run_as_builder "$rustup_init" \
        -y \
        --no-modify-path \
        --profile minimal \
        --default-toolchain stable

    cargo_version="$(run_as_builder cargo --version)"
    rustc_version="$(run_as_builder rustc --version)"
    if ! tool_version_meets_rust_minimum "$cargo_version" ||
        ! tool_version_meets_rust_minimum "$rustc_version"; then
        die "rustup installed Rust older than the required $MINIMUM_RUST_VERSION."
    fi
    log "Installed Rust for '$BUILDER_USER' with rustup: $rustc_version."
}

is_checkout() {
    local directory="$1"

    [[ -f "$directory/Cargo.toml" && -e "$directory/.git" ]]
}

canonical_existing_directory() {
    local directory="$1"

    (cd -- "$directory" && pwd -P)
}

resolve_source_checkout() {
    local requested_source="${WIRE_RELAY_SOURCE_DIR:-}"
    local parent_directory

    if [[ -n "$requested_source" ]]; then
        if [[ -e "$requested_source" ]]; then
            is_checkout "$requested_source" ||
                die "WIRE_RELAY_SOURCE_DIR is not a WireRelay Git checkout: $requested_source"
            SOURCE_DIR="$(canonical_existing_directory "$requested_source")"
            return
        fi
        SOURCE_DIR="$requested_source"
    elif is_checkout "$REPOSITORY_ROOT"; then
        SOURCE_DIR="$REPOSITORY_ROOT"
        return
    else
        SOURCE_DIR="$BUILDER_HOME/wire-relay"
        if [[ -e "$SOURCE_DIR" ]]; then
            is_checkout "$SOURCE_DIR" ||
                die "Default source path exists but is not a WireRelay checkout: $SOURCE_DIR"
            SOURCE_DIR="$(canonical_existing_directory "$SOURCE_DIR")"
            return
        fi
    fi

    [[ "$SOURCE_DIR" = /* ]] ||
        SOURCE_DIR="$(pwd -P)/$SOURCE_DIR"
    parent_directory="$(dirname -- "$SOURCE_DIR")"
    run_as_builder mkdir -p -- "$parent_directory"
    log "Cloning $REPOSITORY_URL into $SOURCE_DIR."
    run_as_builder git clone -- "$REPOSITORY_URL" "$SOURCE_DIR"
    SOURCE_DIR="$(canonical_existing_directory "$SOURCE_DIR")"
}

update_source_checkout() {
    local branch
    local changes
    local upstream

    changes="$(run_as_builder git -C "$SOURCE_DIR" status --porcelain=v1 --untracked-files=normal)"
    [[ -z "$changes" ]] ||
        die "The source checkout has uncommitted or untracked files; update it manually before retrying."

    branch="$(
        run_as_builder git -C "$SOURCE_DIR" symbolic-ref --quiet --short HEAD
    )" || die "The source checkout is detached; switch it to a branch that tracks remote main before upgrading."
    upstream="$(
        run_as_builder git -C "$SOURCE_DIR" rev-parse \
            --abbrev-ref \
            --symbolic-full-name \
            '@{upstream}'
    )" || die "Source branch '$branch' has no upstream; configure it to track remote main before upgrading."
    [[ "$upstream" == */main ]] ||
        die "Source branch '$branch' tracks '$upstream', not remote main; switch to the main release stream before upgrading."

    log "Updating '$branch' from '$upstream' with a fast-forward-only pull."
    run_as_builder git -C "$SOURCE_DIR" pull --ff-only
}

build_and_test() {
    local candidate

    BUILD_TARGET_DIR="$SOURCE_DIR/target"
    log "Building the release binary with all features."
    run_as_builder env CARGO_TARGET_DIR="$BUILD_TARGET_DIR" \
        cargo build --release --all-features --locked --manifest-path "$SOURCE_DIR/Cargo.toml"

    log "Running the complete test suite before installation."
    run_as_builder env CARGO_TARGET_DIR="$BUILD_TARGET_DIR" \
        cargo test --all-features --locked --manifest-path "$SOURCE_DIR/Cargo.toml"

    candidate="$BUILD_TARGET_DIR/release/wire-relay"
    [[ -f "$candidate" && ! -L "$candidate" && -x "$candidate" ]] ||
        die "Cargo did not produce the expected release binary: $candidate"
    run_as_builder "$candidate" --version
}

atomic_install_binary() {
    local source_binary="$1"
    local destination_directory
    local staged_binary

    [[ -f "$source_binary" && ! -L "$source_binary" ]] ||
        {
            warn "Refusing to install an invalid binary: $source_binary"
            return 1
        }
    destination_directory="$(dirname -- "$BINARY_PATH")"
    if ! install -d -o root -g root -m 0755 "$destination_directory"; then
        warn "Failed to prepare binary directory $destination_directory."
        return 1
    fi
    if ! staged_binary="$(mktemp "$destination_directory/.wire-relay.new.XXXXXXXX")"; then
        warn "Failed to create a staging file in $destination_directory."
        return 1
    fi
    register_cleanup_file "$staged_binary"

    if ! install -o root -g root -m 0755 "$source_binary" "$staged_binary"; then
        rm -f -- "$staged_binary"
        warn "Failed to stage the WireRelay binary."
        return 1
    fi
    if ! sync "$staged_binary"; then
        rm -f -- "$staged_binary"
        warn "Failed to flush the staged WireRelay binary."
        return 1
    fi
    if ! mv -f -- "$staged_binary" "$BINARY_PATH"; then
        rm -f -- "$staged_binary"
        warn "Failed to atomically install $BINARY_PATH."
        return 1
    fi
}

save_binary_rollback() {
    local staged_rollback

    [[ -f "$BINARY_PATH" && ! -L "$BINARY_PATH" ]] ||
        die "Cannot save rollback binary: $BINARY_PATH is missing or invalid."
    if [[ -e "$STATE_DIR" && ( ! -d "$STATE_DIR" || -L "$STATE_DIR" ) ]]; then
        die "Refusing unsafe rollback directory: $STATE_DIR"
    fi
    install -d -o root -g root -m 0755 "$STATE_DIR"
    staged_rollback="$(mktemp "$STATE_DIR/.wire-relay.rollback.XXXXXXXX")"
    register_cleanup_file "$staged_rollback"
    if ! install -o root -g root -m 0755 "$BINARY_PATH" "$staged_rollback"; then
        rm -f -- "$staged_rollback"
        die "Failed to stage the rollback binary."
    fi
    sync "$staged_rollback"
    if ! mv -f -- "$staged_rollback" "$ROLLBACK_BINARY_PATH"; then
        rm -f -- "$staged_rollback"
        die "Failed to save the rollback binary."
    fi
}

system_nologin_path() {
    local candidate

    for candidate in /usr/sbin/nologin /sbin/nologin /usr/bin/nologin; do
        if [[ -x "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return
        fi
    done
    printf '%s\n' /bin/false
}

ensure_service_identity() {
    local nologin_shell

    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        groupadd --system "$SERVICE_GROUP"
        log "Created system group '$SERVICE_GROUP'."
    fi

    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        nologin_shell="$(system_nologin_path)"
        useradd \
            --system \
            --gid "$SERVICE_GROUP" \
            --home-dir /nonexistent \
            --no-create-home \
            --shell "$nologin_shell" \
            "$SERVICE_USER"
        log "Created system user '$SERVICE_USER'."
    fi

    if [[ -e "$CONFIG_DIR" && ( ! -d "$CONFIG_DIR" || -L "$CONFIG_DIR" ) ]]; then
        die "Refusing unsafe configuration directory: $CONFIG_DIR"
    fi
    install -d -o root -g "$SERVICE_GROUP" -m 0750 "$CONFIG_DIR"
}

atomic_install_unit() {
    local source_unit="$1"
    local staged_unit

    [[ -f "$source_unit" && ! -L "$source_unit" ]] ||
        {
            warn "Systemd unit is missing or invalid: $source_unit"
            return 1
        }
    if ! staged_unit="$(mktemp "/etc/systemd/system/.wire-relay.service.new.XXXXXXXX")"; then
        warn "Failed to create a staging file in /etc/systemd/system."
        return 1
    fi
    register_cleanup_file "$staged_unit"
    if ! install -o root -g root -m 0644 "$source_unit" "$staged_unit"; then
        rm -f -- "$staged_unit"
        warn "Failed to stage the systemd unit."
        return 1
    fi
    if ! sync "$staged_unit"; then
        rm -f -- "$staged_unit"
        warn "Failed to flush the staged systemd unit."
        return 1
    fi
    if ! mv -f -- "$staged_unit" "$UNIT_PATH"; then
        rm -f -- "$staged_unit"
        warn "Failed to atomically install the systemd unit."
        return 1
    fi
}

save_unit_rollback() {
    local staged_rollback

    UNIT_EXISTED_BEFORE=0
    if [[ ! -e "$UNIT_PATH" ]]; then
        return
    fi
    [[ -f "$UNIT_PATH" && ! -L "$UNIT_PATH" ]] ||
        die "Refusing to back up an invalid systemd unit: $UNIT_PATH"

    UNIT_EXISTED_BEFORE=1
    if [[ -e "$STATE_DIR" && ( ! -d "$STATE_DIR" || -L "$STATE_DIR" ) ]]; then
        die "Refusing unsafe rollback directory: $STATE_DIR"
    fi
    install -d -o root -g root -m 0755 "$STATE_DIR"
    staged_rollback="$(mktemp "$STATE_DIR/.wire-relay.service.rollback.XXXXXXXX")"
    register_cleanup_file "$staged_rollback"
    if ! install -o root -g root -m 0644 "$UNIT_PATH" "$staged_rollback"; then
        rm -f -- "$staged_rollback"
        die "Failed to stage the rollback systemd unit."
    fi
    sync "$staged_rollback"
    if ! mv -f -- "$staged_rollback" "$ROLLBACK_UNIT_PATH"; then
        rm -f -- "$staged_rollback"
        die "Failed to save the rollback systemd unit."
    fi
}

restore_unit_rollback() {
    if (( UNIT_EXISTED_BEFORE == 1 )); then
        [[ -f "$ROLLBACK_UNIT_PATH" ]] ||
            die "Rollback unit is missing: $ROLLBACK_UNIT_PATH"
        atomic_install_unit "$ROLLBACK_UNIT_PATH"
    else
        rm -f -- "$UNIT_PATH"
    fi
}

is_uint_in_range() {
    local value="$1"
    local minimum="$2"
    local maximum="$3"
    local decimal

    [[ "$value" =~ ^[0-9]+$ ]] || return 1
    [[ "$value" == "0" || "$value" != 0* ]] || return 1
    (( ${#value} <= 18 )) || return 1
    decimal=$((10#$value))
    (( decimal >= minimum && decimal <= maximum ))
}

is_port() {
    is_uint_in_range "$1" 1 65535
}

is_ipv4() {
    local address="$1"
    local -a octets
    local octet

    [[ "$address" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    IFS=. read -r -a octets <<<"$address"
    (( ${#octets[@]} == 4 )) || return 1
    for octet in "${octets[@]}"; do
        is_uint_in_range "$octet" 0 255 || return 1
    done
}

is_ipv6() {
    local address="$1"
    local ipv4_tail
    local ipv4_prefix
    local left
    local right
    local group
    local -a groups=()
    local -a ipv4_octets=()
    local group_count=0

    [[ "$address" == *:* ]] || return 1
    if [[ "$address" == *.* ]]; then
        ipv4_tail="${address##*:}"
        is_ipv4 "$ipv4_tail" || return 1
        ipv4_prefix="${address%:*}"
        IFS=. read -r -a ipv4_octets <<<"$ipv4_tail"
        printf -v address '%s:%x%02x:%x%02x' \
            "$ipv4_prefix" \
            "$((10#${ipv4_octets[0]}))" \
            "$((10#${ipv4_octets[1]}))" \
            "$((10#${ipv4_octets[2]}))" \
            "$((10#${ipv4_octets[3]}))"
    fi
    [[ "$address" =~ ^[0-9A-Fa-f:]+$ ]] || return 1
    [[ "$address" != *:::* ]] || return 1

    if [[ "$address" == *::* ]]; then
        left="${address%%::*}"
        right="${address#*::}"
        [[ "$right" != *::* ]] || return 1

        if [[ -n "$left" ]]; then
            IFS=: read -r -a groups <<<"$left"
            for group in "${groups[@]}"; do
                [[ "$group" =~ ^[0-9A-Fa-f]{1,4}$ ]] || return 1
                group_count=$((group_count + 1))
            done
        fi
        if [[ -n "$right" ]]; then
            IFS=: read -r -a groups <<<"$right"
            for group in "${groups[@]}"; do
                [[ "$group" =~ ^[0-9A-Fa-f]{1,4}$ ]] || return 1
                group_count=$((group_count + 1))
            done
        fi
        (( group_count < 8 ))
        return
    fi

    IFS=: read -r -a groups <<<"$address"
    (( ${#groups[@]} == 8 )) || return 1
    for group in "${groups[@]}"; do
        [[ "$group" =~ ^[0-9A-Fa-f]{1,4}$ ]] || return 1
    done
}

is_ip_address() {
    is_ipv4 "$1" || is_ipv6 "$1"
}

is_ipv4_multicast_or_broadcast() {
    local address="$1"
    local -a octets

    is_ipv4 "$address" || return 1
    [[ "$address" == "255.255.255.255" ]] && return 0
    IFS=. read -r -a octets <<<"$address"
    (( 10#${octets[0]} >= 224 && 10#${octets[0]} <= 239 ))
}

is_ipv6_multicast() {
    local address_lower="${1,,}"

    is_ipv6 "$1" || return 1
    [[ "$address_lower" =~ ^ff[0-9a-f]{2}: ]]
}

is_usable_bind_ip() {
    local address="$1"

    is_ip_address "$address" || return 1
    ! is_ipv4_multicast_or_broadcast "$address" &&
        ! is_ipv6_multicast "$address"
}

is_usable_backend_ip() {
    local address="$1"

    is_ip_address "$address" || return 1
    [[ "$address" != "0.0.0.0" ]] &&
        ! is_unspecified_ipv6 "$address" &&
        ! is_ipv4_multicast_or_broadcast "$address" &&
        ! is_ipv6_multicast "$address"
}

is_listener_name() {
    [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$ ]]
}

is_dns_name() {
    local hostname="${1%.}"
    local -a labels
    local label

    (( ${#hostname} >= 1 && ${#hostname} <= 253 )) || return 1
    [[ "$hostname" != *..* ]] || return 1
    IFS=. read -r -a labels <<<"$hostname"
    for label in "${labels[@]}"; do
        (( ${#label} >= 1 && ${#label} <= 63 )) || return 1
        [[ "$label" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]] ||
            return 1
    done
}

is_backend_host() {
    if is_ip_address "$1"; then
        is_usable_backend_ip "$1"
    else
        is_dns_name "$1"
    fi
}

is_duration() {
    local value="$1"
    local amount
    local unit
    local maximum

    [[ "$value" =~ ^([1-9][0-9]*)(ms|s|m|h|d)$ ]] || return 1
    amount="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "$unit" in
        ms)
            maximum=31536000000
            ;;
        s)
            maximum=31536000
            ;;
        m)
            maximum=525600
            ;;
        h)
            maximum=8760
            ;;
        d)
            maximum=365
            ;;
    esac
    is_uint_in_range "$amount" 1 "$maximum"
}

is_idle_duration() {
    local amount
    local unit

    is_duration "$1" || return 1
    [[ "$1" =~ ^([1-9][0-9]*)(ms|s|m|h|d)$ ]] || return 1
    amount="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    [[ "$unit" != "ms" ]] || (( 10#$amount >= 10 ))
}

is_max_datagram_size() {
    is_uint_in_range "$1" 1 65507
}

is_positive_limit() {
    is_uint_in_range "$1" 1 "$MAX_SESSION_LIMIT"
}

is_rate_limit() {
    is_uint_in_range "$1" 1 "$MAX_SESSION_RATE"
}

is_session_datagram_budget() {
    local max_sessions="$1"
    local max_datagram_size="$2"
    local sessions_decimal
    local datagram_decimal

    is_positive_limit "$max_sessions" || return 1
    is_max_datagram_size "$max_datagram_size" || return 1
    sessions_decimal=$((10#$max_sessions))
    datagram_decimal=$((10#$max_datagram_size))
    (( sessions_decimal * (datagram_decimal * SESSION_DATAGRAM_SLOTS + 1) <=
        MAX_SESSION_DATAGRAM_MEMORY_BYTES ))
}

is_metrics_bind() {
    local value="$1"
    local host
    local port

    if [[ "$value" =~ ^\[([^]]+)\]:([0-9]+)$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[2]}"
        is_usable_bind_ip "$host" && is_port "$port"
        return
    fi
    [[ "$value" == *:* && "$value" != *:*:* ]] || return 1
    host="${value%:*}"
    port="${value##*:}"
    is_usable_bind_ip "$host" && is_port "$port"
}

prompt_value() {
    local output_variable="$1"
    local prompt="$2"
    local default_value="$3"
    local validator="$4"
    local error_message="$5"
    local value

    while true; do
        if [[ -n "$default_value" ]]; then
            if ! IFS= read -r -p "$prompt [$default_value]: " value; then
                die "Input ended while reading configuration."
            fi
            value="${value:-$default_value}"
        else
            if ! IFS= read -r -p "$prompt: " value; then
                die "Input ended while reading configuration."
            fi
        fi

        if "$validator" "$value"; then
            printf -v "$output_variable" '%s' "$value"
            return
        fi
        warn "$error_message"
    done
}

confirm() {
    local prompt="$1"
    local default_answer="${2:-n}"
    local answer
    local suffix

    if [[ ! -t 0 ]]; then
        log "$prompt (non-interactive default: $default_answer)"
        [[ "$default_answer" == "y" ]]
        return
    fi

    if [[ "$default_answer" == "y" ]]; then
        suffix="[Y/n]"
    else
        suffix="[y/N]"
    fi

    while true; do
        if ! IFS= read -r -p "$prompt $suffix " answer; then
            return 1
        fi
        answer="${answer:-$default_answer}"
        case "${answer,,}" in
            y | yes)
                return 0
                ;;
            n | no)
                return 1
                ;;
            *)
                warn "Please answer yes or no."
                ;;
        esac
    done
}

require_configuration_terminal() {
    [[ -t 0 && -t 1 ]] ||
        die "The configuration wizard requires an interactive terminal."
}

format_endpoint() {
    local output_variable="$1"
    local host="$2"
    local port="$3"

    if [[ "$host" == *:* ]]; then
        printf -v "$output_variable" '[%s]:%s' "$host" "$port"
    else
        printf -v "$output_variable" '%s:%s' "$host" "$port"
    fi
}

array_contains() {
    local expected="$1"
    shift
    local value

    for value in "$@"; do
        [[ "$value" == "$expected" ]] && return 0
    done
    return 1
}

is_unspecified_ipv6() {
    local without_colons="${1//:/}"

    without_colons="${without_colons//0/}"
    [[ -z "$without_colons" ]]
}

bind_conflicts_with_existing() {
    local bind_ip="$1"
    local bind_port="$2"
    local index
    local existing_ip

    for index in "${!WIZARD_LISTENER_BIND_IPS[@]}"; do
        [[ "${WIZARD_LISTENER_BIND_PORTS[$index]}" == "$bind_port" ]] || continue
        existing_ip="${WIZARD_LISTENER_BIND_IPS[$index]}"
        if [[ "$existing_ip" == "$bind_ip" ]]; then
            return 0
        fi
        if is_ipv4 "$existing_ip" && is_ipv4 "$bind_ip" &&
            [[ "$existing_ip" == "0.0.0.0" || "$bind_ip" == "0.0.0.0" ]]; then
            return 0
        fi
        if is_ipv6 "$existing_ip" && is_ipv6 "$bind_ip" &&
            { is_unspecified_ipv6 "$existing_ip" || is_unspecified_ipv6 "$bind_ip"; }; then
            return 0
        fi
        if is_ipv6 "$existing_ip" && is_unspecified_ipv6 "$existing_ip" &&
            is_ipv4 "$bind_ip"; then
            return 0
        fi
        if is_ipv6 "$bind_ip" && is_unspecified_ipv6 "$bind_ip" &&
            is_ipv4 "$existing_ip"; then
            return 0
        fi
    done
    return 1
}

write_configuration_candidate() {
    local destination="$1"
    local idle_timeout="$2"
    local max_sessions="$3"
    local max_sessions_per_ip="$4"
    local new_sessions_per_second="$5"
    local max_datagram_size="$6"
    local metrics_enabled="$7"
    local metrics_bind="$8"
    local index

    {
        printf '[service]\n'
        printf 'control_socket = "/run/wire-relay/control.sock"\n'
        printf 'log_level = "info"\n'
        printf 'idle_timeout = "%s"\n' "$idle_timeout"
        printf 'max_datagram_size = %s\n' "$max_datagram_size"
        printf 'max_sessions = %s\n' "$max_sessions"
        printf 'max_sessions_per_ip = %s\n' "$max_sessions_per_ip"
        printf 'new_sessions_per_second = %s\n' "$new_sessions_per_second"
        printf 'dns_refresh_interval = "60s"\n'
        printf 'shutdown_timeout = "10s"\n\n'
        printf '[metrics]\n'
        printf 'enabled = %s\n' "$metrics_enabled"
        printf 'bind = "%s"\n' "$metrics_bind"

        for index in "${!WIZARD_LISTENER_NAMES[@]}"; do
            printf '\n[[listeners]]\n'
            printf 'name = "%s"\n' "${WIZARD_LISTENER_NAMES[$index]}"
            printf 'bind = "%s"\n' "${WIZARD_LISTENER_BINDS[$index]}"
            printf 'backend = "%s"\n' "${WIZARD_LISTENER_BACKENDS[$index]}"
        done
    } >"$destination"
}

backup_existing_config() {
    local timestamp

    LAST_CONFIG_BACKUP=""
    if [[ ! -e "$CONFIG_PATH" ]]; then
        return
    fi
    [[ -f "$CONFIG_PATH" && ! -L "$CONFIG_PATH" ]] ||
        die "Refusing to replace a non-regular or symbolic-link config: $CONFIG_PATH"

    timestamp="$(date '+%Y%m%d-%H%M%S')"
    LAST_CONFIG_BACKUP="$CONFIG_PATH.backup-$timestamp"
    [[ ! -e "$LAST_CONFIG_BACKUP" ]] ||
        die "Backup path already exists: $LAST_CONFIG_BACKUP"
    install -o root -g root -m 0600 "$CONFIG_PATH" "$LAST_CONFIG_BACKUP"
    log "Backed up the previous configuration to $LAST_CONFIG_BACKUP."
}

restore_previous_config() {
    local failed=0
    local staged_config

    if (( CONFIG_CHANGED == 0 && CONFIG_METADATA_CHANGED == 0 )); then
        return
    fi

    if (( CONFIG_CHANGED == 1 )); then
        if (( CONFIG_EXISTED_BEFORE == 1 )); then
            if [[ -z "$LAST_CONFIG_BACKUP" ||
                ! -f "$LAST_CONFIG_BACKUP" ||
                -L "$LAST_CONFIG_BACKUP" ]]; then
                warn "The previous configuration backup is missing or invalid."
                return 1
            fi
            if ! staged_config="$(
                mktemp "$CONFIG_DIR/.config.toml.restore.XXXXXXXX"
            )"; then
                warn "Could not create a staged configuration restore file."
                return 1
            fi
            register_cleanup_file "$staged_config"
            if ! install -o root -g "$SERVICE_GROUP" -m 0640 \
                "$LAST_CONFIG_BACKUP" "$staged_config"; then
                rm -f -- "$staged_config" || true
                warn "Could not stage the previous configuration."
                return 1
            fi
            if ! sync "$staged_config"; then
                rm -f -- "$staged_config" || true
                warn "Could not flush the staged configuration restore."
                return 1
            fi
            if ! mv -f -- "$staged_config" "$CONFIG_PATH"; then
                rm -f -- "$staged_config" || true
                warn "Could not atomically restore the previous configuration."
                return 1
            fi
            warn "Restored the previous configuration from $LAST_CONFIG_BACKUP."
        elif ! rm -f -- "$CONFIG_PATH"; then
            warn "Could not remove the newly installed configuration."
            return 1
        else
            warn "Removed the new configuration because applying it failed."
        fi
    fi

    if (( CONFIG_METADATA_CHANGED == 1 )); then
        if [[ ! -f "$CONFIG_PATH" || -L "$CONFIG_PATH" ]]; then
            warn "Cannot restore metadata on the missing or invalid configuration."
            failed=1
        else
            if ! chown "$CONFIG_ORIGINAL_UID:$CONFIG_ORIGINAL_GID" \
                "$CONFIG_PATH"; then
                warn "Could not restore the previous configuration ownership."
                failed=1
            fi
            if ! chmod "$CONFIG_ORIGINAL_MODE" "$CONFIG_PATH"; then
                warn "Could not restore the previous configuration mode."
                failed=1
            fi
        fi
    fi

    if (( failed == 0 )); then
        CONFIG_CHANGED=0
        CONFIG_METADATA_CHANGED=0
    fi
    return "$failed"
}

rollback_install_transaction() {
    local failed=0
    local should_disable=0

    (( INSTALL_TRANSACTION_ACTIVE == 1 )) || return 0
    INSTALL_TRANSACTION_ACTIVE=0
    warn "Installation did not complete; restoring the prior service state."

    if systemctl is-active --quiet "$SERVICE_NAME.service" 2>/dev/null ||
        systemctl is-enabled --quiet "$SERVICE_NAME.service" 2>/dev/null ||
        [[ -e "$UNIT_PATH" ]]; then
        should_disable=1
    fi
    if (( should_disable == 1 )) &&
        ! systemctl disable --now "$SERVICE_NAME.service"; then
        warn "Could not stop and disable the partially installed service."
        failed=1
    fi

    if (( INSTALL_HAD_OLD_BINARY == 1 )); then
        if [[ ! -f "$ROLLBACK_BINARY_PATH" ||
            -L "$ROLLBACK_BINARY_PATH" ]] ||
            ! atomic_install_binary "$ROLLBACK_BINARY_PATH"; then
            warn "Could not restore the previous WireRelay binary."
            failed=1
        fi
    elif ! rm -f -- "$BINARY_PATH"; then
        warn "Could not remove the newly installed WireRelay binary."
        failed=1
    fi

    if (( UNIT_EXISTED_BEFORE == 1 )); then
        if [[ ! -f "$ROLLBACK_UNIT_PATH" ||
            -L "$ROLLBACK_UNIT_PATH" ]] ||
            ! atomic_install_unit "$ROLLBACK_UNIT_PATH"; then
            warn "Could not restore the previous systemd unit."
            failed=1
        fi
    elif ! rm -f -- "$UNIT_PATH"; then
        warn "Could not remove the newly installed systemd unit."
        failed=1
    fi

    if ! restore_previous_config; then
        warn "Could not restore the previous configuration."
        failed=1
    fi

    if (( INSTALL_CONFIG_DIR_EXISTED_BEFORE == 1 )); then
        if [[ ! -d "$CONFIG_DIR" || -L "$CONFIG_DIR" ]]; then
            warn "Cannot restore metadata on the missing or invalid configuration directory."
            failed=1
        else
            if ! chown \
                "$INSTALL_CONFIG_DIR_ORIGINAL_UID:$INSTALL_CONFIG_DIR_ORIGINAL_GID" \
                "$CONFIG_DIR"; then
                warn "Could not restore the previous configuration-directory ownership."
                failed=1
            fi
            if ! chmod "$INSTALL_CONFIG_DIR_ORIGINAL_MODE" "$CONFIG_DIR"; then
                warn "Could not restore the previous configuration-directory mode."
                failed=1
            fi
        fi
    elif [[ -e "$CONFIG_DIR" ]]; then
        if [[ ! -d "$CONFIG_DIR" || -L "$CONFIG_DIR" ]] ||
            ! rmdir -- "$CONFIG_DIR"; then
            warn "Could not remove the configuration directory created by this attempt."
            failed=1
        fi
    fi

    if (( INSTALL_SERVICE_USER_EXISTED_BEFORE == 0 )) &&
        id "$SERVICE_USER" >/dev/null 2>&1 &&
        ! userdel "$SERVICE_USER"; then
        warn "Could not remove the service user created by this attempt."
        failed=1
    fi
    if (( INSTALL_SERVICE_GROUP_EXISTED_BEFORE == 0 )) &&
        getent group "$SERVICE_GROUP" >/dev/null 2>&1 &&
        ! groupdel "$SERVICE_GROUP"; then
        warn "Could not remove the service group created by this attempt."
        failed=1
    fi

    if ! systemctl daemon-reload; then
        warn "Could not reload systemd after restoring the prior unit."
        failed=1
    fi
    if (( INSTALL_SERVICE_WAS_ENABLED == 1 )) &&
        ! systemctl enable "$SERVICE_NAME.service"; then
        warn "Could not restore the service's enabled state."
        failed=1
    fi

    if (( INSTALL_SERVICE_WAS_ACTIVE == 1 &&
        INSTALL_HAD_OLD_BINARY == 1 &&
        UNIT_EXISTED_BEFORE == 1 &&
        INSTALL_CONFIG_EXISTED_BEFORE == 1 )); then
        warn "Attempting to restart the service with the restored installation."
        if ! restart_and_verify_service "$INSTALL_OLD_VERSION"; then
            warn "The restored service did not pass its readiness check."
            failed=1
        fi
    fi

    if (( failed == 0 )); then
        warn "The files and service state from before this attempt were restored."
    fi
    return "$failed"
}

snapshot_install_identity_state() {
    local directory_metadata

    INSTALL_SERVICE_USER_EXISTED_BEFORE=0
    INSTALL_SERVICE_GROUP_EXISTED_BEFORE=0
    INSTALL_CONFIG_DIR_EXISTED_BEFORE=0
    INSTALL_CONFIG_DIR_ORIGINAL_UID=""
    INSTALL_CONFIG_DIR_ORIGINAL_GID=""
    INSTALL_CONFIG_DIR_ORIGINAL_MODE=""

    if id "$SERVICE_USER" >/dev/null 2>&1; then
        INSTALL_SERVICE_USER_EXISTED_BEFORE=1
    fi
    if getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        INSTALL_SERVICE_GROUP_EXISTED_BEFORE=1
    fi
    if [[ -e "$CONFIG_DIR" ]]; then
        [[ -d "$CONFIG_DIR" && ! -L "$CONFIG_DIR" ]] ||
            die "Refusing unsafe configuration directory: $CONFIG_DIR"
        if ! directory_metadata="$(stat --format='%u:%g:%a' -- "$CONFIG_DIR")"; then
            die "Could not inspect existing configuration-directory metadata."
        fi
        IFS=: read -r \
            INSTALL_CONFIG_DIR_ORIGINAL_UID \
            INSTALL_CONFIG_DIR_ORIGINAL_GID \
            INSTALL_CONFIG_DIR_ORIGINAL_MODE <<<"$directory_metadata"
        INSTALL_CONFIG_DIR_EXISTED_BEFORE=1
    fi
}

configuration_wizard() {
    local bind_ip
    local listener_name
    local local_port
    local backend_host
    local backend_port
    local bind_endpoint
    local backend_endpoint
    local idle_timeout
    local max_sessions
    local max_sessions_per_ip
    local new_sessions_per_second
    local max_datagram_size
    local metrics_bind="127.0.0.1:9090"
    local metrics_enabled="false"
    local next_port=40001
    local candidate_config
    local config_metadata

    require_configuration_terminal
    [[ -x "$BINARY_PATH" ]] ||
        die "The installed WireRelay binary is required for configuration validation."
    ensure_service_identity

    CONFIG_CHANGED=0
    CONFIG_EXISTED_BEFORE=0
    CONFIG_METADATA_CHANGED=0
    CONFIG_ORIGINAL_UID=""
    CONFIG_ORIGINAL_GID=""
    CONFIG_ORIGINAL_MODE=""
    LAST_CONFIG_BACKUP=""
    WIZARD_LISTENER_NAMES=()
    WIZARD_LISTENER_BINDS=()
    WIZARD_LISTENER_BACKENDS=()
    WIZARD_LISTENER_BIND_IPS=()
    WIZARD_LISTENER_BIND_PORTS=()
    if [[ -e "$CONFIG_PATH" ]]; then
        CONFIG_EXISTED_BEFORE=1
        [[ -f "$CONFIG_PATH" && ! -L "$CONFIG_PATH" ]] ||
            die "Existing configuration is not a regular file: $CONFIG_PATH"
        if ! config_metadata="$(stat --format='%u:%g:%a' -- "$CONFIG_PATH")"; then
            die "Could not inspect the existing configuration metadata."
        fi
        IFS=: read -r \
            CONFIG_ORIGINAL_UID \
            CONFIG_ORIGINAL_GID \
            CONFIG_ORIGINAL_MODE <<<"$config_metadata"
        if "$BINARY_PATH" check-config --config "$CONFIG_PATH" >/dev/null 2>&1; then
            if ! confirm "A valid configuration already exists. Replace it?" "n"; then
                CONFIG_METADATA_CHANGED=1
                if ! chown root:"$SERVICE_GROUP" "$CONFIG_PATH"; then
                    die "Could not set configuration ownership."
                fi
                if ! chmod 0640 "$CONFIG_PATH"; then
                    die "Could not set configuration mode."
                fi
                log "Retaining the existing valid configuration."
                log "Normalized its ownership and mode for the '$SERVICE_USER' service account."
                return
            fi
        else
            warn "The existing configuration is invalid:"
            "$BINARY_PATH" check-config --config "$CONFIG_PATH" || true
            confirm "Replace the invalid configuration?" "n" ||
                die "Configuration was not changed."
        fi
    fi

    log "Starting the interactive configuration wizard."
    while true; do
        prompt_value listener_name \
            "Listener name" "relay-$(( ${#WIZARD_LISTENER_NAMES[@]} + 1 ))" \
            is_listener_name \
            "Use 1-64 letters, digits, dots, underscores, or hyphens; start with a letter or digit."
        if array_contains "$listener_name" "${WIZARD_LISTENER_NAMES[@]}"; then
            warn "Listener names must be unique."
            continue
        fi

        prompt_value bind_ip \
            "Bind IP address" "0.0.0.0" \
            is_usable_bind_ip \
            "Enter a non-multicast, non-broadcast IPv4 or IPv6 address (not a hostname)."

        while true; do
            prompt_value local_port \
                "Local UDP port" "$next_port" \
                is_port \
                "Enter an integer from 1 through 65535."
            format_endpoint bind_endpoint "$bind_ip" "$local_port"
            if bind_conflicts_with_existing "$bind_ip" "$local_port"; then
                warn "That bind conflicts with another listener on UDP port $local_port."
            else
                break
            fi
        done

        prompt_value backend_host \
            "Backend hostname or IP address" "" \
            is_backend_host \
            "Enter a DNS hostname or a unicast, non-unspecified IP address."
        prompt_value backend_port \
            "Backend UDP port" "51820" \
            is_port \
            "Enter an integer from 1 through 65535."
        format_endpoint backend_endpoint "$backend_host" "$backend_port"

        WIZARD_LISTENER_NAMES+=("$listener_name")
        WIZARD_LISTENER_BINDS+=("$bind_endpoint")
        WIZARD_LISTENER_BACKENDS+=("$backend_endpoint")
        WIZARD_LISTENER_BIND_IPS+=("$bind_ip")
        WIZARD_LISTENER_BIND_PORTS+=("$local_port")
        if (( 10#$local_port < 65535 )); then
            next_port=$((10#$local_port + 1))
        fi

        confirm "Add another listener?" "n" || break
    done

    prompt_value idle_timeout \
        "Idle timeout" "180s" \
        is_idle_duration \
        "Use at least 10ms and at most 365d, ending in ms, s, m, h, or d."
    prompt_value max_sessions \
        "Maximum global sessions" "10000" \
        is_positive_limit \
        "Enter an integer from 1 through 100000."

    while true; do
        prompt_value max_sessions_per_ip \
            "Maximum sessions per source IP" "64" \
            is_positive_limit \
            "Enter an integer from 1 through 100000."
        if (( 10#$max_sessions_per_ip <= 10#$max_sessions )); then
            break
        fi
        warn "The per-IP session limit cannot exceed the global session limit."
    done

    prompt_value new_sessions_per_second \
        "Maximum new sessions per second" "100" \
        is_rate_limit \
        "Enter an integer from 1 through 100000."
    while true; do
        prompt_value max_datagram_size \
            "Maximum UDP datagram size" "4096" \
            is_max_datagram_size \
            "Enter an integer from 1 through 65507."
        if is_session_datagram_budget "$max_sessions" "$max_datagram_size"; then
            break
        fi
        warn "That datagram size and session limit exceed the defensive 4 GiB buffering budget."
        warn "Choose a smaller maximum datagram size, or restart the wizard with fewer sessions."
    done

    if confirm "Enable Prometheus metrics?" "n"; then
        metrics_enabled="true"
        prompt_value metrics_bind \
            "Prometheus bind address" "127.0.0.1:9090" \
            is_metrics_bind \
            "Enter a non-multicast, non-broadcast IP socket address such as 127.0.0.1:9090 or [::1]:9090."
    fi

    candidate_config="$(mktemp "$CONFIG_DIR/.config.toml.new.XXXXXXXX")"
    register_cleanup_file "$candidate_config"
    write_configuration_candidate \
        "$candidate_config" \
        "$idle_timeout" \
        "$max_sessions" \
        "$max_sessions_per_ip" \
        "$new_sessions_per_second" \
        "$max_datagram_size" \
        "$metrics_enabled" \
        "$metrics_bind"
    chown root:"$SERVICE_GROUP" "$candidate_config"
    chmod 0640 "$candidate_config"

    printf '\nProposed configuration:\n\n'
    sed 's/^/    /' "$candidate_config"
    printf '\n'

    "$BINARY_PATH" check-config --config "$candidate_config"
    confirm "Write this configuration to $CONFIG_PATH?" "y" || {
        rm -f -- "$candidate_config"
        die "Configuration was not changed."
    }

    backup_existing_config
    sync "$candidate_config"
    if (( CONFIG_EXISTED_BEFORE == 1 )); then
        CONFIG_METADATA_CHANGED=1
    fi
    CONFIG_CHANGED=1
    if ! mv -f -- "$candidate_config" "$CONFIG_PATH"; then
        rm -f -- "$candidate_config"
        die "Failed to atomically install the configuration."
    fi
    chown root:"$SERVICE_GROUP" "$CONFIG_PATH"
    chmod 0640 "$CONFIG_PATH"
    log "Installed and validated $CONFIG_PATH."
}

restart_and_verify_service() {
    local expected_binary_version="${1:-}"
    local _
    local stable_checks=0

    if ! systemctl restart "$SERVICE_NAME.service"; then
        return 1
    fi

    for _ in {1..12}; do
        sleep 1
        if systemctl is-active --quiet "$SERVICE_NAME.service" &&
            running_daemon_matches "$expected_binary_version"; then
            stable_checks=$((stable_checks + 1))
            if (( stable_checks >= 2 )); then
                return 0
            fi
        else
            stable_checks=0
        fi
    done
    return 1
}

daemon_version_matches_binary() {
    local daemon_version="$1"
    local binary_version="$2"

    [[ "$daemon_version" == "${binary_version} (control protocol "*")" ]]
}

running_daemon_matches() {
    local expected_binary_version="${1:-}"
    local daemon_version

    daemon_version="$(
        "$BINARY_PATH" --config "$CONFIG_PATH" version 2>/dev/null
    )" || return 1
    [[ -z "$expected_binary_version" ]] ||
        daemon_version_matches_binary "$daemon_version" "$expected_binary_version"
}

show_service_diagnostics() {
    warn "Recent service diagnostics follow."
    systemctl --no-pager --full status "$SERVICE_NAME.service" || true
    journalctl --no-pager -u "$SERVICE_NAME.service" -n 50 || true
}

show_examples() {
    cat <<'EOF'

Useful commands:
  wire-relay --version
  sudo wire-relay version
  sudo wire-relay show
  sudo wire-relay listeners
  sudo wire-relay sessions
  sudo wire-relay stats
  sudo wire-relay reload
  sudo systemctl status wire-relay
  sudo journalctl -u wire-relay -f
EOF
}

install_command() {
    local candidate
    local candidate_version

    require_root
    detect_platform
    require_systemd
    INSTALL_TRANSACTION_ACTIVE=0
    INSTALL_HAD_OLD_BINARY=0
    INSTALL_CONFIG_EXISTED_BEFORE=0
    INSTALL_SERVICE_WAS_ACTIVE=0
    INSTALL_SERVICE_WAS_ENABLED=0
    INSTALL_OLD_VERSION=""
    INSTALL_SERVICE_USER_EXISTED_BEFORE=0
    INSTALL_SERVICE_GROUP_EXISTED_BEFORE=0
    INSTALL_CONFIG_DIR_EXISTED_BEFORE=0
    INSTALL_CONFIG_DIR_ORIGINAL_UID=""
    INSTALL_CONFIG_DIR_ORIGINAL_GID=""
    INSTALL_CONFIG_DIR_ORIGINAL_MODE=""
    if systemctl is-active --quiet "$SERVICE_NAME.service" 2>/dev/null; then
        INSTALL_SERVICE_WAS_ACTIVE=1
    fi
    if systemctl is-enabled --quiet "$SERVICE_NAME.service" 2>/dev/null; then
        INSTALL_SERVICE_WAS_ENABLED=1
    fi
    if [[ -e "$CONFIG_PATH" ]]; then
        [[ -f "$CONFIG_PATH" && ! -L "$CONFIG_PATH" ]] ||
            die "Refusing to use an invalid existing config: $CONFIG_PATH"
        INSTALL_CONFIG_EXISTED_BEFORE=1
    fi
    select_builder
    install_system_packages
    ensure_rust
    resolve_source_checkout
    build_and_test
    candidate="$BUILD_TARGET_DIR/release/wire-relay"
    candidate_version="$(run_as_builder "$candidate" --version)"
    [[ -n "$candidate_version" ]] ||
        die "The candidate binary returned an empty version."

    if [[ -e "$BINARY_PATH" ]]; then
        [[ -f "$BINARY_PATH" && ! -L "$BINARY_PATH" ]] ||
            die "Refusing to replace an invalid installed binary: $BINARY_PATH"
        INSTALL_OLD_VERSION="$("$BINARY_PATH" --version)"
        save_binary_rollback
        INSTALL_HAD_OLD_BINARY=1
    fi
    save_unit_rollback
    snapshot_install_identity_state
    INSTALL_TRANSACTION_ACTIVE=1
    ensure_service_identity
    atomic_install_binary "$candidate"
    printf '%s\n' "$candidate_version"

    atomic_install_unit "$SOURCE_DIR/packaging/systemd/wire-relay.service"
    configuration_wizard
    "$BINARY_PATH" check-config --config "$CONFIG_PATH"

    systemctl daemon-reload
    systemctl enable "$SERVICE_NAME.service"
    if ! restart_and_verify_service "$candidate_version"; then
        show_service_diagnostics
        die "WireRelay did not pass its control-plane readiness check."
    fi

    INSTALL_TRANSACTION_ACTIVE=0
    log "WireRelay is installed, enabled, and active."
    systemctl --no-pager --full status "$SERVICE_NAME.service" || true
    show_examples
}

upgrade_command() {
    local candidate
    local installed_binary_version
    local installed_version
    local candidate_binary_version
    local candidate_version
    local restored_binary_version
    local version_comparison

    require_root
    detect_platform
    require_systemd
    select_builder
    [[ -x "$BINARY_PATH" && -f "$BINARY_PATH" && ! -L "$BINARY_PATH" ]] ||
        die "WireRelay is not installed; run the install command first."
    [[ -f "$CONFIG_PATH" && ! -L "$CONFIG_PATH" ]] ||
        die "Existing configuration is missing or invalid: $CONFIG_PATH"

    installed_binary_version="$("$BINARY_PATH" --version)"
    installed_version="$(parse_wire_relay_version "$installed_binary_version")" ||
        die "The installed binary returned an invalid version: $installed_binary_version"
    log "Installed version: $installed_version"
    install_system_packages
    ensure_rust
    resolve_source_checkout
    update_source_checkout
    build_and_test
    candidate="$BUILD_TARGET_DIR/release/wire-relay"
    candidate_binary_version="$(run_as_builder "$candidate" --version)"
    candidate_version="$(parse_wire_relay_version "$candidate_binary_version")" ||
        die "The candidate binary returned an invalid version: $candidate_binary_version"
    log "Candidate version: $candidate_version"
    version_comparison="$(
        compare_semantic_versions "$candidate_version" "$installed_version"
    )" || die "Could not compare installed and candidate versions."

    case "$version_comparison" in
        -1)
            die "Refusing to downgrade WireRelay from $installed_version to $candidate_version."
            ;;
        0 | 1)
            ;;
        *)
            die "Internal version comparison returned an invalid result: $version_comparison"
            ;;
    esac

    log "Validating the existing configuration with the new binary."
    "$candidate" check-config --config "$CONFIG_PATH"

    if [[ "$version_comparison" == "0" ]]; then
        if systemctl is-active --quiet "$SERVICE_NAME.service" &&
            running_daemon_matches "$installed_binary_version"; then
            log "WireRelay $installed_version is already installed and running; no binary replacement or restart is needed."
            return
        fi
        log "WireRelay $installed_version is installed, but the running service does not match; repairing the installation."
    fi

    ensure_service_identity
    save_binary_rollback
    save_unit_rollback

    if atomic_install_binary "$candidate" &&
        atomic_install_unit "$SOURCE_DIR/packaging/systemd/wire-relay.service" &&
        systemctl daemon-reload &&
        restart_and_verify_service "$candidate_binary_version"; then
        log "Upgrade succeeded."
        printf '  Previous: %s\n' "$installed_version"
        printf '  Installed: %s\n' "$candidate_version"
        log "Rollback binary retained at $ROLLBACK_BINARY_PATH."
        return
    fi

    show_service_diagnostics
    warn "The upgraded service failed; restoring the previous binary and unit."
    atomic_install_binary "$ROLLBACK_BINARY_PATH" ||
        die "Could not restore the rollback binary."
    restore_unit_rollback ||
        die "Could not restore the rollback systemd unit."
    systemctl daemon-reload ||
        die "Could not reload systemd after restoring the previous unit."
    restored_binary_version="$("$BINARY_PATH" --version)"
    if [[ "$restored_binary_version" != "$installed_binary_version" ]]; then
        die "Rollback binary verification failed (expected '$installed_binary_version', got '$restored_binary_version')."
    fi

    if restart_and_verify_service "$installed_binary_version"; then
        printf '  Failed upgrade: %s\n' "$candidate_version" >&2
        printf '  Rolled back to: %s\n' "$installed_version" >&2
        die "Upgrade failed; automatic rollback succeeded."
    fi

    show_service_diagnostics
    die "Upgrade failed, and the restored version also failed to start. Inspect the diagnostics above."
}

configure_command() {
    require_root
    require_systemd
    [[ -x "$BINARY_PATH" ]] ||
        die "WireRelay is not installed; run the install command first."

    configuration_wizard
    if (( CONFIG_CHANGED == 0 )); then
        return
    fi
    "$BINARY_PATH" check-config --config "$CONFIG_PATH"

    if systemctl is-active --quiet "$SERVICE_NAME.service"; then
        log "Applying the configuration transactionally through the running daemon."
        if "$BINARY_PATH" reload; then
            log "Configuration applied successfully."
        else
            restore_previous_config
            die "The daemon rejected the reload; the on-disk configuration was restored."
        fi
    else
        log "Configuration is valid. The service is inactive; start it with:"
        printf '  sudo systemctl start %s\n' "$SERVICE_NAME"
    fi
}

find_source_for_uninstall() {
    local candidate

    if [[ -n "${WIRE_RELAY_SOURCE_DIR:-}" ]]; then
        candidate="$WIRE_RELAY_SOURCE_DIR"
    elif is_checkout "$REPOSITORY_ROOT"; then
        candidate="$REPOSITORY_ROOT"
    else
        candidate="$BUILDER_HOME/wire-relay"
    fi

    if [[ -e "$candidate" ]] && is_checkout "$candidate"; then
        canonical_existing_directory "$candidate"
    fi
    return 0
}

safe_remove_source_checkout() {
    local source="$1"
    local canonical_source

    [[ -d "$source" && ! -L "$source" ]] ||
        die "Refusing to remove an invalid source directory: $source"
    is_checkout "$source" ||
        die "Refusing to remove a directory that is not a WireRelay checkout: $source"
    canonical_source="$(canonical_existing_directory "$source")"
    [[ "$canonical_source" = /* &&
        "$canonical_source" != "/" &&
        "$canonical_source" != "$BUILDER_HOME" &&
        "$canonical_source" != "/usr" &&
        "$canonical_source" != "/usr/local" ]] ||
        die "Refusing unsafe source removal target: $canonical_source"

    rm -rf --one-file-system -- "$canonical_source"
    log "Removed source checkout $canonical_source."
}

uninstall_command() {
    local source_checkout=""

    require_root
    require_systemd
    select_builder

    if [[ -e "$STATE_DIR" && ( ! -d "$STATE_DIR" || -L "$STATE_DIR" ) ]]; then
        die "Refusing unsafe rollback directory during uninstall: $STATE_DIR"
    fi
    if systemctl is-active --quiet "$SERVICE_NAME.service" ||
        systemctl is-enabled --quiet "$SERVICE_NAME.service" ||
        [[ -e "$UNIT_PATH" ]]; then
        if ! systemctl disable --now "$SERVICE_NAME.service"; then
            warn "The service could not be stopped or disabled cleanly."
        fi
    fi

    rm -f -- "$UNIT_PATH"
    rm -f -- "$BINARY_PATH"
    rm -f -- "$ROLLBACK_BINARY_PATH" "$ROLLBACK_UNIT_PATH"
    if [[ -d "$STATE_DIR" ]]; then
        rmdir --ignore-fail-on-non-empty "$STATE_DIR"
    fi
    systemctl daemon-reload
    systemctl reset-failed "$SERVICE_NAME.service" >/dev/null 2>&1 || true
    log "Removed the systemd unit and installed WireRelay binaries."

    if [[ -e "$CONFIG_DIR" ]]; then
        if confirm "Remove $CONFIG_DIR and all saved configuration backups?" "n"; then
            [[ "$CONFIG_DIR" == "/etc/wire-relay" && -d "$CONFIG_DIR" && ! -L "$CONFIG_DIR" ]] ||
                die "Refusing unsafe configuration removal target: $CONFIG_DIR"
            rm -rf --one-file-system -- "$CONFIG_DIR"
            log "Removed $CONFIG_DIR; this cannot be recovered by the script."
        else
            log "Retained $CONFIG_DIR."
        fi
    fi

    if id "$SERVICE_USER" >/dev/null 2>&1; then
        if confirm "Remove the '$SERVICE_USER' system user and group?" "n"; then
            userdel "$SERVICE_USER"
            if getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
                if ! groupdel "$SERVICE_GROUP"; then
                    warn "The '$SERVICE_GROUP' group is still in use and was retained."
                fi
            fi
            log "Removed the WireRelay system user and group."
        else
            log "Retained the WireRelay system user and group."
        fi
    fi

    if run_as_builder bash -c 'command -v rustup >/dev/null 2>&1'; then
        if confirm "Remove the rustup-managed Rust installation for '$BUILDER_USER'?" "n"; then
            run_as_builder rustup self uninstall -y
            log "Removed the rustup-managed Rust installation for '$BUILDER_USER'."
        else
            log "Retained Rust."
        fi
    else
        log "No rustup-managed Rust installation was found; Rust was not changed."
    fi

    source_checkout="$(find_source_for_uninstall)"
    if [[ -n "$source_checkout" ]]; then
        if confirm "Permanently remove source checkout '$source_checkout'?" "n"; then
            safe_remove_source_checkout "$source_checkout"
        else
            log "Retained source checkout $source_checkout."
        fi
    fi

    log "WireRelay uninstall completed."
}

status_command() {
    local status=0

    command -v systemctl >/dev/null 2>&1 ||
        die "systemctl is not available on this host."
    if [[ -x "$BINARY_PATH" ]]; then
        "$BINARY_PATH" --version
    else
        warn "$BINARY_PATH is not installed."
        status=1
    fi

    if ! systemctl --no-pager --full status "$SERVICE_NAME.service"; then
        status=1
    fi
    if (( status == 0 )); then
        if ! "$BINARY_PATH" show; then
            warn "The service is active, but its local control plane did not answer."
            status=1
        fi
    fi

    if (( status != 0 )); then
        exit "$status"
    fi
}

interactive_menu() {
    local choice

    [[ -t 0 && -t 1 ]] || {
        usage
        die "No command was supplied and no interactive terminal is available."
    }

    printf '%s\n\n' "$PROGRAM_NAME"
    PS3="Choose an action: "
    select choice in Install Upgrade Configure Uninstall Status Quit; do
        case "$choice" in
            Install)
                SELECTED_COMMAND="install"
                return
                ;;
            Upgrade)
                SELECTED_COMMAND="upgrade"
                return
                ;;
            Configure)
                SELECTED_COMMAND="configure"
                return
                ;;
            Uninstall)
                SELECTED_COMMAND="uninstall"
                return
                ;;
            Status)
                SELECTED_COMMAND="status"
                return
                ;;
            Quit)
                SELECTED_COMMAND="quit"
                return
                ;;
            *)
                warn "Choose a number from 1 through 6."
                ;;
        esac
    done
    SELECTED_COMMAND="quit"
}

main() {
    local command_name

    if (( $# > 1 )); then
        usage
        die "Expected at most one command."
    fi

    if (( $# == 0 )); then
        interactive_menu
        command_name="$SELECTED_COMMAND"
    else
        command_name="$1"
    fi

    case "$command_name" in
        install | upgrade | update | configure | uninstall)
            require_root
            acquire_operation_lock
            ;;
    esac

    case "$command_name" in
        install)
            install_command
            ;;
        upgrade | update)
            upgrade_command
            ;;
        configure)
            configure_command
            ;;
        uninstall)
            uninstall_command
            ;;
        status)
            status_command
            ;;
        help | --help | -h)
            usage
            ;;
        quit)
            ;;
        *)
            usage
            die "Unknown command: $command_name"
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
