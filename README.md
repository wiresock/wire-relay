# WireRelay

WireRelay is a bounded, payload-opaque UDP relay intended primarily for
WireGuard and AmneziaWG deployments. A client sends UDP datagrams to a local
WireRelay listener, and WireRelay forwards each datagram unchanged to that
listener's fixed backend.

```text
WireGuard / AmneziaWG client
             |
             | UDP
             v
     WireRelay listener
             |
             | unchanged UDP datagrams
             v
  configured remote endpoint
```

WireRelay does not decrypt, parse, classify, re-encode, merge, or split
payloads. Modified handshake headers, padding, junk packets, CPS packets, and
DNS/QUIC/SIP/STUN imitation packets are therefore opaque data to the relay.
The same design can carry other datagram protocols, provided their datagrams
fit the configured size limit.

Here, *transparent* describes payload handling. WireRelay is an explicit UDP
endpoint, not a Linux TPROXY implementation: the backend sees traffic from
WireRelay's dedicated upstream socket, not the original client address.

WireRelay provides local UDP forwarding; it does not claim to bypass every
kind of network restriction. Connectivity still depends on the client-to-relay
path, the relay-to-backend path, and the endpoint configuration at both ends.

## Design

Each listener has exactly one configured backend. A session is identified by
the listener plus the client's source IP and port:

```text
(listener ID, client IP, client port)
```

Every active session owns a separately connected upstream UDP socket. That
socket provides correct response demultiplexing without inspecting a
WireGuard or AmneziaWG packet. Existing sessions keep their selected backend
address; newly created sessions use the latest successful DNS result.

The runtime is designed around explicit bounds:

- global, per-source-IP, and new-session-rate limits;
- a configurable maximum datagram size;
- bounded per-session and control-plane queues;
- idle expiration without a hard lifetime for active sessions;
- one long-lived task per session, never one task per packet;
- tracked background tasks and a bounded graceful shutdown.

See [the architecture document](docs/ARCHITECTURE.md) for the concurrency,
reload, DNS, control protocol, and lifecycle decisions.

## Platform and status

WireRelay targets public Linux servers and uses Unix domain sockets for its
local control plane. The crate uses Rust edition 2024 and requires Rust 1.85
or newer.

The `0.x` series is pre-1.0 software. Before deploying a particular revision,
review its changelog and release notes and validate it in an environment that
matches the intended network load.

## Installation

### WireRelay installer

Downloading and reviewing a privileged installer is safer than piping it
directly to a shell:

```console
curl -fsSLo wire-relay-install.sh \
  https://raw.githubusercontent.com/wiresock/wire-relay/main/wire-relay-install.sh
less wire-relay-install.sh
sudo bash wire-relay-install.sh
```

The compact one-command form is:

```console
curl -fsSL https://raw.githubusercontent.com/wiresock/wire-relay/main/wire-relay-install.sh \
  | sudo bash
```

The self-contained installer builds the release binary, installs it, and
configures WireRelay. The full Rust test suite runs in CI and is not repeated
on the target host by default. When the installer is downloaded or piped
outside a WireRelay source checkout, it clones the repository into the build
user's home directory. Its component-specific filename lets it coexist with
installers for other Wiresock services.

To run the full suite before installing from a custom source checkout:

```console
sudo WIRE_RELAY_RUN_TESTS=1 bash wire-relay-install.sh install
```

Other workflows are:

```console
sudo bash wire-relay-install.sh configure
sudo bash wire-relay-install.sh upgrade
sudo bash wire-relay-install.sh status
sudo bash wire-relay-install.sh uninstall
```

`update` remains accepted as a compatibility alias for `upgrade`.

Running a downloaded file without a command opens its interactive menu.
The piped form starts installation directly and reads configuration prompts
from the controlling terminal.

The installer keeps an existing Rust installation, minimizes work performed as
root, validates configuration before starting the service, and places the
binary at `/usr/local/bin/wire-relay`. Review a script before running it with
elevated privileges.

Download and review the current installer before an upgrade, then run:

```console
sudo bash wire-relay-install.sh upgrade
```

The source checkout must be clean, and its current branch must track a remote
`main` branch. The command rejects detached/tag and feature-branch checkouts
with guidance instead of selecting an unintended upgrade stream.

An upgrade does not replace the active configuration. It validates that
configuration with the candidate binary and retains a rollback binary until
the restarted service has been verified. If the source still builds the
installed version, the operation exits without replacing the binary or
restarting the service when the running daemon reports that same version. If
the daemon is stopped, stale, or unreachable, the same-version operation
repairs and restarts the installation. A candidate older than the installed
version is rejected.

### Build from source

For development or a manual installation:

```bash
rustup toolchain install stable --profile minimal
cargo test --all-targets --all-features --locked
cargo build --release --locked
./target/release/wire-relay --version
```

The repository's systemd unit expects the executable at
`/usr/local/bin/wire-relay`, configuration at
`/etc/wire-relay/config.toml`, and a dedicated `wire-relay` user and group.
Use `wire-relay-install.sh` unless you deliberately want to manage those
pieces yourself.

Tagged releases provide Linux x86_64 and ARM64 archives, release notes, a
source archive, and SHA-256 checksums. Verify the checksum before installing a
downloaded artifact.

Every merged pull request advances the application version and creates a
release tag. See [`docs/VERSIONING.md`](docs/VERSIONING.md) for the automatic
patch policy and manual minor/major releases.

## Configuration

WireRelay uses strict TOML configuration. The default location is
`/etc/wire-relay/config.toml`; unknown fields and unusable values are rejected.
Start from [`config/config.example.toml`](config/config.example.toml):

```toml
[service]
control_socket = "/run/wire-relay/control.sock"
log_level = "info"
idle_timeout = "180s"
max_datagram_size = 4096
session_queue_depth = 512
max_queue_memory_bytes = 268435456
max_sessions = 10000
max_sessions_per_ip = 64
new_sessions_per_second = 100
dns_refresh_interval = "60s"
shutdown_timeout = "10s"

[metrics]
enabled = false
bind = "127.0.0.1:9090"

[[listeners]]
name = "germany"
bind = "0.0.0.0:40001"
backend = "de.example.com:51820"

[[listeners]]
name = "netherlands"
bind = "0.0.0.0:40002"
backend = "nl.example.com:51820"
```

Listener names and effective bind addresses must be unique. Listener binds
must use numeric IP addresses; backends may use a numeric IPv4/IPv6 address or
an ASCII/IDNA hostname. Enclose IPv6 addresses in brackets, for example
`"[2001:db8::10]:51820"`.

The control socket must use an absolute path whose directory chain contains
no symbolic links. WireRelay accepts only directories owned by root or its
effective service user. Its immediate parent must not be writable by other
users; root-owned sticky intermediate directories such as `/tmp` are allowed.
The packaged `/run/wire-relay` runtime directory already satisfies these
requirements.

Validate before starting or reloading:

```bash
wire-relay check-config
wire-relay check-config --config /path/to/config.toml
```

Validation is local and does not require a backend to answer synthetic
health-check traffic.

### DNS

Hostnames are resolved at startup and refreshed at the configured interval,
never once per packet. A listener whose hostname has never resolved remains
bound but unavailable for new sessions. Temporary refresh failures retain the
last successful address. A successful address change affects new sessions
only; active sessions continue using the concrete address selected when they
were created. DNS refresh intervals shorter than one second are rejected to
prevent accidental resolver and CPU storms.

The resolver chooses the first usable address returned by the system resolver.
If an operator needs deterministic address selection, configure an IP literal
or control the hostname's resolver response.

### Datagram size and limits

`max_datagram_size` applies to a whole UDP payload. Larger datagrams are
dropped rather than truncated and forwarded. `4096` is a conservative starting
point, not a protocol requirement; increase it when the deployed encapsulation
or padding scheme needs larger datagrams.

Each active session consumes an upstream UDP socket, a task, bounded buffers,
and session metadata. Size `max_sessions`, `max_sessions_per_ip`, the service's
file-descriptor limit, and host memory together. Lowering a session limit on
reload does not evict existing sessions; it blocks admission until usage falls
below the new limit. The compiled ceiling is 100,000 active sessions and the
packaged unit reserves 131,072 file descriptors; real memory and network
capacity will usually require a lower value.

`session_queue_depth` (default `512`, range `1..=65536`) is the number of
client datagrams that may wait behind one session's upstream socket. The
listener normally sends a client datagram straight to its session's connected
upstream socket; a datagram is queued only when that socket cannot accept it
immediately or earlier datagrams are still queued. When the queue is full the
datagram is dropped and counted as `queue_full`. A reloaded depth applies to
new sessions.

`max_queue_memory_bytes` (default `268435456`, 256 MiB) bounds the memory of
all session queues together. It is enforced at runtime: each queued datagram
is charged its length plus a fixed 64-byte overhead for the queue slot and
allocator slack, so even empty datagrams count, and a datagram that would
exceed the limit is dropped as `budget_exhausted`. The limit must hold at
least one full queue of maximum-sized datagrams, so a much deeper
`session_queue_depth` or larger `max_datagram_size` can require raising it.
The default keeps the worst case close to what the fixed eight-datagram
queues of earlier releases could hold at the default limits (about 0.33 GiB);
raise it on hosts with memory to spare, lower it on small ones. A reload
applies a new limit to datagrams queued afterwards.

Datagram memory as a whole is bounded by a defensive 4 GiB budget, which
`max_queue_memory_bytes` cannot exceed. Validation reserves two
`max_datagram_size` receive buffers for every possible session and requires
one completely full queue of maximum-sized datagrams, with that overhead, to
fit beside them, so the largest datagram and session-count ceilings cannot be
selected simultaneously. If the receive buffers leave less than
`max_queue_memory_bytes` of the 4 GiB, the queues get only that remainder.
Receive buffers stay reserved for every live session, at
the current `max_datagram_size`, even when that is more sessions than the
configured `max_sessions`. After a reload that lowers `max_sessions`, that
remainder therefore grows back to the new configuration's full size only as
the surplus sessions end. A reload that raises `max_datagram_size`
while many sessions are live (or lowers `max_sessions` far below the live
count) can leave too little for even one maximum-sized queued datagram, or
nothing at all: for example 40,000 live sessions after a reload to
`max_datagram_size = 65507`. Such a configuration is still valid and the
reload applies, but the daemon logs a warning whenever the remainder is
smaller than one `max_datagram_size` datagram plus its 64-byte overhead, and
datagrams that have to queue are dropped as `budget_exhausted` until enough of
those sessions end. Datagrams the relay drops itself (`queue_full`,
`budget_exhausted`, `session_closed`) do not count as client activity, so
sessions whose clients only send into a full queue or exhausted budget still
idle out, once nothing of theirs is left queued, and release their buffers. A send the OS refuses (`send_error`) does
count, so a backend outage does not expire sessions whose clients keep
sending. Delivering a queued datagram counts too, so a session whose upstream
socket was stalled for longer than `idle_timeout` is not expired, with
datagrams still queued, the moment the socket recovers. A session with
datagrams still queued is never idle-expired, so if its task was held up past
`idle_timeout` (a blocked send, or no CPU), those datagrams are delivered
rather than discarded as `session_closed`.

Closing a session (backend change, listener removal, `session close`, idle
expiry) removes its route and frees its client's `max_sessions_per_ip` slot
at once, so the client's next datagram opens a new session even with
`max_sessions_per_ip = 1`. A closed session does no further forwarding: its
task stops at its next wake-up, without waiting for a pending send, and
discards what is still queued as `session_closed`. Until its task has ended
it still counts toward the session gauges, `active sessions` in `wire-relay
stats`, and `wire-relay sessions`, and until its upstream socket is closed
as well it still counts toward `max_sessions` and the queue budget.
`max_sessions` therefore bounds the upstream sockets that exist, not only
the sessions that can carry traffic, and is never exceeded by old and new
sessions overlapping. The cost is at a relay that is at its global limit:
right after a mass close (for example a backend change) a returning client
is rejected, and counted as `rejected`, for the moment it takes the closed
sessions to end; below the limit it is admitted immediately. A datagram that
arrives just as its session closes is not dropped as `session_closed`: it
opens a new session like the client's next datagram would, unless the
listener itself is stopping.

Idle expiry is decided against the listener's forwarding path: a session is
not expired under a datagram it has just accepted, and once it expires no
further datagram is sent on its upstream socket.

The queue memory is shared, and it can be small. A few sessions whose upstream
sockets are backed up can use all of it, so other sessions' datagrams are
then dropped as `budget_exhausted` even though their own queues are short.
With a high `max_sessions` and a large `max_datagram_size`, the fixed receive
buffers also take most of the 4 GiB and leave less than
`max_queue_memory_bytes`. If `budget_exhausted` drops grow, raise
`max_queue_memory_bytes`, or, where the 4 GiB is what limits it, lower
`max_sessions` or `max_datagram_size`; the warning logged with the drop
reports the limit in effect. `queue_full` instead means one session's queue
reached `session_queue_depth`.

## Running and operating

Start the daemon in the foreground:

```bash
wire-relay run
wire-relay run --config /etc/wire-relay/config.toml
```

The daemon owns the authoritative runtime state. Except for `check-config`,
inspection and mutation commands query the local control socket:

```bash
wire-relay show
wire-relay show --json
wire-relay config
wire-relay config --json
wire-relay config --toml
wire-relay listeners
wire-relay listeners --json
wire-relay sessions
wire-relay sessions --listener germany
wire-relay sessions --client 198.51.100.20
wire-relay sessions --sort bytes
wire-relay sessions --json
wire-relay session show SESSION_ID
wire-relay session show SESSION_ID --json
wire-relay session close SESSION_ID
wire-relay stats
wire-relay stats --json
wire-relay reload
wire-relay version
wire-relay --version
```

`wire-relay --version` reports the installed executable without requiring a
running service. `wire-relay version` queries the running daemon and includes
its control-protocol version, which is useful for detecting a stale daemon
after an installation change.

Session listings use an opaque control-protocol cursor. The daemon takes one
filtered, sorted snapshot for the command and serves every page from that
snapshot, so sessions created or removed while the command is running do not
cause duplicate or missing rows. Abandoned snapshots expire automatically;
an expired or capacity-evicted cursor returns an error and the command can be
run again.

The CLI probes the conventional control socket and can fall back to a valid
configured path. If a custom path must be used while the candidate TOML is
invalid, pass `--control-socket /current/path.sock` explicitly.

Human-readable output is intended for operators. Use `--json` for automation;
major JSON response shapes are intended to remain stable and compatibility
changes are documented. Commands return a nonzero status on failure.

CLI and daemon versions can be mixed while an upgrade is in progress, for
example a new CLI before the daemon has restarted. `wire-relay config`
reports `session_queue_depth` and `max_queue_memory_bytes` only when the
daemon has them. Connected to an older daemon, the CLI leaves both out of the
TOML and JSON output instead of showing defaults that daemon does not use;
the TOML output starts with a comment that says so. The CLI decides that the
daemon is older only when two requests in a row are closed without any
response; a connection error, a timeout, or a response that breaks off is
reported as an error instead. Settings a newer daemon reports that this CLI
does not know are ignored, in every section. A CLI from 0.1.6 or
earlier connected to a newer daemon gets the configuration without those two
settings. Such a CLI still validates the TOML form against its own, older
limits, so for a configuration only a newer release accepts (for example
`max_sessions = 100000` with `max_datagram_size = 5000`) it refuses
`wire-relay config` and `--toml`; `--json` still works. A CLI from this release
does not re-validate: connected to a later daemon, it prints the running
values in `wire-relay config` and `--toml` even when they would not pass its
own configuration-file validation. Loading that output as a configuration
file with this CLI may still fail.

### Transactional reload

`wire-relay reload` asks the running daemon to read its configured file. The
daemon validates and preflights the complete replacement before changing live
state. An invalid or un-bindable replacement leaves the active configuration
unchanged.

Unchanged listeners and sessions are preserved. Added listeners are
pre-bound. Removing a listener or changing its bind closes its sessions. A
backend change on the same bind keeps the listener socket for new traffic but
closes that listener's existing sessions, providing one consistent routing
rule per session.

Directly moving a bound address from one listener identity to another in a
single reload is rejected because it cannot be pre-bound portably. Remove it
in one reload and add it in a second, or restart with the desired
configuration.

The control socket is the process-lifetime rendezvous point.
`service.control_socket` changes require a service restart and are rejected by
reload. Other service limits, DNS cadence, idle timeout, listeners, backends,
and metrics settings are reloadable.

### systemd

After installation:

```bash
sudo systemctl enable --now wire-relay
systemctl status wire-relay
journalctl -u wire-relay -f
sudo systemctl reload wire-relay
sudo systemctl restart wire-relay
```

The packaged unit runs as `wire-relay:wire-relay`, creates
`/run/wire-relay`, validates configuration before startup, and applies
systemd hardening compatible with UDP, DNS, logging, and Unix-socket creation.
To allow a non-root operator to query the control socket, add that trusted
account to the `wire-relay` group and start a new login session:

```bash
sudo usermod -aG wire-relay OPERATOR
```

Membership permits runtime inspection, reload, and session closure; grant it
only to trusted administrators.

### Metrics

When `[metrics]` is enabled, the configured TCP endpoint exports Prometheus
text at `/metrics`:

```bash
curl --fail http://127.0.0.1:9090/metrics
```

The example binds to loopback. Do not expose metrics publicly without an
authenticated reverse proxy or equivalent network controls. Metrics avoid
client IP and session ID labels.

`wire_relay_datagrams_dropped_total` counts every discarded datagram. It is
broken down, without labels, by reason:

| Metric | Reason |
| --- | --- |
| `wire_relay_datagrams_dropped_oversize_total` | larger than `max_datagram_size` |
| `wire_relay_datagrams_dropped_queue_full_total` | the session's queue reached `session_queue_depth` |
| `wire_relay_datagrams_dropped_budget_exhausted_total` | the shared queued-datagram memory budget was exhausted |
| `wire_relay_datagrams_dropped_session_closed_total` | session was closing, including datagrams still queued when it stopped |
| `wire_relay_datagrams_dropped_rejected_total` | no session could be created (admission limits, unresolved backend, socket setup, route change) |
| `wire_relay_datagrams_dropped_send_error_total` | the OS refused or truncated a send in either direction |

`wire-relay stats` shows the same breakdown, and `wire-relay listeners` shows
each listener's total, `queue_full`, and `budget_exhausted` (`BUDGET`) drops.
Connected to an older daemon that reports no per-reason counters, `stats`
omits the breakdown and `listeners` prints `-` for `QUEUE FULL` and `BUDGET`;
a daemon that predates only the budget reason reports it as 0. A growing
`queue_full` count means one session's upstream sends cannot keep up; a
growing `budget_exhausted` count means the shared queue memory is used up.
Each logs its own rate-limited warning.

## Security model

Clients cannot choose an upstream destination: every listener forwards only
to its configured backend. WireRelay never logs payloads and treats all UDP
and control input as untrusted. Session admission, queueing, frame sizes, and
control concurrency are bounded; repeated errors and drops are aggregated or
rate-limited.

These controls reduce risk but do not replace host and network hardening:

- expose only intended UDP listener ports;
- keep the control socket and metrics endpoint local or access-controlled;
- restrict configuration and service-management permissions;
- set session limits for the host's real file-descriptor and memory budget;
- monitor rejected sessions, drops, DNS errors, and socket errors;
- keep WireRelay, Rust-built dependencies, the OS, and DNS resolver updated.

WireRelay does not authenticate relay clients. If a UDP listener is public,
any reachable sender can consume its bounded resources and send traffic to
that listener's fixed backend. Apply firewall policy or source filtering when
the deployment requires access control.

Never put secrets in UDP payloads on the assumption that the relay encrypts
them. Encryption and peer authentication remain the responsibility of the
carried protocol.

See [`SECURITY.md`](SECURITY.md) for vulnerability reporting.

## Performance notes

The normal data path performs one receive and one send per direction while
preserving datagram boundaries. Client-to-backend datagrams are sent from the
listener task directly on the session's connected upstream socket, so the
upload path, like the download path, has no task hand-off or per-packet copy
unless the upstream socket is momentarily busy. A dedicated connected upstream
socket per session is an intentional correctness cost: it avoids parsing a
protocol to demultiplex replies. The implementation does not spawn a task per packet and
uses atomic counters plus short-lived shared-state access.

Bounded queues prefer dropping a datagram under sustained pressure over
unbounded memory growth or head-of-line blocking across all clients. Observe
the per-reason drop counters while tuning, and the kernel's UDP
`RcvbufErrors`/`SndbufErrors` in `/proc/net/snmp`, which are outside the
relay's counters. Socket buffer sizes use the operating-system defaults. DNS resolution and control-plane serialization are
kept out of the packet path. Large session listings are sorted once on a
bounded blocking worker and paged from a bounded, expiring daemon-side
snapshot, avoiding repeated full-table scans on Tokio's packet-processing
workers.

Linux-specific batching (`recvmmsg`/`sendmmsg`), `SO_REUSEPORT`, and io_uring
are intentionally deferred until profiling demonstrates a need and
correctness can be preserved.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Changes to routing, reload, or
lifecycle behavior should preserve the invariants in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and include integration tests
using opaque payloads.

## License

WireRelay is available under the
[GNU Affero General Public License v3 or later](LICENSE). A separate
commercial license from WireSock may also be available; see
[`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md).
