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

### Bootstrap installer

From a source checkout, the service-management script can build, test, install,
and configure WireRelay:

```bash
sudo ./bootstrap.sh install
```

Other workflows are:

```bash
sudo ./bootstrap.sh configure
sudo ./bootstrap.sh update
sudo ./bootstrap.sh status
sudo ./bootstrap.sh uninstall
```

Running the script without a command opens its interactive menu:

```bash
sudo ./bootstrap.sh
```

The installer keeps an existing Rust installation, minimizes work performed as
root, validates configuration before starting the service, and places the
binary at `/usr/local/bin/wire-relay`. Review a script before running it with
elevated privileges.

The exact update command is:

```bash
cd /path/to/wire-relay
sudo ./bootstrap.sh update
```

An update does not replace the active configuration. It validates that
configuration with the candidate binary and retains a rollback binary until
the restarted service has been verified.

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
Use the bootstrap script unless you deliberately want to manage those pieces
yourself.

Tagged releases are expected to provide Linux x86_64 and ARM64 archives,
release notes, a source archive, and SHA-256 checksums. Verify the checksum
before installing a downloaded artifact.

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
capacity will usually require a lower value. Validation also caps the combined
worst-case session datagram buffers and bounded queues at a defensive 4 GiB
estimate, so the largest datagram and session-count ceilings cannot be selected
simultaneously.

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
preserving datagram boundaries. A dedicated connected upstream socket per
session is an intentional correctness cost: it avoids parsing a protocol to
demultiplex replies. The implementation does not spawn a task per packet and
uses atomic counters plus short-lived shared-state access.

Bounded queues prefer dropping a datagram under sustained pressure over
unbounded memory growth or head-of-line blocking across all clients. Observe
drop counters while tuning. DNS resolution and control-plane serialization are
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
[GNU Affero General Public License v3 or later](LICENSE-AGPL-3.0). A separate
commercial license may also be available; see
[`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md).
