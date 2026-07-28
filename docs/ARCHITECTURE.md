# WireRelay architecture

This document records the implementation decisions that must remain true as
WireRelay evolves. WireRelay is an opaque UDP datagram relay. It does not know
about, inspect, or modify WireGuard, AmneziaWG, or any other application
protocol carried in a datagram.

## Scope and invariants

- A configured listener has one fixed backend endpoint. Network clients can
  never select a destination.
- The session key is `(listener_id, client_socket_address)`.
- Every session owns a separately bound and connected upstream UDP socket.
  That socket is the response-demultiplexing boundary.
- Receive and send operations preserve one input UDP datagram as one output
  UDP datagram. Oversized datagrams are dropped; they are never truncated and
  forwarded.
- Payload bytes are never parsed, transformed, or logged.
- All queues, control messages, and task populations have configured or
  compiled bounds.

## Crate and module design

WireRelay is one package with a library and a single `wire-relay` binary.
Edition 2024 is used because the supported toolchain and CI are stable Rust
releases that support it.

```text
src/
  cli.rs                    clap command model and human/JSON rendering
  config.rs                 strict TOML parsing, normalization, validation
  control/
    protocol.rs             versioned, length-prefixed JSON data model
    client.rs               bounded local control client
    server.rs               Unix socket ownership and request dispatch
  relay/
    listener.rs             listener receive loop and admission
    session.rs              connected upstream socket and session task
    session_table.rs        dual key/ID lookup and snapshots
    upstream.rs             address-family-aware upstream socket creation
  dns.rs                    cached startup and periodic resolution
  limits.rs                 atomic limits, per-IP accounting, token bucket
  metrics.rs                counters and optional Prometheus endpoint
  runtime.rs                supervisors, reload transaction, snapshots
  shutdown.rs               signals and bounded graceful shutdown
  error.rs                  reusable error categories
```

Application-boundary context is provided with `anyhow`; reusable modules
return typed errors.

## Concurrency model

The Tokio multi-thread scheduler runs several kinds of long-lived task:

1. one receive loop per configured listener;
2. one task per active session;
3. one DNS refresh task per hostname-backed listener;
4. one control accept loop plus a bounded number of control connections;
5. an optional metrics accept loop;
6. signal and top-level supervision.

There is no task per packet. A listener reads into a buffer sized to
`max_datagram_size + 1`, so it can reliably distinguish an acceptable
datagram from a truncated oversized one. Existing-session datagrams are
offered to a bounded per-session MPSC queue with `try_send`. Queue pressure
drops and counts the datagram rather than growing memory or blocking all
clients on the listener.

For a new session, the listener reserves global/per-IP/rate capacity before
creating a socket. The listener's receive loop serializes creation for that
listener, preventing duplicate creation for the same key. Socket setup is
bounded local work; admission is released on every setup failure.

The session task owns its connected upstream socket and selects among:

- a queued client datagram;
- one backend datagram;
- its idle deadline;
- session or process cancellation.

Tokio's fair selection order is used for client and backend readiness so
sustained ingress cannot starve backend responses. Cancellation is also raced
against the first upstream send.

Backend responses are sent through an `Arc<UdpSocket>` for the original
listener to the session's immutable client address. The per-session upstream
socket means no protocol parsing is required to identify the client.

## Shared state and locking

Session metadata is immutable where possible and uses atomics for counters.
Activity timestamps and small DNS/config snapshots use short synchronous
locks. A watch channel wakes sessions when the configured idle timeout
changes, so reload immediately recomputes existing deadlines. A lock guard is
always dropped before socket, file, channel, or timer awaits. Session maps
support concurrent lookup and return owned handles before any await.

Admission accounting is a short, non-async critical section containing the
global count, per-IP counts, and a token bucket. Configuration limits are
updated atomically during reload. Reducing a limit below current use does not
kill sessions; it prevents new admission until use falls below the limit.

## Session lifecycle

A session receives a UUID identifier and immutable listener, client, selected
backend, creation, and upstream-local information. Its counters are atomic.
Client and backend activity both extend the idle deadline. There is no hard
session lifetime.

Creation follows this order:

1. validate datagram size and check the session table;
2. reserve rate/global/per-IP capacity;
3. read the listener's latest cached backend;
4. bind an address-family-compatible wildcard upstream socket;
5. connect it to that backend;
6. under the listener's short route-commit lock, recheck the route incarnation
   and insert the session in both key and ID indexes;
7. start its task and forward the original datagram unchanged.

Completion removes both indexes only if they still refer to the same session
ID, releases admission exactly once, updates the appropriate statistic, and
drops the socket. Explicit close and shutdown use cancellation tokens.
Tracked tasks are closed and awaited during graceful shutdown.

## DNS behavior

Numeric backend addresses require no resolver task. Hostnames are resolved at
startup and then at `dns_refresh_interval`; DNS is never queried in a packet
path. A successful result atomically replaces the cached address and
resolution timestamp. A failure increments the DNS error metric and retains
the last successful result. Until the first success, the listener remains
bound but unavailable for new sessions. Existing sessions retain the concrete
backend address selected at creation.

The first usable unicast, non-unspecified address returned by the system
resolver is the current address; multicast and IPv4 broadcast answers are
filtered. The refresh interval has a one-second minimum to prevent an
accidental tight resolver loop. Address changes are logged without client data
and are exposed by the control API.

## Control protocol

The daemon is authoritative for runtime configuration and state. CLI display
and mutation commands use a local Unix Domain Socket, normally
`/run/wire-relay/control.sock`; only `check-config` reads configuration
without a daemon.

Each connection handles one request and one response. Messages are
big-endian-u32 length-prefixed UTF-8 JSON envelopes containing:

- protocol version;
- request ID;
- tagged operation or result;
- structured error information.

The compiled maximum frame size, read/write timeouts, bounded accepted-client
count, and paginated session listing prevent memory and connection
exhaustion. The first session-list request filters and sorts one immutable
daemon-side snapshot; later pages carry an opaque protocol-v2 cursor and
therefore remain stable while live sessions change. Cursor snapshots expire
30 seconds after their last page, completed cursors are deleted immediately,
and a periodic tracked task reaps abandoned cursors. The cache retains at
most four snapshots and 200,000 rows, while no more than two snapshot builds
run concurrently. Capacity pressure evicts the least-recently-used snapshot,
and stale or mismatched cursors receive structured errors.

Unsupported versions and malformed input receive structured errors when
safely possible. The socket parent and socket use restrictive Unix
permissions. Startup refuses to replace a non-socket path or a live daemon's
socket.

## Reload transaction

Reload is serialized. It first reads, parses, normalizes, and validates the
entire file without touching live state. It then calculates a plan and
pre-binds every new address that is not already owned by the same preserved
listener. Any validation, resolution setup, bind, or metrics-listener
preflight error leaves the active configuration unchanged.

Commit behavior is:

- listeners with unchanged name, bind, and backend are preserved with all
  sessions;
- service limit and timeout values are updated for subsequent decisions;
- added listeners start from pre-bound sockets;
- a backend change on the same bind keeps the listener socket, replaces its
  DNS route for new sessions, and closes old sessions consistently. Route
  replacement/removal and final session insertion share a non-async commit
  lock, so in-flight old-route sockets cannot escape closure;
- a bind change starts its pre-bound replacement and closes the old
  listener's sessions;
- removed listeners stop accepting and their sessions are closed;
- metrics is replaced only after its replacement bind has passed preflight.

The control socket is a process-lifetime rendezvous point. Changing
`service.control_socket` is rejected during reload and requires a service
restart, so the CLI and service manager do not lose the running daemon during
the operation.

Reassigning an address directly between two active listener identities cannot
be pre-bound portably without platform-specific socket options. Such a reload
is rejected with a detailed result and no changes; it can be expressed as two
reloads (remove, then add) or done during a restart. This explicit limitation
keeps reload failure atomic and avoids premature `SO_REUSEPORT`.

The reload response reports preserved, added, modified, removed, and closed
session counts plus any rejection reason.

## Shutdown

SIGTERM and SIGINT initiate cancellation. Listener and control accept loops
stop first, preventing new work. Session and DNS tasks then observe
cancellation and exit. The control socket is removed by its owner. The
runtime waits up to `shutdown_timeout`; timeout is reported and remaining
tasks are aborted only as the final process-exit fallback.

Every spawned task is either owned by a supervisor handle or a Tokio
`TaskTracker`. Nothing is intentionally detached.

## Resource and abuse controls

- Datagram buffers are capped at the legal UDP payload maximum.
- Session count is capped globally and per source IP.
- A global token bucket caps new sessions per second while allowing at most
  one second of burst capacity.
- Per-session queues and control concurrency are fixed and bounded.
- The session-count/datagram-size combination is rejected when its
  conservative queued-buffer estimate exceeds 4 GiB.
- Fixed configured destinations prevent open-proxy behavior.
- Drop, reject, DNS, and socket counters are exposed without client-IP metric
  labels.
- Repetitive warning sites use time-based log suppression.
- Network input is treated as untrusted and never reaches `unwrap`,
  `expect`, indexing assumptions, or payload logging.

## Metrics

Counters and the active-session gauge use atomics on the data path. The
optional Prometheus listener renders snapshots on request. Labels are limited
to stable listener names where useful; client endpoints and session IDs are
never labels. The HTTP implementation is intentionally small and bounded.

## Important risks and mitigations

- **UDP truncation:** receive with one extra byte and drop over-limit data.
- **Response misdelivery:** one connected upstream socket per session.
- **Unbounded fan-out:** admission limits, bounded queues, bounded control
  clients, and tracked task counts.
- **Session creation races:** listener-serialized creation and atomic
  admission reservation.
- **Stale removal races:** session table removal compares session ID.
- **Locks across await:** snapshot or clone under short locks, then await.
- **Shutdown leaks:** hierarchical cancellation plus tracked-task wait.
- **DNS outage:** retain last success and expose failure/age.
- **Reload partial application:** complete validation and resource preflight
  before commit; reject un-preflightable socket handoffs.
- **Log amplification:** aggregate counters and rate-limit repeated events.
- **Control socket replacement attacks:** reject symlinks/non-sockets and
  check for a live owner before removing a stale socket.

## Deferred optimizations

Listener/session boundaries intentionally isolate packet I/O so Linux-specific
`recvmmsg`, `sendmmsg`, `SO_REUSEPORT`, or io_uring implementations can be
introduced behind those modules. They are not enabled without profiling and
correctness evidence.
