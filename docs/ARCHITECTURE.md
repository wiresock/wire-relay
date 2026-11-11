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
datagram from a truncated oversized one. For an existing session, the
listener sends the datagram directly with a non-blocking `try_send` on that
session's connected upstream socket. Only if the socket reports `WouldBlock`,
or earlier datagrams are still queued, is the datagram copied into a bounded
per-session MPSC queue (`service.session_queue_depth`, default 512). A
per-session pending count makes the listener queue behind earlier datagrams,
so client datagram order is preserved; the listener is the only producer for
its sessions. Queue pressure drops and counts the datagram as `queue_full`
rather than growing memory or blocking all clients on the listener.

Queued datagrams also reserve their length plus a fixed 64-byte per-datagram
overhead from a process-wide queue memory budget:
`service.max_queue_memory_bytes` (default 256 MiB), or, if that is less, the
defensive 4 GiB datagram budget minus the fixed receive buffers of
`max(max_sessions, live sessions)`. The explicit limit keeps the default worst
case near what the earlier fixed eight-datagram queues could hold, instead of
handing the queues whatever the receive buffers leave of the 4 GiB.
The limit is not stored: every reservation derives it from the current
`max_queue_memory_bytes`, `max_sessions` and `max_datagram_size` (updated on
reload, after the admission limits) and an atomic live-session count kept by
each session task. The budget
holds the only runtime copy of `max_datagram_size`: listeners and sessions
size their receive buffers from it, so one store publishes the new size and
the budget shape that charges for it, and buffer sizing and the limit cannot
disagree. A reload
that shrinks `max_sessions` below the live count therefore lowers the limit
only while the surplus sessions exist, and it returns to the steady-state
budget as they end, without a recomputation that a concurrent reload or
session exit could leave stale. The reservation is released when the queued
datagram is dropped, including when a session stops with datagrams still
queued. A lowered limit applies immediately; new reservations fail until use
drains below it. Live sessions resize their receive buffers to the reloaded
`max_datagram_size`, so the limit charges live sessions at the new size: a
reload that raises `max_datagram_size` under a large live population, or
shrinks `max_sessions` far below the live count, can drive the limit below
one maximum-sized datagram, or to zero, although the new configuration is
valid. The reload still applies and logs a warning whenever the limit is below
the charge of one `max_datagram_size` datagram; queued datagrams drop as
`budget_exhausted` until enough sessions end. The limit formula, including
the `max(live, max_sessions)` rule, lives in one function in `config.rs`,
used by both the test-only configuration helpers and the runtime budget.

The listener claims a queue slot before charging the budget or copying the
datagram, so a full queue drops without either. A full per-session queue is
counted as `queue_full`; a failed budget reservation as `budget_exhausted`,
with separate warnings. The budget is shared, not partitioned: under a tight
configuration (high `max_sessions` and large datagrams) the remainder can be
small, and a few back-pressured sessions can exhaust it so that other
sessions drop as `budget_exhausted`.

Rate-limited warnings use one atomic timestamp per log site, indexed by a
`LogSite` enum declared from a single macro list (which also derives the slot
count), so the drop path neither locks nor searches to decide whether to log.

Closing a session (idle expiry, closed idle-timeout channel, explicit close,
listener removal or backend change, shutdown) removes its routing entry,
releases its client's per-IP admission slot, and sets the closed flag in the
handle's atomic state word, which the listener checks without locking, all in
one step under the routing entry's shard lock; it then cancels the token the
task waits on. A listener therefore either still reaches the open session or
finds no route and the client's slot free, and an idle expiry, which takes
the same lock, cannot claim a session that is being closed. The lease is
stored in the handle before the handle is published, and a session that loses
its key is never listed by ID, so a close that races the launch still finds
it. The task closes its own session the moment it decides to stop. A stopping
listener closes every one of its sessions before it cancels its token, while
its route lock keeps new ones from being launched, so no datagram is sent or
queued after `stop` while the session tasks are still waking up.

The next datagram from the same client therefore finds no route and is
admitted as a new session even with a per-IP limit of one (for example a
WireGuard handshake retry after a backend change), while the old task is
still stopping. The admission lease is released in two stages: the per-IP
slot with the route, and the global slot with the session handle itself.
The lease is the handle's last field, and the handle is the only owner of
the upstream socket and of the receive-buffer reservation, so the global
slot is released only after the last reference to the handle is gone and
the socket and reservation went before it. The session task holds the
handle until it ends; the listener holds it only while it forwards one
datagram. Until then the closed session keeps its upstream socket,
receive-buffer reservation, and global slot; its gauges and ID entry go
with the cleanup guard when the task stops. A new session takes its global
slot before it opens its upstream socket, so the number of sessions that
hold an upstream socket or a receive-buffer reservation never exceeds
`max_sessions`, counting old and new sessions together (a reload that lowers
`max_sessions` keeps the sessions already live; new ones are refused until
the count is below the new limit). At the global limit a returning client is
rejected until closed sessions are gone. To keep that short, every wait in the session task (including the
client-direction send and the receive-error back-off) is raced against the
cancellation token, so a closed task exits at its next wake-up. Closing is
reported once: a session that is already closed but still listed is not
counted again by a later listener close or reload, and `session close` on
it returns a `not_found` error saying it is already closed. Route removal is guarded
by session ID, so a stale task never removes a newer session's route, and
each stage of the lease is released once. A listener that fetched the old
handle just before the close does not send on the exiting session's socket:
the closed handle refuses the datagram without counting it, and the
listener treats it as one from a client without a session, once. A datagram
that passes that check but has to queue after the cleanup has closed the
queue is refused the same way. It goes
through admission and normally opens a new session; if the listener is
stopping it is counted as `session_closed` instead, and if admission
refuses it, as `rejected`. A datagram the session accepted before the close
can still be sent once. The cleanup guard is created before the task is spawned and owns the
queue, so it is the one place a session ends, however its task stops
(returning, dropped in mid-run, or dropped before its first poll): it closes
the session, discards the queue, counts an unsent first datagram, removes the
entry, and ends the gauges. The task's references to the handle are dropped
with it, and the last one releases the global admission slot. Queued
datagrams account for themselves: each holds a reference to its session's
accounting and, when it is dropped, releases its budget charge and, unless
its send was settled, counts itself as `session_closed`. That covers one the
task had taken but not sent, every one still queued, and one the listener
was still enqueueing when the session closed (counted when the queue is
freed), so the cleanup never waits for the listener. A forward is registered
in the state word only across its non-blocking send or enqueue; its
accounting and logging run afterwards. Datagrams already sent are not
counted, and delivering a queued datagram refreshes client activity like a
direct send, so a session is not expired on the recovery of an upstream
socket that was stalled for longer than the idle timeout. On idle timer expiry the task re-reads the
current timeout before deciding to close, and decides against the listener's
direct-send path: the listener registers each forward in the handle's state
word, and the task announces the expiry there, re-reads activity, and closes
the session only if the word is unchanged. A forward that starts meanwhile
withdraws the announcement and is sent, so a datagram is refused only by a
session that really closed. If that datagram is then dropped (`queue_full`
or `budget_exhausted`), activity is not refreshed and the deadline has
already passed, so the task tries again after a 10 ms back-off rather than
at once. A session with datagrams still queued is not expired either: a
forward increments the queued count while it is registered in the state
word, so the task sees it in one or the other. If the task was held up past
the idle timeout, it retries after the same back-off, by which time it has
drained the queue (which refreshes activity) instead of discarding those
datagrams as `session_closed`. The expiry runs under the route's shard lock and
removes the route and releases admission in the same step, so the client's
next datagram opens a new session. A session closed for another reason at
that moment is counted as closed, not expired.

For a new session, the listener reserves global/per-IP/rate capacity before
creating a socket. The listener's receive loop serializes creation for that
listener, preventing duplicate creation for the same key. Socket setup is
bounded local work; admission is released on every setup failure.

The session task shares its connected upstream socket with the session handle
and selects among:

- queued client datagrams, of which it drains up to 64 per wakeup;
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

Session metadata is immutable where possible and uses atomics for counters
and activity timestamps. The idle deadline is armed once and recomputed only
when it fires or the timeout changes, not per packet. Small DNS/config
snapshots use short synchronous locks. A watch channel wakes sessions when the configured idle timeout
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
Client and backend activity both extend the idle deadline. Client activity
counts datagrams the session sent, queued, or tried to send (a refused or
partial backend send still counts); datagrams the relay drops itself as
`queue_full`, `budget_exhausted`, or `session_closed` do not extend it. There is no hard session lifetime.

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

The configuration file and the control protocol use separate types. The
file types reject unknown keys. The active configuration travels as wire
types whose service settings added after 0.1.6 are optional, and which
ignore keys they do not know in every section (`[service]`, `[metrics]`, and
each `[[listeners]]` entry). A test fails if a file type gains a setting
the extended wire form does not carry. A CLI from 0.1.6 or earlier rejects
unknown keys, so the `active_config` operation keeps the 0.1.6 shape and
leaves out `session_queue_depth` and `max_queue_memory_bytes`. Current CLIs
ask for `active_config_extended`, which carries every setting. A daemon that
predates that operation cannot decode the request and closes the connection
without a response. The CLI counts a request as unanswered only when it
wrote the whole request and the connection ended before the first byte of a
response; a failure to connect or write, a timeout, or a response that
breaks off is an error. An unanswered request is sent once more on a new
connection, since a current daemon that is restarting, or at its limit of
concurrent control connections, can also close one unanswered. Only if that
is unanswered too does the CLI repeat the request as `active_config` and
treat the missing settings as absent rather than filling in defaults. The
CLI prints the reported values in `config` and `--toml` without
re-validating them against its own configuration-file limits; only loading
a file enforces those. The protocol version stays 2, so every pairing of
these releases still talks.

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
- Session count is capped globally and per source IP. The global cap counts
  every session that still owns an upstream socket, including closed sessions
  whose tasks have not ended.
- A global token bucket caps new sessions per second while allowing at most
  one second of burst capacity.
- Per-session queues are bounded by `session_queue_depth` and a shared,
  runtime-enforced queue memory budget (`max_queue_memory_bytes`, 256 MiB by
  default); control concurrency is fixed and bounded.
- A session-count/datagram-size/queue-depth combination is rejected when every
  session's fixed receive buffers plus one full queue of maximum-sized
  datagrams, each with its per-datagram overhead, exceeds 4 GiB.
- Fixed configured destinations prevent open-proxy behavior.
- Drop (in total and per reason), reject, DNS, and socket counters are exposed
  without client-IP metric labels.
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
