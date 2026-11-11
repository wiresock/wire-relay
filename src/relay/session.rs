// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-client connected upstream socket and lifecycle.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{self, Receiver, Sender, error::TrySendError},
        watch,
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};

use crate::{
    config::{
        MAX_SESSION_DATAGRAM_MEMORY_BYTES, QUEUED_DATAGRAM_OVERHEAD_BYTES, ServiceConfig,
        queued_datagram_budget, queued_datagram_charge,
    },
    limits::{AdmissionLease, LogRateLimiter, LogSite},
    metrics::{DropCounters, DropReason, DropSnapshot, Metrics},
};

use super::{ListenerId, SessionId, SessionKey, SessionTable};

/// Maximum queued client datagrams a session sends before it polls its other
/// event sources again.
pub const QUEUE_DRAIN_BATCH: usize = 64;

/// Cumulative counters owned by one listener incarnation.
#[derive(Debug, Default)]
pub struct ListenerCounters {
    active_sessions: AtomicU64,
    packets_to_backend: AtomicU64,
    packets_to_client: AtomicU64,
    bytes_to_backend: AtomicU64,
    bytes_to_client: AtomicU64,
    drops: DropCounters,
}

impl ListenerCounters {
    pub fn session_started(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_ended(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn to_backend(&self, bytes: usize) {
        self.packets_to_backend.fetch_add(1, Ordering::Relaxed);
        self.bytes_to_backend
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    pub fn to_client(&self, bytes: usize) {
        self.packets_to_client.fetch_add(1, Ordering::Relaxed);
        self.bytes_to_client
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    pub fn dropped(&self, reason: DropReason) {
        self.drops.record(reason);
    }

    #[must_use]
    pub fn snapshot(&self) -> ListenerCounterSnapshot {
        let datagrams_dropped = self.drops.snapshot();
        ListenerCounterSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            packets_to_backend: self.packets_to_backend.load(Ordering::Relaxed),
            packets_to_client: self.packets_to_client.load(Ordering::Relaxed),
            bytes_to_backend: self.bytes_to_backend.load(Ordering::Relaxed),
            bytes_to_client: self.bytes_to_client.load(Ordering::Relaxed),
            // Derived from the breakdown so the two can never disagree.
            dropped: datagrams_dropped.total(),
            datagrams_dropped: Some(datagrams_dropped),
        }
    }
}

/// Serializable listener traffic values.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ListenerCounterSnapshot {
    pub active_sessions: u64,
    pub packets_to_backend: u64,
    pub packets_to_client: u64,
    pub bytes_to_backend: u64,
    pub bytes_to_client: u64,
    pub dropped: u64,
    /// Breakdown of `dropped` by reason; `None` when the daemon predates the
    /// breakdown, which is "unknown", not zero.
    #[serde(default)]
    pub datagrams_dropped: Option<DropSnapshot>,
}

#[cfg(test)]
impl ListenerCounterSnapshot {
    /// The per-reason breakdown of an in-process snapshot.
    pub(crate) fn drops(&self) -> DropSnapshot {
        self.datagrams_dropped.clone().unwrap_or_default()
    }
}

/// Records one discarded datagram in the process and listener counters.
pub fn record_drop(metrics: &Metrics, listener_counters: &ListenerCounters, reason: DropReason) {
    metrics.datagram_dropped(reason);
    listener_counters.dropped(reason);
}

/// Process-wide byte budget for client datagrams waiting in session queues.
///
/// Each reservation is charged with the datagram's actual length plus
/// [`QUEUED_DATAGRAM_OVERHEAD_BYTES`], so even empty datagrams consume budget,
/// and is released when the queued datagram is dropped, whether it was sent
/// or discarded during session teardown.
///
/// The limit is not stored; every reservation derives it from the current
/// configuration and live session count: `service.max_queue_memory_bytes`, or
/// the defensive datagram budget minus fixed receive buffers for
/// `max(live sessions, max_sessions)` if that is less. A reload that
/// lowers `max_sessions` therefore keeps buffers reserved for surplus live
/// sessions, and the limit rises back to the steady-state budget as they end,
/// without a separate recomputation that concurrent session ends or reloads
/// could leave stale. A limit below current use still applies: new
/// reservations fail until enough queued datagrams drain.
#[derive(Debug)]
pub struct QueueBudget {
    memory_bytes: u128,
    /// `max_sessions` in the high 32 bits and `max_datagram_size` in the low
    /// 32 bits, published together by one store.
    ///
    /// Read-mostly: every listener loads it per received datagram, so it is
    /// kept off the cache lines of the counters below, which are written on
    /// session churn and on every queued datagram.
    shape: CacheAligned<AtomicU64>,
    /// `service.max_queue_memory_bytes`; read only when a datagram queues.
    max_queue_memory: AtomicU64,
    live_sessions: CacheAligned<AtomicUsize>,
    used: CacheAligned<AtomicUsize>,
}

/// Keeps a value on cache lines of its own to avoid false sharing.
#[derive(Debug)]
#[repr(align(128))]
struct CacheAligned<T>(T);

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl QueueBudget {
    #[must_use]
    pub fn new(service: &ServiceConfig) -> Arc<Self> {
        Self::with_memory(
            MAX_SESSION_DATAGRAM_MEMORY_BYTES,
            service.max_queue_memory_bytes,
            service.max_sessions,
            service.max_datagram_size,
        )
    }

    fn with_memory(
        memory_bytes: u128,
        max_queue_memory_bytes: u64,
        max_sessions: usize,
        max_datagram_size: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            memory_bytes,
            max_queue_memory: AtomicU64::new(max_queue_memory_bytes),
            shape: CacheAligned(AtomicU64::new(Self::shape(max_sessions, max_datagram_size))),
            live_sessions: CacheAligned(AtomicUsize::new(0)),
            used: CacheAligned(AtomicUsize::new(0)),
        })
    }

    /// A budget of `limit` bytes while no session is live, for 4096-byte
    /// datagrams.
    #[cfg(test)]
    pub(crate) fn with_limit(limit: usize) -> Arc<Self> {
        Self::for_test(limit, 0, 4096)
    }

    /// A budget of `memory_bytes` with the given configured shape.
    #[cfg(test)]
    pub(crate) fn for_test(
        memory_bytes: usize,
        max_sessions: usize,
        max_datagram_size: usize,
    ) -> Arc<Self> {
        Self::with_memory(
            memory_bytes as u128,
            u64::MAX,
            max_sessions,
            max_datagram_size,
        )
    }

    fn shape(max_sessions: usize, max_datagram_size: usize) -> u64 {
        // Both are validated far below `u32::MAX`; clamp defensively.
        let clamp = |value: usize| {
            u64::try_from(value).map_or(u64::from(u32::MAX), |value| value.min(u64::from(u32::MAX)))
        };
        (clamp(max_sessions) << 32) | clamp(max_datagram_size)
    }

    fn unpack(shape: u64) -> (usize, usize) {
        let to_usize = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);
        (to_usize(shape >> 32), to_usize(shape & u64::from(u32::MAX)))
    }

    /// Applies reloaded limits to subsequent reservations.
    pub fn configure(&self, service: &ServiceConfig) {
        // Independent of the shape: until both stores are visible the limit
        // is the smaller of two terms that are each valid on their own.
        self.max_queue_memory
            .store(service.max_queue_memory_bytes, Ordering::Release);
        self.shape.store(
            Self::shape(service.max_sessions, service.max_datagram_size),
            Ordering::Release,
        );
    }

    /// Current `service.max_datagram_size`.
    ///
    /// This is the only runtime copy: listeners and sessions size their
    /// receive buffers from it, so a reload publishes the datagram size and
    /// the budget shape that charges for it in one store.
    #[must_use]
    pub fn max_datagram_size(&self) -> usize {
        self.configured().1
    }

    /// The configured `(max_sessions, max_datagram_size)` the limit uses.
    #[must_use]
    pub fn configured(&self) -> (usize, usize) {
        Self::unpack(self.shape.load(Ordering::Acquire))
    }

    /// The configured `service.max_queue_memory_bytes`.
    #[must_use]
    pub fn max_queue_memory(&self) -> u64 {
        self.max_queue_memory.load(Ordering::Acquire)
    }

    /// Bytes currently charged to queued datagrams.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Sessions currently holding receive buffers.
    #[must_use]
    pub fn live_sessions(&self) -> usize {
        self.live_sessions.load(Ordering::Acquire)
    }

    /// Current limit for queued datagrams.
    #[must_use]
    pub fn limit(&self) -> usize {
        let (max_sessions, max_datagram_size) = self.configured();
        queued_datagram_budget(
            self.memory_bytes,
            self.max_queue_memory(),
            max_sessions,
            self.live_sessions(),
            max_datagram_size,
        )
    }

    /// Whether live sessions' receive buffers leave less than one
    /// maximum-sized queued datagram's charge, so a full-sized datagram that
    /// has to queue drops as `budget_exhausted` (smaller ones soon after)
    /// until enough of those sessions end.
    ///
    /// Validation guarantees room for a full queue of maximum-sized
    /// datagrams beside `max_sessions` sessions, so this only happens when a
    /// reload left more live sessions than the new configuration allows for,
    /// or raised `max_datagram_size` under a large live population.
    #[must_use]
    pub fn starved_by_live_sessions(&self) -> bool {
        let (_, max_datagram_size) = self.configured();
        self.live_sessions() > 0 && self.limit() < Self::charge(max_datagram_size)
    }

    /// Counts one session's receive buffers until the guard is dropped.
    #[must_use]
    pub fn session_started(self: &Arc<Self>) -> LiveSession {
        self.live_sessions.fetch_add(1, Ordering::AcqRel);
        LiveSession {
            budget: Arc::clone(self),
        }
    }

    /// Budget charged for queueing one datagram of `payload_len` bytes.
    #[must_use]
    pub const fn charge(payload_len: usize) -> usize {
        queued_datagram_charge(payload_len)
    }

    /// Charges one datagram of `payload_len` bytes and returns the bytes
    /// charged, or `None` if they do not fit under the current limit.
    fn try_charge(&self, payload_len: usize) -> Option<usize> {
        let bytes = Self::charge(payload_len);
        let limit = self.limit();
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let total = used.checked_add(bytes).filter(|total| *total <= limit)?;
            match self
                .used
                .compare_exchange_weak(used, total, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(bytes),
                Err(current) => used = current,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Keeps one live session's receive buffers reserved in a [`QueueBudget`].
#[derive(Debug)]
pub struct LiveSession {
    budget: Arc<QueueBudget>,
}

impl Drop for LiveSession {
    fn drop(&mut self) {
        self.budget.live_sessions.fetch_sub(1, Ordering::AcqRel);
    }
}

/// What a session's queued datagrams account against: the shared budget
/// they are charged to and the counters an unsent one is recorded in.
#[derive(Debug)]
struct QueueAccounting {
    budget: Arc<QueueBudget>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
}

/// A client datagram waiting for its session's upstream socket.
///
/// It accounts for itself when it is dropped, wherever that happens: its
/// budget charge is released, and unless it was settled it is counted as
/// `session_closed`. A datagram left in the queue when the session stops is
/// therefore counted exactly once without the session's cleanup having to
/// wait for the listener, including one the listener was still enqueueing.
#[derive(Debug)]
pub struct QueuedDatagram {
    payload: Vec<u8>,
    charged: usize,
    // One reference to the session's own accounting, taken only on the
    // queued fallback path, never on direct sends.
    accounting: Arc<QueueAccounting>,
    /// The send completed, or its failure has been counted.
    settled: bool,
}

impl QueuedDatagram {
    /// Consumes a datagram whose send has been accounted for.
    fn settle(mut self) {
        self.settled = true;
    }
}

impl Drop for QueuedDatagram {
    fn drop(&mut self) {
        self.accounting.budget.release(self.charged);
        if !self.settled {
            record_drop(
                &self.accounting.metrics,
                &self.accounting.listener_counters,
                DropReason::SessionClosed,
            );
        }
    }
}

// The fixed per-datagram charge must cover the queue slot itself; the rest
// is slack for allocator rounding and the channel's block bookkeeping.
const _: () = assert!(size_of::<QueuedDatagram>() <= QUEUED_DATAGRAM_OVERHEAD_BYTES);

/// Lock-free activity timestamps, stored as nanoseconds after creation.
#[derive(Debug, Default)]
struct Activity {
    last_client_ns: AtomicU64,
    last_backend_ns: AtomicU64,
}

/// Runtime information for one mapping.
#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    pub listener_id: ListenerId,
    pub listener_name: String,
    pub client_addr: SocketAddr,
    pub upstream_local_addr: SocketAddr,
    pub backend_addr: SocketAddr,
    created_at: Instant,
    activity: Activity,
    packets_to_backend: AtomicU64,
    packets_to_client: AtomicU64,
    bytes_to_backend: AtomicU64,
    bytes_to_client: AtomicU64,
}

impl Session {
    #[must_use]
    pub fn new(
        listener_id: ListenerId,
        listener_name: String,
        client_addr: SocketAddr,
        upstream_local_addr: SocketAddr,
        backend_addr: SocketAddr,
    ) -> Self {
        let now = Instant::now();
        Self {
            id: SessionId::new(),
            listener_id,
            listener_name,
            client_addr,
            upstream_local_addr,
            backend_addr,
            created_at: now,
            activity: Activity::default(),
            packets_to_backend: AtomicU64::new(0),
            packets_to_client: AtomicU64::new(0),
            bytes_to_backend: AtomicU64::new(0),
            bytes_to_client: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub const fn key(&self) -> SessionKey {
        SessionKey::new(self.listener_id, self.client_addr)
    }

    fn elapsed_ns(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.created_at).as_nanos()).unwrap_or(u64::MAX)
    }

    fn touch_client(&self) {
        let now = self.elapsed_ns(Instant::now());
        self.activity
            .last_client_ns
            .fetch_max(now, Ordering::Relaxed);
    }

    fn touch_backend(&self) {
        let now = self.elapsed_ns(Instant::now());
        self.activity
            .last_backend_ns
            .fetch_max(now, Ordering::Relaxed);
    }

    fn at_offset(&self, nanos: u64) -> Instant {
        self.created_at + Duration::from_nanos(nanos)
    }

    fn last_client_activity(&self) -> Instant {
        self.at_offset(self.activity.last_client_ns.load(Ordering::Relaxed))
    }

    fn last_backend_activity(&self) -> Instant {
        self.at_offset(self.activity.last_backend_ns.load(Ordering::Relaxed))
    }

    fn last_activity(&self) -> Instant {
        self.last_client_activity()
            .max(self.last_backend_activity())
    }

    fn record_to_backend(&self, bytes: usize) {
        self.packets_to_backend.fetch_add(1, Ordering::Relaxed);
        self.bytes_to_backend
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    fn record_to_client(&self, bytes: usize) {
        self.packets_to_client.fetch_add(1, Ordering::Relaxed);
        self.bytes_to_client
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> SessionSnapshot {
        let now = Instant::now();
        let last_client = self.last_client_activity();
        let last_backend = self.last_backend_activity();
        SessionSnapshot {
            id: self.id,
            listener_id: self.listener_id,
            listener: self.listener_name.clone(),
            client_addr: self.client_addr,
            upstream_local_addr: self.upstream_local_addr,
            backend_addr: self.backend_addr,
            age_ms: duration_millis(now.saturating_duration_since(self.created_at)),
            idle_ms: duration_millis(now.saturating_duration_since(last_client.max(last_backend))),
            last_client_activity_ms: duration_millis(now.saturating_duration_since(last_client)),
            last_backend_activity_ms: duration_millis(now.saturating_duration_since(last_backend)),
            packets_to_backend: self.packets_to_backend.load(Ordering::Relaxed),
            packets_to_client: self.packets_to_client.load(Ordering::Relaxed),
            bytes_to_backend: self.bytes_to_backend.load(Ordering::Relaxed),
            bytes_to_client: self.bytes_to_client.load(Ordering::Relaxed),
        }
    }
}

/// Stable control-plane view of a session.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub listener_id: ListenerId,
    pub listener: String,
    pub client_addr: SocketAddr,
    pub upstream_local_addr: SocketAddr,
    pub backend_addr: SocketAddr,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub last_client_activity_ms: u64,
    pub last_backend_activity_ms: u64,
    pub packets_to_backend: u64,
    pub packets_to_client: u64,
    pub bytes_to_backend: u64,
    pub bytes_to_client: u64,
}

/// The session is closed; set together with the cancellation token.
const STATE_CLOSED: usize = 1;
/// The session task is deciding whether to expire the session as idle.
const STATE_EXPIRING: usize = 1 << 1;
/// One listener forward in progress.
const STATE_FORWARDING: usize = 1 << 2;

/// Marks one listener forward in progress until dropped.
struct ForwardGuard<'a>(&'a AtomicUsize);

impl Drop for ForwardGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(STATE_FORWARDING, Ordering::SeqCst);
    }
}

/// Forwarding state stored in the session table.
///
/// The listener sends a client datagram directly on the session's connected
/// upstream socket when nothing is queued ahead of it. Only when that socket
/// cannot accept the datagram immediately is the datagram copied into the
/// bounded per-session queue that the session task drains. Because one
/// listener task is the only producer for a session, sending directly only
/// while `pending` is zero preserves client datagram order.
///
/// The handle owns everything a session holds on to, and its fields are
/// dropped in declaration order, so the global admission slot, declared
/// last, is released only once the last reference to the handle is gone and
/// the upstream socket and the live-session reservation went before it.
#[derive(Debug)]
pub struct SessionHandle {
    session: Arc<Session>,
    /// Owned only here, so the socket closes with the handle.
    upstream: UdpSocket,
    client_datagrams: Sender<QueuedDatagram>,
    /// Client datagrams queued or being sent by the session task.
    pending: AtomicUsize,
    accounting: Arc<QueueAccounting>,
    /// [`STATE_CLOSED`] and [`STATE_EXPIRING`] flags plus a count of listener
    /// forwards in progress (in units of [`STATE_FORWARDING`]).
    ///
    /// The listener hot path reads the flags from here because
    /// `CancellationToken::is_cancelled` takes a lock. The in-progress count
    /// lets idle expiry tell that a datagram is being forwarded right now, so
    /// a session is never expired under a datagram it has just accepted.
    state: AtomicUsize,
    cancel: CancellationToken,
    /// The session's receive buffers in the queue budget; `None` only for a
    /// handle that never had a task.
    _live: Option<LiveSession>,
    /// The per-IP slot is released as soon as the route is unpublished, so
    /// the same client can be admitted again while this session stops. The
    /// global slot is released when the handle itself is dropped, after the
    /// task has ended and every other field is gone, so `max_sessions`
    /// bounds the upstream sockets that exist.
    ///
    /// Must stay the last field.
    admission: Mutex<Option<AdmissionLease>>,
}

impl SessionHandle {
    #[cfg(test)]
    pub(super) fn for_test(session: Session) -> Arc<Self> {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind test socket");
        socket
            .set_nonblocking(true)
            .expect("make test socket nonblocking");
        let (client_datagrams, _receiver) = mpsc::channel(1);
        Arc::new(Self {
            session: Arc::new(session),
            upstream: UdpSocket::from_std(socket).expect("register test socket"),
            client_datagrams,
            pending: AtomicUsize::new(0),
            accounting: Arc::new(QueueAccounting {
                budget: QueueBudget::with_limit(usize::MAX),
                metrics: Arc::new(Metrics::default()),
                listener_counters: Arc::new(ListenerCounters::default()),
            }),
            state: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            _live: None,
            admission: Mutex::new(None),
        })
    }

    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Forwards an already size-validated client datagram without waiting.
    ///
    /// Once the session takes the datagram, all success, drop, and error
    /// accounting happens here. This never blocks the listener's receive
    /// loop.
    ///
    /// The session task closes the handle on every exit path, so a handle
    /// still held after the session was closed refuses the datagram with
    /// [`Forward::Closed`] instead of sending on a dying upstream socket;
    /// nothing is counted, and the caller treats the datagram as one from a
    /// client without a session. A datagram that passes this check just
    /// before the session closes can still be sent once; that is the same
    /// window the queued path always had.
    pub fn forward_from_client(
        &self,
        datagram: &[u8],
        metrics: &Metrics,
        listener_counters: &ListenerCounters,
        log_limiter: &LogRateLimiter,
    ) -> Forward {
        // A datagram dropped by the relay itself (full queue, exhausted
        // budget) is not client activity: a client that only sends into a
        // back-pressured session must still idle out and release its
        // admission slot and receive buffers. A send the backend side
        // refused still is, so a backend outage does not expire sessions
        // whose clients keep sending.
        let Some(forwarding) = self.begin_forward() else {
            return Forward::Closed;
        };

        // The forward is registered only across the non-blocking send or
        // enqueue and the activity update. Accounting and logging follow
        // after it ends, so a slow log sink never holds up the cleanup of a
        // session that is closed meanwhile.
        if self.pending.load(Ordering::Acquire) == 0 {
            match self.upstream.try_send(datagram) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => {
                    self.session.touch_client();
                    drop(forwarding);
                    record_backend_send(
                        &self.session,
                        datagram.len(),
                        result,
                        metrics,
                        listener_counters,
                        log_limiter,
                    );
                    return Forward::Taken;
                }
            }
        }

        let queued = self.enqueue(datagram);
        if queued.is_ok() {
            self.session.touch_client();
        }
        drop(forwarding);
        if let Err(reason) = queued {
            record_drop(metrics, listener_counters, reason);
            self.log_enqueue_drop(reason, datagram.len(), log_limiter);
        }
        Forward::Taken
    }

    fn log_enqueue_drop(&self, reason: DropReason, size: usize, log_limiter: &LogRateLimiter) {
        match reason {
            DropReason::QueueFull if log_limiter.should_log(LogSite::SessionQueueFull) => {
                warn!(
                    listener = %self.session.listener_name,
                    session_id = %self.session.id,
                    queue_depth = self.client_datagrams.max_capacity(),
                    "session queue full (service.session_queue_depth); dropping client datagram"
                );
            }
            DropReason::BudgetExhausted
                if log_limiter.should_log(LogSite::QueueBudgetExhausted) =>
            {
                let (max_sessions, max_datagram_size) = self.accounting.budget.configured();
                warn!(
                    listener = %self.session.listener_name,
                    datagram_bytes = size,
                    budget_used_bytes = self.accounting.budget.used(),
                    budget_limit_bytes = self.accounting.budget.limit(),
                    max_queue_memory_bytes = self.accounting.budget.max_queue_memory(),
                    live_sessions = self.accounting.budget.live_sessions(),
                    max_sessions,
                    max_datagram_size,
                    "process-wide queued-datagram memory budget exhausted; dropping client \
                     datagram (raise service.max_queue_memory_bytes, or if the limit is \
                     below it, lower service.max_sessions or service.max_datagram_size to \
                     leave more for queues)"
                );
            }
            _ => {}
        }
    }

    fn enqueue(&self, datagram: &[u8]) -> Result<(), DropReason> {
        // Claim a queue slot first so a full or closed queue drops without
        // copying the datagram or touching the shared budget.
        let permit = self
            .client_datagrams
            .try_reserve()
            .map_err(|error| match error {
                TrySendError::Full(()) => DropReason::QueueFull,
                TrySendError::Closed(()) => DropReason::SessionClosed,
            })?;
        let charged = self
            .accounting
            .budget
            .try_charge(datagram.len())
            .ok_or(DropReason::BudgetExhausted)?;
        self.pending.fetch_add(1, Ordering::AcqRel);
        permit.send(QueuedDatagram {
            payload: datagram.to_vec(),
            charged,
            accounting: Arc::clone(&self.accounting),
            settled: false,
        });
        Ok(())
    }

    fn finish_pending(&self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }

    /// Registers a forward in progress, unless the session is closed.
    ///
    /// A forward never loses to an idle expiry that is still being decided:
    /// it withdraws the expiry attempt instead, so only a session that has
    /// really been closed refuses a datagram.
    fn begin_forward(&self) -> Option<ForwardGuard<'_>> {
        let previous = self.state.fetch_add(STATE_FORWARDING, Ordering::SeqCst);
        let guard = ForwardGuard(&self.state);
        if previous & STATE_CLOSED != 0 {
            return None;
        }
        if previous & STATE_EXPIRING != 0 {
            self.state.fetch_and(!STATE_EXPIRING, Ordering::SeqCst);
        }
        Some(guard)
    }

    /// Decides idle expiry against concurrent listener forwards and, when
    /// the session is still idle, closes it for the forwarding path.
    ///
    /// The attempt is announced in the state word, activity is read, and the
    /// session is closed only if the word is unchanged since. A forward that
    /// started before the announcement is seen in the previous value, one
    /// that finished before it has already recorded its activity, and one
    /// that starts afterwards withdraws the announcement, so the final
    /// exchange fails. The session task still has to cancel and clean up.
    pub(super) fn try_expire(&self, idle_timeout: Duration) -> Expiry {
        let previous = self.state.fetch_or(STATE_EXPIRING, Ordering::SeqCst);
        if previous & STATE_CLOSED != 0 {
            return Expiry::Closed;
        }
        if previous < STATE_FORWARDING
            && Instant::now() >= self.session.last_activity() + idle_timeout
        {
            match self.state.compare_exchange(
                STATE_EXPIRING,
                STATE_EXPIRING | STATE_CLOSED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Expiry::Expired,
                Err(current) if current & STATE_CLOSED != 0 => return Expiry::Closed,
                Err(_) => {}
            }
        }
        self.state.fetch_and(!STATE_EXPIRING, Ordering::SeqCst);
        Expiry::Active
    }

    /// Waits for a listener forward that began before the session was closed.
    #[cfg(test)]
    fn wait_for_forwards(&self) {
        while self.state.load(Ordering::SeqCst) >= STATE_FORWARDING {
            std::thread::yield_now();
        }
    }

    /// Whether the session is closed; its route is then gone as well.
    pub(super) fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & STATE_CLOSED != 0
    }

    /// Closes the session for the forwarding path; idempotent. Returns
    /// whether this call closed it.
    ///
    /// Only [`SessionTable::close`] ends a session: it also removes the
    /// route, frees the client's admission slot, and wakes the task.
    pub(super) fn mark_closed(&self) -> bool {
        let previous = self.state.fetch_or(STATE_CLOSED, Ordering::SeqCst);
        previous & STATE_CLOSED == 0
    }

    /// Wakes the session task so that it stops; idempotent.
    pub(super) fn cancel_task(&self) {
        self.cancel.cancel();
    }

    /// Closes a handle that is in no table.
    #[cfg(test)]
    fn close(&self) -> bool {
        let closed = self.mark_closed();
        self.cancel_task();
        closed
    }

    /// Frees the client's per-IP admission slot, with the route; idempotent.
    /// The global slot stays held until the handle is dropped.
    pub(super) fn release_client_slot(&self) {
        if let Some(lease) = lock_unpoisoned(&self.admission).as_mut() {
            lease.release_client_slot();
        }
    }
}

/// Whether a session took a client datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum Forward {
    /// The session took the datagram and accounted for it: sent, queued,
    /// or dropped with its reason counted.
    Taken,
    /// The session was already closed and did not take the datagram;
    /// nothing was counted.
    Closed,
}

/// Outcome of an idle-expiry attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Expiry {
    /// The session was idle and is now closed for forwarding.
    Expired,
    /// A datagram was accepted meanwhile; the session stays open.
    Active,
    /// The session had already been closed for another reason.
    Closed,
}

/// Why a session task exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionCloseReason {
    Idle,
    Closed,
}

/// All owned resources needed to activate a session.
pub struct SessionLaunch {
    pub session: Arc<Session>,
    pub first_datagram: Vec<u8>,
    pub upstream: UdpSocket,
    pub listener_socket: Arc<UdpSocket>,
    pub table: Arc<SessionTable>,
    pub admission: AdmissionLease,
    pub metrics: Arc<Metrics>,
    pub listener_counters: Arc<ListenerCounters>,
    pub idle_timeout: watch::Receiver<Duration>,
    pub queue_depth: usize,
    pub queue_budget: Arc<QueueBudget>,
    pub parent_cancel: CancellationToken,
    pub tracker: TaskTracker,
    pub log_limiter: Arc<LogRateLimiter>,
}

/// Inserts and starts one long-lived session task.
///
/// Returns `None`, releasing the admission lease, when the client already
/// has a session.
pub fn launch_session(launch: SessionLaunch) -> Option<Arc<SessionHandle>> {
    let SessionLaunch {
        session,
        first_datagram,
        upstream,
        listener_socket,
        table,
        admission,
        metrics,
        listener_counters,
        idle_timeout,
        queue_depth,
        queue_budget,
        parent_cancel,
        tracker,
        log_limiter,
    } = launch;
    let (sender, receiver) = mpsc::channel(queue_depth.max(1));
    let cancel = parent_cancel.child_token();
    // Reserved before the handle is published, so a launched session's
    // buffers are counted from the start, and released with the handle.
    let live = queue_budget.session_started();
    let handle = Arc::new(SessionHandle {
        session: Arc::clone(&session),
        upstream,
        client_datagrams: sender,
        // The first datagram is in flight in the session task, so later
        // datagrams must queue behind it until it has been sent.
        pending: AtomicUsize::new(1),
        accounting: Arc::new(QueueAccounting {
            budget: queue_budget,
            metrics: Arc::clone(&metrics),
            listener_counters: Arc::clone(&listener_counters),
        }),
        state: AtomicUsize::new(0),
        cancel: cancel.clone(),
        _live: Some(live),
        // Stored before the handle is published, so a close that races the
        // insert always finds the lease to release.
        admission: Mutex::new(Some(admission)),
    });

    if table.insert(Arc::clone(&handle)).is_err() {
        // The lease goes with the unpublished handle.
        return None;
    }

    metrics.session_created();
    listener_counters.session_started();
    // Created before spawning so that a task dropped before its first poll
    // or in mid-run (shutdown or abort) still closes the handle, removes this
    // table entry, ends the gauges, and counts the datagrams it never sent.
    // The task then drops its references to the handle, and the last one
    // releases the global admission slot.
    let cleanup = SessionCleanup {
        handle: Arc::clone(&handle),
        table,
        receiver,
        metrics: Arc::clone(&metrics),
        listener_counters: Arc::clone(&listener_counters),
        reason: SessionCloseReason::Closed,
        first_datagram_unsent: true,
    };

    let task = SessionTask {
        first_datagram,
        listener_socket,
        metrics,
        listener_counters,
        idle_timeout,
        log_limiter,
    };
    let task_handle = Arc::clone(&handle);
    tracker.spawn(async move {
        run_session(task_handle, cancel, task, cleanup).await;
    });
    Some(handle)
}

/// Resources owned by a running session task.
struct SessionTask {
    first_datagram: Vec<u8>,
    listener_socket: Arc<UdpSocket>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    idle_timeout: watch::Receiver<Duration>,
    log_limiter: Arc<LogRateLimiter>,
}

async fn run_session(
    handle: Arc<SessionHandle>,
    cancel: CancellationToken,
    mut task: SessionTask,
    mut cleanup: SessionCleanup,
) {
    let upstream = &handle.upstream;

    let first_sent = tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        () = send_to_backend(
            &handle.session,
            upstream,
            &task.first_datagram,
            &task.metrics,
            &task.listener_counters,
            &task.log_limiter,
        ) => true,
    };
    task.first_datagram = Vec::new();
    if first_sent {
        cleanup.first_datagram_unsent = false;
        handle.finish_pending();
        forward_until_stopped(&handle, &cancel, &mut task, &mut cleanup, upstream).await;
    }
    debug!(
        session_id = %handle.session.id,
        reason = ?cleanup.reason,
        "session stopped"
    );
    // Dropping the cleanup guard closes the session for the listener the
    // moment the task decides to exit and discards what is still queued;
    // every datagram the task did not send is counted as `session_closed`.
    // The task's references to the handle go with it, and the last
    // reference releases the global admission slot after the socket.
}

async fn forward_until_stopped(
    handle: &Arc<SessionHandle>,
    cancel: &CancellationToken,
    task: &mut SessionTask,
    cleanup: &mut SessionCleanup,
    upstream: &UdpSocket,
) {
    let mut backend_buffer = vec![0_u8; handle.accounting.budget.max_datagram_size() + 1];

    // The idle deadline is armed once and only recomputed when it fires or
    // the configured timeout changes, so packet forwarding does not touch the
    // timer wheel.
    let mut idle_timeout = *task.idle_timeout.borrow_and_update();
    let idle_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
        handle.session.last_activity() + idle_timeout,
    ));
    tokio::pin!(idle_sleep);

    'session: loop {
        let max_size = handle.accounting.budget.max_datagram_size();
        if backend_buffer.len() != max_size.saturating_add(1) {
            backend_buffer.resize(max_size.saturating_add(1), 0);
        }

        tokio::select! {
            () = cancel.cancelled() => break,
            changed = task.idle_timeout.changed() => {
                if changed.is_err() {
                    break;
                }
                idle_timeout = *task.idle_timeout.borrow_and_update();
                idle_sleep.as_mut().reset(tokio::time::Instant::from_std(
                    handle.session.last_activity() + idle_timeout,
                ));
            }
            datagram = cleanup.receiver.recv() => {
                let Some(mut datagram) = datagram else {
                    break;
                };
                let mut drained = 0;
                loop {
                    tokio::select! {
                        biased;
                        // The unsent datagram counts itself when dropped.
                        () = cancel.cancelled() => break 'session,
                        () = send_to_backend(
                            &handle.session,
                            upstream,
                            &datagram.payload,
                            &task.metrics,
                            &task.listener_counters,
                            &task.log_limiter,
                        ) => {}
                    }
                    datagram.settle();
                    // Delivering a datagram the client sent earlier is
                    // client activity, as on the direct path. Without it, a
                    // session whose upstream socket was stalled for longer
                    // than the idle timeout would expire the moment the
                    // socket recovers, with accepted datagrams still queued.
                    handle.session.touch_client();
                    handle.finish_pending();
                    drained += 1;
                    if drained >= QUEUE_DRAIN_BATCH {
                        break;
                    }
                    match cleanup.receiver.try_recv() {
                        Ok(next) => datagram = next,
                        Err(_) => break,
                    }
                }
            }
            received = upstream.recv(&mut backend_buffer) => {
                match received {
                    Ok(size) if size <= max_size => {
                        // Backend activity is independent of whether the local
                        // client send succeeds.
                        handle.session.touch_backend();
                        let payload = &backend_buffer[..size];
                        // A closed session must not outlive a slow client-side
                        // send: it holds a global admission slot, its socket,
                        // and its buffers until its task ends.
                        let sent = tokio::select! {
                            biased;
                            () = cancel.cancelled() => None,
                            sent = task.listener_socket.send_to(
                                payload,
                                handle.session.client_addr,
                            ) => Some(sent),
                        };
                        let Some(sent) = sent else {
                            // Received from the backend but never delivered.
                            record_drop(
                                &task.metrics,
                                &task.listener_counters,
                                DropReason::SessionClosed,
                            );
                            break 'session;
                        };
                        match sent {
                            Ok(sent) if sent == size => {
                                handle.session.record_to_client(size);
                                task.listener_counters.to_client(size);
                                task.metrics.packet_to_client(size);
                            }
                            Ok(sent) => {
                                task.metrics.socket_error();
                                record_drop(
                                    &task.metrics,
                                    &task.listener_counters,
                                    DropReason::SendError,
                                );
                                if task.log_limiter.should_log(LogSite::PartialClientSend) {
                                    warn!(
                                        session_id = %handle.session.id,
                                        expected = size,
                                        sent,
                                        "UDP send reported a partial datagram"
                                    );
                                }
                            }
                            Err(error) => {
                                task.metrics.socket_error();
                                record_drop(
                                    &task.metrics,
                                    &task.listener_counters,
                                    DropReason::SendError,
                                );
                                if task.log_limiter.should_log(LogSite::ClientSendError) {
                                    warn!(
                                        session_id = %handle.session.id,
                                        %error,
                                        "failed to send backend datagram to client"
                                    );
                                }
                            }
                        }
                    }
                    Ok(_) => {
                        record_drop(&task.metrics, &task.listener_counters, DropReason::Oversize);
                    }
                    Err(error) => {
                        task.metrics.socket_error();
                        if task.log_limiter.should_log(LogSite::UpstreamRecvError) {
                            warn!(
                                session_id = %handle.session.id,
                                %error,
                                "upstream receive failed"
                            );
                        }
                        tokio::select! {
                            biased;
                            () = cancel.cancelled() => break 'session,
                            () = tokio::time::sleep(Duration::from_millis(10)) => {}
                        }
                    }
                }
            }
            () = &mut idle_sleep => {
                // A timeout change may not have been observed through
                // `changed()` yet; always decide with the current value.
                idle_timeout = *task.idle_timeout.borrow_and_update();
                let deadline = handle.session.last_activity() + idle_timeout;
                if Instant::now() >= deadline {
                    // The listener forwards on this session's socket from
                    // its own task, so the decision is made against it, and
                    // the route and the client's admission slot go in the
                    // same step: its next datagram opens a new session.
                    match cleanup.table.expire(handle, idle_timeout) {
                        Expiry::Expired => {
                            cleanup.reason = SessionCloseReason::Idle;
                            break;
                        }
                        Expiry::Closed => break,
                        // A datagram was just accepted or is being forwarded
                        // right now; let it record its activity before the
                        // deadline is computed again.
                        Expiry::Active => tokio::task::yield_now().await,
                    }
                }
                idle_sleep.as_mut().reset(tokio::time::Instant::from_std(
                    handle.session.last_activity() + idle_timeout,
                ));
            }
        }
    }
}

async fn send_to_backend(
    session: &Session,
    upstream: &UdpSocket,
    datagram: &[u8],
    metrics: &Metrics,
    listener_counters: &ListenerCounters,
    log_limiter: &LogRateLimiter,
) {
    let result = upstream.send(datagram).await;
    record_backend_send(
        session,
        datagram.len(),
        result,
        metrics,
        listener_counters,
        log_limiter,
    );
}

/// Accounts for one client-to-backend send.
fn record_backend_send(
    session: &Session,
    expected: usize,
    result: io::Result<usize>,
    metrics: &Metrics,
    listener_counters: &ListenerCounters,
    log_limiter: &LogRateLimiter,
) {
    match result {
        Ok(sent) if sent == expected => {
            session.record_to_backend(sent);
            listener_counters.to_backend(sent);
            metrics.packet_to_backend(sent);
        }
        Ok(sent) => {
            metrics.socket_error();
            record_drop(metrics, listener_counters, DropReason::SendError);
            if log_limiter.should_log(LogSite::PartialBackendSend) {
                warn!(
                    session_id = %session.id,
                    expected,
                    sent,
                    "UDP send reported a partial datagram"
                );
            }
        }
        Err(error) => {
            metrics.socket_error();
            record_drop(metrics, listener_counters, DropReason::SendError);
            if log_limiter.should_log(LogSite::BackendSendError) {
                warn!(
                    session_id = %session.id,
                    %error,
                    "failed to send client datagram to backend"
                );
            }
        }
    }
}

/// Ends a session exactly once, however its task stops.
struct SessionCleanup {
    handle: Arc<SessionHandle>,
    table: Arc<SessionTable>,
    receiver: Receiver<QueuedDatagram>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    reason: SessionCloseReason,
    /// The first datagram was never sent; counted as `session_closed` once.
    /// Queued datagrams count themselves.
    first_datagram_unsent: bool,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
        // The only close on the task's side, for every exit path including a
        // task dropped in mid-run or before its first poll. After it the
        // listener refuses this session.
        self.table.close(&self.handle);
        // Every datagram still queued counts itself as `session_closed` as
        // it is dropped here; so does one the listener was still enqueueing
        // when the session closed, once the queue itself is freed. Nothing
        // here waits for the listener.
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        if self.first_datagram_unsent {
            record_drop(
                &self.metrics,
                &self.listener_counters,
                DropReason::SessionClosed,
            );
        }
        self.table.remove(&self.handle);
        self.listener_counters.session_ended();
        self.metrics
            .session_ended(self.reason == SessionCloseReason::Idle);
        // The global admission slot is not released here: it goes with the
        // last reference to the handle, after the upstream socket and the
        // live-session reservation, so a replacement is never admitted on
        // top of a session that still owns its socket.
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct Harness {
        handle: Arc<SessionHandle>,
        receiver: Receiver<QueuedDatagram>,
        backend: UdpSocket,
        metrics: Arc<Metrics>,
        counters: Arc<ListenerCounters>,
        log_limiter: LogRateLimiter,
        budget: Arc<QueueBudget>,
    }

    async fn harness(capacity: usize, pending: usize, budget_bytes: usize) -> Harness {
        harness_with_budget(capacity, pending, QueueBudget::with_limit(budget_bytes)).await
    }

    async fn harness_with_budget(
        capacity: usize,
        pending: usize,
        budget: Arc<QueueBudget>,
    ) -> Harness {
        let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        upstream
            .connect(backend.local_addr().unwrap())
            .await
            .unwrap();
        let session = Session::new(
            ListenerId::new(1),
            "test".to_owned(),
            "127.0.0.1:40000".parse().unwrap(),
            upstream.local_addr().unwrap(),
            backend.local_addr().unwrap(),
        );
        let (client_datagrams, receiver) = mpsc::channel(capacity);
        let metrics = Arc::new(Metrics::default());
        let counters = Arc::new(ListenerCounters::default());
        let handle = Arc::new(SessionHandle {
            session: Arc::new(session),
            upstream,
            client_datagrams,
            pending: AtomicUsize::new(pending),
            accounting: Arc::new(QueueAccounting {
                budget: Arc::clone(&budget),
                metrics: Arc::clone(&metrics),
                listener_counters: Arc::clone(&counters),
            }),
            state: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            _live: None,
            admission: Mutex::new(None),
        });
        Harness {
            handle,
            receiver,
            backend,
            metrics,
            counters,
            log_limiter: LogRateLimiter::new(Duration::from_secs(60)),
            budget,
        }
    }

    /// What happened to one datagram, observed through the same counters
    /// operators see; production code does not return an outcome.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ForwardOutcome {
        Sent,
        Queued,
        Dropped(DropReason),
        /// The session was closed and did not take the datagram.
        Refused,
    }

    fn observe(
        handle: &SessionHandle,
        metrics: &Metrics,
        counters: &ListenerCounters,
        log_limiter: &LogRateLimiter,
        datagram: &[u8],
    ) -> ForwardOutcome {
        let before = metrics.snapshot();
        let pending_before = handle.pending.load(Ordering::Acquire);
        if handle.forward_from_client(datagram, metrics, counters, log_limiter) == Forward::Closed {
            assert_eq!(
                metrics.snapshot(),
                before,
                "a refused datagram must not be counted"
            );
            return ForwardOutcome::Refused;
        }
        let after = metrics.snapshot();
        let drops = (&before.drops(), &after.drops());
        if after.packets_to_backend_total > before.packets_to_backend_total {
            ForwardOutcome::Sent
        } else if drops.1.queue_full > drops.0.queue_full {
            ForwardOutcome::Dropped(DropReason::QueueFull)
        } else if drops.1.budget_exhausted > drops.0.budget_exhausted {
            ForwardOutcome::Dropped(DropReason::BudgetExhausted)
        } else if drops.1.session_closed > drops.0.session_closed {
            ForwardOutcome::Dropped(DropReason::SessionClosed)
        } else if drops.1.send_error > drops.0.send_error {
            ForwardOutcome::Dropped(DropReason::SendError)
        } else {
            assert_eq!(
                handle.pending.load(Ordering::Acquire),
                pending_before + 1,
                "a datagram that was neither sent nor dropped must be queued"
            );
            ForwardOutcome::Queued
        }
    }

    impl Harness {
        fn forward(&self, datagram: &[u8]) -> ForwardOutcome {
            observe(
                &self.handle,
                &self.metrics,
                &self.counters,
                &self.log_limiter,
                datagram,
            )
        }
    }

    #[tokio::test]
    async fn idle_upstream_sends_directly_without_queueing() {
        let harness = harness(4, 0, usize::MAX).await;
        // A freshly registered socket may not have reported writability yet;
        // either outcome is valid, and a queued datagram must stay queued.
        let outcome = harness.forward(b"direct");
        let mut buffer = [0_u8; 16];
        match outcome {
            ForwardOutcome::Sent => {
                let size =
                    tokio::time::timeout(Duration::from_secs(5), harness.backend.recv(&mut buffer))
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(&buffer[..size], b"direct");
                assert_eq!(harness.metrics.snapshot().packets_to_backend_total, 1);
                assert_eq!(harness.handle.pending.load(Ordering::Acquire), 0);
            }
            ForwardOutcome::Queued => {
                assert_eq!(harness.handle.pending.load(Ordering::Acquire), 1);
            }
            ForwardOutcome::Dropped(reason) => panic!("unexpected drop: {reason:?}"),
            ForwardOutcome::Refused => panic!("an open session refused a datagram"),
        }
        assert_eq!(harness.metrics.snapshot().datagrams_dropped_total, 0);
    }

    #[tokio::test]
    async fn datagrams_queue_behind_pending_work_to_preserve_order() {
        let mut harness = harness(4, 1, usize::MAX).await;
        assert_eq!(harness.forward(b"first"), ForwardOutcome::Queued);
        assert_eq!(harness.forward(b"second"), ForwardOutcome::Queued);
        assert_eq!(harness.receiver.try_recv().unwrap().payload, b"first");
        assert_eq!(harness.receiver.try_recv().unwrap().payload, b"second");
        assert_eq!(harness.metrics.snapshot().packets_to_backend_total, 0);
    }

    #[tokio::test]
    async fn queue_overflow_is_counted_as_queue_full() {
        let harness = harness(2, 1, usize::MAX).await;
        assert_eq!(harness.forward(b"one"), ForwardOutcome::Queued);
        assert_eq!(harness.forward(b"two"), ForwardOutcome::Queued);
        assert_eq!(
            harness.forward(b"three"),
            ForwardOutcome::Dropped(DropReason::QueueFull)
        );
        let stats = harness.metrics.snapshot();
        assert_eq!(stats.datagrams_dropped_total, 1);
        assert_eq!(stats.drops().queue_full, 1);
        assert_eq!(stats.drops().oversize, 0);
        let listener = harness.counters.snapshot();
        assert_eq!(listener.dropped, 1);
        assert_eq!(listener.drops().queue_full, 1);
        // The failed enqueue must not leave a phantom pending datagram.
        assert_eq!(harness.handle.pending.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn queue_memory_budget_charges_size_plus_overhead_and_is_released() {
        let charge = |len| len + QUEUED_DATAGRAM_OVERHEAD_BYTES;
        let mut harness = harness(16, 1, charge(60) + charge(40)).await;
        assert_eq!(harness.forward(&[1; 60]), ForwardOutcome::Queued);
        assert_eq!(harness.budget.used(), charge(60));
        assert_eq!(
            harness.forward(&[2; 60]),
            ForwardOutcome::Dropped(DropReason::BudgetExhausted)
        );
        // Budget exhaustion is reported separately from a full session queue.
        let drops = harness.metrics.snapshot().drops();
        assert_eq!((drops.budget_exhausted, drops.queue_full), (1, 0));
        let drops = harness.counters.snapshot().drops();
        assert_eq!((drops.budget_exhausted, drops.queue_full), (1, 0));
        assert_eq!(harness.forward(&[3; 40]), ForwardOutcome::Queued);
        assert_eq!(harness.budget.used(), charge(60) + charge(40));
        drop(harness.receiver.try_recv().unwrap());
        assert_eq!(harness.budget.used(), charge(40));
        harness.receiver.close();
        while harness.receiver.try_recv().is_ok() {}
        assert_eq!(harness.budget.used(), 0);
    }

    #[tokio::test]
    async fn empty_datagrams_consume_the_fixed_overhead() {
        let harness = harness(16, 1, usize::MAX).await;
        assert_eq!(harness.forward(&[]), ForwardOutcome::Queued);
        assert_eq!(harness.budget.used(), QUEUED_DATAGRAM_OVERHEAD_BYTES);
    }

    #[tokio::test]
    async fn tiny_datagrams_cannot_exceed_the_budget_entry_count() {
        let entries = 5;
        let budget = entries * QueueBudget::charge(1);
        let harness = harness(1024, 1, budget).await;
        for _ in 0..entries {
            assert_eq!(harness.forward(&[0]), ForwardOutcome::Queued);
        }
        for datagram in [&[][..], &[0][..]] {
            assert_eq!(
                harness.forward(datagram),
                ForwardOutcome::Dropped(DropReason::BudgetExhausted)
            );
        }
        assert_eq!(harness.budget.used(), budget);
    }

    #[tokio::test]
    async fn a_lowered_limit_applies_while_more_is_already_used() {
        let charge = QueueBudget::charge(100);
        // A 50-byte maximum datagram reserves 2 * 50 + 1 bytes per session.
        let budget = QueueBudget::for_test(2 * charge, 0, 50);
        let mut harness = harness_with_budget(16, 1, budget).await;
        assert_eq!(harness.forward(&[1; 100]), ForwardOutcome::Queued);
        assert_eq!(harness.forward(&[2; 100]), ForwardOutcome::Queued);
        // A new live session's receive buffers lower the limit below use.
        let _live = harness.budget.session_started();
        assert_eq!(harness.budget.limit(), 2 * charge - 101);
        assert_eq!(
            harness.forward(&[3; 1]),
            ForwardOutcome::Dropped(DropReason::BudgetExhausted)
        );
        drop(harness.receiver.try_recv().unwrap());
        // Still at the new limit: a small datagram does not fit beside one.
        assert_eq!(
            harness.forward(&[4; 1]),
            ForwardOutcome::Dropped(DropReason::BudgetExhausted)
        );
        drop(harness.receiver.try_recv().unwrap());
        assert_eq!(harness.forward(&[5; 100]), ForwardOutcome::Queued);
    }

    #[tokio::test]
    async fn closed_sessions_refuse_datagrams_without_counting_them() {
        let harness = harness(4, 0, usize::MAX).await;
        harness.handle.close();
        // The listener gives a refused datagram a new session instead.
        assert_eq!(harness.forward(b"late"), ForwardOutcome::Refused);
        assert_eq!(harness.metrics.snapshot().drops().session_closed, 0);
        assert_eq!(harness.handle.pending.load(Ordering::Acquire), 0);
    }

    struct Running {
        handle: Arc<SessionHandle>,
        backend: UdpSocket,
        table: Arc<SessionTable>,
        idle: watch::Sender<Duration>,
        metrics: Arc<Metrics>,
        counters: Arc<ListenerCounters>,
        log_limiter: Arc<LogRateLimiter>,
        tracker: TaskTracker,
        parent: CancellationToken,
        admission: Arc<crate::limits::AdmissionController>,
        budget: Arc<QueueBudget>,
    }

    async fn launch_running(idle_timeout: Duration) -> Running {
        let running = launch_unstarted(idle_timeout, QueueBudget::with_limit(usize::MAX)).await;
        let mut buffer = [0_u8; 64];
        let size = tokio::time::timeout(Duration::from_secs(5), running.backend.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..size], b"first");
        running
    }

    /// Launches a session whose task has not been polled yet: the tests run
    /// on a current-thread runtime, so it starts at the next yield.
    async fn launch_unstarted(idle_timeout: Duration, budget: Arc<QueueBudget>) -> Running {
        let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        upstream
            .connect(backend.local_addr().unwrap())
            .await
            .unwrap();
        let listener_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_addr: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let session = Arc::new(Session::new(
            ListenerId::new(1),
            "test".to_owned(),
            client_addr,
            upstream.local_addr().unwrap(),
            backend.local_addr().unwrap(),
        ));
        let table = SessionTable::new();
        // One session per IP, so a replacement for the same client can only
        // be admitted once the old session's per-IP slot is released, and
        // two globally, so the old session's global slot shows up as well.
        let admission_controller = crate::limits::AdmissionController::new(2, 1, 100);
        let admission = admission_controller.try_acquire(client_addr.ip()).unwrap();
        let (idle, idle_receiver) = watch::channel(idle_timeout);
        let metrics = Arc::new(Metrics::default());
        let counters = Arc::new(ListenerCounters::default());
        let log_limiter = Arc::new(LogRateLimiter::new(Duration::from_secs(60)));
        let tracker = TaskTracker::new();
        let parent = CancellationToken::new();
        let handle = launch_session(SessionLaunch {
            session,
            first_datagram: b"first".to_vec(),
            upstream,
            listener_socket,
            table: Arc::clone(&table),
            admission,
            metrics: Arc::clone(&metrics),
            listener_counters: Arc::clone(&counters),
            idle_timeout: idle_receiver,
            queue_depth: 16,
            queue_budget: Arc::clone(&budget),
            parent_cancel: parent.clone(),
            tracker: tracker.clone(),
            log_limiter: Arc::clone(&log_limiter),
        })
        .unwrap();
        Running {
            handle,
            backend,
            table,
            idle,
            metrics,
            counters,
            log_limiter,
            tracker,
            parent,
            admission: admission_controller,
            budget,
        }
    }

    impl Running {
        fn in_table(&self) -> bool {
            self.table.get_by_id(&self.handle.session().id).is_some()
        }

        async fn wait_until_removed(&self) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while self.in_table() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("session task must exit");
        }

        async fn stop(self) {
            self.parent.cancel();
            self.tracker.close();
            tokio::time::timeout(Duration::from_secs(5), self.tracker.wait())
                .await
                .expect("session task must stop");
        }
    }

    #[tokio::test]
    async fn idle_expiry_uses_a_timeout_raised_without_a_change_notification() {
        let running = launch_running(Duration::from_millis(100)).await;
        // Update the value without waking `changed()`, so only the idle timer
        // firing can observe it.
        running.idle.send_if_modified(|timeout| {
            *timeout = Duration::from_secs(30);
            false
        });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            running.in_table(),
            "session expired using a stale idle timeout"
        );
        running.stop().await;
    }

    #[tokio::test]
    async fn a_session_that_exited_on_its_own_rejects_direct_sends() {
        let running = launch_running(Duration::from_millis(50)).await;
        // Idle expiry: nobody calls `close()` from outside.
        running.wait_until_removed().await;
        assert_eq!(
            observe(
                &running.handle,
                &running.metrics,
                &running.counters,
                &running.log_limiter,
                b"late",
            ),
            ForwardOutcome::Refused
        );
        let mut buffer = [0_u8; 64];
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                running.backend.recv(&mut buffer)
            )
            .await
            .is_err(),
            "a datagram reached the backend through an exited session"
        );
        assert_eq!(running.metrics.snapshot().packets_to_backend_total, 1);
        running.stop().await;
    }

    impl Running {
        fn forward(&self, datagram: &[u8]) -> ForwardOutcome {
            observe(
                &self.handle,
                &self.metrics,
                &self.counters,
                &self.log_limiter,
                datagram,
            )
        }

        /// Asserts `expected` `session_closed` drops in the process and
        /// listener counters, with totals derived from the breakdown.
        fn assert_session_closed_drops(&self, expected: u64) {
            let process = self.metrics.snapshot();
            let listener = self.counters.snapshot();
            assert_eq!(process.drops().session_closed, expected);
            assert_eq!(listener.drops().session_closed, expected);
            assert_eq!(process.datagrams_dropped_total, expected);
            assert_eq!(listener.dropped, expected);
        }
    }

    #[tokio::test]
    async fn queue_full_drops_do_not_charge_the_budget() {
        let harness = harness(1, 1, usize::MAX).await;
        assert_eq!(harness.forward(&[1; 100]), ForwardOutcome::Queued);
        let used = harness.budget.used();
        assert_eq!(used, QueueBudget::charge(100));
        assert_eq!(
            harness.forward(&[2; 1000]),
            ForwardOutcome::Dropped(DropReason::QueueFull)
        );
        assert_eq!(harness.budget.used(), used);
        assert_eq!(harness.handle.pending.load(Ordering::Acquire), 2);
    }

    #[test]
    fn the_budget_recovers_once_surplus_sessions_end() {
        let lowered = ServiceConfig {
            max_sessions: 1,
            max_datagram_size: 65_507,
            ..ServiceConfig::default()
        };
        let budget = QueueBudget::new(&ServiceConfig::default());
        // Enough live sessions that their receive buffers use the whole
        // budget once `max_sessions` shrinks.
        let live: Vec<_> = (0..40_000).map(|_| budget.session_started()).collect();
        budget.configure(&lowered);
        assert_eq!(budget.limit(), 0);
        assert!(budget.starved_by_live_sessions());
        assert_eq!(
            budget.limit(),
            lowered.queued_datagram_budget_with_live_sessions(live.len())
        );

        // End the sessions on several threads while reloads race with them.
        let mut live = live;
        let chunks: Vec<Vec<_>> = (0..4)
            .map(|_| live.split_off(live.len() - 10_000))
            .collect();
        std::thread::scope(|scope| {
            for chunk in chunks {
                scope.spawn(move || drop(chunk));
            }
            for _ in 0..1_000 {
                budget.configure(&ServiceConfig::default());
                budget.configure(&lowered);
            }
        });
        assert_eq!(budget.live_sessions(), 0);
        assert_eq!(budget.limit(), lowered.queued_datagram_budget());
        assert!(budget.limit() > 0);
        assert!(!budget.starved_by_live_sessions());
    }

    #[test]
    fn raising_the_datagram_size_under_many_live_sessions_is_detected() {
        let before = ServiceConfig {
            max_sessions: 50_000,
            max_datagram_size: 4096,
            ..ServiceConfig::default()
        };
        // Valid on its own: a positive steady-state budget for 1000 sessions.
        let after = ServiceConfig {
            max_sessions: 1_000,
            max_datagram_size: 65_507,
            ..ServiceConfig::default()
        };
        assert!(after.queued_datagram_budget() > 0);

        let budget = QueueBudget::new(&before);
        let live: Vec<_> = (0..40_000).map(|_| budget.session_started()).collect();
        assert!(budget.limit() > 0);
        assert!(!budget.starved_by_live_sessions());

        // Live sessions resize their receive buffers to the new maximum, so
        // 40 000 * (2 * 65507 + 1) bytes exceed the 4 GiB ceiling.
        budget.configure(&after);
        assert_eq!(budget.limit(), 0);
        assert!(budget.starved_by_live_sessions());

        // With 32 782 sessions left, 33 566 bytes remain: not zero, but less
        // than one maximum-sized datagram's charge, so still starved.
        let mut live = live;
        live.truncate(32_782);
        assert!(budget.limit() > 0);
        assert!(budget.limit() < QueueBudget::charge(after.max_datagram_size));
        assert!(budget.starved_by_live_sessions());

        drop(live);
        assert!(!budget.starved_by_live_sessions());
        assert_eq!(budget.limit(), after.queued_datagram_budget());
    }

    #[test]
    fn the_budget_matches_the_configured_steady_state() {
        // By default the queue-memory limit is what applies.
        let capped = ServiceConfig::default();
        let budget = QueueBudget::new(&capped);
        assert_eq!(
            budget.limit(),
            usize::try_from(crate::config::DEFAULT_MAX_QUEUE_MEMORY_BYTES).unwrap()
        );
        // With enough live sessions the receive buffers leave less than it.
        let live: Vec<_> = (0..500_000).map(|_| budget.session_started()).collect();
        assert!(budget.limit() < usize::try_from(capped.max_queue_memory_bytes).unwrap());
        assert_eq!(
            budget.limit(),
            capped.queued_datagram_budget_with_live_sessions(live.len())
        );
        drop(live);

        // Without that limit, the budget is what the receive buffers leave.
        let service = ServiceConfig {
            max_queue_memory_bytes: u64::try_from(MAX_SESSION_DATAGRAM_MEMORY_BYTES).unwrap(),
            ..ServiceConfig::default()
        };
        let budget = QueueBudget::new(&service);
        assert_eq!(
            budget.limit(),
            usize::try_from(
                MAX_SESSION_DATAGRAM_MEMORY_BYTES
                    - crate::config::fixed_datagram_memory(
                        service.max_sessions,
                        service.max_datagram_size
                    )
            )
            .unwrap()
        );
        assert_eq!(budget.limit(), service.queued_datagram_budget());
        let extra = service.max_sessions + 3;
        let live: Vec<_> = (0..extra).map(|_| budget.session_started()).collect();
        assert_eq!(
            budget.limit(),
            service.queued_datagram_budget_with_live_sessions(extra)
        );
        drop(live);
        assert_eq!(budget.limit(), service.queued_datagram_budget());
    }

    #[tokio::test]
    async fn a_launched_session_reserves_its_buffers_immediately() {
        let budget = QueueBudget::with_limit(usize::MAX);
        let running = launch_unstarted(Duration::from_secs(30), Arc::clone(&budget)).await;
        assert_eq!(budget.live_sessions(), 1);
        running.stop().await;
        assert_eq!(budget.live_sessions(), 0);
    }

    #[tokio::test]
    async fn closing_a_session_counts_its_queued_datagrams() {
        let budget = QueueBudget::with_limit(usize::MAX);
        let running = launch_unstarted(Duration::from_secs(30), Arc::clone(&budget)).await;
        let mut buffer = [0_u8; 64];
        tokio::time::timeout(Duration::from_secs(5), running.backend.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        // Force the queued path; the session task cannot run until this
        // test yields, so every datagram is still queued at close.
        running.handle.pending.fetch_add(1, Ordering::AcqRel);
        for _ in 0..5 {
            assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        }
        running.handle.close();
        running.wait_until_removed().await;
        running.tracker.close();
        running.tracker.wait().await;

        running.assert_session_closed_drops(5);
        assert_eq!(running.metrics.snapshot().packets_to_backend_total, 1);
        assert_eq!(budget.used(), 0);
        running.stop().await;
    }

    #[tokio::test]
    async fn cancelling_before_the_first_send_counts_every_datagram_once() {
        let budget = QueueBudget::with_limit(usize::MAX);
        let running = launch_unstarted(Duration::from_secs(30), Arc::clone(&budget)).await;
        for _ in 0..3 {
            assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        }
        let metrics = Arc::clone(&running.metrics);
        let counters = Arc::clone(&running.counters);
        running.stop().await;

        // The unsent first datagram plus the three queued behind it.
        assert_eq!(metrics.snapshot().drops().session_closed, 4);
        assert_eq!(metrics.snapshot().datagrams_dropped_total, 4);
        assert_eq!(counters.snapshot().drops().session_closed, 4);
        assert_eq!(counters.snapshot().dropped, 4);
        assert_eq!(metrics.snapshot().packets_to_backend_total, 0);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.live_sessions(), 0);
    }

    /// Asserts whether forwarding `datagram` moved the client activity time.
    async fn assert_touch(
        harness: &Harness,
        datagram: &[u8],
        expected: ForwardOutcome,
        touched: bool,
    ) {
        let before = harness
            .handle
            .session
            .activity
            .last_client_ns
            .load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(harness.forward(datagram), expected);
        let after = harness
            .handle
            .session
            .activity
            .last_client_ns
            .load(Ordering::Relaxed);
        assert_eq!(
            after > before,
            touched,
            "{expected:?} touched={}",
            after > before
        );
    }

    #[tokio::test]
    async fn only_accepted_datagrams_refresh_client_activity() {
        // A queued datagram counts as activity.
        let queued = harness(1, 1, usize::MAX).await;
        assert_touch(&queued, b"one", ForwardOutcome::Queued, true).await;
        // A full session queue does not.
        assert_touch(
            &queued,
            b"two",
            ForwardOutcome::Dropped(DropReason::QueueFull),
            false,
        )
        .await;

        // An exhausted shared budget does not.
        let starved = harness(16, 1, QueueBudget::charge(1)).await;
        assert_touch(
            &starved,
            &[0; 2],
            ForwardOutcome::Dropped(DropReason::BudgetExhausted),
            false,
        )
        .await;

        // A closed session does not.
        let closed = harness(16, 0, usize::MAX).await;
        closed.handle.close();
        assert_touch(&closed, b"late", ForwardOutcome::Refused, false).await;

        // A successful direct send does.
        let direct = harness(16, 0, usize::MAX).await;
        assert_touch(&direct, b"direct", ForwardOutcome::Sent, true).await;

        // So does a send the OS refused: the client is still active while the
        // backend side fails. A UDP payload above 65535 bytes always fails.
        let failing = harness(16, 0, usize::MAX).await;
        assert_touch(
            &failing,
            &vec![0; 70_000],
            ForwardOutcome::Dropped(DropReason::SendError),
            true,
        )
        .await;
        assert_eq!(failing.metrics.snapshot().drops().send_error, 1);
    }

    #[tokio::test]
    async fn idle_expiry_yields_to_a_forward_in_progress() {
        let harness = harness(16, 0, usize::MAX).await;
        let timeout = Duration::from_millis(20);
        tokio::time::sleep(timeout * 2).await;

        // A datagram is being forwarded right now: the session stays open
        // and keeps accepting datagrams.
        let forwarding = harness.handle.begin_forward().expect("session is open");
        assert_eq!(harness.handle.try_expire(timeout), Expiry::Active);
        drop(forwarding);
        assert_eq!(
            observe(
                &harness.handle,
                &harness.metrics,
                &harness.counters,
                &harness.log_limiter,
                b"late",
            ),
            ForwardOutcome::Sent
        );

        // That datagram counted as activity, so the session is not idle.
        assert_eq!(harness.handle.try_expire(timeout), Expiry::Active);
        assert_eq!(harness.handle.state.load(Ordering::SeqCst), 0);

        // A forward that starts while an expiry is being decided withdraws
        // it and is sent; it is not refused on a session that stays open.
        tokio::time::sleep(timeout * 2).await;
        harness
            .handle
            .state
            .fetch_or(STATE_EXPIRING, Ordering::SeqCst);
        assert_eq!(
            observe(
                &harness.handle,
                &harness.metrics,
                &harness.counters,
                &harness.log_limiter,
                b"during",
            ),
            ForwardOutcome::Sent
        );
        assert_eq!(harness.handle.state.load(Ordering::SeqCst), 0);

        // Idle with nothing in progress: the session expires, and from then
        // on a datagram is refused instead of being sent on the dying socket.
        tokio::time::sleep(timeout * 2).await;
        assert_eq!(harness.handle.try_expire(timeout), Expiry::Expired);
        assert!(harness.handle.is_closed());
        assert_eq!(
            observe(
                &harness.handle,
                &harness.metrics,
                &harness.counters,
                &harness.log_limiter,
                b"too late",
            ),
            ForwardOutcome::Refused
        );
        assert_eq!(harness.metrics.snapshot().packets_to_backend_total, 2);
    }

    #[tokio::test]
    async fn idle_expiry_frees_the_route_and_the_client_slot_in_one_step() {
        let running = launch_running(Duration::from_secs(30)).await;
        let key = running.handle.session().key();
        let client_ip = running.handle.session().client_addr.ip();

        // Not idle yet: nothing changes.
        assert_eq!(
            running
                .table
                .expire(&running.handle, Duration::from_secs(30)),
            Expiry::Active
        );
        assert!(running.table.get_by_key(&key).is_some());
        assert!(running.admission.try_acquire(client_ip).is_err());

        // Idle: the session is closed for forwarding, unrouted, and its
        // client's admission slot is free before its task has run again.
        assert_eq!(
            running.table.expire(&running.handle, Duration::ZERO),
            Expiry::Expired
        );
        assert!(running.handle.is_closed());
        assert!(running.table.get_by_key(&key).is_none());
        drop(
            running
                .admission
                .try_acquire(client_ip)
                .expect("an expired session must free its client's slot"),
        );
        // It expired; a later close does not count it as closed again.
        assert!(!running.table.close(&running.handle));
        assert_eq!(
            running.table.expire(&running.handle, Duration::ZERO),
            Expiry::Closed
        );
        running.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_or_closed_session_sends_nothing_further_under_concurrent_forwards() {
        const FORWARDS: u64 = 500;
        for round in 0..100 {
            let harness = harness(16, 0, usize::MAX).await;
            let table = SessionTable::new();
            assert!(table.insert(Arc::clone(&harness.handle)).is_ok());
            // Direct sends need the socket's write readiness, which the
            // runtime reports only once it has polled the new socket.
            harness.handle.upstream.writable().await.unwrap();
            // Alternate between idle expiry and an operator close racing the
            // listener's forwarding path.
            let expire = round % 2 == 0;

            // Set once the session is stopped and a forward that had
            // already begun has finished.
            let stopped = AtomicBool::new(false);
            let forwarder_done = AtomicBool::new(false);
            // Refused by the closed session; the listener reopens these.
            let refused = AtomicU64::new(0);
            let sent_after_stop = std::thread::scope(|scope| {
                let forwarder = scope.spawn(|| {
                    let mut sent_after_stop = 0;
                    for _ in 0..FORWARDS {
                        let stopped_before = stopped.load(Ordering::SeqCst);
                        // Only this thread forwards, so the difference is
                        // this datagram's own outcome.
                        let sent_before = harness.metrics.snapshot().packets_to_backend_total;
                        let forward = harness.handle.forward_from_client(
                            b"x",
                            &harness.metrics,
                            &harness.counters,
                            &harness.log_limiter,
                        );
                        if forward == Forward::Closed {
                            refused.fetch_add(1, Ordering::SeqCst);
                        }
                        let sent = harness.metrics.snapshot().packets_to_backend_total;
                        if stopped_before && sent != sent_before {
                            sent_after_stop += 1;
                        }
                    }
                    forwarder_done.store(true, Ordering::SeqCst);
                    sent_after_stop
                });
                scope.spawn(|| {
                    // Stop in mid-stream, after a varying number of sends.
                    while harness.metrics.snapshot().packets_to_backend_total < round % 17
                        && !forwarder_done.load(Ordering::SeqCst)
                    {
                        std::hint::spin_loop();
                    }
                    if expire {
                        while table.expire(&harness.handle, Duration::ZERO) != Expiry::Expired {
                            std::hint::spin_loop();
                        }
                    } else {
                        assert!(table.close(&harness.handle));
                    }
                    harness.handle.wait_for_forwards();
                    stopped.store(true, Ordering::SeqCst);
                });
                forwarder.join().unwrap()
            });

            // Nothing was sent on the upstream socket after the stop.
            assert_eq!(sent_after_stop, 0);
            let stats = harness.metrics.snapshot();
            assert!(harness.handle.is_closed());
            assert!(
                table.get_by_key(&harness.handle.session().key()).is_none(),
                "a stopped session must be unrouted"
            );
            // The two can never both claim the session.
            assert_eq!(
                table.expire(&harness.handle, Duration::ZERO),
                Expiry::Closed
            );
            assert!(!table.close(&harness.handle));
            // Every datagram is sent, queued, counted as dropped, or refused
            // uncounted for the listener to reopen the session.
            let queued = u64::try_from(harness.handle.pending.load(Ordering::Acquire)).unwrap();
            assert_eq!(
                stats.packets_to_backend_total
                    + stats.drops().total()
                    + queued
                    + refused.load(Ordering::SeqCst),
                FORWARDS
            );
            assert_eq!(
                harness.handle.state.load(Ordering::SeqCst) & !STATE_EXPIRING,
                STATE_CLOSED
            );
        }
    }

    #[tokio::test]
    async fn delivering_a_queued_datagram_refreshes_client_activity() {
        let running = launch_running(Duration::from_secs(30)).await;
        // Queue behind a datagram the task is "still sending", as when the
        // upstream socket is stalled.
        running.handle.pending.fetch_add(1, Ordering::AcqRel);
        assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        let enqueued = running.handle.session.last_client_activity();

        // The stall: the session task cannot run while this thread blocks,
        // and nothing the client sends meanwhile is accepted.
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(running.handle.session.last_client_activity(), enqueued);

        // The socket recovers and the queued datagram is delivered. That
        // counts as activity now, so an idle timeout that elapsed during
        // the stall does not expire a session that is forwarding again.
        let mut buffer = [0_u8; 64];
        let size = tokio::time::timeout(Duration::from_secs(5), running.backend.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..size], b"queued");
        tokio::task::yield_now().await;
        assert!(
            running.handle.session.last_client_activity() >= enqueued + Duration::from_millis(40)
        );
        running.stop().await;
    }

    #[tokio::test]
    async fn queued_datagrams_account_for_themselves_wherever_they_are_dropped() {
        let mut harness = harness(16, 1, usize::MAX).await;
        for _ in 0..3 {
            assert_eq!(harness.forward(&[7; 10]), ForwardOutcome::Queued);
        }
        assert_eq!(harness.budget.used(), 3 * QueueBudget::charge(10));

        // One is delivered: settled, so released but not counted as a drop.
        harness.receiver.recv().await.unwrap().settle();
        assert_eq!(harness.budget.used(), 2 * QueueBudget::charge(10));
        assert_eq!(harness.metrics.snapshot().drops().total(), 0);

        // One is taken by the task but dropped unsent.
        drop(harness.receiver.recv().await.unwrap());
        assert_eq!(harness.metrics.snapshot().drops().session_closed, 1);

        // The session's cleanup has drained and closed the queue, and the
        // listener then finishes an enqueue it had already begun. Nobody
        // receives that datagram; it is counted when the queue is freed.
        let permit = harness.handle.client_datagrams.try_reserve().unwrap();
        harness.receiver.close();
        drop(harness.receiver.try_recv().unwrap());
        assert_eq!(harness.metrics.snapshot().drops().session_closed, 2);
        let charged = harness.budget.try_charge(10).unwrap();
        permit.send(QueuedDatagram {
            payload: vec![7; 10],
            charged,
            accounting: Arc::clone(&harness.handle.accounting),
            settled: false,
        });
        assert_eq!(harness.budget.used(), QueueBudget::charge(10));
        drop(harness.receiver);
        drop(harness.handle);
        assert_eq!(harness.budget.used(), 0);
        assert_eq!(harness.metrics.snapshot().drops().session_closed, 3);
        assert_eq!(harness.counters.snapshot().drops().session_closed, 3);
    }

    #[test]
    fn a_session_task_dropped_in_mid_run_counts_its_queued_datagrams() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (metrics, counters, admission) = runtime.block_on(async {
            let running = launch_running(Duration::from_secs(30)).await;
            // Queued behind a datagram the task is still sending.
            running.handle.pending.fetch_add(1, Ordering::AcqRel);
            for _ in 0..3 {
                assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
            }
            (
                Arc::clone(&running.metrics),
                Arc::clone(&running.counters),
                Arc::clone(&running.admission),
            )
        });
        // Dropping the runtime drops the task without running it again.
        drop(runtime);
        assert_eq!(metrics.snapshot().drops().session_closed, 3);
        assert_eq!(counters.snapshot().drops().session_closed, 3);
        assert_eq!(metrics.snapshot().active_sessions, 0);
        assert_eq!(admission.active(), 0);
    }

    #[tokio::test]
    async fn a_closed_session_reports_expiry_and_close_only_once() {
        let harness = harness(16, 0, usize::MAX).await;
        assert!(harness.handle.close(), "the first close closes the session");
        assert!(!harness.handle.close(), "a repeated close is not counted");
        assert!(harness.handle.is_closed());
        // An already closed session stops, but not as an idle expiry.
        assert_eq!(harness.handle.try_expire(Duration::ZERO), Expiry::Closed);
    }

    #[tokio::test]
    async fn a_close_racing_the_launch_still_frees_the_client_slot() {
        let running =
            launch_unstarted(Duration::from_secs(30), QueueBudget::with_limit(usize::MAX)).await;
        let client_ip = running.handle.session().client_addr.ip();
        // The lease is in the handle from the moment it is published, so a
        // close before the task has run at all frees the client's slot.
        assert!(running.admission.try_acquire(client_ip).is_err());
        assert!(running.table.close(&running.handle));
        let replacement = running
            .admission
            .try_acquire(client_ip)
            .expect("closing a just-launched session must free its client's slot");
        drop(replacement);
        let admission = Arc::clone(&running.admission);
        running.stop().await;
        assert_eq!(admission.active(), 0);
    }

    #[tokio::test]
    async fn closing_frees_the_client_slot_at_once_and_the_global_slot_with_the_task() {
        use crate::limits::AdmissionRejection;

        let running = launch_running(Duration::from_secs(30)).await;
        let client_ip = running.handle.session().client_addr.ip();
        let other_ip: std::net::IpAddr = "127.0.0.2".parse().unwrap();
        // One session per IP, held by this session.
        assert_eq!(
            running.admission.try_acquire(client_ip).unwrap_err(),
            AdmissionRejection::PerIpLimit
        );

        // Keep the old task from ending: it cannot run until this test yields.
        running.handle.pending.fetch_add(1, Ordering::AcqRel);
        assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        running.table.close(&running.handle);
        assert!(
            running
                .table
                .get_by_id(&running.handle.session().id)
                .is_some()
        );

        // The replacement for the same client is admitted right away.
        let replacement = running
            .admission
            .try_acquire(client_ip)
            .expect("a closed session must free its client's slot");

        // The old session still owns its upstream socket, so it still counts
        // toward the global limit of two: nothing is admitted on top of it.
        assert_eq!(running.admission.active(), 2);
        assert_eq!(
            running.admission.try_acquire(other_ip).unwrap_err(),
            AdmissionRejection::GlobalLimit
        );

        running.wait_until_removed().await;
        running.tracker.close();
        tokio::time::timeout(Duration::from_secs(5), running.tracker.wait())
            .await
            .expect("session task must end");
        running.assert_session_closed_drops(1);
        // Its task has ended, but this test still holds the handle, as the
        // listener does while it forwards: the upstream socket and the
        // receive buffers still exist, so the global slot is still held.
        assert_eq!(running.admission.active(), 2);
        assert_eq!(running.budget.live_sessions(), 1);
        assert_eq!(
            running.admission.try_acquire(other_ip).unwrap_err(),
            AdmissionRejection::GlobalLimit
        );

        // With the last reference the socket closes, and only then the
        // global slot is free; neither slot was released twice.
        let Running {
            handle,
            admission,
            budget,
            ..
        } = running;
        let upstream_addr = handle.session().upstream_local_addr;
        drop(handle);
        assert_eq!(budget.live_sessions(), 0);
        drop(
            std::net::UdpSocket::bind(upstream_addr)
                .expect("the old session's upstream socket must be closed"),
        );
        assert_eq!(admission.active(), 1);
        drop(
            admission
                .try_acquire(other_ip)
                .expect("an ended session must free its global slot"),
        );
        assert_eq!(
            admission.try_acquire(client_ip).unwrap_err(),
            AdmissionRejection::PerIpLimit
        );
        drop(replacement);
        assert_eq!(admission.active(), 0);
    }

    #[tokio::test]
    async fn a_stopping_listener_closes_its_sessions_for_the_hot_path_at_once() {
        let running = launch_running(Duration::from_secs(30)).await;
        // What `ListenerHandle::stop` does: close its sessions, then cancel.
        // The session task has not run since, so only the closed state can
        // stop the send.
        assert_eq!(running.table.close_listener(ListenerId::new(1)), 1);
        running.parent.cancel();
        assert_eq!(running.forward(b"late"), ForwardOutcome::Refused);
        assert_eq!(running.metrics.snapshot().packets_to_backend_total, 1);
        running.stop().await;
    }

    #[tokio::test]
    async fn a_closed_session_frees_its_route_for_the_next_datagram() {
        let running = launch_running(Duration::from_secs(30)).await;
        let key = running.handle.session().key();
        // Keep the old task draining: it cannot run until this test yields.
        running.handle.pending.fetch_add(1, Ordering::AcqRel);
        assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        running.table.close(&running.handle);

        // The listener's lookup misses, so it would create a new session for
        // the same client instead of dropping the datagram as closed.
        assert!(running.table.get_by_key(&key).is_none());
        let replacement = SessionHandle::for_test(Session::new(
            key.listener_id,
            "test".to_owned(),
            key.client_addr,
            "127.0.0.1:40001".parse().unwrap(),
            "127.0.0.1:40002".parse().unwrap(),
        ));
        assert!(running.table.insert(Arc::clone(&replacement)).is_ok());

        running.wait_until_removed().await;
        running.assert_session_closed_drops(1);
        assert!(Arc::ptr_eq(
            &running
                .table
                .get_by_key(&key)
                .expect("replacement stays routed"),
            &replacement
        ));
        assert_eq!(running.table.len(), 1);
        running.stop().await;
    }

    #[test]
    fn a_session_task_dropped_before_its_first_poll_cleans_up() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // `launch_unstarted` never yields after spawning, so the session
        // task is still unpolled when the runtime is dropped.
        let budget = QueueBudget::with_limit(usize::MAX);
        let running = runtime.block_on(launch_unstarted(
            Duration::from_secs(30),
            Arc::clone(&budget),
        ));
        assert_eq!(running.table.len(), 1);
        assert_eq!(running.metrics.snapshot().active_sessions, 1);
        drop(runtime);

        // Nothing was sent, so the task really never ran.
        assert_eq!(running.metrics.snapshot().packets_to_backend_total, 0);
        assert!(running.table.is_empty());
        assert!(!running.in_table());
        assert_eq!(running.metrics.snapshot().active_sessions, 0);
        assert_eq!(running.counters.snapshot().active_sessions, 0);
        assert!(running.handle.is_closed());
        // The unsent first datagram is counted exactly once, in both scopes.
        running.assert_session_closed_drops(1);
        // The task is gone; this test's reference still owns the socket and
        // buffers, so the global slot (and only it) is still held.
        assert_eq!(budget.live_sessions(), 1);
        assert_eq!(running.admission.active(), 1);
        let client_ip = running.handle.session().client_addr.ip();
        drop(
            running
                .admission
                .try_acquire(client_ip)
                .expect("the client's slot was freed with the route"),
        );
        let Running {
            handle, admission, ..
        } = running;
        drop(handle);
        assert_eq!(budget.live_sessions(), 0);
        assert_eq!(admission.active(), 0);
    }

    /// The slot size `QUEUED_DATAGRAM_OVERHEAD_BYTES` documents.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn a_queue_slot_is_48_bytes_on_64_bit_targets() {
        assert_eq!(size_of::<QueuedDatagram>(), 48);
    }

    #[test]
    fn snapshots_report_both_direction_counters() {
        let session = Session::new(
            ListenerId::new(1),
            "test".to_owned(),
            "127.0.0.1:40000".parse().unwrap(),
            "127.0.0.1:40001".parse().unwrap(),
            "127.0.0.1:40002".parse().unwrap(),
        );
        session.record_to_backend(12);
        session.record_to_client(34);
        let snapshot = session.snapshot();
        assert_eq!(snapshot.packets_to_backend, 1);
        assert_eq!(snapshot.bytes_to_backend, 12);
        assert_eq!(snapshot.packets_to_client, 1);
        assert_eq!(snapshot.bytes_to_client, 34);
    }
}
