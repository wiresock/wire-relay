// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-client connected upstream socket and lifecycle.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
        queued_datagram_budget,
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
            datagrams_dropped,
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
    /// Breakdown of `dropped` by reason.
    #[serde(default)]
    pub datagrams_dropped: DropSnapshot,
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
/// configuration and live session count: the defensive datagram budget minus
/// fixed receive buffers for `max(live sessions, max_sessions)`. A reload that
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
    shape: AtomicU64,
    live_sessions: AtomicUsize,
    used: AtomicUsize,
}

impl QueueBudget {
    #[must_use]
    pub fn new(service: &ServiceConfig) -> Arc<Self> {
        Self::with_memory(
            MAX_SESSION_DATAGRAM_MEMORY_BYTES,
            service.max_sessions,
            service.max_datagram_size,
        )
    }

    fn with_memory(memory_bytes: u128, max_sessions: usize, max_datagram_size: usize) -> Arc<Self> {
        Arc::new(Self {
            memory_bytes,
            shape: AtomicU64::new(Self::shape(max_sessions, max_datagram_size)),
            live_sessions: AtomicUsize::new(0),
            used: AtomicUsize::new(0),
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
        Self::with_memory(memory_bytes as u128, max_sessions, max_datagram_size)
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
        payload_len.saturating_add(QUEUED_DATAGRAM_OVERHEAD_BYTES)
    }

    fn try_reserve(self: &Arc<Self>, payload_len: usize) -> Option<QueueReservation> {
        let bytes = Self::charge(payload_len);
        let limit = self.limit();
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let total = used.checked_add(bytes).filter(|total| *total <= limit)?;
            match self
                .used
                .compare_exchange_weak(used, total, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => used = current,
            }
        }
        Some(QueueReservation {
            budget: Arc::clone(self),
            bytes,
        })
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

#[derive(Debug)]
struct QueueReservation {
    // An owned handle keeps release correct on every drop path, including
    // datagrams discarded with the receiver at session teardown; the
    // refcount is paid only on the queued fallback path, never on direct
    // sends.
    budget: Arc<QueueBudget>,
    bytes: usize,
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// A client datagram waiting for its session's upstream socket.
#[derive(Debug)]
pub struct QueuedDatagram {
    payload: Vec<u8>,
    _reservation: QueueReservation,
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

/// Forwarding state stored in the session table.
///
/// The listener sends a client datagram directly on the session's connected
/// upstream socket when nothing is queued ahead of it. Only when that socket
/// cannot accept the datagram immediately is the datagram copied into the
/// bounded per-session queue that the session task drains. Because one
/// listener task is the only producer for a session, sending directly only
/// while `pending` is zero preserves client datagram order.
#[derive(Debug)]
pub struct SessionHandle {
    session: Arc<Session>,
    upstream: Arc<UdpSocket>,
    client_datagrams: Sender<QueuedDatagram>,
    /// Client datagrams queued or being sent by the session task.
    pending: AtomicUsize,
    queue_budget: Arc<QueueBudget>,
    /// Set with the cancellation token; checked on the listener hot path
    /// because `CancellationToken::is_cancelled` takes a lock.
    closed: AtomicBool,
    /// Set by the owning listener before it cancels the parent token, so the
    /// hot path sees a stopping listener without locking.
    parent_stopping: Arc<AtomicBool>,
    /// Released as soon as the route is unpublished, so the same client can
    /// be admitted again while this session drains.
    admission: std::sync::Mutex<Option<AdmissionLease>>,
    cancel: CancellationToken,
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
            upstream: Arc::new(UdpSocket::from_std(socket).expect("register test socket")),
            client_datagrams,
            pending: AtomicUsize::new(0),
            queue_budget: QueueBudget::with_limit(usize::MAX),
            closed: AtomicBool::new(false),
            parent_stopping: Arc::new(AtomicBool::new(false)),
            admission: std::sync::Mutex::new(None),
            cancel: CancellationToken::new(),
        })
    }

    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Forwards an already size-validated client datagram without waiting.
    ///
    /// All success, drop, and error accounting happens here. This never blocks
    /// the listener's receive loop.
    ///
    /// The session task closes the handle on every exit path, so a handle
    /// still held after the task decided to stop drops the datagram as
    /// `session_closed` instead of sending on a dying upstream socket. A
    /// datagram that passes this check just before the task exits can still
    /// be sent once; that is the same window the queued path always had.
    pub fn forward_from_client(
        &self,
        datagram: &[u8],
        metrics: &Metrics,
        listener_counters: &ListenerCounters,
        log_limiter: &LogRateLimiter,
    ) {
        // A datagram dropped by the relay itself (closed session, full queue,
        // exhausted budget) is not client activity: a client that only sends
        // into a closed or back-pressured session must still idle out and
        // release its admission slot and receive buffers. A send the backend
        // side refused still is, so a backend outage does not expire sessions
        // whose clients keep sending.
        if self.is_closed() {
            record_drop(metrics, listener_counters, DropReason::SessionClosed);
            return;
        }

        if self.pending.load(Ordering::Acquire) == 0 {
            match self.upstream.try_send(datagram) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => {
                    self.session.touch_client();
                    record_backend_send(
                        &self.session,
                        datagram.len(),
                        result,
                        metrics,
                        listener_counters,
                        log_limiter,
                    );
                    return;
                }
            }
        }

        match self.enqueue(datagram) {
            Ok(()) => self.session.touch_client(),
            Err(reason) => {
                record_drop(metrics, listener_counters, reason);
                self.log_enqueue_drop(reason, datagram.len(), log_limiter);
            }
        }
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
                let (max_sessions, max_datagram_size) = self.queue_budget.configured();
                warn!(
                    listener = %self.session.listener_name,
                    datagram_bytes = size,
                    budget_used_bytes = self.queue_budget.used(),
                    budget_limit_bytes = self.queue_budget.limit(),
                    live_sessions = self.queue_budget.live_sessions(),
                    max_sessions,
                    max_datagram_size,
                    "process-wide queued-datagram memory budget exhausted; dropping client \
                     datagram (lower service.max_sessions or service.max_datagram_size to \
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
        let reservation = self
            .queue_budget
            .try_reserve(datagram.len())
            .ok_or(DropReason::BudgetExhausted)?;
        self.pending.fetch_add(1, Ordering::AcqRel);
        permit.send(QueuedDatagram {
            payload: datagram.to_vec(),
            _reservation: reservation,
        });
        Ok(())
    }

    fn finish_pending(&self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.parent_stopping.load(Ordering::Acquire)
    }

    /// Stops the session; idempotent.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancel.cancel();
    }

    /// Releases the admission lease once; later calls do nothing.
    pub(super) fn release_admission(&self) {
        let lease = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(lease);
    }
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
    /// Set by the owning listener before it cancels `parent_cancel`.
    pub parent_stopping: Arc<AtomicBool>,
    pub tracker: TaskTracker,
    pub log_limiter: Arc<LogRateLimiter>,
}

/// Inserts and starts one long-lived session task.
pub fn launch_session(launch: SessionLaunch) -> Result<Arc<SessionHandle>, AdmissionLease> {
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
        parent_stopping,
        tracker,
        log_limiter,
    } = launch;
    let (sender, receiver) = mpsc::channel(queue_depth.max(1));
    let cancel = parent_cancel.child_token();
    let handle = Arc::new(SessionHandle {
        session: Arc::clone(&session),
        upstream: Arc::new(upstream),
        client_datagrams: sender,
        // The first datagram is in flight in the session task, so later
        // datagrams must queue behind it until it has been sent.
        pending: AtomicUsize::new(1),
        queue_budget,
        closed: AtomicBool::new(false),
        parent_stopping,
        admission: std::sync::Mutex::new(None),
        cancel: cancel.clone(),
    });

    if table.insert(Arc::clone(&handle)).is_err() {
        return Err(admission);
    }
    *handle
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(admission);
    let live = handle.queue_budget.session_started();

    metrics.session_created();
    listener_counters.session_started();
    // Created before spawning so that a task dropped before its first poll
    // (shutdown or abort) still closes the handle, removes this table entry,
    // ends the gauges, releases admission, and counts the unsent first
    // datagram.
    let cleanup = SessionCleanup {
        handle: Arc::clone(&handle),
        table,
        session: Arc::clone(&handle.session),
        metrics: Arc::clone(&metrics),
        listener_counters: Arc::clone(&listener_counters),
        reason: SessionCloseReason::Closed,
        first_datagram_unsent: true,
    };

    let task = SessionTask {
        _live: live,
        first_datagram,
        listener_socket,
        metrics,
        listener_counters,
        idle_timeout,
        log_limiter,
    };
    let task_handle = Arc::clone(&handle);
    tracker.spawn(async move {
        run_session(task_handle, receiver, cancel, task, cleanup).await;
    });
    Ok(handle)
}

/// Resources owned by a running session task.
struct SessionTask {
    /// Released when the task, and with it the receive buffer, is gone.
    _live: LiveSession,
    first_datagram: Vec<u8>,
    listener_socket: Arc<UdpSocket>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    idle_timeout: watch::Receiver<Duration>,
    log_limiter: Arc<LogRateLimiter>,
}

async fn run_session(
    handle: Arc<SessionHandle>,
    mut receiver: Receiver<QueuedDatagram>,
    cancel: CancellationToken,
    mut task: SessionTask,
    mut cleanup: SessionCleanup,
) {
    let upstream = Arc::clone(&handle.upstream);

    let first_sent = tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        () = send_to_backend(
            &handle.session,
            &upstream,
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
        forward_until_stopped(
            &handle,
            &mut receiver,
            &cancel,
            &mut task,
            &mut cleanup,
            &upstream,
        )
        .await;
    }
    // An unsent first datagram is counted once, by the cleanup guard, so a
    // task dropped before its first poll counts it too.

    // Stop the listener's direct-send path as soon as the task decides to
    // exit, before the table entry is removed, then count every datagram
    // still queued. After `close`, the listener's enqueues fail as closed
    // and are counted there; `recv` also waits out a slot the listener
    // reserved just before, so nothing is released uncounted.
    cleanup.table.close(&handle);
    receiver.close();
    while let Some(datagram) = receiver.recv().await {
        drop(datagram);
        record_drop(
            &task.metrics,
            &task.listener_counters,
            DropReason::SessionClosed,
        );
    }
    debug!(
        session_id = %handle.session.id,
        reason = ?cleanup.reason,
        "session stopped"
    );
}

async fn forward_until_stopped(
    handle: &Arc<SessionHandle>,
    receiver: &mut Receiver<QueuedDatagram>,
    cancel: &CancellationToken,
    task: &mut SessionTask,
    cleanup: &mut SessionCleanup,
    upstream: &UdpSocket,
) {
    let mut backend_buffer = vec![0_u8; handle.queue_budget.max_datagram_size() + 1];

    // The idle deadline is armed once and only recomputed when it fires or
    // the configured timeout changes, so packet forwarding does not touch the
    // timer wheel.
    let mut idle_timeout = *task.idle_timeout.borrow_and_update();
    let idle_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
        handle.session.last_activity() + idle_timeout,
    ));
    tokio::pin!(idle_sleep);

    'session: loop {
        let max_size = handle.queue_budget.max_datagram_size();
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
            datagram = receiver.recv() => {
                let Some(mut datagram) = datagram else {
                    break;
                };
                let mut drained = 0;
                loop {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            // Taken from the queue but never sent.
                            record_drop(
                                &task.metrics,
                                &task.listener_counters,
                                DropReason::SessionClosed,
                            );
                            break 'session;
                        }
                        () = send_to_backend(
                            &handle.session,
                            upstream,
                            &datagram.payload,
                            &task.metrics,
                            &task.listener_counters,
                            &task.log_limiter,
                        ) => {}
                    }
                    drop(datagram);
                    handle.finish_pending();
                    drained += 1;
                    if drained >= QUEUE_DRAIN_BATCH {
                        break;
                    }
                    match receiver.try_recv() {
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
                        match task.listener_socket.send_to(
                            payload,
                            handle.session.client_addr,
                        ).await {
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
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            () = &mut idle_sleep => {
                // A timeout change may not have been observed through
                // `changed()` yet; always decide with the current value.
                idle_timeout = *task.idle_timeout.borrow_and_update();
                let deadline = handle.session.last_activity() + idle_timeout;
                if Instant::now() >= deadline {
                    cleanup.reason = SessionCloseReason::Idle;
                    break;
                }
                idle_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::from_std(deadline));
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

struct SessionCleanup {
    handle: Arc<SessionHandle>,
    table: Arc<SessionTable>,
    session: Arc<Session>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    reason: SessionCloseReason,
    /// The first datagram was never sent; counted as `session_closed` once.
    first_datagram_unsent: bool,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
        // Backstop for every exit path, including a task that is dropped
        // without running to completion or before its first poll.
        self.table.close(&self.handle);
        if self.first_datagram_unsent {
            record_drop(
                &self.metrics,
                &self.listener_counters,
                DropReason::SessionClosed,
            );
        }
        self.table.remove(&self.session.key(), self.session.id);
        self.listener_counters.session_ended();
        self.metrics
            .session_ended(self.reason == SessionCloseReason::Idle);
    }
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

    struct Harness {
        handle: Arc<SessionHandle>,
        receiver: Receiver<QueuedDatagram>,
        backend: UdpSocket,
        metrics: Metrics,
        counters: ListenerCounters,
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
        let handle = Arc::new(SessionHandle {
            session: Arc::new(session),
            upstream: Arc::new(upstream),
            client_datagrams,
            pending: AtomicUsize::new(pending),
            queue_budget: Arc::clone(&budget),
            closed: AtomicBool::new(false),
            parent_stopping: Arc::new(AtomicBool::new(false)),
            admission: std::sync::Mutex::new(None),
            cancel: CancellationToken::new(),
        });
        Harness {
            handle,
            receiver,
            backend,
            metrics: Metrics::default(),
            counters: ListenerCounters::default(),
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
        handle.forward_from_client(datagram, metrics, counters, log_limiter);
        let after = metrics.snapshot();
        let drops = (&before.datagrams_dropped, &after.datagrams_dropped);
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
        assert_eq!(stats.datagrams_dropped.queue_full, 1);
        assert_eq!(stats.datagrams_dropped.oversize, 0);
        let listener = harness.counters.snapshot();
        assert_eq!(listener.dropped, 1);
        assert_eq!(listener.datagrams_dropped.queue_full, 1);
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
        let drops = harness.metrics.snapshot().datagrams_dropped;
        assert_eq!((drops.budget_exhausted, drops.queue_full), (1, 0));
        let drops = harness.counters.snapshot().datagrams_dropped;
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
    async fn closed_sessions_count_session_closed_drops() {
        let harness = harness(4, 0, usize::MAX).await;
        harness.handle.close();
        assert_eq!(
            harness.forward(b"late"),
            ForwardOutcome::Dropped(DropReason::SessionClosed)
        );
        assert_eq!(
            harness.metrics.snapshot().datagrams_dropped.session_closed,
            1
        );
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
        parent_stopping: Arc<AtomicBool>,
        admission: Arc<crate::limits::AdmissionController>,
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
        // One session per IP and globally, so a replacement can only be
        // admitted once the old session's lease is released.
        let admission_controller = crate::limits::AdmissionController::new(1, 1, 100);
        let admission = admission_controller.try_acquire(client_addr.ip()).unwrap();
        let parent_stopping = Arc::new(AtomicBool::new(false));
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
            queue_budget: budget,
            parent_cancel: parent.clone(),
            parent_stopping: Arc::clone(&parent_stopping),
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
            parent_stopping,
            admission: admission_controller,
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
            ForwardOutcome::Dropped(DropReason::SessionClosed)
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
            assert_eq!(process.datagrams_dropped.session_closed, expected);
            assert_eq!(listener.datagrams_dropped.session_closed, expected);
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
        let service = ServiceConfig::default();
        let budget = QueueBudget::new(&service);
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
        assert_eq!(metrics.snapshot().datagrams_dropped.session_closed, 4);
        assert_eq!(metrics.snapshot().datagrams_dropped_total, 4);
        assert_eq!(counters.snapshot().datagrams_dropped.session_closed, 4);
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
        assert_touch(
            &closed,
            b"late",
            ForwardOutcome::Dropped(DropReason::SessionClosed),
            false,
        )
        .await;

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
        assert_eq!(failing.metrics.snapshot().datagrams_dropped.send_error, 1);
    }

    #[tokio::test]
    async fn closing_releases_admission_while_the_old_session_drains() {
        let running = launch_running(Duration::from_secs(30)).await;
        let client_ip = running.handle.session().client_addr.ip();
        // Global and per-IP limits are both one, held by this session.
        assert!(running.admission.try_acquire(client_ip).is_err());

        // Keep the old task draining: it cannot run until this test yields.
        running.handle.pending.fetch_add(1, Ordering::AcqRel);
        assert_eq!(running.forward(b"queued"), ForwardOutcome::Queued);
        running.table.close(&running.handle);
        assert!(
            running
                .table
                .get_by_id(&running.handle.session().id)
                .is_some()
        );

        // The replacement for the same client is admitted right away, while
        // the old session still holds its receive-buffer reservation.
        let replacement = running
            .admission
            .try_acquire(client_ip)
            .expect("a closed session must free its admission");
        assert_eq!(running.admission.active(), 1);

        running.wait_until_removed().await;
        // The old cleanup must not release the lease a second time.
        assert_eq!(running.admission.active(), 1);
        drop(replacement);
        assert_eq!(running.admission.active(), 0);
        running.assert_session_closed_drops(1);
        running.stop().await;
    }

    #[tokio::test]
    async fn a_stopping_listener_closes_its_sessions_for_the_hot_path_at_once() {
        let running = launch_running(Duration::from_secs(30)).await;
        // What `ListenerHandle::stop` does: flag first, then cancel. The
        // session task has not run since, so only the flag can stop the send.
        running.parent_stopping.store(true, Ordering::Release);
        running.parent.cancel();
        assert_eq!(
            running.forward(b"late"),
            ForwardOutcome::Dropped(DropReason::SessionClosed)
        );
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
        assert_eq!(budget.live_sessions(), 0);
        assert!(running.handle.closed.load(Ordering::Acquire));
        // The unsent first datagram is counted exactly once, in both scopes.
        running.assert_session_closed_drops(1);
        assert_eq!(running.admission.active(), 0);
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
