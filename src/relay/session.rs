// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-client connected upstream socket and lifecycle.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
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
    config::QUEUED_DATAGRAM_OVERHEAD_BYTES,
    limits::{AdmissionLease, LogRateLimiter},
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
    dropped: AtomicU64,
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
        self.dropped.fetch_add(1, Ordering::Relaxed);
        self.drops.record(reason);
    }

    #[must_use]
    pub fn snapshot(&self) -> ListenerCounterSnapshot {
        ListenerCounterSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            packets_to_backend: self.packets_to_backend.load(Ordering::Relaxed),
            packets_to_client: self.packets_to_client.load(Ordering::Relaxed),
            bytes_to_backend: self.bytes_to_backend.load(Ordering::Relaxed),
            bytes_to_client: self.bytes_to_client.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            datagrams_dropped: self.drops.snapshot(),
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
/// or discarded during session teardown. A limit lowered below current use
/// still applies: new reservations fail until enough queued datagrams drain.
#[derive(Debug)]
pub struct QueueBudget {
    limit: AtomicUsize,
    used: AtomicUsize,
}

impl QueueBudget {
    #[must_use]
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: AtomicUsize::new(limit),
            used: AtomicUsize::new(0),
        })
    }

    /// Applies a reloaded limit to subsequent reservations.
    pub fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    /// Bytes currently charged to queued datagrams.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Current limit for queued datagrams.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Budget charged for queueing one datagram of `payload_len` bytes.
    #[must_use]
    pub const fn charge(payload_len: usize) -> usize {
        payload_len.saturating_add(QUEUED_DATAGRAM_OVERHEAD_BYTES)
    }

    fn try_reserve(self: &Arc<Self>, payload_len: usize) -> Option<QueueReservation> {
        let bytes = Self::charge(payload_len);
        let limit = self.limit.load(Ordering::Relaxed);
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
            queue_budget: QueueBudget::new(usize::MAX),
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
    /// The session task cancels its token on every exit path, so a handle
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
        self.session.touch_client();
        if self.cancel.is_cancelled() {
            record_drop(metrics, listener_counters, DropReason::SessionClosed);
            return;
        }

        if self.pending.load(Ordering::Acquire) == 0 {
            match self.upstream.try_send(datagram) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => {
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
            Ok(()) => {}
            Err(reason) => {
                record_drop(metrics, listener_counters, reason);
                if reason == DropReason::QueueFull && log_limiter.should_log("session-queue-full") {
                    warn!(
                        listener = %self.session.listener_name,
                        session_id = %self.session.id,
                        queued_bytes = self.queue_budget.used(),
                        "session queue full; dropping client datagram"
                    );
                }
            }
        }
    }

    fn enqueue(&self, datagram: &[u8]) -> Result<(), DropReason> {
        let reservation = self
            .queue_budget
            .try_reserve(datagram.len())
            .ok_or(DropReason::QueueFull)?;
        self.pending.fetch_add(1, Ordering::AcqRel);
        let queued = QueuedDatagram {
            payload: datagram.to_vec(),
            _reservation: reservation,
        };
        self.client_datagrams.try_send(queued).map_err(|error| {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            match error {
                TrySendError::Full(_) => DropReason::QueueFull,
                TrySendError::Closed(_) => DropReason::SessionClosed,
            }
        })
    }

    fn finish_pending(&self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn close(&self) {
        self.cancel.cancel();
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
    pub max_datagram_size: Arc<AtomicUsize>,
    pub queue_depth: usize,
    pub queue_budget: Arc<QueueBudget>,
    pub parent_cancel: CancellationToken,
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
        max_datagram_size,
        queue_depth,
        queue_budget,
        parent_cancel,
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
        cancel: cancel.clone(),
    });

    if table.insert(Arc::clone(&handle)).is_err() {
        return Err(admission);
    }

    metrics.session_created();
    listener_counters.session_started();

    let task = SessionTask {
        first_datagram,
        listener_socket,
        table,
        admission,
        metrics,
        listener_counters,
        idle_timeout,
        max_datagram_size,
        log_limiter,
    };
    let task_handle = Arc::clone(&handle);
    tracker.spawn(async move {
        run_session(task_handle, receiver, cancel, task).await;
    });
    Ok(handle)
}

/// Resources owned by a running session task.
struct SessionTask {
    first_datagram: Vec<u8>,
    listener_socket: Arc<UdpSocket>,
    table: Arc<SessionTable>,
    admission: AdmissionLease,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    idle_timeout: watch::Receiver<Duration>,
    max_datagram_size: Arc<AtomicUsize>,
    log_limiter: Arc<LogRateLimiter>,
}

async fn run_session(
    handle: Arc<SessionHandle>,
    mut receiver: Receiver<QueuedDatagram>,
    cancel: CancellationToken,
    mut task: SessionTask,
) {
    let mut cleanup = SessionCleanup {
        cancel: cancel.clone(),
        table: Arc::clone(&task.table),
        session: Arc::clone(&handle.session),
        admission: Some(task.admission),
        metrics: Arc::clone(&task.metrics),
        listener_counters: Arc::clone(&task.listener_counters),
        reason: SessionCloseReason::Closed,
    };
    let upstream = Arc::clone(&handle.upstream);
    let mut backend_buffer = vec![0_u8; task.max_datagram_size.load(Ordering::Relaxed) + 1];

    tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        () = send_to_backend(
            &handle.session,
            &upstream,
            &task.first_datagram,
            &task.metrics,
            &task.listener_counters,
            &task.log_limiter,
        ) => {}
    }
    task.first_datagram = Vec::new();
    handle.finish_pending();

    // The idle deadline is armed once and only recomputed when it fires or
    // the configured timeout changes, so packet forwarding does not touch the
    // timer wheel.
    let mut idle_timeout = *task.idle_timeout.borrow_and_update();
    let idle_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
        handle.session.last_activity() + idle_timeout,
    ));
    tokio::pin!(idle_sleep);

    'session: loop {
        let max_size = task.max_datagram_size.load(Ordering::Relaxed);
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
                        () = cancel.cancelled() => break 'session,
                        () = send_to_backend(
                            &handle.session,
                            &upstream,
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
                                if task.log_limiter.should_log("partial-client-send") {
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
                                if task.log_limiter.should_log("client-send-error") {
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
                        if task.log_limiter.should_log("upstream-recv-error") {
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

    // Stop the listener's direct-send path as soon as the task decides to
    // exit, before the table entry is removed.
    cancel.cancel();
    debug!(
        session_id = %handle.session.id,
        reason = ?cleanup.reason,
        "session stopped"
    );
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

/// Accounts for one client-to-backend send and reports whether it succeeded.
fn record_backend_send(
    session: &Session,
    expected: usize,
    result: io::Result<usize>,
    metrics: &Metrics,
    listener_counters: &ListenerCounters,
    log_limiter: &LogRateLimiter,
) -> bool {
    match result {
        Ok(sent) if sent == expected => {
            session.record_to_backend(sent);
            listener_counters.to_backend(sent);
            metrics.packet_to_backend(sent);
            true
        }
        Ok(sent) => {
            metrics.socket_error();
            record_drop(metrics, listener_counters, DropReason::SendError);
            if log_limiter.should_log("partial-backend-send") {
                warn!(
                    session_id = %session.id,
                    expected,
                    sent,
                    "UDP send reported a partial datagram"
                );
            }
            false
        }
        Err(error) => {
            metrics.socket_error();
            record_drop(metrics, listener_counters, DropReason::SendError);
            if log_limiter.should_log("backend-send-error") {
                warn!(
                    session_id = %session.id,
                    %error,
                    "failed to send client datagram to backend"
                );
            }
            false
        }
    }
}

struct SessionCleanup {
    cancel: CancellationToken,
    table: Arc<SessionTable>,
    session: Arc<Session>,
    admission: Option<AdmissionLease>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    reason: SessionCloseReason,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
        // Backstop for every exit path, including the early return before the
        // first datagram is sent.
        self.cancel.cancel();
        self.table.remove(&self.session.key(), self.session.id);
        self.listener_counters.session_ended();
        self.metrics
            .session_ended(self.reason == SessionCloseReason::Idle);
        drop(self.admission.take());
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
        let budget = QueueBudget::new(budget_bytes);
        let handle = Arc::new(SessionHandle {
            session: Arc::new(session),
            upstream: Arc::new(upstream),
            client_datagrams,
            pending: AtomicUsize::new(pending),
            queue_budget: Arc::clone(&budget),
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
            ForwardOutcome::Dropped(DropReason::QueueFull)
        );
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
                ForwardOutcome::Dropped(DropReason::QueueFull)
            );
        }
        assert_eq!(harness.budget.used(), budget);
    }

    #[tokio::test]
    async fn a_lowered_limit_applies_while_more_is_already_used() {
        let mut harness = harness(16, 1, usize::MAX).await;
        assert_eq!(harness.forward(&[1; 100]), ForwardOutcome::Queued);
        assert_eq!(harness.forward(&[2; 100]), ForwardOutcome::Queued);
        harness.budget.set_limit(QueueBudget::charge(100));
        assert_eq!(harness.budget.limit(), QueueBudget::charge(100));
        assert_eq!(
            harness.forward(&[3; 1]),
            ForwardOutcome::Dropped(DropReason::QueueFull)
        );
        drop(harness.receiver.try_recv().unwrap());
        // Still at the new limit: nothing more fits.
        assert_eq!(
            harness.forward(&[4; 1]),
            ForwardOutcome::Dropped(DropReason::QueueFull)
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
    }

    async fn launch_running(idle_timeout: Duration) -> Running {
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
        let admission = crate::limits::AdmissionController::new(4, 4, 100)
            .try_acquire(client_addr.ip())
            .unwrap();
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
            max_datagram_size: Arc::new(AtomicUsize::new(4096)),
            queue_depth: 16,
            queue_budget: QueueBudget::new(usize::MAX),
            parent_cancel: parent.clone(),
            tracker: tracker.clone(),
            log_limiter: Arc::clone(&log_limiter),
        })
        .unwrap();
        let mut buffer = [0_u8; 64];
        let size = tokio::time::timeout(Duration::from_secs(5), backend.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..size], b"first");
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
