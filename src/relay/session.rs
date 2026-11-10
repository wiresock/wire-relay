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
/// Reservations are charged with each datagram's actual length and released
/// when the queued datagram is dropped, whether it was sent or discarded
/// during session teardown.
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

    /// Bytes currently held by queued datagrams.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<QueueReservation> {
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

/// What happened to one client datagram offered to a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForwardOutcome {
    /// Sent directly on the upstream socket.
    Sent,
    /// Queued for the session task because the upstream socket was busy or
    /// earlier datagrams were still queued.
    Queued,
    /// Discarded and counted.
    Dropped(DropReason),
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
    pub fn forward_from_client(
        &self,
        datagram: &[u8],
        metrics: &Metrics,
        listener_counters: &ListenerCounters,
        log_limiter: &LogRateLimiter,
    ) -> ForwardOutcome {
        self.session.touch_client();
        if self.cancel.is_cancelled() {
            record_drop(metrics, listener_counters, DropReason::SessionClosed);
            return ForwardOutcome::Dropped(DropReason::SessionClosed);
        }

        if self.pending.load(Ordering::Acquire) == 0 {
            match self.upstream.try_send(datagram) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => {
                    return if record_backend_send(
                        &self.session,
                        datagram.len(),
                        result,
                        metrics,
                        listener_counters,
                        log_limiter,
                    ) {
                        ForwardOutcome::Sent
                    } else {
                        ForwardOutcome::Dropped(DropReason::SendError)
                    };
                }
            }
        }

        match self.enqueue(datagram) {
            Ok(()) => ForwardOutcome::Queued,
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
                ForwardOutcome::Dropped(reason)
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
    table: Arc<SessionTable>,
    session: Arc<Session>,
    admission: Option<AdmissionLease>,
    metrics: Arc<Metrics>,
    listener_counters: Arc<ListenerCounters>,
    reason: SessionCloseReason,
}

impl Drop for SessionCleanup {
    fn drop(&mut self) {
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

    impl Harness {
        fn forward(&self, datagram: &[u8]) -> ForwardOutcome {
            self.handle.forward_from_client(
                datagram,
                &self.metrics,
                &self.counters,
                &self.log_limiter,
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
    async fn queue_memory_budget_uses_actual_sizes_and_is_released() {
        let mut harness = harness(16, 1, 100).await;
        assert_eq!(harness.forward(&[1; 60]), ForwardOutcome::Queued);
        assert_eq!(harness.budget.used(), 60);
        assert_eq!(
            harness.forward(&[2; 60]),
            ForwardOutcome::Dropped(DropReason::QueueFull)
        );
        assert_eq!(harness.forward(&[3; 40]), ForwardOutcome::Queued);
        assert_eq!(harness.budget.used(), 100);
        drop(harness.receiver.try_recv().unwrap());
        assert_eq!(harness.budget.used(), 40);
        harness.receiver.close();
        while harness.receiver.try_recv().is_ok() {}
        assert_eq!(harness.budget.used(), 0);
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
