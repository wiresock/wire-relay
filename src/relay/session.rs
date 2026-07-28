// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-client connected upstream socket and lifecycle.

use std::{
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
    config::MAX_QUEUED_CLIENT_DATAGRAMS,
    limits::{AdmissionLease, LogRateLimiter},
    metrics::Metrics,
};

use super::{ListenerId, SessionId, SessionKey, SessionTable};

/// Number of client datagrams that may wait behind an upstream socket.
pub const SESSION_QUEUE_CAPACITY: usize = MAX_QUEUED_CLIENT_DATAGRAMS;

/// Cumulative counters owned by one listener incarnation.
#[derive(Debug, Default)]
pub struct ListenerCounters {
    active_sessions: AtomicU64,
    packets_to_backend: AtomicU64,
    packets_to_client: AtomicU64,
    bytes_to_backend: AtomicU64,
    bytes_to_client: AtomicU64,
    dropped: AtomicU64,
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

    pub fn dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
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
}

#[derive(Debug)]
struct Activity {
    last_client: Instant,
    last_backend: Instant,
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
    activity: Mutex<Activity>,
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
            activity: Mutex::new(Activity {
                last_client: now,
                last_backend: now,
            }),
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

    fn touch_client(&self) {
        lock_unpoisoned(&self.activity).last_client = Instant::now();
    }

    fn touch_backend(&self) {
        lock_unpoisoned(&self.activity).last_backend = Instant::now();
    }

    fn last_activity(&self) -> Instant {
        let activity = lock_unpoisoned(&self.activity);
        activity.last_client.max(activity.last_backend)
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
        let activity = lock_unpoisoned(&self.activity);
        SessionSnapshot {
            id: self.id,
            listener_id: self.listener_id,
            listener: self.listener_name.clone(),
            client_addr: self.client_addr,
            upstream_local_addr: self.upstream_local_addr,
            backend_addr: self.backend_addr,
            age_ms: duration_millis(now.saturating_duration_since(self.created_at)),
            idle_ms: duration_millis(
                now.saturating_duration_since(activity.last_client.max(activity.last_backend)),
            ),
            last_client_activity_ms: duration_millis(
                now.saturating_duration_since(activity.last_client),
            ),
            last_backend_activity_ms: duration_millis(
                now.saturating_duration_since(activity.last_backend),
            ),
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

/// Sender and cancellation handle stored in the session table.
#[derive(Debug)]
pub struct SessionHandle {
    session: Arc<Session>,
    client_datagrams: Sender<Vec<u8>>,
    cancel: CancellationToken,
}

impl SessionHandle {
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Queue an already size-validated client datagram without waiting.
    pub fn try_send(&self, datagram: Vec<u8>) -> Result<(), QueueError> {
        self.session.touch_client();
        self.client_datagrams
            .try_send(datagram)
            .map_err(|error| match error {
                TrySendError::Full(_) => QueueError::Full,
                TrySendError::Closed(_) => QueueError::Closed,
            })
    }

    pub fn close(&self) {
        self.cancel.cancel();
    }
}

/// A bounded session queue refused a datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueError {
    Full,
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
    pub max_datagram_size: Arc<AtomicUsize>,
    pub parent_cancel: CancellationToken,
    pub tracker: TaskTracker,
    pub log_limiter: Arc<LogRateLimiter>,
}

/// Inserts and starts one long-lived session task.
pub fn launch_session(launch: SessionLaunch) -> Result<Arc<SessionHandle>, AdmissionLease> {
    let (sender, receiver) = mpsc::channel(SESSION_QUEUE_CAPACITY);
    let cancel = launch.parent_cancel.child_token();
    let handle = Arc::new(SessionHandle {
        session: Arc::clone(&launch.session),
        client_datagrams: sender,
        cancel: cancel.clone(),
    });

    if launch.table.insert(Arc::clone(&handle)).is_err() {
        return Err(launch.admission);
    }

    launch.metrics.session_created();
    launch.listener_counters.session_started();

    let task_handle = Arc::clone(&handle);
    launch.tracker.clone().spawn(async move {
        run_session(task_handle, receiver, cancel, launch).await;
    });
    Ok(handle)
}

async fn run_session(
    handle: Arc<SessionHandle>,
    mut receiver: Receiver<Vec<u8>>,
    cancel: CancellationToken,
    mut launch: SessionLaunch,
) {
    let mut cleanup = SessionCleanup {
        table: Arc::clone(&launch.table),
        session: Arc::clone(&handle.session),
        admission: Some(launch.admission),
        metrics: Arc::clone(&launch.metrics),
        listener_counters: Arc::clone(&launch.listener_counters),
        reason: SessionCloseReason::Closed,
    };
    let mut backend_buffer = vec![0_u8; launch.max_datagram_size.load(Ordering::Relaxed) + 1];

    tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        () = send_to_backend(
            &handle.session,
            &launch.upstream,
            &launch.first_datagram,
            &launch.metrics,
            &launch.listener_counters,
            &launch.log_limiter,
        ) => {}
    }

    loop {
        let max_size = launch.max_datagram_size.load(Ordering::Relaxed);
        if backend_buffer.len() != max_size.saturating_add(1) {
            backend_buffer.resize(max_size.saturating_add(1), 0);
        }
        let idle_timeout = *launch.idle_timeout.borrow_and_update();
        let elapsed = Instant::now().saturating_duration_since(handle.session.last_activity());
        let idle_sleep = tokio::time::sleep(idle_timeout.saturating_sub(elapsed));
        tokio::pin!(idle_sleep);

        tokio::select! {
            () = cancel.cancelled() => break,
            changed = launch.idle_timeout.changed() => {
                if changed.is_err() {
                    break;
                }
                continue;
            }
            datagram = receiver.recv() => {
                let Some(datagram) = datagram else {
                    break;
                };
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = send_to_backend(
                        &handle.session,
                        &launch.upstream,
                        &datagram,
                        &launch.metrics,
                        &launch.listener_counters,
                        &launch.log_limiter,
                    ) => {}
                }
            }
            received = launch.upstream.recv(&mut backend_buffer) => {
                match received {
                    Ok(size) if size <= max_size => {
                        // Backend activity is independent of whether the local
                        // client send succeeds.
                        handle.session.touch_backend();
                        let payload = &backend_buffer[..size];
                        match launch.listener_socket.send_to(
                            payload,
                            handle.session.client_addr,
                        ).await {
                            Ok(sent) if sent == size => {
                                handle.session.record_to_client(size);
                                launch.listener_counters.to_client(size);
                                launch.metrics.packet_to_client(size);
                            }
                            Ok(sent) => {
                                launch.metrics.socket_error();
                                launch.metrics.datagram_dropped();
                                launch.listener_counters.dropped();
                                if launch.log_limiter.should_log("partial-client-send") {
                                    warn!(
                                        session_id = %handle.session.id,
                                        expected = size,
                                        sent,
                                        "UDP send reported a partial datagram"
                                    );
                                }
                            }
                            Err(error) => {
                                launch.metrics.socket_error();
                                launch.metrics.datagram_dropped();
                                launch.listener_counters.dropped();
                                if launch.log_limiter.should_log("client-send-error") {
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
                        launch.metrics.datagram_dropped();
                        launch.listener_counters.dropped();
                    }
                    Err(error) => {
                        launch.metrics.socket_error();
                        if launch.log_limiter.should_log("upstream-recv-error") {
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
                let current_timeout = *launch.idle_timeout.borrow_and_update();
                if Instant::now().saturating_duration_since(handle.session.last_activity())
                    >= current_timeout
                {
                    cleanup.reason = SessionCloseReason::Idle;
                    break;
                }
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
    match upstream.send(datagram).await {
        Ok(sent) if sent == datagram.len() => {
            session.record_to_backend(sent);
            listener_counters.to_backend(sent);
            metrics.packet_to_backend(sent);
        }
        Ok(sent) => {
            metrics.socket_error();
            metrics.datagram_dropped();
            listener_counters.dropped();
            if log_limiter.should_log("partial-backend-send") {
                warn!(
                    session_id = %session.id,
                    expected = datagram.len(),
                    sent,
                    "UDP send reported a partial datagram"
                );
            }
        }
        Err(error) => {
            metrics.socket_error();
            metrics.datagram_dropped();
            listener_counters.dropped();
            if log_limiter.should_log("backend-send-error") {
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

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

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
