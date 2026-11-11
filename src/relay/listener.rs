// SPDX-License-Identifier: AGPL-3.0-or-later

//! UDP listener receive loop and new-session admission.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{info, warn};

use crate::{
    config::ListenerConfig,
    dns::{BackendResolver, DnsSnapshot, PreparedBackend},
    limits::{AdmissionController, AdmissionRejection, LogRateLimiter, LogSite},
    metrics::{DropReason, Metrics},
};

use super::{
    Forward, ListenerId, Session, SessionKey, SessionTable,
    session::{
        ListenerCounterSnapshot, ListenerCounters, QueueBudget, SessionLaunch, launch_session,
        record_drop,
    },
    upstream,
};

/// Runtime dependencies shared by listener and session tasks.
#[derive(Clone)]
pub struct ListenerContext {
    pub sessions: Arc<SessionTable>,
    pub admission: Arc<AdmissionController>,
    pub metrics: Arc<Metrics>,
    pub idle_timeout: watch::Sender<Duration>,
    pub session_queue_depth: Arc<AtomicUsize>,
    pub queue_budget: Arc<QueueBudget>,
    pub dns_refresh_interval_ms: Arc<AtomicU64>,
    pub parent_cancel: CancellationToken,
    pub tracker: TaskTracker,
    pub log_limiter: Arc<LogRateLimiter>,
}

/// Bound listener and initial DNS result created during startup/reload preflight.
#[derive(Debug)]
pub struct PreparedListener {
    id: ListenerId,
    config: ListenerConfig,
    socket: UdpSocket,
    backend: PreparedBackend,
}

impl PreparedListener {
    pub async fn bind(
        id: ListenerId,
        config: ListenerConfig,
        metrics: &Metrics,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind(config.bind).await?;
        let backend = PreparedBackend::resolve(config.backend.clone(), metrics).await;
        Ok(Self {
            id,
            config,
            socket,
            backend,
        })
    }

    #[must_use]
    pub const fn id(&self) -> ListenerId {
        self.id
    }

    #[must_use]
    pub fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Starts accepting datagrams.
    #[must_use]
    pub fn activate(self, context: ListenerContext) -> Arc<ListenerHandle> {
        let cancel = context.parent_cancel.child_token();
        let socket = Arc::new(self.socket);
        let interval =
            Duration::from_millis(context.dns_refresh_interval_ms.load(Ordering::Relaxed));
        let resolver = BackendResolver::activate(
            self.backend,
            interval,
            cancel.clone(),
            Arc::clone(&context.metrics),
            Arc::clone(&context.log_limiter),
            context.tracker.clone(),
        );
        let handle = Arc::new(ListenerHandle {
            id: self.id,
            config: RwLock::new(self.config),
            socket,
            resolver,
            cancel,
            counters: Arc::new(ListenerCounters::default()),
            sessions: Arc::clone(&context.sessions),
            route_epoch: AtomicU64::new(1),
            route_commit: Mutex::new(()),
        });

        let task_handle = Arc::clone(&handle);
        context.tracker.clone().spawn(async move {
            receive_loop(task_handle, context).await;
        });
        handle
    }
}

/// Live listener handle retained by the runtime.
#[derive(Debug)]
pub struct ListenerHandle {
    id: ListenerId,
    config: RwLock<ListenerConfig>,
    socket: Arc<UdpSocket>,
    resolver: Arc<BackendResolver>,
    cancel: CancellationToken,
    counters: Arc<ListenerCounters>,
    sessions: Arc<SessionTable>,
    route_epoch: AtomicU64,
    route_commit: Mutex<()>,
}

impl ListenerHandle {
    #[must_use]
    pub const fn id(&self) -> ListenerId {
        self.id
    }

    #[must_use]
    pub fn config(&self) -> ListenerConfig {
        read_unpoisoned(&self.config).clone()
    }

    #[must_use]
    pub fn bind(&self) -> SocketAddr {
        read_unpoisoned(&self.config).bind
    }

    #[must_use]
    pub fn snapshot(&self) -> ListenerSnapshot {
        let config = self.config();
        let dns = self.resolver.snapshot();
        ListenerSnapshot {
            id: self.id,
            name: config.name,
            bind: config.bind,
            configured_backend: dns.configured_backend.clone(),
            resolved_backend: dns.resolved_backend,
            status: if self.cancel.is_cancelled() {
                ListenerStatus::Stopping
            } else if dns.available {
                ListenerStatus::Available
            } else {
                ListenerStatus::Unresolved
            },
            dns,
            counters: self.counters.snapshot(),
        }
    }

    /// Applies a pre-resolved backend to this listener for new sessions.
    pub fn replace_backend(&self, prepared: PreparedBackend, refresh_interval: Duration) -> usize {
        let _route_commit = lock_unpoisoned(&self.route_commit);
        self.route_epoch.fetch_add(1, Ordering::AcqRel);
        {
            let mut config = write_unpoisoned(&self.config);
            config.backend = prepared.endpoint().clone();
        }
        self.resolver.replace(prepared, refresh_interval);
        self.sessions.close_listener(self.id)
    }

    /// Applies a new DNS refresh cadence without discarding cached state.
    pub fn set_dns_refresh_interval(&self, refresh_interval: Duration) {
        self.resolver.set_refresh_interval(refresh_interval);
    }

    /// Whether [`Self::stop`] has run, or is running, for this listener.
    ///
    /// Read under the route lock, which `stop` holds from closing the
    /// listener's sessions until after it has cancelled the token. A caller
    /// that saw one of those sessions closed therefore gets `true` if the
    /// close was this listener's stop, instead of reading the token in the
    /// moment between the close and the cancellation.
    fn is_stopping(&self) -> bool {
        let _route_commit = lock_unpoisoned(&self.route_commit);
        self.cancel.is_cancelled()
    }

    /// Stops admission and all sessions belonging to this incarnation.
    pub fn stop(&self) -> usize {
        let _route_commit = lock_unpoisoned(&self.route_commit);
        self.route_epoch.fetch_add(1, Ordering::AcqRel);
        // Every session is closed before the token is cancelled, while the
        // route lock keeps new ones from being launched: the listener's
        // forwarding path refuses them at once, without waiting for their
        // tasks to wake up, and the count is not lowered by tasks that see
        // the cancellation and close themselves. The lock is held until
        // after the cancellation, so a datagram refused by one of these
        // sessions sees the listener as stopping (`is_stopping`) and is
        // counted as `session_closed`, not sent to admission.
        let closed = self.sessions.close_listener(self.id);
        self.cancel.cancel();
        self.resolver.stop();
        closed
    }
}

/// Availability state exposed by the control API.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListenerStatus {
    Available,
    Unresolved,
    Stopping,
}

/// Runtime listener model exposed by CLI/control.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ListenerSnapshot {
    pub id: ListenerId,
    pub name: String,
    pub bind: SocketAddr,
    pub configured_backend: String,
    pub resolved_backend: Option<SocketAddr>,
    pub status: ListenerStatus,
    pub dns: DnsSnapshot,
    pub counters: ListenerCounterSnapshot,
}

async fn receive_loop(handle: Arc<ListenerHandle>, context: ListenerContext) {
    let mut buffer = vec![0_u8; context.queue_budget.max_datagram_size() + 1];
    info!(
        listener = %handle.config().name,
        bind = %handle.bind(),
        "UDP listener started"
    );

    loop {
        let max_size = context.queue_budget.max_datagram_size();
        if buffer.len() != max_size.saturating_add(1) {
            buffer.resize(max_size.saturating_add(1), 0);
        }

        let received = tokio::select! {
            biased;
            () = handle.cancel.cancelled() => break,
            received = handle.socket.recv_from(&mut buffer) => received,
        };
        let (size, client_addr) = match received {
            Ok(result) => result,
            Err(error) => {
                context.metrics.socket_error();
                if context.log_limiter.should_log(LogSite::ListenerRecvError) {
                    warn!(
                        listener = %handle.config().name,
                        %error,
                        "listener receive failed"
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        if size > max_size {
            record_drop(&context.metrics, &handle.counters, DropReason::Oversize);
            continue;
        }
        let key = SessionKey::new(handle.id, client_addr);

        // Whether the datagram already went to a session that turned out to
        // be closed (idle expiry or a close that won the race with the
        // lookup). It then gets one new session, as if the lookup had missed.
        let mut stale_session = false;
        if let Some(session) = context.sessions.get_by_key(&key) {
            // Fast path: send on the session's connected upstream socket from
            // this task, queueing only when that socket is busy.
            match session.forward_from_client(
                &buffer[..size],
                &context.metrics,
                &handle.counters,
                &context.log_limiter,
            ) {
                Forward::Taken => continue,
                Forward::Closed => stale_session = true,
            }
            // Not kept across admission: the old session's last reference
            // releases its global slot.
            drop(session);
            if handle.is_stopping() {
                // The listener is stopping and opens no new session.
                record_drop(
                    &context.metrics,
                    &handle.counters,
                    DropReason::SessionClosed,
                );
                break;
            }
        }

        let admission = match context.admission.try_acquire(client_addr.ip()) {
            Ok(admission) => admission,
            Err(rejection) => {
                let rate_limited = rejection == AdmissionRejection::RateLimited;
                context.metrics.session_rejected(rate_limited);
                record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
                if context.log_limiter.should_log(match rejection {
                    AdmissionRejection::GlobalLimit => LogSite::GlobalSessionLimit,
                    AdmissionRejection::PerIpLimit => LogSite::PerIpSessionLimit,
                    AdmissionRejection::RateLimited => LogSite::SessionRateLimit,
                }) {
                    warn!(
                        listener = %handle.config().name,
                        reason = ?rejection,
                        "new session rejected"
                    );
                }
                continue;
            }
        };

        let backend_selection = {
            let _route_commit = lock_unpoisoned(&handle.route_commit);
            if handle.cancel.is_cancelled() {
                None
            } else {
                handle
                    .resolver
                    .current()
                    .map(|backend_addr| (backend_addr, handle.route_epoch.load(Ordering::Acquire)))
            }
        };
        let Some((backend_addr, route_epoch)) = backend_selection else {
            if handle.cancel.is_cancelled() {
                drop(admission);
                if stale_session {
                    record_drop(
                        &context.metrics,
                        &handle.counters,
                        DropReason::SessionClosed,
                    );
                }
                break;
            }
            context.metrics.session_rejected(false);
            record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
            drop(admission);
            if context.log_limiter.should_log(LogSite::BackendUnresolved) {
                warn!(
                    listener = %handle.config().name,
                    "new session rejected because backend has never resolved"
                );
            }
            continue;
        };

        let upstream = match upstream::connect(backend_addr).await {
            Ok(socket) => socket,
            Err(error) => {
                context.metrics.socket_error();
                context.metrics.session_rejected(false);
                record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
                drop(admission);
                if context
                    .log_limiter
                    .should_log(LogSite::UpstreamConnectError)
                {
                    warn!(
                        listener = %handle.config().name,
                        %backend_addr,
                        %error,
                        "failed to create connected upstream socket"
                    );
                }
                continue;
            }
        };
        let upstream_local_addr = match upstream.local_addr() {
            Ok(address) => address,
            Err(error) => {
                context.metrics.socket_error();
                context.metrics.session_rejected(false);
                record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
                drop(admission);
                if context
                    .log_limiter
                    .should_log(LogSite::UpstreamLocalAddressError)
                {
                    warn!(
                        listener = %handle.config().name,
                        %error,
                        "failed to read upstream local address"
                    );
                }
                continue;
            }
        };
        let listener_name = handle.config().name;
        let session = Session::new(
            handle.id,
            listener_name,
            client_addr,
            upstream_local_addr,
            backend_addr,
        );
        let launch = SessionLaunch {
            session: Arc::new(session),
            // Copied only after admission and upstream socket setup, so a
            // datagram rejected by the session limits or the rate limit (a
            // new-session flood) costs no allocation.
            first_datagram: buffer[..size].to_vec(),
            upstream,
            listener_socket: Arc::clone(&handle.socket),
            table: Arc::clone(&context.sessions),
            admission,
            metrics: Arc::clone(&context.metrics),
            listener_counters: Arc::clone(&handle.counters),
            idle_timeout: context.idle_timeout.subscribe(),
            queue_depth: context.session_queue_depth.load(Ordering::Relaxed),
            queue_budget: Arc::clone(&context.queue_budget),
            parent_cancel: handle.cancel.clone(),
            tracker: context.tracker.clone(),
            log_limiter: Arc::clone(&context.log_limiter),
        };
        let route_commit = lock_unpoisoned(&handle.route_commit);
        if handle.cancel.is_cancelled() || handle.route_epoch.load(Ordering::Acquire) != route_epoch
        {
            drop(route_commit);
            drop(launch);
            context.metrics.session_rejected(false);
            record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
            continue;
        }
        let launched = launch_session(launch);
        drop(route_commit);
        if launched.is_none() {
            // A newer mapping won the key race. This path is defensive because
            // one receive loop serializes session creation per listener.
            context.metrics.session_rejected(false);
            record_drop(&context.metrics, &handle.counters, DropReason::Rejected);
        }
    }

    info!(
        listener = %handle.config().name,
        "UDP listener stopped"
    );
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServiceConfig;

    struct Running {
        listener: Arc<ListenerHandle>,
        context: ListenerContext,
        backend: UdpSocket,
        client: UdpSocket,
    }

    async fn start() -> Running {
        start_with_rate(1_000).await
    }

    async fn start_with_rate(new_sessions_per_second: u32) -> Running {
        let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = Arc::new(Metrics::default());
        let service = ServiceConfig::default();
        let context = ListenerContext {
            sessions: SessionTable::new(),
            // One session per IP: a replacement needs the old slot freed.
            admission: AdmissionController::new(16, 1, new_sessions_per_second),
            metrics: Arc::clone(&metrics),
            idle_timeout: watch::channel(Duration::from_secs(30)).0,
            session_queue_depth: Arc::new(AtomicUsize::new(16)),
            queue_budget: QueueBudget::new(&service),
            dns_refresh_interval_ms: Arc::new(AtomicU64::new(60_000)),
            parent_cancel: CancellationToken::new(),
            tracker: TaskTracker::new(),
            log_limiter: Arc::new(LogRateLimiter::new(Duration::from_secs(60))),
        };
        let config = ListenerConfig {
            name: "test".to_owned(),
            bind: "127.0.0.1:0".parse().unwrap(),
            backend: backend.local_addr().unwrap().to_string().parse().unwrap(),
        };
        let prepared = PreparedListener::bind(ListenerId::new(1), config, &metrics)
            .await
            .unwrap();
        let bind = prepared.socket.local_addr().unwrap();
        let listener = prepared.activate(context.clone());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(bind).await.unwrap();
        Running {
            listener,
            context,
            backend,
            client,
        }
    }

    impl Running {
        async fn backend_receives(&self, expected: &[u8]) -> SocketAddr {
            let mut buffer = [0_u8; 64];
            let (size, from) =
                tokio::time::timeout(Duration::from_secs(5), self.backend.recv_from(&mut buffer))
                    .await
                    .expect("the datagram must reach the backend")
                    .unwrap();
            assert_eq!(&buffer[..size], expected);
            from
        }

        async fn stop(self) {
            self.listener.stop();
            self.context.parent_cancel.cancel();
            self.context.tracker.close();
            tokio::time::timeout(Duration::from_secs(5), self.context.tracker.wait())
                .await
                .expect("listener and sessions must stop");
        }
    }

    #[tokio::test]
    async fn a_datagram_refused_by_a_session_closed_after_the_lookup_opens_a_new_one() {
        let running = start().await;
        running.client.send(b"one").await.unwrap();
        let first = running.backend_receives(b"one").await;

        // The session is closed between the listener's lookup and its
        // forward, as by an idle expiry or a backend replacement.
        running
            .context
            .sessions
            .after_next_lookup(|table, session| assert!(table.close(session)));
        running.client.send(b"two").await.unwrap();
        let second = running.backend_receives(b"two").await;
        assert_ne!(first, second, "the datagram must go out on a new session");

        let stats = running.context.metrics.snapshot();
        assert_eq!(stats.sessions_created_total, 2);
        assert_eq!(stats.drops().total(), 0, "{stats:?}");
        assert_eq!(running.listener.snapshot().counters.dropped, 0);
        running.stop().await;
    }

    #[tokio::test]
    async fn a_stopping_listener_does_not_reopen_a_closed_session() {
        let running = start().await;
        running.client.send(b"one").await.unwrap();
        running.backend_receives(b"one").await;

        // The listener is stopped between its lookup and its forward.
        let stopping = Arc::clone(&running.listener);
        running.context.sessions.after_next_lookup(move |_, _| {
            assert_eq!(stopping.stop(), 1);
        });
        running.client.send(b"two").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while running.context.metrics.snapshot().drops().session_closed == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refused datagram must be counted");

        let stats = running.context.metrics.snapshot();
        assert_eq!(stats.sessions_created_total, 1);
        assert_eq!(stats.drops().session_closed, 1);
        assert_eq!(stats.drops().total(), 1);
        assert_eq!(stats.sessions_rejected_total, 0);
        running.stop().await;
    }

    /// The real `stop` runs on another thread while the listener is between
    /// its lookup and its forward. However the two interleave, the refused
    /// datagram is seen on a stopping listener: counted as `session_closed`,
    /// never sent to admission. With one token per second the first session
    /// leaves none, so admission would refuse it (`rejected`); with two it
    /// would accept it and spend the second.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_datagram_refused_during_stop_is_counted_as_session_closed() {
        for rate in [1, 2] {
            let running = start_with_rate(rate).await;
            running.client.send(b"one").await.unwrap();
            running.backend_receives(b"one").await;

            let stopping = Arc::clone(&running.listener);
            let (stopper_tx, stopper_rx) = std::sync::mpsc::channel();
            running
                .context
                .sessions
                .after_next_lookup(move |_, session| {
                    let stopper = std::thread::spawn(move || stopping.stop());
                    // Continue as soon as `stop` has closed the session,
                    // possibly before it has cancelled the token.
                    while !session.is_closed() {
                        std::hint::spin_loop();
                    }
                    stopper_tx.send(stopper).unwrap();
                });
            running.client.send(b"two").await.unwrap();
            let stopper = tokio::task::spawn_blocking(move || stopper_rx.recv().unwrap())
                .await
                .unwrap();
            assert_eq!(stopper.join().unwrap(), 1, "rate {rate}");
            tokio::time::timeout(Duration::from_secs(5), async {
                while running.context.metrics.snapshot().drops().session_closed == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the refused datagram must be counted");
            running.context.tracker.close();
            tokio::time::timeout(Duration::from_secs(5), running.context.tracker.wait())
                .await
                .expect("listener and sessions must stop");

            let stats = running.context.metrics.snapshot();
            assert_eq!(stats.sessions_created_total, 1, "rate {rate}");
            assert_eq!(stats.drops().session_closed, 1, "rate {rate}: {stats:?}");
            assert_eq!(stats.drops().total(), 1, "rate {rate}: {stats:?}");
            assert_eq!(stats.sessions_rejected_total, 0, "rate {rate}");
            let listener = running.listener.snapshot().counters;
            assert_eq!(listener.dropped, 1, "rate {rate}");
            assert_eq!(listener.drops().session_closed, 1, "rate {rate}");
            if rate == 2 {
                // The stopping path spent no admission token.
                drop(
                    running
                        .context
                        .admission
                        .try_acquire("192.0.2.1".parse().unwrap())
                        .expect("the second token must be left"),
                );
            }
            running.stop().await;
        }
    }
}
