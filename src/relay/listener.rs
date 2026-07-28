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
    limits::{AdmissionController, AdmissionRejection, LogRateLimiter},
    metrics::Metrics,
};

use super::{
    ListenerId, Session, SessionKey, SessionTable,
    session::{
        ListenerCounterSnapshot, ListenerCounters, QueueError, SessionLaunch, launch_session,
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
    pub max_datagram_size: Arc<AtomicUsize>,
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

    /// Stops admission and all sessions belonging to this incarnation.
    pub fn stop(&self) -> usize {
        let _route_commit = lock_unpoisoned(&self.route_commit);
        self.route_epoch.fetch_add(1, Ordering::AcqRel);
        self.cancel.cancel();
        self.resolver.stop();
        self.sessions.close_listener(self.id)
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
    let mut buffer = vec![0_u8; context.max_datagram_size.load(Ordering::Relaxed) + 1];
    info!(
        listener = %handle.config().name,
        bind = %handle.bind(),
        "UDP listener started"
    );

    loop {
        let max_size = context.max_datagram_size.load(Ordering::Relaxed);
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
                if context.log_limiter.should_log("listener-recv-error") {
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
            context.metrics.datagram_dropped();
            handle.counters.dropped();
            continue;
        }
        let datagram = buffer[..size].to_vec();
        let key = SessionKey::new(handle.id, client_addr);

        if let Some(session) = context.sessions.get_by_key(&key) {
            if matches!(
                session.try_send(datagram),
                Err(QueueError::Full | QueueError::Closed)
            ) {
                context.metrics.datagram_dropped();
                handle.counters.dropped();
            }
            continue;
        }

        let admission = match context.admission.try_acquire(client_addr.ip()) {
            Ok(admission) => admission,
            Err(rejection) => {
                let rate_limited = rejection == AdmissionRejection::RateLimited;
                context.metrics.session_rejected(rate_limited);
                context.metrics.datagram_dropped();
                handle.counters.dropped();
                if context.log_limiter.should_log(match rejection {
                    AdmissionRejection::GlobalLimit => "global-session-limit",
                    AdmissionRejection::PerIpLimit => "per-ip-session-limit",
                    AdmissionRejection::RateLimited => "session-rate-limit",
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
                break;
            }
            context.metrics.session_rejected(false);
            context.metrics.datagram_dropped();
            handle.counters.dropped();
            drop(admission);
            if context.log_limiter.should_log("backend-unresolved") {
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
                context.metrics.datagram_dropped();
                handle.counters.dropped();
                drop(admission);
                if context.log_limiter.should_log("upstream-connect-error") {
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
                context.metrics.datagram_dropped();
                handle.counters.dropped();
                drop(admission);
                if context
                    .log_limiter
                    .should_log("upstream-local-address-error")
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
            first_datagram: datagram,
            upstream,
            listener_socket: Arc::clone(&handle.socket),
            table: Arc::clone(&context.sessions),
            admission,
            metrics: Arc::clone(&context.metrics),
            listener_counters: Arc::clone(&handle.counters),
            idle_timeout: context.idle_timeout.subscribe(),
            max_datagram_size: Arc::clone(&context.max_datagram_size),
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
            context.metrics.datagram_dropped();
            handle.counters.dropped();
            continue;
        }
        let launched = launch_session(launch);
        drop(route_commit);
        if let Err(admission) = launched {
            // A newer mapping won the key race. This path is defensive because
            // one receive loop serializes session creation per listener.
            drop(admission);
            context.metrics.session_rejected(false);
            context.metrics.datagram_dropped();
            handle.counters.dropped();
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
