// SPDX-License-Identifier: AGPL-3.0-or-later

//! Runtime supervision, live state, and transactional reload.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{info, warn};

#[cfg(unix)]
use crate::control::server::PreparedControlServer;
use crate::{
    CONTROL_PROTOCOL_VERSION, VERSION,
    config::{Config, ListenerConfig, MAX_CONFIGURED_SESSIONS, NormalizedConfig},
    control::{
        ControlError, ControlErrorCode, ControlRequest, ControlResponse,
        protocol::{
            ReloadResult, SessionCursor, SessionPage, SessionQuery, SessionSort, StatusSnapshot,
            VersionSnapshot,
        },
        server::{ControlHandler, ControlServerHandle},
    },
    dns::PreparedBackend,
    error::RuntimeError,
    limits::{AdmissionController, LogRateLimiter},
    logging,
    metrics::{Metrics, MetricsServerHandle, PreparedMetricsServer},
    relay::{
        ListenerHandle, ListenerId, ListenerSnapshot, PreparedListener, SessionSnapshot,
        SessionTable, listener::ListenerContext,
    },
};

const SESSION_CURSOR_TTL: Duration = Duration::from_secs(30);
const SESSION_CURSOR_REAP_INTERVAL: Duration = Duration::from_secs(5);
const MAX_SESSION_CURSOR_SNAPSHOTS: usize = 4;
const MAX_SESSION_CURSOR_ROWS: usize = MAX_CONFIGURED_SESSIONS * 2;
const MAX_CONCURRENT_SESSION_SNAPSHOT_BUILDS: usize = 2;

/// Optional services used by embedding/integration tests.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeOptions {
    pub control: bool,
    pub metrics: bool,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            control: true,
            metrics: true,
        }
    }
}

/// Owner of a running WireRelay instance.
#[derive(Clone)]
pub struct Runtime {
    core: Arc<RuntimeCore>,
}

struct RuntimeCore {
    config_path: PathBuf,
    started_at: Instant,
    config: RwLock<NormalizedConfig>,
    listeners: DashMap<String, Arc<ListenerHandle>>,
    next_listener_id: AtomicU64,
    sessions: Arc<SessionTable>,
    session_cursors: Mutex<SessionCursorCache>,
    session_snapshot_builds: Arc<Semaphore>,
    admission: Arc<AdmissionController>,
    metrics: Arc<Metrics>,
    idle_timeout: watch::Sender<Duration>,
    max_datagram_size: Arc<AtomicUsize>,
    dns_refresh_interval_ms: Arc<AtomicU64>,
    cancel: CancellationToken,
    tracker: TaskTracker,
    log_limiter: Arc<LogRateLimiter>,
    control_server: Mutex<Option<ControlServerHandle>>,
    metrics_server: Mutex<Option<MetricsServerHandle>>,
    metrics_enabled: bool,
    reload_in_progress: AtomicBool,
    shutting_down: AtomicBool,
    lifecycle_commit: Mutex<()>,
    shutdown_outcome: Mutex<Option<ShutdownOutcome>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SessionSnapshotQuery {
    listener: Option<String>,
    client_ip: Option<IpAddr>,
    sort: SessionSort,
}

impl From<&SessionQuery> for SessionSnapshotQuery {
    fn from(query: &SessionQuery) -> Self {
        Self {
            listener: query.listener.clone(),
            client_ip: query.client_ip,
            sort: query.sort,
        }
    }
}

struct CachedSessionSnapshot {
    query: SessionSnapshotQuery,
    sessions: Vec<SessionSnapshot>,
    next_offset: usize,
    expires_at: Instant,
}

#[derive(Default)]
struct SessionCursorCache {
    entries: HashMap<SessionCursor, CachedSessionSnapshot>,
    retained_rows: usize,
}

enum ListenerPreflight {
    Backend {
        existing: Arc<ListenerHandle>,
        prepared: PreparedBackend,
    },
    Replacement {
        existing: Arc<ListenerHandle>,
        prepared: PreparedListener,
    },
    Addition(PreparedListener),
}

impl Runtime {
    /// Starts listeners, metrics, and the local control plane after all
    /// bindable resources pass preflight.
    pub async fn start(
        config: NormalizedConfig,
        config_path: impl Into<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        Self::start_with_options(config, config_path, RuntimeOptions::default()).await
    }

    /// Starts with explicitly selected optional local services.
    pub async fn start_with_options(
        config: NormalizedConfig,
        config_path: impl Into<PathBuf>,
        options: RuntimeOptions,
    ) -> Result<Self, RuntimeError> {
        config
            .validate()
            .map_err(|error| RuntimeError::Reload(error.to_string()))?;

        #[cfg(not(unix))]
        if options.control {
            return Err(RuntimeError::Control(
                "Unix-domain control sockets are unsupported on this platform".to_owned(),
            ));
        }

        let config_path = config_path.into();
        let metrics = Arc::new(Metrics::default());
        let sessions = SessionTable::new();
        let admission = AdmissionController::new(
            config.service.max_sessions,
            config.service.max_sessions_per_ip,
            config.service.new_sessions_per_second,
        );
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let log_limiter = Arc::new(LogRateLimiter::new(Duration::from_secs(5)));
        let (idle_timeout, _idle_timeout_receiver) = watch::channel(config.service.idle_timeout);

        let mut listener_preflights = JoinSet::new();
        for (index, listener) in config.listeners.iter().cloned().enumerate() {
            let id = ListenerId::new(
                u64::try_from(index)
                    .unwrap_or(u64::MAX.saturating_sub(1))
                    .saturating_add(1),
            );
            let name = listener.name.clone();
            let bind = listener.bind;
            let metrics = Arc::clone(&metrics);
            listener_preflights.spawn(async move {
                PreparedListener::bind(id, listener, &metrics)
                    .await
                    .map_err(|source| RuntimeError::ListenerBind { name, bind, source })
            });
        }
        let mut prepared_listeners = Vec::with_capacity(config.listeners.len());
        while let Some(joined) = listener_preflights.join_next().await {
            let prepared = joined.map_err(|error| RuntimeError::Startup(error.to_string()))??;
            prepared_listeners.push(prepared);
        }

        let prepared_metrics = if options.metrics {
            prepare_metrics_for_config(&config).await?
        } else {
            None
        };

        #[cfg(unix)]
        let prepared_control = if options.control {
            Some(
                PreparedControlServer::bind(config.service.control_socket.clone())
                    .await
                    .map_err(|error| RuntimeError::Control(error.to_string()))?,
            )
        } else {
            None
        };

        let next_listener_id = u64::try_from(config.listeners.len())
            .unwrap_or(u64::MAX.saturating_sub(1))
            .saturating_add(1);
        let core = Arc::new(RuntimeCore {
            config_path,
            started_at: Instant::now(),
            config: RwLock::new(config.clone()),
            listeners: DashMap::new(),
            next_listener_id: AtomicU64::new(next_listener_id),
            sessions,
            session_cursors: Mutex::new(SessionCursorCache::default()),
            session_snapshot_builds: Arc::new(Semaphore::new(
                MAX_CONCURRENT_SESSION_SNAPSHOT_BUILDS,
            )),
            admission,
            metrics,
            idle_timeout,
            max_datagram_size: Arc::new(AtomicUsize::new(config.service.max_datagram_size)),
            dns_refresh_interval_ms: Arc::new(AtomicU64::new(duration_millis_nonzero(
                config.service.dns_refresh_interval,
            ))),
            cancel,
            tracker,
            log_limiter,
            control_server: Mutex::new(None),
            metrics_server: Mutex::new(None),
            metrics_enabled: options.metrics,
            reload_in_progress: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            lifecycle_commit: Mutex::new(()),
            shutdown_outcome: Mutex::new(None),
        });

        if options.control {
            core.spawn_session_cursor_reaper();
        }

        for prepared in prepared_listeners {
            let name = prepared.config().name.clone();
            let handle = prepared.activate(core.listener_context());
            core.listeners.insert(name, handle);
        }

        if let Some(prepared) = prepared_metrics {
            let handle = prepared.activate(Arc::clone(&core.metrics), &core.cancel, &core.tracker);
            *lock_unpoisoned(&core.metrics_server) = Some(handle);
        }

        #[cfg(unix)]
        if let Some(prepared) = prepared_control {
            let handler: Arc<dyn ControlHandler> = core.clone();
            let handle = prepared.activate(handler, &core.cancel, &core.tracker);
            *lock_unpoisoned(&core.control_server) = Some(handle);
        }

        info!(
            version = VERSION,
            config = %core.config_path.display(),
            listeners = core.listeners.len(),
            "WireRelay started"
        );
        Ok(Self { core })
    }

    #[must_use]
    pub fn status(&self) -> StatusSnapshot {
        self.core.status()
    }

    #[must_use]
    pub fn listeners(&self) -> Vec<ListenerSnapshot> {
        self.core.listener_snapshots()
    }

    #[must_use]
    pub fn sessions(&self) -> Vec<SessionSnapshot> {
        self.core.sessions.snapshots()
    }

    #[must_use]
    pub fn active_config(&self) -> NormalizedConfig {
        self.core.active_config()
    }

    #[must_use]
    pub fn control_socket(&self) -> PathBuf {
        self.active_config().service.control_socket
    }

    /// Applies the configuration currently stored at the runtime's path.
    pub async fn reload(&self) -> Result<ReloadResult, String> {
        self.core.reload_from_disk().await
    }

    /// Stops accept loops, cancels sessions, and waits for every tracked task.
    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        self.core.shutdown().await
    }
}

impl RuntimeCore {
    fn listener_context(&self) -> ListenerContext {
        ListenerContext {
            sessions: Arc::clone(&self.sessions),
            admission: Arc::clone(&self.admission),
            metrics: Arc::clone(&self.metrics),
            idle_timeout: self.idle_timeout.clone(),
            max_datagram_size: Arc::clone(&self.max_datagram_size),
            dns_refresh_interval_ms: Arc::clone(&self.dns_refresh_interval_ms),
            parent_cancel: self.cancel.clone(),
            tracker: self.tracker.clone(),
            log_limiter: Arc::clone(&self.log_limiter),
        }
    }

    fn active_config(&self) -> NormalizedConfig {
        read_unpoisoned(&self.config).clone()
    }

    fn listener_snapshots(&self) -> Vec<ListenerSnapshot> {
        let mut listeners: Vec<_> = self
            .listeners
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect();
        listeners.sort_by(|left, right| left.name.cmp(&right.name));
        listeners
    }

    fn status(&self) -> StatusSnapshot {
        let config = self.active_config();
        StatusSnapshot {
            version: VERSION.to_owned(),
            protocol_version: CONTROL_PROTOCOL_VERSION,
            uptime_ms: duration_millis(Instant::now().saturating_duration_since(self.started_at)),
            active_sessions: self.sessions.len(),
            control_socket: config.service.control_socket,
            listeners: self.listener_snapshots(),
            stats: self.metrics.snapshot(),
        }
    }

    async fn session_page(&self, query: &SessionQuery) -> Result<SessionPage, ControlError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(ControlError::new(
                ControlErrorCode::Unavailable,
                "runtime is shutting down",
            ));
        }

        if query.cursor.is_some() {
            return lock_unpoisoned(&self.session_cursors).continue_page(query, Instant::now());
        }
        if query.offset != 0 {
            return Err(ControlError::new(
                ControlErrorCode::InvalidRequest,
                "a nonzero session offset requires the cursor returned by the previous page",
            ));
        }

        let permit = Arc::clone(&self.session_snapshot_builds)
            .acquire_owned()
            .await
            .map_err(|_| {
                ControlError::new(
                    ControlErrorCode::Unavailable,
                    "session snapshot service is unavailable",
                )
            })?;
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(ControlError::new(
                ControlErrorCode::Unavailable,
                "runtime is shutting down",
            ));
        }

        let sessions = Arc::clone(&self.sessions);
        let snapshot_query = SessionSnapshotQuery::from(query);
        let build_query = snapshot_query.clone();
        let sorted = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            build_session_snapshot(&sessions, &build_query)
        })
        .await
        .map_err(|error| {
            ControlError::new(
                ControlErrorCode::Internal,
                format!("session snapshot worker failed: {error}"),
            )
        })?;

        let mut cursors = lock_unpoisoned(&self.session_cursors);
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(ControlError::new(
                ControlErrorCode::Unavailable,
                "runtime is shutting down",
            ));
        }
        cursors.start_page(snapshot_query, sorted, query.page_size(), Instant::now())
    }

    fn spawn_session_cursor_reaper(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            let mut interval = tokio::time::interval(SESSION_CURSOR_REAP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let Some(core) = weak.upgrade() else {
                            break;
                        };
                        lock_unpoisoned(&core.session_cursors).remove_expired(Instant::now());
                    }
                }
            }
        });
    }

    async fn reload_from_disk(&self) -> Result<ReloadResult, String> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err("runtime is shutting down".to_owned());
        }
        let _guard = ReloadGuard::begin(&self.reload_in_progress)?;
        let path = self.config_path.clone();
        let parsed = tokio::task::spawn_blocking(move || Config::load(path))
            .await
            .map_err(|error| format!("configuration load task failed: {error}"))?
            .map_err(|error| error.to_string())?;
        let normalized = parsed
            .into_normalized()
            .map_err(|error| error.to_string())?;
        self.apply_reload(normalized).await
    }

    async fn apply_reload(&self, new_config: NormalizedConfig) -> Result<ReloadResult, String> {
        new_config.validate().map_err(|error| error.to_string())?;
        let old_config = self.active_config();
        if old_config.service.control_socket != new_config.service.control_socket {
            return Err("changing service.control_socket requires a service restart".to_owned());
        }
        if old_config == new_config {
            return Ok(ReloadResult {
                applied: true,
                preserved: old_config
                    .listeners
                    .iter()
                    .map(|listener| listener.name.clone())
                    .collect(),
                message: "configuration is unchanged".to_owned(),
                ..ReloadResult::default()
            });
        }

        let current: HashMap<String, Arc<ListenerHandle>> = self
            .listeners
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect();
        let new_names: HashSet<_> = new_config
            .listeners
            .iter()
            .map(|listener| listener.name.as_str())
            .collect();

        let mut preserved = Vec::new();
        let mut backend_changes = Vec::new();
        let mut replacements = Vec::new();
        let mut additions = Vec::new();
        let mut preflights = JoinSet::new();

        for listener in &new_config.listeners {
            if let Some(existing) = current.get(&listener.name) {
                let existing_config = existing.config();
                if existing_config == *listener {
                    preserved.push(listener.name.clone());
                } else if existing_config.bind == listener.bind {
                    let existing = Arc::clone(existing);
                    let endpoint = listener.backend.clone();
                    let metrics = Arc::clone(&self.metrics);
                    preflights.spawn(async move {
                        let prepared = PreparedBackend::resolve(endpoint, &metrics).await;
                        Ok::<ListenerPreflight, String>(ListenerPreflight::Backend {
                            existing,
                            prepared,
                        })
                    });
                } else {
                    self.reject_active_bind_handoff(listener, &current)?;
                    let existing = Arc::clone(existing);
                    let listener = listener.clone();
                    let id = ListenerId::new(self.next_listener_id.fetch_add(1, Ordering::Relaxed));
                    let metrics = Arc::clone(&self.metrics);
                    preflights.spawn(async move {
                        prepare_listener_change(id, listener, metrics)
                            .await
                            .map(|prepared| ListenerPreflight::Replacement { existing, prepared })
                    });
                }
            } else {
                self.reject_active_bind_handoff(listener, &current)?;
                let listener = listener.clone();
                let id = ListenerId::new(self.next_listener_id.fetch_add(1, Ordering::Relaxed));
                let metrics = Arc::clone(&self.metrics);
                preflights.spawn(async move {
                    prepare_listener_change(id, listener, metrics)
                        .await
                        .map(ListenerPreflight::Addition)
                });
            }
        }

        while let Some(joined) = preflights.join_next().await {
            match joined.map_err(|error| format!("listener preflight task failed: {error}"))?? {
                ListenerPreflight::Backend { existing, prepared } => {
                    backend_changes.push((existing, prepared));
                }
                ListenerPreflight::Replacement { existing, prepared } => {
                    replacements.push((existing, prepared));
                }
                ListenerPreflight::Addition(prepared) => additions.push(prepared),
            }
        }

        let removals: Vec<_> = current
            .iter()
            .filter(|(name, _)| !new_names.contains(name.as_str()))
            .map(|(_, listener)| Arc::clone(listener))
            .collect();

        let old_metrics_bind = configured_metrics_bind(&old_config);
        let new_metrics_bind = configured_metrics_bind(&new_config);
        let metrics_changed = old_metrics_bind != new_metrics_bind;
        let prepared_metrics = if self.metrics_enabled && metrics_changed {
            match new_metrics_bind {
                Some(bind) => Some(
                    PreparedMetricsServer::bind(bind)
                        .await
                        .map_err(|error| format!("cannot pre-bind metrics endpoint: {error}"))?,
                ),
                None => None,
            }
        } else {
            None
        };

        // Everything that can fail has completed. The remaining operations are
        // synchronous swaps, task activation, and cancellation.
        let _lifecycle = lock_unpoisoned(&self.lifecycle_commit);
        if self.shutting_down.load(Ordering::Acquire) {
            return Err("runtime began shutting down during reload preflight".to_owned());
        }
        logging::reload_level(new_config.service.log_level)?;
        self.idle_timeout
            .send_replace(new_config.service.idle_timeout);
        self.max_datagram_size
            .store(new_config.service.max_datagram_size, Ordering::Relaxed);
        self.dns_refresh_interval_ms.store(
            duration_millis_nonzero(new_config.service.dns_refresh_interval),
            Ordering::Relaxed,
        );
        self.admission.update(
            new_config.service.max_sessions,
            new_config.service.max_sessions_per_ip,
            new_config.service.new_sessions_per_second,
        );

        let refresh_interval = new_config.service.dns_refresh_interval;
        let mut result = ReloadResult {
            applied: true,
            preserved,
            message: "configuration reloaded transactionally".to_owned(),
            ..ReloadResult::default()
        };

        for (existing, prepared) in backend_changes {
            let name = existing.config().name;
            result.sessions_closed += existing.replace_backend(prepared, refresh_interval);
            result.modified.push(name);
        }

        for (existing, prepared) in replacements {
            let name = prepared.config().name.clone();
            let replacement = prepared.activate(self.listener_context());
            self.listeners.insert(name.clone(), replacement);
            result.sessions_closed += existing.stop();
            result.modified.push(name);
        }

        for prepared in additions {
            let name = prepared.config().name.clone();
            let handle = prepared.activate(self.listener_context());
            self.listeners.insert(name.clone(), handle);
            result.added.push(name);
        }

        for listener in removals {
            let name = listener.config().name;
            self.listeners.remove(&name);
            result.sessions_closed += listener.stop();
            result.removed.push(name);
        }

        if old_config.service.dns_refresh_interval != new_config.service.dns_refresh_interval {
            for listener in &self.listeners {
                listener.set_dns_refresh_interval(refresh_interval);
            }
        }

        if self.metrics_enabled && metrics_changed {
            let old = {
                let mut metrics_server = lock_unpoisoned(&self.metrics_server);
                let new_handle = prepared_metrics.map(|prepared| {
                    prepared.activate(Arc::clone(&self.metrics), &self.cancel, &self.tracker)
                });
                std::mem::replace(&mut *metrics_server, new_handle)
            };
            if let Some(old) = old {
                old.stop();
            }
        }

        *write_unpoisoned(&self.config) = new_config;
        result.preserved.sort();
        result.added.sort();
        result.modified.sort();
        result.removed.sort();
        info!(
            preserved = result.preserved.len(),
            added = result.added.len(),
            modified = result.modified.len(),
            removed = result.removed.len(),
            sessions_closed = result.sessions_closed,
            "configuration reloaded"
        );
        Ok(result)
    }

    fn reject_active_bind_handoff(
        &self,
        candidate: &ListenerConfig,
        current: &HashMap<String, Arc<ListenerHandle>>,
    ) -> Result<(), String> {
        for (name, listener) in current {
            if name != &candidate.name && bind_addresses_conflict(candidate.bind, listener.bind()) {
                return Err(format!(
                    "listener `{}` cannot take active bind {} from `{name}` in one reload; \
                     remove it first or restart the service",
                    candidate.name, candidate.bind
                ));
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RuntimeError> {
        {
            let _lifecycle = lock_unpoisoned(&self.lifecycle_commit);
            if self
                .shutting_down
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                info!("WireRelay shutdown started");
                if let Some(control) = lock_unpoisoned(&self.control_server).as_ref() {
                    control.stop();
                }
                if let Some(metrics) = lock_unpoisoned(&self.metrics_server).take() {
                    metrics.stop();
                }
                for listener in &self.listeners {
                    listener.stop();
                }
                self.sessions.close_all();
                lock_unpoisoned(&self.session_cursors).clear();
                self.cancel.cancel();
                self.tracker.close();
            }
        }

        self.wait_for_or_complete_shutdown().await
    }

    async fn wait_for_or_complete_shutdown(&self) -> Result<(), RuntimeError> {
        if let Some(outcome) = lock_unpoisoned(&self.shutdown_outcome).clone() {
            return outcome.into_result();
        }
        let timeout = self.active_config().service.shutdown_timeout;
        let observed = if tokio::time::timeout(timeout, self.tracker.wait())
            .await
            .is_ok()
        {
            lock_unpoisoned(&self.control_server)
                .as_ref()
                .and_then(ControlServerHandle::cleanup_error)
                .map_or(ShutdownOutcome::Complete, ShutdownOutcome::ControlCleanup)
        } else {
            ShutdownOutcome::TimedOut(timeout)
        };
        let outcome = {
            let mut stored = lock_unpoisoned(&self.shutdown_outcome);
            stored.get_or_insert(observed).clone()
        };
        outcome.into_result()
    }
}

impl SessionCursorCache {
    fn start_page(
        &mut self,
        query: SessionSnapshotQuery,
        sessions: Vec<SessionSnapshot>,
        page_size: usize,
        now: Instant,
    ) -> Result<SessionPage, ControlError> {
        self.remove_expired(now);
        let total = sessions.len();
        let end = page_size.min(total);
        let page = sessions[..end].to_vec();
        if end == total {
            return Ok(SessionPage {
                sessions: page,
                total,
                next_offset: None,
                next_cursor: None,
            });
        }
        if total > MAX_SESSION_CURSOR_ROWS {
            return Err(ControlError::new(
                ControlErrorCode::Unavailable,
                "session snapshot exceeds the daemon cursor-cache row limit",
            ));
        }

        self.make_room(total);
        let cursor = loop {
            let candidate = SessionCursor::new();
            if !self.entries.contains_key(&candidate) {
                break candidate;
            }
        };
        self.retained_rows = self.retained_rows.saturating_add(total);
        self.entries.insert(
            cursor,
            CachedSessionSnapshot {
                query,
                sessions,
                next_offset: end,
                expires_at: now + SESSION_CURSOR_TTL,
            },
        );
        Ok(SessionPage {
            sessions: page,
            total,
            next_offset: Some(end),
            next_cursor: Some(cursor),
        })
    }

    fn continue_page(
        &mut self,
        query: &SessionQuery,
        now: Instant,
    ) -> Result<SessionPage, ControlError> {
        self.remove_expired(now);
        let cursor = query.cursor.ok_or_else(|| {
            ControlError::new(
                ControlErrorCode::InvalidRequest,
                "session cursor is required for a continuation page",
            )
        })?;
        let requested_query = SessionSnapshotQuery::from(query);
        let (page, total, end, complete) = {
            let entry = self.entries.get_mut(&cursor).ok_or_else(|| {
                ControlError::new(
                    ControlErrorCode::NotFound,
                    format!("session cursor `{cursor}` is unknown or expired; restart the listing"),
                )
            })?;
            if entry.query != requested_query {
                return Err(ControlError::new(
                    ControlErrorCode::InvalidRequest,
                    "session cursor filters or sort order do not match the original request",
                ));
            }
            if query.offset != entry.next_offset {
                return Err(ControlError::new(
                    ControlErrorCode::InvalidRequest,
                    format!(
                        "session cursor expected offset {}, received {}",
                        entry.next_offset, query.offset
                    ),
                ));
            }

            let total = entry.sessions.len();
            let end = query.offset.saturating_add(query.page_size()).min(total);
            let page = entry.sessions[query.offset..end].to_vec();
            let complete = end == total;
            if !complete {
                entry.next_offset = end;
                entry.expires_at = now + SESSION_CURSOR_TTL;
            }
            (page, total, end, complete)
        };

        if complete {
            self.remove(&cursor);
        }
        Ok(SessionPage {
            sessions: page,
            total,
            next_offset: (!complete).then_some(end),
            next_cursor: (!complete).then_some(cursor),
        })
    }

    fn make_room(&mut self, incoming_rows: usize) {
        while self.entries.len() >= MAX_SESSION_CURSOR_SNAPSHOTS
            || self.retained_rows.saturating_add(incoming_rows) > MAX_SESSION_CURSOR_ROWS
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(cursor, _)| *cursor)
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    fn remove_expired(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(cursor, _)| *cursor)
            .collect();
        for cursor in expired {
            self.remove(&cursor);
        }
    }

    fn remove(&mut self, cursor: &SessionCursor) {
        if let Some(entry) = self.entries.remove(cursor) {
            self.retained_rows = self.retained_rows.saturating_sub(entry.sessions.len());
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.retained_rows = 0;
    }
}

fn build_session_snapshot(
    sessions: &SessionTable,
    query: &SessionSnapshotQuery,
) -> Vec<SessionSnapshot> {
    let mut snapshots: Vec<_> = sessions
        .snapshots()
        .into_iter()
        .filter(|session| {
            query
                .listener
                .as_ref()
                .is_none_or(|listener| &session.listener == listener)
                && query
                    .client_ip
                    .is_none_or(|ip| session.client_addr.ip() == ip)
        })
        .collect();
    snapshots.sort_by(|left, right| compare_sessions(left, right, query.sort));
    snapshots
}

impl ControlHandler for RuntimeCore {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ControlResponse, ControlError>> + Send + '_>> {
        Box::pin(async move {
            match request {
                ControlRequest::Status => Ok(ControlResponse::Status(self.status())),
                ControlRequest::ActiveConfig => {
                    Ok(ControlResponse::ActiveConfig(self.active_config()))
                }
                ControlRequest::Listeners => {
                    Ok(ControlResponse::Listeners(self.listener_snapshots()))
                }
                ControlRequest::Sessions(query) => self
                    .session_page(&query)
                    .await
                    .map(ControlResponse::Sessions),
                ControlRequest::Session { id } => self
                    .sessions
                    .get_by_id(&id)
                    .map(|session| ControlResponse::Session(session.session().snapshot()))
                    .ok_or_else(|| {
                        ControlError::new(
                            ControlErrorCode::NotFound,
                            format!("session `{id}` was not found"),
                        )
                    }),
                ControlRequest::Stats => Ok(ControlResponse::Stats(self.metrics.snapshot())),
                ControlRequest::Reload => self
                    .reload_from_disk()
                    .await
                    .map(ControlResponse::Reload)
                    .map_err(|message| {
                        warn!(%message, "configuration reload rejected");
                        ControlError::new(ControlErrorCode::ReloadRejected, message)
                    }),
                ControlRequest::CloseSession { id } => {
                    let session = self.sessions.get_by_id(&id).ok_or_else(|| {
                        ControlError::new(
                            ControlErrorCode::NotFound,
                            format!("session `{id}` was not found"),
                        )
                    })?;
                    session.close();
                    Ok(ControlResponse::SessionClosed { id })
                }
                ControlRequest::Version => Ok(ControlResponse::Version(VersionSnapshot {
                    application: VERSION.to_owned(),
                    protocol: CONTROL_PROTOCOL_VERSION,
                })),
            }
        })
    }
}

struct ReloadGuard<'a>(&'a AtomicBool);

#[derive(Clone)]
enum ShutdownOutcome {
    Complete,
    TimedOut(Duration),
    ControlCleanup(String),
}

impl ShutdownOutcome {
    fn into_result(self) -> Result<(), RuntimeError> {
        match self {
            Self::Complete => {
                info!("WireRelay shutdown complete");
                Ok(())
            }
            Self::TimedOut(timeout) => Err(RuntimeError::ShutdownTimeout(timeout)),
            Self::ControlCleanup(error) => Err(RuntimeError::ControlCleanup(error)),
        }
    }
}

impl<'a> ReloadGuard<'a> {
    fn begin(flag: &'a AtomicBool) -> Result<Self, String> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self(flag))
            .map_err(|_| "another reload is already in progress".to_owned())
    }
}

impl Drop for ReloadGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn prepare_metrics_for_config(
    config: &NormalizedConfig,
) -> Result<Option<PreparedMetricsServer>, RuntimeError> {
    let Some(bind) = configured_metrics_bind(config) else {
        return Ok(None);
    };
    PreparedMetricsServer::bind(bind)
        .await
        .map(Some)
        .map_err(|source| RuntimeError::MetricsBind { bind, source })
}

async fn prepare_listener_change(
    id: ListenerId,
    listener: ListenerConfig,
    metrics: Arc<Metrics>,
) -> Result<PreparedListener, String> {
    let name = listener.name.clone();
    let bind = listener.bind;
    PreparedListener::bind(id, listener, &metrics)
        .await
        .map_err(|error| format!("cannot pre-bind listener `{name}` at {bind}: {error}"))
}

fn configured_metrics_bind(config: &NormalizedConfig) -> Option<SocketAddr> {
    config
        .metrics
        .as_ref()
        .filter(|metrics| metrics.enabled)
        .map(|metrics| metrics.bind)
}

fn session_bytes(session: &SessionSnapshot) -> u64 {
    session
        .bytes_to_backend
        .saturating_add(session.bytes_to_client)
}

fn compare_sessions(
    left: &SessionSnapshot,
    right: &SessionSnapshot,
    sort: SessionSort,
) -> std::cmp::Ordering {
    match sort {
        SessionSort::Id => left.id.cmp(&right.id),
        SessionSort::Bytes => session_bytes(right)
            .cmp(&session_bytes(left))
            .then_with(|| left.id.cmp(&right.id)),
        SessionSort::Age => right
            .age_ms
            .cmp(&left.age_ms)
            .then_with(|| left.id.cmp(&right.id)),
        SessionSort::Idle => right
            .idle_ms
            .cmp(&left.idle_ms)
            .then_with(|| left.id.cmp(&right.id)),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_millis_nonzero(duration: Duration) -> u64 {
    duration_millis(duration).max(1)
}

fn bind_addresses_conflict(left: SocketAddr, right: SocketAddr) -> bool {
    if left.port() != right.port() {
        return false;
    }
    match (left.ip(), right.ip()) {
        (IpAddr::V4(left), IpAddr::V4(right)) => {
            left == right || left.is_unspecified() || right.is_unspecified()
        }
        (IpAddr::V6(left), IpAddr::V6(right)) => {
            left == right || left.is_unspecified() || right.is_unspecified()
        }
        (IpAddr::V6(v6), IpAddr::V4(v4)) | (IpAddr::V4(v4), IpAddr::V6(v6)) => {
            v6.is_unspecified()
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped == v4 || v4.is_unspecified())
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_snapshot(label: &str, bytes: u64) -> SessionSnapshot {
        SessionSnapshot {
            id: crate::relay::SessionId::new(),
            listener_id: ListenerId::new(1),
            listener: label.to_owned(),
            client_addr: "198.51.100.10:40000".parse().unwrap(),
            upstream_local_addr: "127.0.0.1:50000".parse().unwrap(),
            backend_addr: "192.0.2.20:51820".parse().unwrap(),
            age_ms: 1,
            idle_ms: 1,
            last_client_activity_ms: 1,
            last_backend_activity_ms: 1,
            packets_to_backend: 1,
            packets_to_client: 1,
            bytes_to_backend: bytes,
            bytes_to_client: 0,
        }
    }

    #[test]
    fn session_cursor_pages_are_stable_and_removed_when_complete() {
        let now = Instant::now();
        let mut cache = SessionCursorCache::default();
        let mut query = SessionQuery {
            limit: 2,
            ..SessionQuery::default()
        };
        let first = cache
            .start_page(
                SessionSnapshotQuery::from(&query),
                vec![
                    session_snapshot("first", 1),
                    session_snapshot("second", 2),
                    session_snapshot("third", 3),
                ],
                query.page_size(),
                now,
            )
            .unwrap();
        assert_eq!(
            first
                .sessions
                .iter()
                .map(|session| session.listener.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        let cursor = first.next_cursor.unwrap();
        query.cursor = Some(cursor);
        query.offset = first.next_offset.unwrap();

        let second = cache.continue_page(&query, now).unwrap();
        assert_eq!(second.total, 3);
        assert_eq!(second.sessions[0].listener, "third");
        assert!(second.next_cursor.is_none());
        assert!(second.next_offset.is_none());
        assert!(cache.entries.is_empty());
        assert_eq!(cache.retained_rows, 0);

        assert_eq!(
            cache.continue_page(&query, now).unwrap_err().code,
            ControlErrorCode::NotFound
        );
    }

    #[test]
    fn session_cursor_rejects_changed_query_and_expires() {
        let now = Instant::now();
        let mut cache = SessionCursorCache::default();
        let initial = SessionQuery {
            listener: Some("one".to_owned()),
            limit: 1,
            ..SessionQuery::default()
        };
        let first = cache
            .start_page(
                SessionSnapshotQuery::from(&initial),
                vec![session_snapshot("one", 1), session_snapshot("one", 2)],
                initial.page_size(),
                now,
            )
            .unwrap();
        let mut changed = initial.clone();
        changed.cursor = first.next_cursor;
        changed.offset = first.next_offset.unwrap();
        changed.sort = SessionSort::Bytes;
        assert_eq!(
            cache.continue_page(&changed, now).unwrap_err().code,
            ControlErrorCode::InvalidRequest
        );

        changed.sort = initial.sort;
        assert_eq!(
            cache
                .continue_page(&changed, now + SESSION_CURSOR_TTL)
                .unwrap_err()
                .code,
            ControlErrorCode::NotFound
        );
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn session_cursor_cache_evicts_oldest_at_capacity() {
        let now = Instant::now();
        let mut cache = SessionCursorCache::default();
        let query = SessionQuery {
            limit: 1,
            ..SessionQuery::default()
        };
        let mut first_cursor = None;
        for index in 0..=MAX_SESSION_CURSOR_SNAPSHOTS {
            let page = cache
                .start_page(
                    SessionSnapshotQuery::from(&query),
                    vec![
                        session_snapshot(&format!("{index}-a"), 1),
                        session_snapshot(&format!("{index}-b"), 2),
                    ],
                    query.page_size(),
                    now + Duration::from_millis(u64::try_from(index).unwrap()),
                )
                .unwrap();
            first_cursor.get_or_insert(page.next_cursor.unwrap());
        }
        assert_eq!(cache.entries.len(), MAX_SESSION_CURSOR_SNAPSHOTS);
        assert_eq!(cache.retained_rows, MAX_SESSION_CURSOR_SNAPSHOTS * 2);
        assert!(!cache.entries.contains_key(&first_cursor.unwrap()));
    }

    #[tokio::test]
    async fn cancelling_the_first_shutdown_waiter_does_not_wedge_later_callers() {
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let listener = reservation.local_addr().unwrap();
        let config = Config::parse_str(&format!(
            r#"
[service]
shutdown_timeout = "1s"

[[listeners]]
name = "shutdown-test"
bind = "{listener}"
backend = "127.0.0.1:9"
"#
        ))
        .unwrap()
        .into_normalized()
        .unwrap();
        drop(reservation);

        let runtime = Runtime::start_with_options(
            config,
            PathBuf::from("shutdown-cancellation-test.toml"),
            RuntimeOptions {
                control: false,
                metrics: false,
            },
        )
        .await
        .unwrap();
        let blocker = CancellationToken::new();
        let task_blocker = blocker.clone();
        runtime
            .core
            .tracker
            .spawn(async move { task_blocker.cancelled().await });

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !runtime.core.shutting_down.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        let _ = first.await;

        blocker.cancel();
        tokio::time::timeout(Duration::from_secs(2), runtime.shutdown())
            .await
            .expect("a later shutdown caller must not hang")
            .expect("tracked tasks complete after the blocker is released");
    }
}
