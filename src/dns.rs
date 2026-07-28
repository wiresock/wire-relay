// SPDX-License-Identifier: AGPL-3.0-or-later

//! Startup and periodically refreshed backend DNS cache.

use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{info, warn};

use crate::{config::BackendEndpoint, limits::LogRateLimiter, metrics::Metrics};

const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct ResolutionState {
    current: Option<SocketAddr>,
    last_success: Option<Instant>,
    last_attempt: Instant,
    last_error: Option<String>,
}

/// Control-plane view of one backend route.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DnsSnapshot {
    pub configured_backend: String,
    pub resolved_backend: Option<SocketAddr>,
    pub available: bool,
    pub last_success_age_ms: Option<u64>,
    pub last_attempt_age_ms: u64,
    pub last_error: Option<String>,
}

/// A resolved backend created before listener activation.
#[derive(Clone, Debug)]
pub struct PreparedBackend {
    endpoint: BackendEndpoint,
    state: ResolutionState,
}

impl PreparedBackend {
    /// Resolves a hostname once. Resolution failure is retained as unavailable
    /// state rather than preventing the listener from binding.
    pub async fn resolve(endpoint: BackendEndpoint, metrics: &Metrics) -> Self {
        let now = Instant::now();
        match resolve_endpoint(&endpoint).await {
            Ok(addresses) => Self {
                endpoint,
                state: ResolutionState {
                    current: addresses.first().copied(),
                    last_success: Some(now),
                    last_attempt: now,
                    last_error: None,
                },
            },
            Err(error) => {
                metrics.dns_error();
                warn!(
                    configured_backend = %endpoint,
                    %error,
                    "initial backend DNS resolution failed; listener is unavailable"
                );
                Self {
                    endpoint,
                    state: ResolutionState {
                        current: None,
                        last_success: None,
                        last_attempt: now,
                        last_error: Some(bounded_error(&error)),
                    },
                }
            }
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> &BackendEndpoint {
        &self.endpoint
    }
}

/// Live, atomically readable backend route for new sessions.
#[derive(Debug)]
pub struct BackendResolver {
    endpoint: RwLock<BackendEndpoint>,
    state: RwLock<ResolutionState>,
    generation: AtomicU64,
    task_cancel: Mutex<CancellationToken>,
    parent_cancel: CancellationToken,
    metrics: Arc<Metrics>,
    log_limiter: Arc<LogRateLimiter>,
    tracker: TaskTracker,
}

impl BackendResolver {
    /// Activates periodic refresh for a prepared backend.
    #[must_use]
    pub fn activate(
        prepared: PreparedBackend,
        refresh_interval: Duration,
        parent_cancel: CancellationToken,
        metrics: Arc<Metrics>,
        log_limiter: Arc<LogRateLimiter>,
        tracker: TaskTracker,
    ) -> Arc<Self> {
        let task_cancel = parent_cancel.child_token();
        let resolver = Arc::new(Self {
            endpoint: RwLock::new(prepared.endpoint),
            state: RwLock::new(prepared.state),
            generation: AtomicU64::new(1),
            task_cancel: Mutex::new(task_cancel.clone()),
            parent_cancel,
            metrics,
            log_limiter,
            tracker,
        });
        resolver.spawn_refresh_task(1, refresh_interval, task_cancel);
        resolver
    }

    /// Replaces the configured route after reload preflight. Existing sessions
    /// are unaffected because they store a concrete address.
    pub fn replace(self: &Arc<Self>, prepared: PreparedBackend, refresh_interval: Duration) {
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        {
            *write_unpoisoned(&self.endpoint) = prepared.endpoint;
            *write_unpoisoned(&self.state) = prepared.state;
        }

        let new_cancel = self.parent_cancel.child_token();
        let old_cancel = {
            let mut task_cancel = lock_unpoisoned(&self.task_cancel);
            std::mem::replace(&mut *task_cancel, new_cancel.clone())
        };
        old_cancel.cancel();
        self.spawn_refresh_task(generation, refresh_interval, new_cancel);
    }

    /// Restarts only the refresh schedule while retaining the last successful
    /// address and error state.
    pub fn set_refresh_interval(self: &Arc<Self>, refresh_interval: Duration) {
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let new_cancel = self.parent_cancel.child_token();
        let old_cancel = {
            let mut task_cancel = lock_unpoisoned(&self.task_cancel);
            std::mem::replace(&mut *task_cancel, new_cancel.clone())
        };
        old_cancel.cancel();
        self.spawn_refresh_task(generation, refresh_interval, new_cancel);
    }

    /// Concrete backend selected for a new session.
    #[must_use]
    pub fn current(&self) -> Option<SocketAddr> {
        read_unpoisoned(&self.state).current
    }

    #[must_use]
    pub fn endpoint(&self) -> BackendEndpoint {
        read_unpoisoned(&self.endpoint).clone()
    }

    #[must_use]
    pub fn snapshot(&self) -> DnsSnapshot {
        let now = Instant::now();
        let endpoint = read_unpoisoned(&self.endpoint).to_string();
        let state = read_unpoisoned(&self.state);
        DnsSnapshot {
            configured_backend: endpoint,
            resolved_backend: state.current,
            available: state.current.is_some(),
            last_success_age_ms: state
                .last_success
                .map(|time| duration_millis(now.saturating_duration_since(time))),
            last_attempt_age_ms: duration_millis(now.saturating_duration_since(state.last_attempt)),
            last_error: state.last_error.clone(),
        }
    }

    pub fn stop(&self) {
        lock_unpoisoned(&self.task_cancel).cancel();
    }

    fn spawn_refresh_task(
        self: &Arc<Self>,
        generation: u64,
        refresh_interval: Duration,
        cancel: CancellationToken,
    ) {
        if self.endpoint().socket_addr().is_some() {
            return;
        }

        let resolver = Arc::clone(self);
        self.tracker.spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The prepared backend already performed the startup lookup.
            interval.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        resolver.refresh_once(generation).await;
                    }
                }
            }
        });
    }

    async fn refresh_once(&self, generation: u64) {
        let endpoint = self.endpoint();
        let result = resolve_endpoint(&endpoint).await;
        self.apply_resolution(generation, &endpoint, result);
    }

    fn apply_resolution(
        &self,
        generation: u64,
        endpoint: &BackendEndpoint,
        result: io::Result<Vec<SocketAddr>>,
    ) {
        let now = Instant::now();
        match result.and_then(select_backend_address) {
            Ok(selected) => {
                let old = {
                    let mut state = write_unpoisoned(&self.state);
                    if self.generation.load(Ordering::Acquire) != generation {
                        return;
                    }
                    let old = state.current;
                    state.current = Some(selected);
                    state.last_success = Some(now);
                    state.last_attempt = now;
                    state.last_error = None;
                    old
                };
                if old != Some(selected) {
                    info!(
                        configured_backend = %endpoint,
                        old_backend = ?old,
                        new_backend = %selected,
                        "backend DNS address changed"
                    );
                }
            }
            Err(error) => {
                {
                    let mut state = write_unpoisoned(&self.state);
                    if self.generation.load(Ordering::Acquire) != generation {
                        return;
                    }
                    state.last_attempt = now;
                    state.last_error = Some(bounded_error(&error));
                    // Deliberately retain state.current and last_success.
                }
                self.metrics.dns_error();
                if self.log_limiter.should_log("dns-refresh-error") {
                    warn!(
                        configured_backend = %endpoint,
                        %error,
                        "backend DNS refresh failed; retaining last successful address"
                    );
                }
            }
        }
    }
}

/// Resolve an endpoint once, de-duplicating addresses while retaining the
/// system resolver's order.
pub async fn resolve_endpoint(endpoint: &BackendEndpoint) -> io::Result<Vec<SocketAddr>> {
    if let Some(address) = endpoint.socket_addr() {
        return is_usable_backend_address(address)
            .then_some(vec![address])
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "backend address must be unicast and non-unspecified",
                )
            });
    }
    let hostname = endpoint.hostname().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend has neither an IP address nor hostname",
        )
    })?;

    let lookup = tokio::net::lookup_host((hostname, endpoint.port()));
    let addresses = tokio::time::timeout(DNS_LOOKUP_TIMEOUT, lookup)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DNS lookup timed out"))??;

    let mut seen = HashSet::new();
    let addresses: Vec<_> = addresses
        .filter(|address| is_usable_backend_address(*address) && seen.insert(*address))
        .collect();
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "DNS lookup returned no usable unicast addresses",
        ));
    }
    Ok(addresses)
}

fn select_backend_address(addresses: Vec<SocketAddr>) -> io::Result<SocketAddr> {
    addresses
        .into_iter()
        .find(|address| is_usable_backend_address(*address))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "resolver returned no usable unicast addresses",
            )
        })
}

fn is_usable_backend_address(address: SocketAddr) -> bool {
    let ip = address.ip();
    !ip.is_unspecified()
        && !ip.is_multicast()
        && !matches!(ip, std::net::IpAddr::V4(address) if address.is_broadcast())
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn bounded_error(error: &io::Error) -> String {
    const MAX_ERROR_CHARS: usize = 512;
    error.to_string().chars().take(MAX_ERROR_CHARS).collect()
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
    use crate::relay::{ListenerId, Session};

    fn prepared_hostname(endpoint: BackendEndpoint, selected: SocketAddr) -> PreparedBackend {
        let now = Instant::now();
        PreparedBackend {
            endpoint,
            state: ResolutionState {
                current: Some(selected),
                last_success: Some(now),
                last_attempt: now,
                last_error: None,
            },
        }
    }

    fn activate_test_resolver(
        prepared: PreparedBackend,
        metrics: Arc<Metrics>,
    ) -> (Arc<BackendResolver>, TaskTracker) {
        let tracker = TaskTracker::new();
        let resolver = BackendResolver::activate(
            prepared,
            Duration::from_secs(86_400),
            CancellationToken::new(),
            metrics,
            Arc::new(LogRateLimiter::new(Duration::from_secs(1))),
            tracker.clone(),
        );
        (resolver, tracker)
    }

    async fn stop_test_resolver(resolver: &BackendResolver, tracker: TaskTracker) {
        resolver.stop();
        tracker.close();
        tracker.wait().await;
    }

    #[tokio::test]
    async fn numeric_backend_does_not_need_dns() {
        let endpoint = BackendEndpoint::parse("192.0.2.20:51820").unwrap();
        assert_eq!(
            resolve_endpoint(&endpoint).await.unwrap(),
            vec!["192.0.2.20:51820".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn unusable_numeric_backend_addresses_are_rejected_defensively() {
        for value in [
            "0.0.0.0:51820",
            "224.0.0.1:51820",
            "255.255.255.255:51820",
            "[::]:51820",
            "[ff02::1]:51820",
        ] {
            let endpoint = BackendEndpoint::parse(value).unwrap();
            let error = resolve_endpoint(&endpoint).await.unwrap_err();
            assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                ),
                "{value}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn stale_refresh_result_cannot_replace_a_new_backend_generation() {
        let metrics = Arc::new(Metrics::default());
        let old_endpoint = BackendEndpoint::parse("192.0.2.20:51820").unwrap();
        let prepared = PreparedBackend::resolve(old_endpoint.clone(), &metrics).await;
        let resolver = BackendResolver::activate(
            prepared,
            Duration::from_secs(60),
            CancellationToken::new(),
            Arc::clone(&metrics),
            Arc::new(LogRateLimiter::new(Duration::from_secs(1))),
            TaskTracker::new(),
        );
        let new_endpoint = BackendEndpoint::parse("192.0.2.30:51820").unwrap();
        resolver.replace(
            PreparedBackend::resolve(new_endpoint, &metrics).await,
            Duration::from_secs(60),
        );

        resolver.apply_resolution(
            1,
            &old_endpoint,
            Ok(vec!["192.0.2.20:51820".parse().unwrap()]),
        );

        assert_eq!(
            resolver.current(),
            Some("192.0.2.30:51820".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn failed_refresh_retains_the_last_successful_address() {
        let metrics = Arc::new(Metrics::default());
        let endpoint = BackendEndpoint::parse("relay.example:51820").unwrap();
        let selected = "192.0.2.20:51820".parse().unwrap();
        let (resolver, tracker) = activate_test_resolver(
            prepared_hostname(endpoint.clone(), selected),
            Arc::clone(&metrics),
        );

        resolver.apply_resolution(
            1,
            &endpoint,
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "simulated resolver timeout",
            )),
        );

        assert_eq!(resolver.current(), Some(selected));
        let snapshot = resolver.snapshot();
        assert!(snapshot.available);
        assert_eq!(snapshot.resolved_backend, Some(selected));
        assert!(
            snapshot
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("simulated resolver timeout"))
        );
        assert_eq!(metrics.snapshot().dns_errors_total, 1);

        stop_test_resolver(&resolver, tracker).await;
    }

    #[tokio::test]
    async fn refresh_changes_new_session_selection_without_mutating_existing_sessions() {
        let metrics = Arc::new(Metrics::default());
        let endpoint = BackendEndpoint::parse("relay.example:51820").unwrap();
        let old_backend = "192.0.2.20:51820".parse().unwrap();
        let new_backend = "192.0.2.30:51820".parse().unwrap();
        let (resolver, tracker) =
            activate_test_resolver(prepared_hostname(endpoint.clone(), old_backend), metrics);

        let existing = Session::new(
            ListenerId::new(1),
            "dns-test".to_owned(),
            "198.51.100.10:40000".parse().unwrap(),
            "127.0.0.1:50000".parse().unwrap(),
            resolver.current().unwrap(),
        );
        resolver.apply_resolution(1, &endpoint, Ok(vec![new_backend]));
        let created_after_refresh = Session::new(
            ListenerId::new(1),
            "dns-test".to_owned(),
            "198.51.100.11:40000".parse().unwrap(),
            "127.0.0.1:50001".parse().unwrap(),
            resolver.current().unwrap(),
        );

        assert_eq!(existing.backend_addr, old_backend);
        assert_eq!(created_after_refresh.backend_addr, new_backend);

        stop_test_resolver(&resolver, tracker).await;
    }
}
