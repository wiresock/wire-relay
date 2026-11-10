// SPDX-License-Identifier: AGPL-3.0-or-later

//! Session admission and repetitive-log rate limiting.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Why a new session was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRejection {
    /// The global active-session limit is full.
    GlobalLimit,
    /// This source IP has reached its active-session limit.
    PerIpLimit,
    /// The global new-session token bucket is empty.
    RateLimited,
}

#[derive(Debug)]
struct AdmissionState {
    active: usize,
    per_ip: HashMap<IpAddr, usize>,
    max_sessions: usize,
    max_sessions_per_ip: usize,
    rate: u32,
    tokens: f64,
    last_refill: Instant,
}

/// Thread-safe admission controller shared by every listener.
#[derive(Debug)]
pub struct AdmissionController {
    state: Mutex<AdmissionState>,
}

impl AdmissionController {
    /// Creates a controller with one second of initial burst capacity.
    #[must_use]
    pub fn new(max_sessions: usize, max_sessions_per_ip: usize, rate: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AdmissionState {
                active: 0,
                per_ip: HashMap::new(),
                max_sessions,
                max_sessions_per_ip,
                rate,
                tokens: f64::from(rate),
                last_refill: Instant::now(),
            }),
        })
    }

    /// Applies new limits without terminating already-admitted sessions.
    pub fn update(&self, max_sessions: usize, max_sessions_per_ip: usize, rate: u32) {
        let mut state = lock_unpoisoned(&self.state);
        refill(&mut state, Instant::now());
        state.max_sessions = max_sessions;
        state.max_sessions_per_ip = max_sessions_per_ip;
        state.rate = rate;
        state.tokens = state.tokens.min(f64::from(rate));
    }

    /// Attempts to reserve all resources needed by a new session.
    pub fn try_acquire(
        self: &Arc<Self>,
        source_ip: IpAddr,
    ) -> Result<AdmissionLease, AdmissionRejection> {
        self.try_acquire_at(source_ip, Instant::now())
    }

    fn try_acquire_at(
        self: &Arc<Self>,
        source_ip: IpAddr,
        now: Instant,
    ) -> Result<AdmissionLease, AdmissionRejection> {
        let mut state = lock_unpoisoned(&self.state);

        if state.active >= state.max_sessions {
            return Err(AdmissionRejection::GlobalLimit);
        }

        if state.per_ip.get(&source_ip).copied().unwrap_or(0) >= state.max_sessions_per_ip {
            return Err(AdmissionRejection::PerIpLimit);
        }

        refill(&mut state, now);
        if state.tokens < 1.0 {
            return Err(AdmissionRejection::RateLimited);
        }

        state.tokens -= 1.0;
        state.active += 1;
        *state.per_ip.entry(source_ip).or_default() += 1;
        drop(state);

        Ok(AdmissionLease {
            controller: Arc::clone(self),
            source_ip,
            released: false,
        })
    }

    /// Returns the number of currently reserved sessions.
    #[must_use]
    pub fn active(&self) -> usize {
        lock_unpoisoned(&self.state).active
    }

    fn release(&self, source_ip: IpAddr) {
        let mut state = lock_unpoisoned(&self.state);
        state.active = state.active.saturating_sub(1);

        let should_remove = if let Some(count) = state.per_ip.get_mut(&source_ip) {
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if should_remove {
            state.per_ip.remove(&source_ip);
        }
    }
}

fn refill(state: &mut AdmissionState, now: Instant) {
    let elapsed = now.saturating_duration_since(state.last_refill);
    state.last_refill = now;
    let capacity = f64::from(state.rate);
    state.tokens = (state.tokens + elapsed.as_secs_f64() * f64::from(state.rate)).min(capacity);
}

/// RAII reservation. Dropping it releases global and per-IP capacity.
#[derive(Debug)]
pub struct AdmissionLease {
    controller: Arc<AdmissionController>,
    source_ip: IpAddr,
    released: bool,
}

impl AdmissionLease {
    /// Releases this reservation immediately. Dropping an unreleased lease has
    /// the same effect.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            self.controller.release(self.source_ip);
            self.released = true;
        }
    }
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// Every rate-limited log site in the daemon.
///
/// The limiter keeps one atomic timestamp per entry, so `should_log` never
/// locks. A site missing from this table shares the last, catch-all slot (and
/// trips a debug assertion in tests).
pub const LOG_SITES: &[&str] = &[
    "backend-send-error",
    "backend-unresolved",
    "client-send-error",
    "dns-refresh-error",
    "global-session-limit",
    "listener-recv-error",
    "partial-backend-send",
    "partial-client-send",
    "per-ip-session-limit",
    "queue-budget-exhausted",
    "session-queue-full",
    "session-rate-limit",
    "upstream-connect-error",
    "upstream-local-address-error",
    "upstream-recv-error",
];

/// A small helper for suppressing repetitive log sites.
///
/// Each site allows one event per interval. The state is a lock-free atomic
/// timestamp per site, because sites such as `session-queue-full` fire on the
/// datagram drop path exactly when the relay is overloaded.
#[derive(Debug)]
pub struct LogRateLimiter {
    interval_nanos: u64,
    origin: Instant,
    /// Nanoseconds since `origin` plus one of each site's last event; zero
    /// means the site has not logged yet. One extra slot for unknown sites.
    last: Box<[AtomicU64]>,
}

impl LogRateLimiter {
    /// Creates a limiter that allows each named site once per `interval`.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval_nanos: u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX),
            origin: Instant::now(),
            last: (0..=LOG_SITES.len()).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Returns true when the caller should emit this log event.
    pub fn should_log(&self, site: &'static str) -> bool {
        self.should_log_at(site, Instant::now())
    }

    fn should_log_at(&self, site: &'static str, now: Instant) -> bool {
        let index = LOG_SITES.iter().position(|known| *known == site);
        debug_assert!(index.is_some(), "unregistered log site {site:?}");
        let slot = &self.last[index.unwrap_or(LOG_SITES.len())];
        let now = u64::try_from(now.saturating_duration_since(self.origin).as_nanos())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1);
        let last = slot.load(Ordering::Relaxed);
        if last != 0 && now.saturating_sub(last) < self.interval_nanos {
            return false;
        }
        // Only one caller wins a given interval; the losers stay quiet.
        slot.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
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
    fn enforces_global_and_per_ip_limits_and_releases() {
        let controller = AdmissionController::new(2, 1, 100);
        let first_ip = IpAddr::from([192, 0, 2, 1]);
        let second_ip = IpAddr::from([192, 0, 2, 2]);

        let first = controller.try_acquire(first_ip).unwrap();
        assert_eq!(
            controller.try_acquire(first_ip).unwrap_err(),
            AdmissionRejection::PerIpLimit
        );
        let second = controller.try_acquire(second_ip).unwrap();
        assert_eq!(controller.active(), 2);
        drop(first);
        assert_eq!(controller.active(), 1);
        drop(second);
        assert_eq!(controller.active(), 0);
    }

    #[test]
    fn log_sites_allow_one_event_per_interval_independently() {
        let limiter = LogRateLimiter::new(Duration::from_secs(5));
        let start = limiter.origin;
        assert!(limiter.should_log_at("session-queue-full", start));
        assert!(!limiter.should_log_at("session-queue-full", start));
        assert!(!limiter.should_log_at("session-queue-full", start + Duration::from_millis(4_999)));
        // Another site has its own slot.
        assert!(limiter.should_log_at("queue-budget-exhausted", start));
        assert!(limiter.should_log_at("session-queue-full", start + Duration::from_secs(5)));
        assert!(!limiter.should_log_at("session-queue-full", start + Duration::from_secs(6)));
        assert!(limiter.should_log_at("session-queue-full", start + Duration::from_secs(10)));
    }

    #[test]
    fn concurrent_callers_log_a_site_once_per_interval() {
        let limiter = LogRateLimiter::new(Duration::from_secs(3_600));
        let logged = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        if limiter.should_log("backend-send-error") {
                            logged.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(logged.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn every_log_site_in_the_tree_is_registered() {
        let sources = [
            include_str!("relay/listener.rs"),
            include_str!("relay/session.rs"),
            include_str!("dns.rs"),
        ]
        .concat();
        let marker = "should_log(\"";
        for (offset, _) in sources.match_indices(marker) {
            let rest = &sources[offset + marker.len()..];
            let site = &rest[..rest.find('"').unwrap()];
            assert!(LOG_SITES.contains(&site), "unregistered log site {site:?}");
        }
        // Every registered site is used, including the admission-rejection
        // sites chosen by a `match` before the call.
        for site in LOG_SITES {
            assert!(
                sources.contains(&format!("\"{site}\"")),
                "stale log site {site:?}"
            );
        }
    }

    #[test]
    fn token_bucket_refills() {
        let controller = AdmissionController::new(10, 10, 1);
        let ip = IpAddr::from([198, 51, 100, 1]);
        let start = Instant::now();

        let first = controller.try_acquire_at(ip, start).unwrap();
        assert_eq!(
            controller
                .try_acquire_at(IpAddr::from([198, 51, 100, 2]), start)
                .unwrap_err(),
            AdmissionRejection::RateLimited
        );
        let second = controller
            .try_acquire_at(
                IpAddr::from([198, 51, 100, 2]),
                start + Duration::from_secs(1),
            )
            .unwrap();
        drop((first, second));
    }
}
