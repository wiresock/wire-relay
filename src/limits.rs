// SPDX-License-Identifier: AGPL-3.0-or-later

//! Session admission and repetitive-log rate limiting.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex, MutexGuard},
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

/// A small helper for suppressing repetitive log sites.
#[derive(Debug)]
pub struct LogRateLimiter {
    interval: Duration,
    sites: Mutex<HashMap<&'static str, Instant>>,
}

impl LogRateLimiter {
    /// Creates a limiter that allows each named site once per `interval`.
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            sites: Mutex::new(HashMap::new()),
        }
    }

    /// Returns true when the caller should emit this log event.
    pub fn should_log(&self, site: &'static str) -> bool {
        let now = Instant::now();
        let mut sites = lock_unpoisoned(&self.sites);
        match sites.get_mut(site) {
            Some(last) if now.saturating_duration_since(*last) < self.interval => false,
            Some(last) => {
                *last = now;
                true
            }
            None => {
                sites.insert(site, now);
                true
            }
        }
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
