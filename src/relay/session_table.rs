// SPDX-License-Identifier: AGPL-3.0-or-later

//! Concurrent session lookup by routing key and stable public ID.

use std::{
    fmt,
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{SessionHandle, SessionSnapshot};

/// Stable identity of one listener incarnation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ListenerId(u64);

impl ListenerId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ListenerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable, non-guess-dependent public session identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SessionId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Exact routing key for a client mapping.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey {
    pub listener_id: ListenerId,
    pub client_addr: SocketAddr,
}

impl SessionKey {
    #[must_use]
    pub const fn new(listener_id: ListenerId, client_addr: SocketAddr) -> Self {
        Self {
            listener_id,
            client_addr,
        }
    }
}

/// Concurrent indexes for active session handles.
#[derive(Debug, Default)]
pub struct SessionTable {
    by_key: DashMap<SessionKey, Arc<SessionHandle>>,
    by_id: DashMap<SessionId, Arc<SessionHandle>>,
    count: AtomicU64,
}

impl SessionTable {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Inserts a session unless its routing key already exists.
    pub fn insert(&self, session: Arc<SessionHandle>) -> Result<(), Arc<SessionHandle>> {
        let key = session.session().key();
        let id = session.session().id;
        match self.by_id.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(_) => return Err(session),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&session));
            }
        }

        match self.by_key.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                self.by_id.remove(&id);
                Err(session)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&session));
                self.count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn get_by_key(&self, key: &SessionKey) -> Option<Arc<SessionHandle>> {
        self.by_key.get(key).map(|entry| Arc::clone(entry.value()))
    }

    #[must_use]
    pub fn get_by_id(&self, id: &SessionId) -> Option<Arc<SessionHandle>> {
        self.by_id.get(id).map(|entry| Arc::clone(entry.value()))
    }

    /// Removes only the exact session incarnation, protecting a newer mapping
    /// from a stale task's cleanup.
    pub fn remove(&self, key: &SessionKey, id: SessionId) -> Option<Arc<SessionHandle>> {
        let matches = self
            .by_key
            .get(key)
            .is_some_and(|entry| entry.session().id == id);
        if !matches {
            return None;
        }

        let removed = self.by_key.remove(key).map(|(_, session)| session);
        if removed.is_some() {
            self.by_id.remove(&id);
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    #[must_use]
    pub fn len(&self) -> usize {
        usize::try_from(self.count.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns snapshot models without retaining map guards.
    #[must_use]
    pub fn snapshots(&self) -> Vec<SessionSnapshot> {
        self.by_id
            .iter()
            .map(|entry| entry.session().snapshot())
            .collect()
    }

    /// Cancels every mapping owned by a listener and returns the number found.
    pub fn close_listener(&self, listener_id: ListenerId) -> usize {
        let sessions: Vec<_> = self
            .by_id
            .iter()
            .filter(|entry| entry.session().listener_id == listener_id)
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        for session in &sessions {
            session.close();
        }
        sessions.len()
    }

    /// Cancels every active session.
    pub fn close_all(&self) -> usize {
        let sessions: Vec<_> = self
            .by_id
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        for session in &sessions {
            session.close();
        }
        sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_includes_listener_and_source_port() {
        let client_a: SocketAddr = "192.0.2.10:40000".parse().unwrap();
        let client_b: SocketAddr = "192.0.2.10:40001".parse().unwrap();
        assert_ne!(
            SessionKey::new(ListenerId::new(1), client_a),
            SessionKey::new(ListenerId::new(1), client_b)
        );
        assert_ne!(
            SessionKey::new(ListenerId::new(1), client_a),
            SessionKey::new(ListenerId::new(2), client_a)
        );
    }

    #[test]
    fn session_ids_round_trip() {
        let id = SessionId::new();
        assert_eq!(id.to_string().parse::<SessionId>().unwrap(), id);
    }
}
