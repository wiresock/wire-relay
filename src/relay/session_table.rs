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
    time::Duration,
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{SessionHandle, SessionSnapshot, session::Expiry};

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
    #[cfg(test)]
    lookup_hook: LookupHook,
}

/// Runs once right after the next successful [`SessionTable::get_by_key`],
/// so a test can act between a listener's lookup and its forward.
#[cfg(test)]
type LookupHookFn = Box<dyn FnOnce(&SessionTable, &Arc<SessionHandle>) + Send>;

#[cfg(test)]
#[derive(Default)]
struct LookupHook(std::sync::Mutex<Option<LookupHookFn>>);

#[cfg(test)]
impl fmt::Debug for LookupHook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LookupHook")
    }
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
        // The route is claimed first and held while the ID entry is added,
        // so a session that loses the key is never visible by ID, not even
        // briefly, and a close found through the ID waits for the route.
        match self.by_key.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => Err(session),
            dashmap::mapref::entry::Entry::Vacant(route) => {
                match self.by_id.entry(id) {
                    dashmap::mapref::entry::Entry::Occupied(_) => return Err(session),
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        entry.insert(Arc::clone(&session));
                    }
                }
                route.insert(session);
                self.count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn get_by_key(&self, key: &SessionKey) -> Option<Arc<SessionHandle>> {
        let found = self.by_key.get(key).map(|entry| Arc::clone(entry.value()));
        #[cfg(test)]
        if let Some(session) = &found {
            let hook = self
                .lookup_hook
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(hook) = hook {
                hook(self, session);
            }
        }
        found
    }

    /// Runs `hook` once, right after the next lookup by key that finds a
    /// session (with no table lock held).
    #[cfg(test)]
    pub(crate) fn after_next_lookup(
        &self,
        hook: impl FnOnce(&Self, &Arc<SessionHandle>) + Send + 'static,
    ) {
        *self
            .lookup_hook
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
    }

    #[must_use]
    pub fn get_by_id(&self, id: &SessionId) -> Option<Arc<SessionHandle>> {
        self.by_id.get(id).map(|entry| Arc::clone(entry.value()))
    }

    /// Unpublishes this exact incarnation from routing and closes it.
    /// Returns whether this call closed it, so an already closed session that
    /// is still listed is not counted twice.
    ///
    /// The route, the client's per-IP admission slot, and the forwarding
    /// path go in one step under the route's shard lock. A listener
    /// therefore either still reaches the open session, or finds no route
    /// and the client's slot free (even with a per-IP limit of one); and an
    /// idle expiry, which takes the same lock, can never claim a session
    /// that is being closed. The ID entry and gauges stay until the session
    /// task's cleanup calls [`Self::remove`]; the queue-budget reservation and
    /// the global admission slot stay until the handle itself is dropped,
    /// after its upstream socket, so the global limit bounds the sockets that
    /// exist.
    pub fn close(&self, session: &SessionHandle) -> bool {
        let id = session.session().id;
        let mut closed = None;
        self.by_key
            .remove_if(&session.session().key(), |_, current| {
                let published = current.session().id == id;
                if published {
                    session.release_client_slot();
                    closed = Some(session.mark_closed());
                }
                published
            });
        // A session whose route was already gone or never published.
        let closed = closed.unwrap_or_else(|| {
            session.release_client_slot();
            session.mark_closed()
        });
        // Waking the task takes its own locks; keep that out of the route's.
        session.cancel_task();
        closed
    }

    /// Expires an idle session: if it is still idle, its route, the client's
    /// per-IP admission slot, and its forwarding path all go in one step
    /// under the route's shard lock, as in [`Self::close`].
    pub(super) fn expire(&self, session: &SessionHandle, idle_timeout: Duration) -> Expiry {
        let id = session.session().id;
        let mut outcome = None;
        self.by_key
            .remove_if(&session.session().key(), |_, current| {
                if current.session().id != id {
                    return false;
                }
                let expiry = session.try_expire(idle_timeout);
                outcome = Some(expiry);
                if expiry == Expiry::Expired {
                    session.release_client_slot();
                }
                expiry == Expiry::Expired
            });
        // A session without a route of its own was closed, which this then
        // reports, or was never published; decide on the handle alone.
        outcome.unwrap_or_else(|| session.try_expire(idle_timeout))
    }

    /// Removes an ended session from the table, closing it first if it is
    /// still open.
    ///
    /// A closed session has no route left (they go together), so the normal
    /// end of a session, which closes before it removes, takes only the ID
    /// index here and never the route lock a second time. IDs are unique per
    /// incarnation, so a newer session for the same client is never touched.
    pub fn remove(&self, session: &SessionHandle) -> Option<Arc<SessionHandle>> {
        if !session.is_closed() {
            self.close(session);
        }
        let (_, removed) = self.by_id.remove(&session.session().id)?;
        self.count.fetch_sub(1, Ordering::Relaxed);
        Some(removed)
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

    /// Closes every session owned by a listener and returns how many this
    /// call closed; sessions that were already closed and are only still
    /// listed while their tasks stop are not counted again.
    pub fn close_listener(&self, listener_id: ListenerId) -> usize {
        self.close_where(|session| session.session().listener_id == listener_id)
    }

    /// Closes every active session and returns how many this call closed.
    pub fn close_all(&self) -> usize {
        self.close_where(|_| true)
    }

    fn close_where(&self, mut selected: impl FnMut(&SessionHandle) -> bool) -> usize {
        // Collected first: closing takes route locks, which must not nest
        // inside the ID index's.
        let sessions: Vec<_> = self
            .by_id
            .iter()
            .filter(|entry| selected(entry.value()))
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        sessions
            .iter()
            .filter(|session| self.close(session))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::Session;

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

    #[tokio::test]
    async fn removal_requires_the_current_session_incarnation() {
        let table = SessionTable::new();
        let session = test_session_handle();
        let key = session.session().key();
        let id = session.session().id;
        // Another incarnation for the same client that is not in the table.
        let stranger = test_session_handle();
        assert_eq!(stranger.session().key(), key);
        assert_ne!(stranger.session().id, id);
        assert!(table.insert(Arc::clone(&session)).is_ok());

        assert!(table.remove(&stranger).is_none());
        assert_eq!(table.len(), 1);
        assert!(Arc::ptr_eq(
            &table.get_by_key(&key).expect("mapping must remain by key"),
            &session
        ));
        assert!(Arc::ptr_eq(
            &table.get_by_id(&id).expect("mapping must remain by ID"),
            &session
        ));

        // Removing a session that is still open closes and unroutes it.
        let removed = table
            .remove(&session)
            .expect("exact session must be removed");
        assert!(session.is_closed());
        assert!(Arc::ptr_eq(&removed, &session));
        assert!(table.get_by_key(&key).is_none());
        assert!(table.get_by_id(&id).is_none());
        assert!(table.is_empty());

        let replacement = test_session_handle();
        let replacement_id = replacement.session().id;
        assert_eq!(replacement.session().key(), key);
        assert_ne!(replacement_id, id);
        assert!(table.insert(Arc::clone(&replacement)).is_ok());

        assert!(table.remove(&session).is_none());
        assert_eq!(table.len(), 1);
        assert!(table.get_by_id(&id).is_none());
        assert!(Arc::ptr_eq(
            &table
                .get_by_key(&key)
                .expect("replacement must remain by key"),
            &replacement
        ));
        assert!(Arc::ptr_eq(
            &table
                .get_by_id(&replacement_id)
                .expect("replacement must remain by ID"),
            &replacement
        ));
    }

    #[tokio::test]
    async fn closing_unpublishes_the_route_but_keeps_the_session_until_removed() {
        let table = SessionTable::new();
        let old = test_session_handle();
        let key = old.session().key();
        let old_id = old.session().id;
        assert!(table.insert(Arc::clone(&old)).is_ok());

        table.close(&old);
        assert!(
            table.get_by_key(&key).is_none(),
            "closed route must be gone"
        );
        assert!(
            table.get_by_id(&old_id).is_some(),
            "draining session stays listed"
        );
        assert_eq!(table.len(), 1);

        // The same client can open a new session while the old one drains.
        let replacement = test_session_handle();
        assert_eq!(replacement.session().key(), key);
        assert!(table.insert(Arc::clone(&replacement)).is_ok());
        assert_eq!(table.len(), 2);

        // The old task's cleanup must not delete the newer route.
        assert!(table.remove(&old).is_some());
        assert_eq!(table.len(), 1);
        assert!(Arc::ptr_eq(
            &table
                .get_by_key(&key)
                .expect("replacement must stay routed"),
            &replacement
        ));
        assert!(table.get_by_id(&old_id).is_none());
    }

    #[tokio::test]
    async fn closing_a_listener_frees_its_routes_for_new_sessions() {
        let table = SessionTable::new();
        let old = test_session_handle();
        let key = old.session().key();
        assert!(table.insert(Arc::clone(&old)).is_ok());
        assert_eq!(table.close_listener(ListenerId::new(7)), 1);
        assert!(table.get_by_key(&key).is_none());
        assert!(table.insert(test_session_handle()).is_ok());
    }

    #[tokio::test]
    async fn a_session_that_loses_its_key_is_never_listed() {
        let table = SessionTable::new();
        let current = test_session_handle();
        assert!(table.insert(Arc::clone(&current)).is_ok());

        let loser = test_session_handle();
        let loser_id = loser.session().id;
        assert!(table.insert(loser).is_err());
        assert!(table.get_by_id(&loser_id).is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(table.close_all(), 1);
    }

    #[tokio::test]
    async fn a_closed_session_cannot_expire_afterwards() {
        let table = SessionTable::new();
        let session = test_session_handle();
        assert!(table.insert(Arc::clone(&session)).is_ok());

        // The route and the closed state go together, so an idle expiry that
        // finds no route reports the close instead of claiming the session.
        assert!(table.close(&session));
        assert_eq!(table.expire(&session, Duration::ZERO), Expiry::Closed);
        assert!(!table.close(&session));

        // And the other way round: an expired session is not closed again.
        let idle = test_session_handle();
        assert!(table.remove(&session).is_some());
        assert!(table.insert(Arc::clone(&idle)).is_ok());
        assert_eq!(table.expire(&idle, Duration::ZERO), Expiry::Expired);
        assert!(table.get_by_key(&idle.session().key()).is_none());
        assert!(!table.close(&idle));
    }

    #[tokio::test]
    async fn sessions_already_closed_are_not_counted_again() {
        let table = SessionTable::new();
        let old = test_session_handle();
        assert!(table.insert(Arc::clone(&old)).is_ok());
        assert!(table.close(&old));
        assert!(!table.close(&old));

        // The closed session is still listed until its task removes it, next
        // to the replacement for the same client.
        let replacement = test_session_handle();
        assert!(table.insert(Arc::clone(&replacement)).is_ok());
        assert_eq!(table.len(), 2);
        assert_eq!(table.close_listener(ListenerId::new(7)), 1);
        assert_eq!(table.close_listener(ListenerId::new(7)), 0);
        assert_eq!(table.close_all(), 0);
    }

    fn test_session_handle() -> Arc<SessionHandle> {
        SessionHandle::for_test(Session::new(
            ListenerId::new(7),
            "test".to_owned(),
            "192.0.2.10:40000".parse().unwrap(),
            "127.0.0.1:40001".parse().unwrap(),
            "192.0.2.20:51820".parse().unwrap(),
        ))
    }
}
