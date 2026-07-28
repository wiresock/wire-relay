// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
    fmt::Write as _,
    fs,
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    path::Path,
    time::Duration,
};

use tempfile::tempdir;
use tokio::{net::UdpSocket, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use wire_relay::{
    config::{Config, NormalizedConfig},
    relay::{ListenerId, SessionId, SessionSnapshot},
    runtime::{Runtime, RuntimeOptions},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Copy)]
struct ListenerSpec {
    name: &'static str,
    bind: SocketAddr,
    backend: SocketAddr,
}

struct TestBackend {
    address: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl TestBackend {
    async fn start(prefix: &'static [u8]) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test backend must bind");
        let address = socket
            .local_addr()
            .expect("test backend must have an address");
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let mut buffer = [0_u8; 4096];
            loop {
                let received = tokio::select! {
                    biased;
                    () = task_cancel.cancelled() => break,
                    received = socket.recv_from(&mut buffer) => received,
                };
                let (size, peer) = received.expect("test backend receive must succeed");
                let mut response = Vec::with_capacity(prefix.len().saturating_add(size));
                response.extend_from_slice(prefix);
                response.extend_from_slice(&buffer[..size]);
                let sent = socket
                    .send_to(&response, peer)
                    .await
                    .expect("test backend response must send");
                assert_eq!(sent, response.len());
            }
        });
        Self {
            address,
            cancel,
            task,
        }
    }

    async fn stop(self) {
        self.cancel.cancel();
        tokio::time::timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("test backend shutdown must not time out")
            .expect("test backend task must not panic");
    }
}

fn reserve_listener_address() -> (SocketAddr, StdUdpSocket) {
    let socket = StdUdpSocket::bind("127.0.0.1:0").expect("listener port must be reserved");
    let address = socket
        .local_addr()
        .expect("reserved listener must have an address");
    (address, socket)
}

fn write_valid_config(path: &Path, listeners: &[ListenerSpec]) -> NormalizedConfig {
    write_config_with_idle(path, listeners, "30s")
}

fn write_config_with_idle(
    path: &Path,
    listeners: &[ListenerSpec],
    idle_timeout: &str,
) -> NormalizedConfig {
    let mut text = format!(
        r#"[service]
idle_timeout = "{idle_timeout}"
max_datagram_size = 4096
max_sessions = 128
max_sessions_per_ip = 32
new_sessions_per_second = 1000
dns_refresh_interval = "30s"
shutdown_timeout = "3s"
"#
    );
    for listener in listeners {
        writeln!(
            text,
            r#"
[[listeners]]
name = "{}"
bind = "{}"
backend = "{}""#,
            listener.name, listener.bind, listener.backend
        )
        .expect("writing configuration text cannot fail");
    }
    fs::write(path, &text).expect("configuration file must be written");
    Config::parse_str(&text)
        .expect("test configuration must parse")
        .into_normalized()
        .expect("test configuration must normalize")
}

async fn start_runtime(config: NormalizedConfig, config_path: &Path) -> Runtime {
    Runtime::start_with_options(
        config,
        config_path,
        RuntimeOptions {
            control: false,
            metrics: false,
        },
    )
    .await
    .expect("runtime must start")
}

async fn round_trip(client: &UdpSocket, relay: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let sent = client
        .send_to(payload, relay)
        .await
        .expect("client datagram must send");
    assert_eq!(sent, payload.len());

    let mut response = [0_u8; 4096];
    let (size, source) = tokio::time::timeout(TEST_TIMEOUT, client.recv_from(&mut response))
        .await
        .expect("relay response must not time out")
        .expect("relay response must be received");
    assert_eq!(source, relay);
    response[..size].to_vec()
}

fn expected_response(prefix: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut expected = Vec::with_capacity(prefix.len().saturating_add(payload.len()));
    expected.extend_from_slice(prefix);
    expected.extend_from_slice(payload);
    expected
}

fn listener_id(runtime: &Runtime, name: &str) -> ListenerId {
    runtime
        .listeners()
        .into_iter()
        .find(|listener| listener.name == name)
        .unwrap_or_else(|| panic!("listener `{name}` must exist"))
        .id
}

fn only_session(runtime: &Runtime) -> SessionSnapshot {
    let sessions = runtime.sessions();
    assert_eq!(sessions.len(), 1, "exactly one session must be active");
    sessions
        .into_iter()
        .next()
        .expect("one session was asserted")
}

async fn wait_for_session_count(runtime: &Runtime, expected: usize) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if runtime.sessions().len() == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("session count did not become {expected}"));
}

#[tokio::test]
async fn reload_adds_listener_while_preserving_unchanged_listener_and_session() {
    let _serial = TEST_LOCK.lock().await;
    let temporary = tempdir().expect("temporary directory must be created");
    let config_path = temporary.path().join("config.toml");
    let backend_a = TestBackend::start(b"backend-a/").await;
    let backend_b = TestBackend::start(b"backend-b/").await;
    let (stable_bind, stable_reservation) = reserve_listener_address();
    let (added_bind, added_reservation) = reserve_listener_address();
    let stable = ListenerSpec {
        name: "stable",
        bind: stable_bind,
        backend: backend_a.address,
    };
    let added = ListenerSpec {
        name: "added",
        bind: added_bind,
        backend: backend_b.address,
    };
    let initial = write_valid_config(&config_path, &[stable]);
    drop(stable_reservation);
    let runtime = start_runtime(initial, &config_path).await;

    let stable_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("stable client must bind");
    let first_payload = b"create-stable-session";
    assert_eq!(
        round_trip(&stable_client, stable_bind, first_payload).await,
        expected_response(b"backend-a/", first_payload)
    );
    let stable_listener_id = listener_id(&runtime, "stable");
    let stable_session_id = only_session(&runtime).id;

    write_valid_config(&config_path, &[stable, added]);
    drop(added_reservation);
    let result = runtime.reload().await.expect("valid reload must apply");
    assert!(result.applied);
    assert_eq!(result.preserved, ["stable"]);
    assert_eq!(result.added, ["added"]);
    assert!(result.modified.is_empty());
    assert!(result.removed.is_empty());
    assert_eq!(result.sessions_closed, 0);
    assert_eq!(listener_id(&runtime, "stable"), stable_listener_id);
    assert!(
        runtime
            .sessions()
            .iter()
            .any(|session| session.id == stable_session_id),
        "unchanged listener session must be preserved"
    );

    let preserved_payload = b"preserved-session";
    assert_eq!(
        round_trip(&stable_client, stable_bind, preserved_payload).await,
        expected_response(b"backend-a/", preserved_payload)
    );

    let added_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("added-listener client must bind");
    let added_payload = b"new-listener";
    assert_eq!(
        round_trip(&added_client, added_bind, added_payload).await,
        expected_response(b"backend-b/", added_payload)
    );

    runtime.shutdown().await.expect("runtime must shut down");
    backend_a.stop().await;
    backend_b.stop().await;
}

#[tokio::test]
async fn backend_change_closes_old_session_and_routes_new_session_to_new_backend() {
    let _serial = TEST_LOCK.lock().await;
    let temporary = tempdir().expect("temporary directory must be created");
    let config_path = temporary.path().join("config.toml");
    let backend_a = TestBackend::start(b"backend-a/").await;
    let backend_b = TestBackend::start(b"backend-b/").await;
    let (relay_bind, relay_reservation) = reserve_listener_address();
    let initial_listener = ListenerSpec {
        name: "relay",
        bind: relay_bind,
        backend: backend_a.address,
    };
    let initial = write_valid_config(&config_path, &[initial_listener]);
    drop(relay_reservation);
    let runtime = start_runtime(initial, &config_path).await;
    let client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client must bind");

    let first_payload = b"old-backend";
    assert_eq!(
        round_trip(&client, relay_bind, first_payload).await,
        expected_response(b"backend-a/", first_payload)
    );
    let old_session_id = only_session(&runtime).id;
    let listener_before = listener_id(&runtime, "relay");

    let changed_listener = ListenerSpec {
        backend: backend_b.address,
        ..initial_listener
    };
    write_valid_config(&config_path, &[changed_listener]);
    let result = runtime.reload().await.expect("backend reload must apply");
    assert!(result.applied);
    assert_eq!(result.modified, ["relay"]);
    assert!(result.preserved.is_empty());
    assert_eq!(result.sessions_closed, 1);
    assert_eq!(listener_id(&runtime, "relay"), listener_before);
    wait_for_session_count(&runtime, 0).await;

    let new_payload = b"new-backend";
    assert_eq!(
        round_trip(&client, relay_bind, new_payload).await,
        expected_response(b"backend-b/", new_payload)
    );
    let new_session = only_session(&runtime);
    assert_ne!(new_session.id, old_session_id);
    assert_eq!(new_session.backend_addr, backend_b.address);

    runtime.shutdown().await.expect("runtime must shut down");
    backend_a.stop().await;
    backend_b.stop().await;
}

#[tokio::test]
async fn invalid_reload_preserves_active_config_listener_and_session() {
    let _serial = TEST_LOCK.lock().await;
    let temporary = tempdir().expect("temporary directory must be created");
    let config_path = temporary.path().join("config.toml");
    let backend = TestBackend::start(b"backend/").await;
    let (relay_bind, relay_reservation) = reserve_listener_address();
    let listener = ListenerSpec {
        name: "stable",
        bind: relay_bind,
        backend: backend.address,
    };
    let initial = write_valid_config(&config_path, &[listener]);
    drop(relay_reservation);
    let runtime = start_runtime(initial, &config_path).await;
    let client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client must bind");

    let first_payload = b"before-invalid-reload";
    assert_eq!(
        round_trip(&client, relay_bind, first_payload).await,
        expected_response(b"backend/", first_payload)
    );
    let active_before = runtime.active_config();
    let listener_before = listener_id(&runtime, "stable");
    let session_before: SessionId = only_session(&runtime).id;

    fs::write(&config_path, "[service\nthis is not valid TOML")
        .expect("invalid configuration must be written");
    let error = runtime
        .reload()
        .await
        .expect_err("invalid reload must be rejected");
    assert!(error.contains("malformed TOML"), "{error}");
    assert_eq!(runtime.active_config(), active_before);
    assert_eq!(listener_id(&runtime, "stable"), listener_before);
    assert!(
        runtime
            .sessions()
            .iter()
            .any(|session| session.id == session_before),
        "session must survive a rejected reload"
    );

    let after_payload = b"after-invalid-reload";
    assert_eq!(
        round_trip(&client, relay_bind, after_payload).await,
        expected_response(b"backend/", after_payload)
    );

    let changed_control = write_valid_config(&config_path, &[listener]);
    let changed_control_text = changed_control
        .to_toml()
        .expect("changed control configuration must serialize")
        .replace(
            "control_socket = \"/run/wire-relay/control.sock\"",
            "control_socket = \"/run/wire-relay/replacement.sock\"",
        );
    fs::write(&config_path, changed_control_text)
        .expect("changed control configuration must be written");
    let control_error = runtime
        .reload()
        .await
        .expect_err("control socket change must require restart");
    assert!(control_error.contains("requires a service restart"));
    assert_eq!(runtime.active_config(), active_before);

    let (first_shutdown, second_shutdown) = tokio::join!(runtime.shutdown(), runtime.shutdown());
    first_shutdown.expect("first shutdown waiter must succeed");
    second_shutdown.expect("second shutdown waiter must receive the shared result");
    backend.stop().await;
}

#[tokio::test]
async fn reload_idle_timeout_wakes_and_updates_existing_session_deadlines() {
    let _serial = TEST_LOCK.lock().await;
    let temporary = tempdir().expect("temporary directory must be created");
    let config_path = temporary.path().join("config.toml");
    let backend = TestBackend::start(b"backend/").await;
    let (relay_bind, relay_reservation) = reserve_listener_address();
    let listener = ListenerSpec {
        name: "idle-reload",
        bind: relay_bind,
        backend: backend.address,
    };
    let initial = write_config_with_idle(&config_path, &[listener], "1s");
    drop(relay_reservation);
    let runtime = start_runtime(initial, &config_path).await;
    let client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client must bind");

    let payload = b"idle-reload-session";
    assert_eq!(
        round_trip(&client, relay_bind, payload).await,
        expected_response(b"backend/", payload)
    );
    let session_id = only_session(&runtime).id;

    tokio::time::sleep(Duration::from_millis(100)).await;
    write_config_with_idle(&config_path, &[listener], "5s");
    runtime.reload().await.expect("longer timeout must apply");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        runtime
            .sessions()
            .iter()
            .any(|session| session.id == session_id),
        "raising the timeout must cancel the old shorter deadline"
    );

    write_config_with_idle(&config_path, &[listener], "100ms");
    runtime.reload().await.expect("shorter timeout must apply");
    wait_for_session_count(&runtime, 0).await;

    runtime.shutdown().await.expect("runtime must shut down");
    backend.stop().await;
}
