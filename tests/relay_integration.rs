// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
    collections::HashSet,
    io,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use wire_relay::{
    config::{BackendEndpoint, Config, ListenerConfig, NormalizedConfig, ServiceConfig},
    runtime::{Runtime, RuntimeOptions},
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const QUIET_WINDOW: Duration = Duration::from_millis(250);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const CLIENT_BUFFER_SIZE: usize = 65_536;

#[derive(Debug)]
struct ObservedDatagram {
    payload: Vec<u8>,
    source: SocketAddr,
}

struct EchoBackend {
    address: SocketAddr,
    received: mpsc::Receiver<ObservedDatagram>,
    cancel: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl EchoBackend {
    async fn start() -> Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .context("failed to bind echo backend")?;
        let address = socket
            .local_addr()
            .context("failed to read echo backend address")?;
        ensure!(address.port() != 0, "the OS returned a zero backend port");

        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (sender, received) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut buffer = vec![0_u8; CLIENT_BUFFER_SIZE];
            loop {
                let received = tokio::select! {
                    biased;
                    () = task_cancel.cancelled() => return Ok(()),
                    received = socket.recv_from(&mut buffer) => received,
                };
                let (size, source) = received?;
                let payload = buffer[..size].to_vec();
                let _ = sender
                    .send(ObservedDatagram {
                        payload: payload.clone(),
                        source,
                    })
                    .await;
                let sent = socket.send_to(&payload, source).await?;
                if sent != payload.len() {
                    return Err(io::Error::other(format!(
                        "echo backend sent {sent} of {} bytes",
                        payload.len()
                    )));
                }
            }
        });

        Ok(Self {
            address,
            received,
            cancel,
            task,
        })
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    async fn next_datagram(&mut self) -> Result<ObservedDatagram> {
        timeout(IO_TIMEOUT, self.received.recv())
            .await
            .context("timed out waiting for the echo backend")?
            .context("echo backend observation channel closed")
    }

    async fn ensure_quiet(&mut self) -> Result<()> {
        match timeout(QUIET_WINDOW, self.received.recv()).await {
            Err(_) => Ok(()),
            Ok(Some(datagram)) => bail!(
                "echo backend unexpectedly received {} bytes from {}",
                datagram.payload.len(),
                datagram.source
            ),
            Ok(None) => bail!("echo backend observation channel closed"),
        }
    }

    async fn shutdown(self) -> Result<()> {
        self.cancel.cancel();
        let mut task = self.task;
        match timeout(IO_TIMEOUT, &mut task).await {
            Ok(joined) => joined
                .context("echo backend task panicked")?
                .context("echo backend failed"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                bail!("timed out joining echo backend")
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserves_opaque_datagrams_boundaries_and_independent_mappings() -> Result<()> {
    let backend = EchoBackend::start().await?;
    let listener = match reserve_listener_addresses(1) {
        Ok(addresses) => addresses
            .into_iter()
            .next()
            .expect("one listener reservation must include one address"),
        Err(error) => {
            backend.shutdown().await?;
            return Err(error);
        }
    };
    let backend_address = backend.address();
    let config = make_config(
        ServiceConfig::default(),
        vec![make_listener("opaque", listener, backend_address)],
    );
    let (runtime, mut backends) = start_environment(config, vec![backend]).await?;

    let exercise = async {
        let first_client = connected_client(listener).await?;
        let second_client = connected_client(listener).await?;
        let first_client_address = first_client.local_addr()?;
        let second_client_address = second_client.local_addr()?;

        let first_payload = vec![
            0x00, 0xff, 0x7e, 0x00, 0x04, 0x00, 0x13, 0x37, 0xaa, 0x55, 0x00,
        ];
        let second_payload = vec![0x01, 0x00, 0x00, 0x00, 0xf1, 0xe2, 0xd3, 0xc4, 0x00, 0xff];

        send_datagram(&first_client, &first_payload).await?;
        ensure!(
            receive_datagram(&first_client).await? == first_payload,
            "first opaque payload changed in transit"
        );
        let first_observed = backend_mut(&mut backends, 0)?.next_datagram().await?;
        ensure!(
            first_observed.payload == first_payload,
            "backend did not receive the first payload byte-for-byte"
        );

        send_datagram(&second_client, &second_payload).await?;
        ensure!(
            receive_datagram(&second_client).await? == second_payload,
            "second opaque payload changed in transit"
        );
        let second_observed = backend_mut(&mut backends, 0)?.next_datagram().await?;
        ensure!(
            second_observed.payload == second_payload,
            "backend did not receive the second payload byte-for-byte"
        );
        ensure!(
            first_observed.source != second_observed.source,
            "two clients shared one upstream UDP socket"
        );

        let boundary_payloads = vec![Vec::new(), vec![0x11, 0x22, 0x00, 0x33], vec![0x44; 257]];
        for payload in &boundary_payloads {
            send_datagram(&first_client, payload).await?;
        }

        let mut responses = Vec::with_capacity(boundary_payloads.len());
        let mut observed = Vec::with_capacity(boundary_payloads.len());
        for _ in 0..boundary_payloads.len() {
            responses.push(receive_datagram(&first_client).await?);
            let datagram = backend_mut(&mut backends, 0)?.next_datagram().await?;
            ensure!(
                datagram.source == first_observed.source,
                "one client mapping changed upstream sockets"
            );
            observed.push(datagram.payload);
        }

        let mut expected = boundary_payloads;
        expected.sort();
        responses.sort();
        observed.sort();
        ensure!(
            responses == expected,
            "response datagram boundaries or bytes changed"
        );
        ensure!(
            observed == expected,
            "backend datagram boundaries or bytes changed"
        );

        wait_for_session_count(&runtime, 2).await?;
        let sessions = runtime.sessions();
        let clients: HashSet<_> = sessions.iter().map(|session| session.client_addr).collect();
        ensure!(
            clients == HashSet::from([first_client_address, second_client_address]),
            "runtime session keys do not match both client endpoints"
        );
        let upstreams: HashSet<_> = sessions
            .iter()
            .map(|session| session.upstream_local_addr)
            .collect();
        ensure!(
            upstreams.len() == 2,
            "two clients did not receive independent upstream mappings"
        );
        ensure!(
            sessions
                .iter()
                .all(|session| session.backend_addr == backend_address),
            "a session selected an unexpected backend"
        );

        Ok(())
    }
    .await;

    let cleanup = shutdown_environment(runtime, backends).await;
    combine_results(exercise, cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_listeners_route_to_distinct_backends() -> Result<()> {
    let first_backend = EchoBackend::start().await?;
    let second_backend = match EchoBackend::start().await {
        Ok(backend) => backend,
        Err(error) => {
            first_backend.shutdown().await?;
            return Err(error);
        }
    };
    let mut addresses = match reserve_listener_addresses(2) {
        Ok(addresses) => addresses.into_iter(),
        Err(error) => {
            shutdown_backends(vec![first_backend, second_backend]).await?;
            return Err(error);
        }
    };
    let first_listener = addresses
        .next()
        .expect("two listener reservations must include the first address");
    let second_listener = addresses
        .next()
        .expect("two listener reservations must include the second address");
    let first_backend_address = first_backend.address();
    let second_backend_address = second_backend.address();
    let config = make_config(
        ServiceConfig::default(),
        vec![
            make_listener("first", first_listener, first_backend_address),
            make_listener("second", second_listener, second_backend_address),
        ],
    );
    let (runtime, mut backends) =
        start_environment(config, vec![first_backend, second_backend]).await?;

    let exercise = async {
        let first_client = connected_client(first_listener).await?;
        let second_client = connected_client(second_listener).await?;
        let first_payload = b"only-backend-one".to_vec();
        let second_payload = b"only-backend-two".to_vec();

        send_datagram(&first_client, &first_payload).await?;
        ensure!(
            receive_datagram(&first_client).await? == first_payload,
            "first listener response changed"
        );
        let observed_first = backend_mut(&mut backends, 0)?.next_datagram().await?;
        ensure!(
            observed_first.payload == first_payload,
            "first listener routed to the wrong backend"
        );

        send_datagram(&second_client, &second_payload).await?;
        ensure!(
            receive_datagram(&second_client).await? == second_payload,
            "second listener response changed"
        );
        let observed_second = backend_mut(&mut backends, 1)?.next_datagram().await?;
        ensure!(
            observed_second.payload == second_payload,
            "second listener routed to the wrong backend"
        );

        wait_for_session_count(&runtime, 2).await?;
        let sessions = runtime.sessions();
        ensure!(
            sessions.iter().any(|session| session.listener == "first"
                && session.backend_addr == first_backend_address),
            "first listener session has the wrong backend"
        );
        ensure!(
            sessions.iter().any(|session| session.listener == "second"
                && session.backend_addr == second_backend_address),
            "second listener session has the wrong backend"
        );

        Ok(())
    }
    .await;

    let cleanup = shutdown_environment(runtime, backends).await;
    combine_results(exercise, cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_datagrams_are_dropped_without_truncated_forwarding() -> Result<()> {
    let backend = EchoBackend::start().await?;
    let listener = match reserve_listener_addresses(1) {
        Ok(addresses) => addresses
            .into_iter()
            .next()
            .expect("one listener reservation must include one address"),
        Err(error) => {
            backend.shutdown().await?;
            return Err(error);
        }
    };
    let service = ServiceConfig {
        max_datagram_size: 64,
        ..ServiceConfig::default()
    };
    let config = make_config(
        service,
        vec![make_listener("size-limit", listener, backend.address())],
    );
    let (runtime, mut backends) = start_environment(config, vec![backend]).await?;

    let exercise = async {
        let client = connected_client(listener).await?;
        send_datagram(&client, &[0xa5; 65]).await?;
        wait_until("oversized datagram drop", || {
            runtime.status().stats.datagrams_dropped_total >= 1
        })
        .await?;
        backend_mut(&mut backends, 0)?.ensure_quiet().await?;
        ensure!(
            runtime.sessions().is_empty(),
            "an oversized first datagram created a session"
        );

        let valid = vec![0x5a; 64];
        send_datagram(&client, &valid).await?;
        ensure!(
            receive_datagram(&client).await? == valid,
            "a maximum-sized valid datagram changed in transit"
        );
        let observed = backend_mut(&mut backends, 0)?.next_datagram().await?;
        ensure!(
            observed.payload == valid,
            "backend received a truncated oversized datagram"
        );
        wait_for_session_count(&runtime, 1).await?;
        ensure!(
            runtime.status().stats.sessions_created_total == 1,
            "oversized datagram affected session creation accounting"
        );

        Ok(())
    }
    .await;

    let cleanup = shutdown_environment(runtime, backends).await;
    combine_results(exercise, cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_session_limit_rejects_a_second_mapping() -> Result<()> {
    run_limit_scenario(1, 1, "global session limit").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_session_limit_rejects_a_second_mapping() -> Result<()> {
    run_limit_scenario(2, 1, "per-IP session limit").await
}

async fn run_limit_scenario(
    max_sessions: usize,
    max_sessions_per_ip: usize,
    description: &str,
) -> Result<()> {
    let backend = EchoBackend::start().await?;
    let listener = match reserve_listener_addresses(1) {
        Ok(addresses) => addresses
            .into_iter()
            .next()
            .expect("one listener reservation must include one address"),
        Err(error) => {
            backend.shutdown().await?;
            return Err(error);
        }
    };
    let service = ServiceConfig {
        max_sessions,
        max_sessions_per_ip,
        new_sessions_per_second: 1_000,
        idle_timeout: Duration::from_secs(5),
        ..ServiceConfig::default()
    };
    let config = make_config(
        service,
        vec![make_listener("limited", listener, backend.address())],
    );
    let (runtime, mut backends) = start_environment(config, vec![backend]).await?;

    let exercise = async {
        let first_client = connected_client(listener).await?;
        let second_client = connected_client(listener).await?;
        ensure!(
            first_client.local_addr()?.ip() == second_client.local_addr()?.ip(),
            "test clients must share a source IP"
        );

        let accepted = b"accepted-session".to_vec();
        send_datagram(&first_client, &accepted).await?;
        ensure!(
            receive_datagram(&first_client).await? == accepted,
            "first mapping was not admitted"
        );
        ensure!(
            backend_mut(&mut backends, 0)?
                .next_datagram()
                .await?
                .payload
                == accepted,
            "backend did not receive the admitted datagram"
        );
        wait_for_session_count(&runtime, 1).await?;

        send_datagram(&second_client, b"must-be-rejected").await?;
        wait_until(description, || {
            runtime.status().stats.sessions_rejected_total >= 1
        })
        .await?;
        ensure_client_quiet(&second_client).await?;
        backend_mut(&mut backends, 0)?.ensure_quiet().await?;
        ensure!(
            runtime.sessions().len() == 1,
            "{description} allowed an extra session"
        );
        let stats = runtime.status().stats;
        ensure!(
            stats.sessions_created_total == 1,
            "{description} created a rejected session"
        );
        ensure!(
            stats.datagrams_dropped_total >= 1,
            "{description} did not count the dropped datagram"
        );

        Ok(())
    }
    .await;

    let cleanup = shutdown_environment(runtime, backends).await;
    combine_results(exercise, cleanup)
}

fn make_config(service: ServiceConfig, listeners: Vec<ListenerConfig>) -> NormalizedConfig {
    Config {
        service,
        metrics: None,
        listeners,
    }
    .into_normalized()
    .expect("test configuration assembled from validated OS addresses must be valid")
}

fn make_listener(name: &str, bind: SocketAddr, backend: SocketAddr) -> ListenerConfig {
    ListenerConfig {
        name: name.to_owned(),
        bind,
        backend: BackendEndpoint::parse(&backend.to_string())
            .expect("OS-provided numeric backend address must parse"),
    }
}

fn reserve_listener_addresses(count: usize) -> Result<Vec<SocketAddr>> {
    let mut sockets = Vec::with_capacity(count);
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        let socket = std::net::UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .context("failed to reserve a listener port")?;
        let address = socket
            .local_addr()
            .context("failed to read reserved listener address")?;
        ensure!(address.port() != 0, "the OS returned a zero listener port");
        addresses.push(address);
        sockets.push(socket);
    }
    drop(sockets);
    Ok(addresses)
}

async fn start_environment(
    config: NormalizedConfig,
    backends: Vec<EchoBackend>,
) -> Result<(Runtime, Vec<EchoBackend>)> {
    match Runtime::start_with_options(
        config,
        PathBuf::from("relay-integration-test.toml"),
        RuntimeOptions {
            control: false,
            metrics: false,
        },
    )
    .await
    {
        Ok(runtime) => Ok((runtime, backends)),
        Err(error) => {
            let cleanup = shutdown_backends(backends).await;
            cleanup?;
            Err(error).context("failed to start test runtime")
        }
    }
}

async fn shutdown_environment(runtime: Runtime, backends: Vec<EchoBackend>) -> Result<()> {
    let mut first_error = match timeout(IO_TIMEOUT, runtime.shutdown()).await {
        Err(_) => Some(anyhow::anyhow!("timed out shutting down runtime")),
        Ok(Err(error)) => Some(anyhow::Error::new(error).context("runtime shutdown failed")),
        Ok(Ok(())) => None,
    };

    if let Err(error) = shutdown_backends(backends).await {
        if first_error.is_none() {
            first_error = Some(error);
        }
    }

    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

async fn shutdown_backends(backends: Vec<EchoBackend>) -> Result<()> {
    let mut first_error = None;
    for backend in backends {
        if let Err(error) = backend.shutdown().await {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

fn combine_results(exercise: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (exercise, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => {
            Err(error.context(format!("cleanup also failed: {cleanup_error:#}")))
        }
    }
}

fn backend_mut(backends: &mut [EchoBackend], index: usize) -> Result<&mut EchoBackend> {
    backends
        .get_mut(index)
        .with_context(|| format!("missing echo backend at index {index}"))
}

async fn connected_client(listener: SocketAddr) -> Result<UdpSocket> {
    let client = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .context("failed to bind UDP client")?;
    client
        .connect(listener)
        .await
        .with_context(|| format!("failed to connect UDP client to {listener}"))?;
    Ok(client)
}

async fn send_datagram(client: &UdpSocket, payload: &[u8]) -> Result<()> {
    let sent = client
        .send(payload)
        .await
        .context("UDP client send failed")?;
    ensure!(
        sent == payload.len(),
        "UDP client sent {sent} of {} bytes",
        payload.len()
    );
    Ok(())
}

async fn receive_datagram(client: &UdpSocket) -> Result<Vec<u8>> {
    let mut buffer = vec![0_u8; CLIENT_BUFFER_SIZE];
    let size = timeout(IO_TIMEOUT, client.recv(&mut buffer))
        .await
        .context("timed out waiting for relayed response")?
        .context("UDP client receive failed")?;
    buffer.truncate(size);
    Ok(buffer)
}

async fn ensure_client_quiet(client: &UdpSocket) -> Result<()> {
    let mut buffer = vec![0_u8; CLIENT_BUFFER_SIZE];
    match timeout(QUIET_WINDOW, client.recv(&mut buffer)).await {
        Err(_) => Ok(()),
        Ok(Ok(size)) => bail!("rejected client unexpectedly received {size} bytes"),
        Ok(Err(error)) => Err(error).context("rejected client receive failed"),
    }
}

async fn wait_for_session_count(runtime: &Runtime, expected: usize) -> Result<()> {
    wait_until(
        &format!("runtime to report {expected} active sessions"),
        || runtime.sessions().len() == expected,
    )
    .await
}

async fn wait_until(description: &str, mut predicate: impl FnMut() -> bool) -> Result<()> {
    timeout(IO_TIMEOUT, async {
        loop {
            if predicate() {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {description}"))
}
