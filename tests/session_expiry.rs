// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use tokio::{net::UdpSocket, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use wire_relay::{
    config::{BackendEndpoint, Config, ListenerConfig, ServiceConfig},
    runtime::{Runtime, RuntimeOptions},
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_millis(300);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

struct EchoBackend {
    address: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl EchoBackend {
    async fn start() -> Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .context("failed to bind expiry-test backend")?;
        let address = socket.local_addr()?;
        ensure!(address.port() != 0, "the OS returned a zero backend port");
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let mut buffer = vec![0_u8; 65_536];
            loop {
                let received = tokio::select! {
                    biased;
                    () = task_cancel.cancelled() => return Ok(()),
                    received = socket.recv_from(&mut buffer) => received,
                };
                let (size, source) = received?;
                let sent = socket.send_to(&buffer[..size], source).await?;
                if sent != size {
                    return Err(io::Error::other(format!(
                        "echo backend sent {sent} of {size} bytes"
                    )));
                }
            }
        });
        Ok(Self {
            address,
            cancel,
            task,
        })
    }

    async fn shutdown(self) -> Result<()> {
        self.cancel.cancel();
        let mut task = self.task;
        match timeout(IO_TIMEOUT, &mut task).await {
            Ok(joined) => joined
                .context("expiry-test backend task panicked")?
                .context("expiry-test backend failed"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                bail!("timed out joining expiry-test backend")
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_session_expires_and_releases_its_mapping() -> Result<()> {
    let backend = EchoBackend::start().await?;
    let listener = match reserve_listener_address() {
        Ok(listener) => listener,
        Err(error) => {
            backend.shutdown().await?;
            return Err(error);
        }
    };
    let service = ServiceConfig {
        idle_timeout: IDLE_TIMEOUT,
        shutdown_timeout: Duration::from_secs(2),
        ..ServiceConfig::default()
    };
    let config = Config {
        service,
        metrics: None,
        listeners: vec![ListenerConfig {
            name: "expiry".to_owned(),
            bind: listener,
            backend: BackendEndpoint::parse(&backend.address.to_string())
                .expect("OS-provided numeric backend address must parse"),
        }],
    }
    .into_normalized()
    .expect("expiry test configuration assembled from OS addresses must be valid");

    let runtime = match Runtime::start_with_options(
        config,
        PathBuf::from("session-expiry-test.toml"),
        RuntimeOptions {
            control: false,
            metrics: false,
        },
    )
    .await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            backend.shutdown().await?;
            return Err(error).context("failed to start expiry-test runtime");
        }
    };

    let exercise = async {
        let client = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        client.connect(listener).await?;

        let first_payload = b"create-expiring-session";
        ensure!(
            client.send(first_payload).await? == first_payload.len(),
            "UDP client reported a partial first send"
        );
        let mut buffer = [0_u8; 128];
        let first_size = timeout(IO_TIMEOUT, client.recv(&mut buffer))
            .await
            .context("timed out waiting for first response")??;
        ensure!(
            &buffer[..first_size] == first_payload,
            "first response changed in transit"
        );

        wait_for_session_count(&runtime, 1).await?;
        let first_id = runtime
            .sessions()
            .into_iter()
            .next()
            .context("session disappeared before its ID was inspected")?
            .id;
        wait_for_session_count(&runtime, 0).await?;
        let expired_stats = runtime.status().stats;
        ensure!(
            expired_stats.active_sessions == 0,
            "active-session metric did not fall to zero"
        );
        ensure!(
            expired_stats.sessions_expired_total == 1,
            "idle expiration was not counted exactly once"
        );

        let second_payload = b"mapping-after-expiry";
        ensure!(
            client.send(second_payload).await? == second_payload.len(),
            "UDP client reported a partial second send"
        );
        let second_size = timeout(IO_TIMEOUT, client.recv(&mut buffer))
            .await
            .context("timed out waiting for response after expiry")??;
        ensure!(
            &buffer[..second_size] == second_payload,
            "response after expiry changed in transit"
        );
        wait_for_session_count(&runtime, 1).await?;
        let replacement = runtime
            .sessions()
            .into_iter()
            .next()
            .context("replacement session disappeared before inspection")?;
        ensure!(
            replacement.id != first_id,
            "expired mapping was reused instead of recreated"
        );
        ensure!(
            runtime.status().stats.sessions_created_total == 2,
            "replacement mapping was not counted"
        );

        Ok(())
    }
    .await;

    let runtime_shutdown = match timeout(IO_TIMEOUT, runtime.shutdown()).await {
        Err(_) => Err(anyhow::anyhow!(
            "timed out shutting down expiry-test runtime"
        )),
        Ok(Err(error)) => Err(anyhow::Error::new(error).context("runtime shutdown failed")),
        Ok(Ok(())) => Ok(()),
    };
    let backend_shutdown = backend.shutdown().await;

    combine_results(exercise, runtime_shutdown, backend_shutdown)
}

fn reserve_listener_address() -> Result<SocketAddr> {
    let reservation = std::net::UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .context("failed to reserve expiry-test listener")?;
    let address = reservation.local_addr()?;
    ensure!(address.port() != 0, "the OS returned a zero listener port");
    drop(reservation);
    Ok(address)
}

async fn wait_for_session_count(runtime: &Runtime, expected: usize) -> Result<()> {
    timeout(IO_TIMEOUT, async {
        loop {
            if runtime.sessions().len() == expected {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {expected} active sessions"))
}

fn combine_results(
    exercise: Result<()>,
    runtime_shutdown: Result<()>,
    backend_shutdown: Result<()>,
) -> Result<()> {
    let cleanup = runtime_shutdown.and(backend_shutdown);
    match (exercise, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup_error)) => {
            Err(error.context(format!("cleanup also failed: {cleanup_error:#}")))
        }
    }
}
