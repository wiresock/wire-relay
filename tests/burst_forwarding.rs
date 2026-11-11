// SPDX-License-Identifier: AGPL-3.0-or-later

//! Bursty bidirectional forwarding regression test.
//!
//! WireGuard and AmneziaWG senders transmit encrypted datagrams in bursts. A
//! relay that drops client-to-backend bursts while forwarding the reverse
//! direction intact collapses tunneled upload throughput. This test pushes the
//! same bursts through both directions on loopback and requires both to arrive
//! essentially intact, with no relay-side queue drops.

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use tokio::{
    net::UdpSocket,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use wire_relay::{
    config::{BackendEndpoint, Config, ListenerConfig, NormalizedConfig, ServiceConfig},
    runtime::{Runtime, RuntimeOptions},
};

const DATAGRAM_SIZE: usize = 1_200;
/// Four times the eight-slot queue that previously dropped upload bursts, but
/// small enough (about 38 KB) to fit default UDP receive buffers on the Linux
/// and Windows CI hosts.
const BURST: usize = 32;
const BURSTS: usize = 32;
const BURST_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Allowed fraction of loss caused by the host rather than the relay.
const MAX_LOSS_PERCENT: usize = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_bursts_are_forwarded_like_download_bursts() -> Result<()> {
    let backend = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .context("bind backend")?;
    let backend_addr = backend.local_addr()?;
    let (runtime, listener_addr) = start_on_free_port(backend_addr).await?;

    let result = exercise(&runtime, backend, listener_addr).await;
    let shutdown = timeout(IO_TIMEOUT, runtime.shutdown())
        .await
        .context("timed out shutting down runtime")?
        .context("runtime shutdown failed");
    result.and(shutdown)
}

/// Listener binds must name a concrete port, so pick one the OS reports as
/// free and retry if another process claims it before the relay binds.
async fn start_on_free_port(backend: SocketAddr) -> Result<(Runtime, SocketAddr)> {
    const ATTEMPTS: usize = 10;
    let mut last_error = None;
    for _ in 0..ATTEMPTS {
        let candidate =
            std::net::UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?.local_addr()?;
        match start(config_for(candidate, backend)?).await {
            Ok(runtime) => return Ok((runtime, candidate)),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.expect("at least one attempt was made")).context(format!(
        "failed to start the relay after {ATTEMPTS} port attempts"
    ))
}

fn config_for(listener: SocketAddr, backend: SocketAddr) -> Result<NormalizedConfig> {
    Config {
        service: ServiceConfig::default(),
        metrics: None,
        listeners: vec![ListenerConfig {
            name: "burst".to_owned(),
            bind: listener,
            backend: BackendEndpoint::parse(&backend.to_string()).context("parse backend")?,
        }],
    }
    .into_normalized()
    .context("valid test configuration")
}

async fn start(config: NormalizedConfig) -> Result<Runtime> {
    Runtime::start_with_options(
        config,
        PathBuf::from("burst-forwarding-test.toml"),
        RuntimeOptions {
            control: false,
            metrics: false,
        },
    )
    .await
    .context("failed to start test runtime")
}

async fn exercise(runtime: &Runtime, backend: UdpSocket, listener: SocketAddr) -> Result<()> {
    let client = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    client.connect(listener).await?;

    // Create the session and learn the relay's upstream address.
    client.send(b"hello").await?;
    let mut buffer = vec![0_u8; 2048];
    let (_, upstream) = timeout(IO_TIMEOUT, backend.recv_from(&mut buffer))
        .await
        .context("timed out waiting for the first datagram")??;
    backend.connect(upstream).await?;

    let client = Arc::new(client);
    let backend = Arc::new(backend);
    let payload = vec![0x5a_u8; DATAGRAM_SIZE];

    let upload = run_bursts(Arc::clone(&client), Arc::clone(&backend), &payload).await?;
    let stats = runtime.status().stats;
    ensure!(
        stats.drops().queue_full == 0,
        "relay dropped {} upload datagrams because a session queue was full",
        stats.drops().queue_full
    );
    let download = run_bursts(Arc::clone(&backend), Arc::clone(&client), &payload).await?;

    let sent = BURST * BURSTS;
    let minimum = sent - sent * MAX_LOSS_PERCENT / 100;
    ensure!(
        upload >= minimum,
        "upload delivered {upload} of {sent} burst datagrams (download delivered {download})"
    );
    ensure!(
        download >= minimum,
        "download delivered {download} of {sent} burst datagrams (upload delivered {upload})"
    );
    let stats = runtime.status().stats;
    // Only relay queueing drops fail the test; a transient host send error
    // is not what this test guards against.
    ensure!(
        stats.drops().queue_full == 0 && stats.drops().budget_exhausted == 0,
        "relay dropped queued datagrams during bursts: {:?}",
        stats.datagrams_dropped
    );
    Ok(())
}

/// Sends `BURSTS` back-to-back bursts and returns how many datagrams arrived.
async fn run_bursts(
    sender: Arc<UdpSocket>,
    receiver: Arc<UdpSocket>,
    payload: &[u8],
) -> Result<usize> {
    let received = Arc::new(AtomicUsize::new(0));
    let counter: JoinHandle<()> = {
        let received = Arc::clone(&received);
        tokio::spawn(async move {
            let mut buffer = vec![0_u8; 2048];
            while let Ok(Ok(_)) = timeout(IO_TIMEOUT, receiver.recv(&mut buffer)).await {
                received.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    for burst in 0..BURSTS {
        for _ in 0..BURST {
            let sent = sender.send(payload).await.context("burst send")?;
            ensure!(sent == payload.len(), "short burst send");
        }
        // Let the burst drain so only intra-burst behavior is measured.
        let expected = (burst + 1) * BURST;
        let deadline = Instant::now() + BURST_DRAIN_TIMEOUT;
        while received.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
            sleep(Duration::from_millis(1)).await;
        }
    }
    let total = received.load(Ordering::Relaxed);
    counter.abort();
    let _ = counter.await;
    Ok(total)
}
