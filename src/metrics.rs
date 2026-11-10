// SPDX-License-Identifier: AGPL-3.0-or-later

//! Lock-free internal counters and a bounded Prometheus text endpoint.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};

const MAX_HTTP_REQUEST: usize = 8 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_METRICS_CONNECTIONS: usize = 16;

/// Why a datagram was discarded instead of being forwarded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    /// The datagram exceeded `service.max_datagram_size`.
    Oversize,
    /// The session's client-to-backend queue had no free slot
    /// (`service.session_queue_depth`).
    QueueFull,
    /// The process-wide queued-datagram memory budget was exhausted.
    BudgetExhausted,
    /// The session was closing or had already stopped.
    SessionClosed,
    /// No session could be created: admission limits, an unresolved backend,
    /// upstream socket setup failure, or a concurrent route change.
    Rejected,
    /// The operating system refused or truncated a send in either direction.
    SendError,
}

/// Atomic per-reason drop counters.
#[derive(Debug, Default)]
pub struct DropCounters {
    oversize: AtomicU64,
    queue_full: AtomicU64,
    budget_exhausted: AtomicU64,
    session_closed: AtomicU64,
    rejected: AtomicU64,
    send_error: AtomicU64,
}

impl DropCounters {
    pub fn record(&self, reason: DropReason) {
        let counter = match reason {
            DropReason::Oversize => &self.oversize,
            DropReason::QueueFull => &self.queue_full,
            DropReason::BudgetExhausted => &self.budget_exhausted,
            DropReason::SessionClosed => &self.session_closed,
            DropReason::Rejected => &self.rejected,
            DropReason::SendError => &self.send_error,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> DropSnapshot {
        DropSnapshot {
            oversize: self.oversize.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
            budget_exhausted: self.budget_exhausted.load(Ordering::Relaxed),
            session_closed: self.session_closed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            send_error: self.send_error.load(Ordering::Relaxed),
        }
    }
}

/// Serializable per-reason drop counts.
///
/// Every field defaults to zero so clients and daemons from releases without
/// this breakdown remain wire-compatible.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct DropSnapshot {
    pub oversize: u64,
    pub queue_full: u64,
    pub budget_exhausted: u64,
    pub session_closed: u64,
    pub rejected: u64,
    pub send_error: u64,
}

impl DropSnapshot {
    /// Sum of all reasons; the reported total for in-process counters.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.oversize
            .saturating_add(self.queue_full)
            .saturating_add(self.budget_exhausted)
            .saturating_add(self.session_closed)
            .saturating_add(self.rejected)
            .saturating_add(self.send_error)
    }
}

/// A serializable snapshot used by the control API.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub active_sessions: u64,
    pub sessions_created_total: u64,
    pub sessions_expired_total: u64,
    pub sessions_closed_total: u64,
    pub sessions_rejected_total: u64,
    pub packets_to_backend_total: u64,
    pub packets_to_client_total: u64,
    pub bytes_to_backend_total: u64,
    pub bytes_to_client_total: u64,
    pub datagrams_dropped_total: u64,
    pub rate_limited_total: u64,
    pub dns_errors_total: u64,
    pub socket_errors_total: u64,
    /// Breakdown of `datagrams_dropped_total` by reason; `None` when the
    /// daemon predates the breakdown, which is "unknown", not zero.
    #[serde(default)]
    pub datagrams_dropped: Option<DropSnapshot>,
}

impl MetricsSnapshot {
    /// The per-reason breakdown, all zero when the daemon did not report one.
    #[must_use]
    pub fn drops(&self) -> DropSnapshot {
        self.datagrams_dropped.clone().unwrap_or_default()
    }
}

/// Internal application metrics.
#[derive(Debug, Default)]
pub struct Metrics {
    active_sessions: AtomicU64,
    sessions_created_total: AtomicU64,
    sessions_expired_total: AtomicU64,
    sessions_closed_total: AtomicU64,
    sessions_rejected_total: AtomicU64,
    packets_to_backend_total: AtomicU64,
    packets_to_client_total: AtomicU64,
    bytes_to_backend_total: AtomicU64,
    bytes_to_client_total: AtomicU64,
    rate_limited_total: AtomicU64,
    dns_errors_total: AtomicU64,
    socket_errors_total: AtomicU64,
    datagrams_dropped: DropCounters,
}

impl Metrics {
    pub fn session_created(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.sessions_created_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_ended(&self, expired: bool) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
        if expired {
            self.sessions_expired_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sessions_closed_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn session_rejected(&self, rate_limited: bool) {
        self.sessions_rejected_total.fetch_add(1, Ordering::Relaxed);
        if rate_limited {
            self.rate_limited_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn packet_to_backend(&self, bytes: usize) {
        self.packets_to_backend_total
            .fetch_add(1, Ordering::Relaxed);
        self.bytes_to_backend_total
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    pub fn packet_to_client(&self, bytes: usize) {
        self.packets_to_client_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_to_client_total
            .fetch_add(saturating_u64(bytes), Ordering::Relaxed);
    }

    pub fn datagram_dropped(&self, reason: DropReason) {
        self.datagrams_dropped.record(reason);
    }

    pub fn dns_error(&self) {
        self.dns_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn socket_error(&self) {
        self.socket_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let datagrams_dropped = self.datagrams_dropped.snapshot();
        MetricsSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            sessions_created_total: self.sessions_created_total.load(Ordering::Relaxed),
            sessions_expired_total: self.sessions_expired_total.load(Ordering::Relaxed),
            sessions_closed_total: self.sessions_closed_total.load(Ordering::Relaxed),
            sessions_rejected_total: self.sessions_rejected_total.load(Ordering::Relaxed),
            packets_to_backend_total: self.packets_to_backend_total.load(Ordering::Relaxed),
            packets_to_client_total: self.packets_to_client_total.load(Ordering::Relaxed),
            bytes_to_backend_total: self.bytes_to_backend_total.load(Ordering::Relaxed),
            bytes_to_client_total: self.bytes_to_client_total.load(Ordering::Relaxed),
            // Derived from the breakdown so the two can never disagree.
            datagrams_dropped_total: datagrams_dropped.total(),
            rate_limited_total: self.rate_limited_total.load(Ordering::Relaxed),
            dns_errors_total: self.dns_errors_total.load(Ordering::Relaxed),
            socket_errors_total: self.socket_errors_total.load(Ordering::Relaxed),
            datagrams_dropped: Some(datagrams_dropped),
        }
    }

    /// Renders the stable, label-free Prometheus exposition.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let values = self.snapshot();
        let drops = values.drops();
        let metrics = [
            ("wire_relay_active_sessions", values.active_sessions),
            (
                "wire_relay_sessions_created_total",
                values.sessions_created_total,
            ),
            (
                "wire_relay_sessions_expired_total",
                values.sessions_expired_total,
            ),
            (
                "wire_relay_sessions_closed_total",
                values.sessions_closed_total,
            ),
            (
                "wire_relay_sessions_rejected_total",
                values.sessions_rejected_total,
            ),
            (
                "wire_relay_packets_to_backend_total",
                values.packets_to_backend_total,
            ),
            (
                "wire_relay_packets_to_client_total",
                values.packets_to_client_total,
            ),
            (
                "wire_relay_bytes_to_backend_total",
                values.bytes_to_backend_total,
            ),
            (
                "wire_relay_bytes_to_client_total",
                values.bytes_to_client_total,
            ),
            (
                "wire_relay_datagrams_dropped_total",
                values.datagrams_dropped_total,
            ),
            (
                "wire_relay_datagrams_dropped_oversize_total",
                drops.oversize,
            ),
            (
                "wire_relay_datagrams_dropped_queue_full_total",
                drops.queue_full,
            ),
            (
                "wire_relay_datagrams_dropped_budget_exhausted_total",
                drops.budget_exhausted,
            ),
            (
                "wire_relay_datagrams_dropped_session_closed_total",
                drops.session_closed,
            ),
            (
                "wire_relay_datagrams_dropped_rejected_total",
                drops.rejected,
            ),
            (
                "wire_relay_datagrams_dropped_send_error_total",
                drops.send_error,
            ),
            ("wire_relay_rate_limited_total", values.rate_limited_total),
            ("wire_relay_dns_errors_total", values.dns_errors_total),
            ("wire_relay_socket_errors_total", values.socket_errors_total),
        ];

        let mut output = String::with_capacity(metrics.len() * 96);
        for (name, value) in metrics {
            output.push_str("# TYPE ");
            output.push_str(name);
            output.push_str(if name == "wire_relay_active_sessions" {
                " gauge\n"
            } else {
                " counter\n"
            });
            output.push_str(name);
            output.push(' ');
            output.push_str(&value.to_string());
            output.push('\n');
        }
        output
    }
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A metrics TCP socket that has passed reload preflight but is not accepting.
#[derive(Debug)]
pub struct PreparedMetricsServer {
    listener: TcpListener,
    bind: SocketAddr,
}

impl PreparedMetricsServer {
    pub async fn bind(bind: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(bind).await?;
        let bind = listener.local_addr()?;
        Ok(Self { listener, bind })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.bind
    }

    pub fn activate(
        self,
        metrics: Arc<Metrics>,
        parent_cancel: &CancellationToken,
        tracker: &TaskTracker,
    ) -> MetricsServerHandle {
        let cancel = parent_cancel.child_token();
        let task_cancel = cancel.clone();
        let semaphore = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));
        let connection_tracker = tracker.clone();

        tracker.spawn(async move {
            let mut last_accept_warning = None;
            loop {
                tokio::select! {
                    () = task_cancel.cancelled() => break,
                    accepted = self.listener.accept() => {
                        match accepted {
                            Ok((stream, peer)) => {
                                let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                                    debug!(%peer, "dropping excess metrics connection");
                                    continue;
                                };
                                let connection_metrics = Arc::clone(&metrics);
                                connection_tracker.spawn(async move {
                                    let _permit = permit;
                                    if let Err(error) = serve_metrics_connection(
                                        stream,
                                        connection_metrics,
                                    ).await {
                                        debug!(%peer, %error, "metrics connection failed");
                                    }
                                });
                            }
                            Err(error) => {
                                let now = Instant::now();
                                if last_accept_warning.is_none_or(|last| {
                                    now.saturating_duration_since(last) >= Duration::from_secs(5)
                                }) {
                                    warn!(%error, "metrics accept failed");
                                    last_accept_warning = Some(now);
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            }
        });

        MetricsServerHandle {
            bind: self.bind,
            cancel,
        }
    }
}

/// Handle used to stop a running metrics server.
#[derive(Debug)]
pub struct MetricsServerHandle {
    bind: SocketAddr,
    cancel: CancellationToken,
}

impl MetricsServerHandle {
    #[must_use]
    pub fn bind(&self) -> SocketAddr {
        self.bind
    }

    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

async fn serve_metrics_connection(mut stream: TcpStream, metrics: Arc<Metrics>) -> io::Result<()> {
    let request = tokio::time::timeout(HTTP_TIMEOUT, read_http_request(&mut stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "metrics request timed out"))??;

    let is_metrics = request.starts_with(b"GET /metrics ")
        || request.starts_with(b"GET /metrics?")
        || request.starts_with(b"GET / ");
    let (status, content_type, body) = if is_metrics {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.render_prometheus(),
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            String::from("not found\n"),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    tokio::time::timeout(HTTP_TIMEOUT, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "metrics response timed out"))??;
    Ok(())
}

async fn read_http_request<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let remaining = MAX_HTTP_REQUEST.saturating_sub(request.len());
        if remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metrics request exceeds size limit",
            ));
        }
        let chunk_size = remaining.min(chunk.len());
        let size = reader.read(&mut chunk[..chunk_size]).await?;
        if size == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..size]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_expected_metric_names_without_labels() {
        let metrics = Metrics::default();
        metrics.session_created();
        metrics.packet_to_backend(42);
        let output = metrics.render_prometheus();
        assert!(output.contains("wire_relay_active_sessions 1\n"));
        assert!(output.contains("wire_relay_bytes_to_backend_total 42\n"));
        assert!(!output.contains('{'));
    }

    #[test]
    fn drop_reasons_are_counted_separately_and_in_the_total() {
        let metrics = Metrics::default();
        metrics.datagram_dropped(DropReason::QueueFull);
        metrics.datagram_dropped(DropReason::QueueFull);
        metrics.datagram_dropped(DropReason::Oversize);
        metrics.datagram_dropped(DropReason::SendError);
        metrics.datagram_dropped(DropReason::BudgetExhausted);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.datagrams_dropped_total, 5);
        assert_eq!(
            snapshot.datagrams_dropped,
            Some(DropSnapshot {
                oversize: 1,
                queue_full: 2,
                budget_exhausted: 1,
                session_closed: 0,
                rejected: 0,
                send_error: 1,
            })
        );
        let output = metrics.render_prometheus();
        assert!(output.contains("wire_relay_datagrams_dropped_total 5\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_queue_full_total 2\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_budget_exhausted_total 1\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_oversize_total 1\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_session_closed_total 0\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_rejected_total 0\n"));
        assert!(output.contains("wire_relay_datagrams_dropped_send_error_total 1\n"));
    }

    #[test]
    fn in_process_drop_totals_always_equal_the_sum_of_reasons() {
        let metrics = Metrics::default();
        let listener = crate::relay::session::ListenerCounters::default();
        let reasons = [
            DropReason::Oversize,
            DropReason::QueueFull,
            DropReason::BudgetExhausted,
            DropReason::SessionClosed,
            DropReason::Rejected,
            DropReason::SendError,
        ];
        for (index, reason) in reasons.into_iter().cycle().take(23).enumerate() {
            metrics.datagram_dropped(reason);
            listener.dropped(reason);
            let process = metrics.snapshot();
            let listener = listener.snapshot();
            let expected = u64::try_from(index + 1).unwrap();
            assert_eq!(process.drops().total(), expected);
            assert_eq!(process.datagrams_dropped_total, expected);
            assert_eq!(listener.drops().total(), expected);
            assert_eq!(listener.dropped, expected);
        }
        assert!(
            metrics
                .render_prometheus()
                .contains("wire_relay_datagrams_dropped_total 23\n")
        );
    }

    #[test]
    fn snapshots_without_a_drop_breakdown_still_deserialize() {
        // An older daemon reports only the stored total; decoding keeps it
        // and the breakdown is absent rather than zero.
        let mut value = serde_json::to_value(Metrics::default().snapshot()).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("datagrams_dropped");
        object.insert("datagrams_dropped_total".to_owned(), 5.into());
        let snapshot: MetricsSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(snapshot.datagrams_dropped_total, 5);
        assert_eq!(snapshot.datagrams_dropped, None);
        assert_eq!(snapshot.drops(), DropSnapshot::default());

        // A current daemon always reports the breakdown, as a plain object
        // that older clients ignore, even when nothing was dropped.
        let value = serde_json::to_value(Metrics::default().snapshot()).unwrap();
        assert!(value["datagrams_dropped"].is_object());
        let snapshot: MetricsSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(snapshot.datagrams_dropped, Some(DropSnapshot::default()));

        // A daemon from before the budget reason omits that field; it
        // defaults to zero while the other reasons still decode.
        let mut value = serde_json::to_value(DropSnapshot {
            queue_full: 3,
            ..DropSnapshot::default()
        })
        .unwrap();
        value.as_object_mut().unwrap().remove("budget_exhausted");
        let drops: DropSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(drops.budget_exhausted, 0);
        assert_eq!(drops.queue_full, 3);
    }

    #[tokio::test]
    async fn accepts_a_fragmented_bounded_http_request() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        let write = tokio::spawn(async move {
            writer.write_all(b"GET /met").await.unwrap();
            tokio::task::yield_now().await;
            writer
                .write_all(b"rics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
        });
        let request = read_http_request(&mut reader).await.unwrap();
        write.await.unwrap();
        assert!(request.starts_with(b"GET /metrics "));
        assert!(request.ends_with(b"\r\n\r\n"));
    }
}
