// SPDX-License-Identifier: AGPL-3.0-or-later

//! Versioned, bounded length-prefixed JSON control protocol.

use std::{fmt, io, net::IpAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::{
    CONTROL_PROTOCOL_VERSION,
    config::NormalizedConfig,
    metrics::MetricsSnapshot,
    relay::{ListenerSnapshot, SessionId, SessionSnapshot},
};

/// Maximum encoded request or response frame.
pub const MAX_CONTROL_FRAME_SIZE: usize = 1024 * 1024;

/// Maximum sessions returned in one response.
pub const MAX_SESSION_PAGE_SIZE: u16 = 1_024;

/// Default control I/O timeout.
pub const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// One request envelope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol_version: u16,
    pub request_id: Uuid,
    pub request: ControlRequest,
}

impl RequestEnvelope {
    #[must_use]
    pub fn new(request: ControlRequest) -> Self {
        Self {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request_id: Uuid::new_v4(),
            request,
        }
    }
}

/// Supported daemon operations.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "operation", content = "parameters", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    ActiveConfig,
    Listeners,
    Sessions(SessionQuery),
    Session { id: SessionId },
    Stats,
    Reload,
    CloseSession { id: SessionId },
    Version,
}

/// Server-side session filtering and pagination.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SessionQuery {
    pub listener: Option<String>,
    pub client_ip: Option<IpAddr>,
    pub sort: SessionSort,
    /// Opaque daemon-issued token for continuing an immutable listing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SessionCursor>,
    pub offset: usize,
    pub limit: u16,
}

impl SessionQuery {
    #[must_use]
    pub fn page_size(&self) -> usize {
        if self.limit == 0 {
            usize::from(MAX_SESSION_PAGE_SIZE)
        } else {
            usize::from(self.limit.min(MAX_SESSION_PAGE_SIZE))
        }
    }
}

/// Opaque identifier for one bounded, immutable server-side session snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionCursor(Uuid);

impl SessionCursor {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable sort choices accepted by the CLI and protocol.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionSort {
    #[default]
    Id,
    Bytes,
    Age,
    Idle,
}

/// One response envelope. Exactly one of `response` and `error` is populated.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub protocol_version: u16,
    pub request_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<ControlResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ControlError>,
}

impl ResponseEnvelope {
    #[must_use]
    pub fn success(request_id: Uuid, response: ControlResponse) -> Self {
        Self {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request_id,
            response: Some(response),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(request_id: Uuid, error: ControlError) -> Self {
        Self {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request_id,
            response: None,
            error: Some(error),
        }
    }

    pub fn into_result(self) -> Result<ControlResponse, ControlError> {
        match (self.response, self.error) {
            (Some(response), None) => Ok(response),
            (None, Some(error)) => Err(error),
            _ => Err(ControlError::new(
                ControlErrorCode::Internal,
                "invalid response envelope",
            )),
        }
    }
}

/// Operation-specific response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ControlResponse {
    Status(StatusSnapshot),
    ActiveConfig(NormalizedConfig),
    Listeners(Vec<ListenerSnapshot>),
    Sessions(SessionPage),
    Session(SessionSnapshot),
    Stats(MetricsSnapshot),
    Reload(ReloadResult),
    SessionClosed { id: SessionId },
    Version(VersionSnapshot),
}

/// Summary shown by `wire-relay show`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StatusSnapshot {
    pub version: String,
    pub protocol_version: u16,
    pub uptime_ms: u64,
    pub active_sessions: usize,
    pub control_socket: PathBuf,
    pub listeners: Vec<ListenerSnapshot>,
    pub stats: MetricsSnapshot,
}

/// One bounded page of sessions.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SessionPage {
    pub sessions: Vec<SessionSnapshot>,
    pub total: usize,
    pub next_offset: Option<usize>,
    /// Opaque token that must accompany `next_offset` on the next request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<SessionCursor>,
}

/// Detailed transactional reload outcome.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReloadResult {
    pub applied: bool,
    pub preserved: Vec<String>,
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub removed: Vec<String>,
    pub sessions_closed: usize,
    pub message: String,
}

/// Protocol and application versions.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub application: String,
    pub protocol: u16,
}

/// Structured daemon error.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ControlError {
    pub code: ControlErrorCode,
    pub message: String,
}

impl ControlError {
    #[must_use]
    pub fn new(code: ControlErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Stable machine-readable control error category.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    InvalidRequest,
    UnsupportedVersion,
    NotFound,
    ReloadRejected,
    Unavailable,
    Internal,
}

/// Reads and deserializes exactly one bounded frame.
pub async fn read_frame<R, T>(reader: &mut R) -> io::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32().await?;
    let length = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid frame length"))?;
    if length == 0 || length > MAX_CONTROL_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("control frame length {length} is outside 1..={MAX_CONTROL_FRAME_SIZE}"),
        ));
    }

    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Serializes and writes exactly one bounded frame.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if payload.is_empty() || payload.len() > MAX_CONTROL_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "encoded control frame length {} is outside 1..={MAX_CONTROL_FRAME_SIZE}",
                payload.len()
            ),
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "control frame too large"))?;
    writer.write_u32(length).await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::ListenerId;

    #[tokio::test]
    async fn request_round_trips_through_framing() {
        let request = RequestEnvelope::new(ControlRequest::Stats);
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let expected = request.clone();
        let write = tokio::spawn(async move { write_frame(&mut writer, &request).await });
        let decoded: RequestEnvelope = read_frame(&mut reader).await.unwrap();
        write.await.unwrap().unwrap();
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn oversized_prefix_is_rejected_without_allocating_payload() {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        let write = tokio::spawn(async move {
            writer
                .write_u32(u32::try_from(MAX_CONTROL_FRAME_SIZE + 1).unwrap())
                .await
        });
        let error = read_frame::<_, RequestEnvelope>(&mut reader)
            .await
            .unwrap_err();
        write.await.unwrap().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn unsupported_fields_are_rejected() {
        let json = r#"{
            "protocol_version": 2,
            "request_id": "00000000-0000-0000-0000-000000000001",
            "request": {"operation": "stats"},
            "extra": true
        }"#;
        assert!(serde_json::from_str::<RequestEnvelope>(json).is_err());
    }

    #[test]
    fn maximum_session_page_fits_the_bounded_control_frame() {
        let sample = SessionSnapshot {
            id: SessionId::new(),
            listener_id: ListenerId::new(u64::MAX),
            listener: "\\\"".repeat(64),
            client_addr: "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .unwrap(),
            upstream_local_addr: "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .unwrap(),
            backend_addr: "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .unwrap(),
            age_ms: u64::MAX,
            idle_ms: u64::MAX,
            last_client_activity_ms: u64::MAX,
            last_backend_activity_ms: u64::MAX,
            packets_to_backend: u64::MAX,
            packets_to_client: u64::MAX,
            bytes_to_backend: u64::MAX,
            bytes_to_client: u64::MAX,
        };
        let response = ResponseEnvelope::success(
            Uuid::nil(),
            ControlResponse::Sessions(SessionPage {
                sessions: vec![sample; usize::from(MAX_SESSION_PAGE_SIZE)],
                total: usize::MAX,
                next_offset: Some(usize::MAX),
                next_cursor: Some(SessionCursor::new()),
            }),
        );
        let encoded = serde_json::to_vec(&response).unwrap();
        assert!(
            encoded.len() <= MAX_CONTROL_FRAME_SIZE,
            "maximum page encoded to {} bytes",
            encoded.len()
        );
    }
}
