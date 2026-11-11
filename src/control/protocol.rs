// SPDX-License-Identifier: AGPL-3.0-or-later

//! Versioned, bounded length-prefixed JSON control protocol.

use std::{fmt, io, net::IpAddr, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::{
    CONTROL_PROTOCOL_VERSION,
    config::{
        ConfigError, ListenerConfig, LogLevel, MetricsConfig, NormalizedConfig, ServiceConfig,
    },
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
    /// The active configuration in the shape every 0.1.x CLI decodes: only
    /// the service settings those releases know, so a CLI that rejects
    /// unknown fields can still read it.
    ActiveConfig,
    /// The active configuration with every setting this daemon has.
    ///
    /// A daemon that predates this operation cannot decode the request and
    /// closes the connection without answering; the CLI then falls back to
    /// [`ControlRequest::ActiveConfig`].
    ActiveConfigExtended,
    Listeners,
    Sessions(SessionQuery),
    Session {
        id: SessionId,
    },
    Stats,
    Reload,
    CloseSession {
        id: SessionId,
    },
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
    ActiveConfig(ActiveConfigSnapshot),
    ActiveConfigExtended(ActiveConfigSnapshot),
    Listeners(Vec<ListenerSnapshot>),
    Sessions(SessionPage),
    Session(SessionSnapshot),
    Stats(MetricsSnapshot),
    Reload(ReloadResult),
    SessionClosed { id: SessionId },
    Version(VersionSnapshot),
}

/// The active configuration as carried by the control protocol.
///
/// This is the wire form of [`NormalizedConfig`], kept apart from the strict
/// file format. The configuration file still rejects unknown keys; this type
/// does not, so a CLI can read a newer daemon's extended configuration, and
/// service settings added after 0.1.6 are optional, so a CLI can tell a
/// daemon that does not have them from one that uses their defaults.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveConfigSnapshot {
    pub service: ActiveServiceSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsConfig>,
    pub listeners: Vec<ListenerConfig>,
}

/// Service settings in an [`ActiveConfigSnapshot`].
///
/// The required fields are the 0.1.6 set, in the configuration file's order.
/// Each later setting is `None` when the daemon does not report it: in the
/// [`ControlResponse::ActiveConfig`] shape, or from a daemon that predates it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveServiceSnapshot {
    pub control_socket: PathBuf,
    pub log_level: LogLevel,
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    pub max_datagram_size: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_queue_depth: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queue_memory_bytes: Option<u64>,
    pub max_sessions: usize,
    pub max_sessions_per_ip: usize,
    pub new_sessions_per_second: u32,
    #[serde(with = "humantime_serde")]
    pub dns_refresh_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

impl ActiveConfigSnapshot {
    /// Every setting of `config`, for [`ControlResponse::ActiveConfigExtended`].
    #[must_use]
    pub fn extended(config: &NormalizedConfig) -> Self {
        let service = &config.service;
        Self {
            service: ActiveServiceSnapshot {
                control_socket: service.control_socket.clone(),
                log_level: service.log_level,
                idle_timeout: service.idle_timeout,
                max_datagram_size: service.max_datagram_size,
                session_queue_depth: Some(service.session_queue_depth),
                max_queue_memory_bytes: Some(service.max_queue_memory_bytes),
                max_sessions: service.max_sessions,
                max_sessions_per_ip: service.max_sessions_per_ip,
                new_sessions_per_second: service.new_sessions_per_second,
                dns_refresh_interval: service.dns_refresh_interval,
                shutdown_timeout: service.shutdown_timeout,
            },
            metrics: config.metrics.clone(),
            listeners: config.listeners.clone(),
        }
    }

    /// `config` in the 0.1.6 shape, for [`ControlResponse::ActiveConfig`]:
    /// later service settings are left out.
    #[must_use]
    pub fn legacy(config: &NormalizedConfig) -> Self {
        let mut snapshot = Self::extended(config);
        snapshot.service.session_queue_depth = None;
        snapshot.service.max_queue_memory_bytes = None;
        snapshot
    }

    /// Configuration keys of service settings the daemon did not report.
    #[must_use]
    pub fn unreported_service_settings(&self) -> Vec<&'static str> {
        let service = &self.service;
        [
            (
                service.session_queue_depth.is_none(),
                "service.session_queue_depth",
            ),
            (
                service.max_queue_memory_bytes.is_none(),
                "service.max_queue_memory_bytes",
            ),
        ]
        .into_iter()
        .filter_map(|(missing, key)| missing.then_some(key))
        .collect()
    }

    /// Validates the reported settings. A setting the daemon did not report
    /// is checked at its default, which every configuration valid for a
    /// daemon without that setting also satisfies.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let service = &self.service;
        let defaults = ServiceConfig::default();
        NormalizedConfig {
            service: ServiceConfig {
                control_socket: service.control_socket.clone(),
                log_level: service.log_level,
                idle_timeout: service.idle_timeout,
                max_datagram_size: service.max_datagram_size,
                session_queue_depth: service
                    .session_queue_depth
                    .unwrap_or(defaults.session_queue_depth),
                max_queue_memory_bytes: service
                    .max_queue_memory_bytes
                    .unwrap_or(defaults.max_queue_memory_bytes),
                max_sessions: service.max_sessions,
                max_sessions_per_ip: service.max_sessions_per_ip,
                new_sessions_per_second: service.new_sessions_per_second,
                dns_refresh_interval: service.dns_refresh_interval,
                shutdown_timeout: service.shutdown_timeout,
            },
            metrics: self.metrics.clone(),
            listeners: self.listeners.clone(),
        }
        .validate()
    }

    /// Serializes the reported configuration as normalized TOML; settings the
    /// daemon did not report are omitted, not filled in with defaults.
    pub fn to_toml(&self) -> Result<String, ConfigError> {
        self.validate()?;
        toml::to_string_pretty(self).map_err(|source| ConfigError::Serialize { source })
    }
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

    /// A 0.1.6 daemon's real `active_config` response.
    const OLD_DAEMON_ACTIVE_CONFIG: &str = r#"{"protocol_version":2,"request_id":"00000000-0000-0000-0000-000000000001","response":{"kind":"active_config","data":{"service":{"control_socket":"/tmp/wr-interop/control.sock","log_level":"info","idle_timeout":"3m","max_datagram_size":4096,"max_sessions":10000,"max_sessions_per_ip":64,"new_sessions_per_second":100,"dns_refresh_interval":"1m","shutdown_timeout":"10s"},"metrics":{"enabled":true,"bind":"127.0.0.1:19090"},"listeners":[{"name":"t","bind":"127.0.0.1:47811","backend":"relay.example.com:47812"}]}}}"#;

    /// The strict response types of a 0.1.6 CLI, which reject unknown keys.
    mod old_cli {
        // Only decoding matters; the values are never read.
        #![allow(dead_code)]

        use serde::Deserialize;

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Envelope {
            pub protocol_version: u16,
            pub request_id: uuid::Uuid,
            pub response: Option<Response>,
            pub error: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        #[serde(tag = "kind", content = "data", rename_all = "snake_case")]
        pub enum Response {
            ActiveConfig(Config),
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct Config {
            pub service: Service,
            #[serde(default)]
            pub metrics: Option<serde_json::Value>,
            pub listeners: Vec<serde_json::Value>,
        }

        #[derive(Deserialize)]
        #[serde(default, deny_unknown_fields)]
        #[derive(Default)]
        pub struct Service {
            pub control_socket: serde_json::Value,
            pub log_level: serde_json::Value,
            pub idle_timeout: serde_json::Value,
            pub max_datagram_size: serde_json::Value,
            pub max_sessions: serde_json::Value,
            pub max_sessions_per_ip: serde_json::Value,
            pub new_sessions_per_second: serde_json::Value,
            pub dns_refresh_interval: serde_json::Value,
            pub shutdown_timeout: serde_json::Value,
        }
    }

    fn sample_config() -> NormalizedConfig {
        crate::config::Config::parse_str(
            "[service]\nsession_queue_depth = 64\nmax_queue_memory_bytes = 33554432\n\n\
             [metrics]\nenabled = true\nbind = \"127.0.0.1:19090\"\n\n\
             [[listeners]]\nname = \"t\"\nbind = \"127.0.0.1:47811\"\n\
             backend = \"relay.example.com:47812\"\n",
        )
        .unwrap()
        .into_normalized()
        .unwrap()
    }

    #[test]
    fn an_old_cli_decodes_the_active_config_of_this_daemon() {
        let response = ResponseEnvelope::success(
            Uuid::nil(),
            ControlResponse::ActiveConfig(ActiveConfigSnapshot::legacy(&sample_config())),
        );
        let encoded = serde_json::to_string(&response).unwrap();
        let decoded: old_cli::Envelope = serde_json::from_str(&encoded)
            .unwrap_or_else(|error| panic!("a 0.1.6 CLI cannot decode {encoded}: {error}"));
        assert!(matches!(
            decoded.response,
            Some(old_cli::Response::ActiveConfig(_))
        ));
        assert!(!encoded.contains("session_queue_depth"));
        assert!(!encoded.contains("max_queue_memory_bytes"));

        // The extended form is what an old CLI could not decode; only a new
        // CLI asks for it.
        let extended =
            serde_json::to_value(ActiveConfigSnapshot::extended(&sample_config())).unwrap();
        assert_eq!(extended["service"]["session_queue_depth"], 64);
        assert_eq!(extended["service"]["max_queue_memory_bytes"], 33_554_432);
        assert!(serde_json::from_value::<old_cli::Config>(extended).is_err());
    }

    #[test]
    fn an_old_daemon_active_config_does_not_invent_new_settings() {
        let envelope: ResponseEnvelope = serde_json::from_str(OLD_DAEMON_ACTIVE_CONFIG).unwrap();
        let ControlResponse::ActiveConfig(config) = envelope.into_result().unwrap() else {
            panic!("wrong response variant");
        };
        assert_eq!(config.service.session_queue_depth, None);
        assert_eq!(config.service.max_queue_memory_bytes, None);
        assert_eq!(
            config.unreported_service_settings(),
            [
                "service.session_queue_depth",
                "service.max_queue_memory_bytes"
            ]
        );
        assert_eq!(config.service.max_sessions, 10_000);
        assert_eq!(config.service.idle_timeout, Duration::from_secs(180));
        assert_eq!(config.listeners.len(), 1);
        let toml = config.to_toml().unwrap();
        assert!(toml.contains("max_datagram_size = 4096\n"));
        assert!(!toml.contains("session_queue_depth"), "{toml}");
        assert!(!toml.contains("max_queue_memory_bytes"), "{toml}");
        let json = serde_json::to_string(&config).unwrap();
        assert!(!json.contains("session_queue_depth"), "{json}");
    }

    #[test]
    fn the_extended_active_config_matches_the_configuration_file_form() {
        let config = sample_config();
        let snapshot = ActiveConfigSnapshot::extended(&config);
        assert!(snapshot.unreported_service_settings().is_empty());
        // Same keys, order, and formatting as the strict file type.
        assert_eq!(snapshot.to_toml().unwrap(), config.to_toml().unwrap());
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            serde_json::to_value(&config).unwrap()
        );
        let response = ControlResponse::ActiveConfigExtended(snapshot.clone());
        let decoded: ControlResponse =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(decoded, response);

        // A later daemon may report settings this CLI does not know.
        let mut value = serde_json::to_value(&snapshot).unwrap();
        value["service"]["a_later_setting"] = 1.into();
        assert_eq!(
            serde_json::from_value::<ActiveConfigSnapshot>(value).unwrap(),
            snapshot
        );
    }

    #[test]
    fn the_configuration_file_still_rejects_unknown_keys() {
        let error = crate::config::Config::parse_str(
            "[service]\na_later_setting = 1\n\n[[listeners]]\nname = \"t\"\n\
             bind = \"127.0.0.1:47811\"\nbackend = \"127.0.0.1:47812\"\n",
        )
        .unwrap_err();
        assert!(error.to_string().contains("a_later_setting"), "{error}");
    }

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
