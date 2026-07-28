// SPDX-License-Identifier: AGPL-3.0-or-later

//! Strict, validated TOML configuration for `WireRelay`.
//!
//! Parsing configuration is deliberately separate from DNS resolution. Listener
//! bind addresses must be numeric [`SocketAddr`] values, while a backend is
//! represented as a canonical host and a non-zero port for the DNS subsystem to
//! resolve at startup and on its refresh interval.

use std::fmt;
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Default location of the daemon configuration on Linux.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/wire-relay/config.toml";

/// Largest payload that can be carried by an IPv4 UDP datagram.
pub const MAX_UDP_DATAGRAM_SIZE: usize = 65_507;

/// Maximum number of listeners kept in one bounded control response.
pub const MAX_LISTENERS: usize = 256;

/// Defensive ceiling for configured active mappings.
pub const MAX_CONFIGURED_SESSIONS: usize = 100_000;

/// Defensive ceiling for the new-session token bucket.
pub const MAX_NEW_SESSIONS_PER_SECOND: u32 = 100_000;

/// Maximum client datagrams waiting behind one upstream socket.
pub const MAX_QUEUED_CLIENT_DATAGRAMS: usize = 8;

/// Default local control-plane Unix-domain socket.
pub const DEFAULT_CONTROL_SOCKET: &str = "/run/wire-relay/control.sock";

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 180;
const DEFAULT_MAX_DATAGRAM_SIZE: usize = 4_096;
const DEFAULT_MAX_SESSIONS: usize = 10_000;
const DEFAULT_MAX_SESSIONS_PER_IP: usize = 64;
const DEFAULT_NEW_SESSIONS_PER_SECOND: u32 = 100;
const DEFAULT_DNS_REFRESH_INTERVAL_SECS: u64 = 60;
const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 10;
const MAX_LISTENER_NAME_BYTES: usize = 128;
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 107;
const MIN_IDLE_TIMEOUT: Duration = Duration::from_millis(10);
const MIN_DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const MIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MAX_DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_SESSION_DATAGRAM_MEMORY_BYTES: u128 = 4 * 1024 * 1024 * 1024;
const SESSION_DATAGRAM_SLOTS: u128 = MAX_QUEUED_CLIENT_DATAGRAMS as u128 + 2;

/// Fully typed configuration loaded from TOML.
///
/// `Config::parse_str` and `Config::load` always validate before returning.
/// Code that constructs this type directly should call [`Config::validate`]
/// before applying it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub service: ServiceConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsConfig>,
    #[serde(default)]
    pub listeners: Vec<ListenerConfig>,
}

impl Config {
    /// Load, parse, and validate a UTF-8 TOML configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let input = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse_str(&input)
    }

    /// Load the conventional `/etc/wire-relay/config.toml` path.
    pub fn load_default() -> Result<Self, ConfigError> {
        Self::load(DEFAULT_CONFIG_PATH)
    }

    /// Parse and validate TOML configuration text.
    pub fn parse_str(input: &str) -> Result<Self, ConfigError> {
        let config =
            toml::from_str::<Self>(input).map_err(|source| ConfigError::Parse { source })?;
        config.validate()?;
        Ok(config)
    }

    /// Validate a directly constructed configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_config(&self.service, self.metrics.as_ref(), &self.listeners)
    }

    /// Return the complete active configuration in a stable, serializable form.
    ///
    /// Defaults have already been materialized by Serde, addresses are numeric
    /// and canonical, and backend hostnames are lower-case.
    pub fn normalized(&self) -> Result<NormalizedConfig, ConfigError> {
        self.validate()?;
        Ok(NormalizedConfig {
            service: self.service.clone(),
            metrics: self.metrics.clone(),
            listeners: self.listeners.clone(),
        })
    }

    /// Validate and consume the parsed configuration into its active form.
    pub fn into_normalized(self) -> Result<NormalizedConfig, ConfigError> {
        self.validate()?;
        Ok(NormalizedConfig {
            service: self.service,
            metrics: self.metrics,
            listeners: self.listeners,
        })
    }
}

impl FromStr for Config {
    type Err = ConfigError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse_str(input)
    }
}

/// Active configuration suitable for the control protocol and reload
/// comparisons.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedConfig {
    pub service: ServiceConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsConfig>,
    pub listeners: Vec<ListenerConfig>,
}

impl NormalizedConfig {
    /// Validate the representation before applying one received over a control
    /// boundary or created directly.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_config(&self.service, self.metrics.as_ref(), &self.listeners)
    }

    /// Serialize the active configuration as normalized TOML.
    pub fn to_toml(&self) -> Result<String, ConfigError> {
        self.validate()?;
        toml::to_string_pretty(self).map_err(|source| ConfigError::Serialize { source })
    }
}

impl TryFrom<NormalizedConfig> for Config {
    type Error = ConfigError;

    fn try_from(config: NormalizedConfig) -> Result<Self, Self::Error> {
        config.validate()?;
        Ok(Self {
            service: config.service,
            metrics: config.metrics,
            listeners: config.listeners,
        })
    }
}

/// Daemon-wide service settings.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceConfig {
    pub control_socket: PathBuf,
    pub log_level: LogLevel,
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    pub max_datagram_size: usize,
    pub max_sessions: usize,
    pub max_sessions_per_ip: usize,
    pub new_sessions_per_second: u32,
    #[serde(with = "humantime_serde")]
    pub dns_refresh_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            control_socket: PathBuf::from(DEFAULT_CONTROL_SOCKET),
            log_level: LogLevel::Info,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            max_datagram_size: DEFAULT_MAX_DATAGRAM_SIZE,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_sessions_per_ip: DEFAULT_MAX_SESSIONS_PER_IP,
            new_sessions_per_second: DEFAULT_NEW_SESSIONS_PER_SECOND,
            dns_refresh_interval: Duration::from_secs(DEFAULT_DNS_REFRESH_INTERVAL_SECS),
            shutdown_timeout: Duration::from_secs(DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        }
    }
}

/// Supported tracing verbosity.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for LogLevel {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Optional Prometheus exporter configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub enabled: bool,
    #[serde(with = "socket_addr_serde")]
    pub bind: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: SocketAddr::from(([127, 0, 0, 1], 9090)),
        }
    }
}

/// One UDP listener and its fixed upstream destination.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    pub name: String,
    #[serde(with = "socket_addr_serde")]
    pub bind: SocketAddr,
    pub backend: BackendEndpoint,
}

/// A backend host that either needs DNS resolution or is already an IP literal.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum BackendHost {
    Ip(IpAddr),
    Name(String),
}

impl BackendHost {
    /// Return an IP literal without allocating, when this backend has one.
    #[must_use]
    pub const fn ip_addr(&self) -> Option<IpAddr> {
        match self {
            Self::Ip(ip) => Some(*ip),
            Self::Name(_) => None,
        }
    }

    /// Return the canonical DNS hostname, when this backend needs resolution.
    #[must_use]
    pub fn hostname(&self) -> Option<&str> {
        match self {
            Self::Ip(_) => None,
            Self::Name(name) => Some(name),
        }
    }
}

impl fmt::Display for BackendHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => ip.fmt(formatter),
            Self::Name(name) => formatter.write_str(name),
        }
    }
}

impl Serialize for BackendHost {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BackendHost {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let host = String::deserialize(deserializer)?;
        parse_backend_host(&host).map_err(serde::de::Error::custom)
    }
}

/// Canonical backend authority.
///
/// IPv6 literals are serialized with brackets, while IPv4 literals and
/// hostnames use `host:port`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BackendEndpoint {
    host: BackendHost,
    port: u16,
}

impl BackendEndpoint {
    /// Parse and canonicalize a backend `host:port` authority.
    pub fn parse(authority: &str) -> Result<Self, BackendEndpointError> {
        authority.parse()
    }

    /// Build a backend from separate host and port fields.
    pub fn from_parts(host: &str, port: u16) -> Result<Self, BackendEndpointError> {
        if port == 0 {
            return Err(BackendEndpointError::ZeroPort);
        }
        let host = parse_backend_host(host)?;
        Ok(Self { host, port })
    }

    #[must_use]
    pub const fn host(&self) -> &BackendHost {
        &self.host
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Return the directly usable socket address for an IP-literal backend.
    #[must_use]
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.host.ip_addr().map(|ip| SocketAddr::new(ip, self.port))
    }

    #[must_use]
    pub fn hostname(&self) -> Option<&str> {
        self.host.hostname()
    }

    #[must_use]
    pub const fn ip_addr(&self) -> Option<IpAddr> {
        self.host.ip_addr()
    }
}

impl FromStr for BackendEndpoint {
    type Err = BackendEndpointError;

    fn from_str(authority: &str) -> Result<Self, Self::Err> {
        if authority.trim() != authority {
            return Err(BackendEndpointError::SurroundingWhitespace);
        }

        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let Some(close_bracket) = bracketed.find(']') else {
                return Err(BackendEndpointError::InvalidBracketedIpv6);
            };
            let host = &bracketed[..close_bracket];
            let remainder = &bracketed[close_bracket + 1..];
            let Some(port) = remainder.strip_prefix(':') else {
                return Err(BackendEndpointError::MissingPort);
            };
            if port.contains(':') || port.is_empty() {
                return Err(BackendEndpointError::InvalidPort(port.to_owned()));
            }

            let ip = host
                .parse::<Ipv6Addr>()
                .map_err(|_| BackendEndpointError::InvalidBracketedIpv6)?;
            (BackendHost::Ip(IpAddr::V6(ip)), parse_backend_port(port)?)
        } else {
            let Some(separator) = authority.rfind(':') else {
                return Err(BackendEndpointError::MissingPort);
            };
            let host = &authority[..separator];
            let port = &authority[separator + 1..];
            if host.contains(':') {
                return Err(BackendEndpointError::UnbracketedIpv6);
            }
            (parse_backend_host(host)?, parse_backend_port(port)?)
        };

        Ok(Self { host, port })
    }
}

impl fmt::Display for BackendEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.host {
            BackendHost::Ip(IpAddr::V6(ip)) => write!(formatter, "[{ip}]:{}", self.port),
            _ => write!(formatter, "{}:{}", self.host, self.port),
        }
    }
}

impl Serialize for BackendEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BackendEndpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let authority = String::deserialize(deserializer)?;
        authority.parse().map_err(serde::de::Error::custom)
    }
}

/// A precise backend authority parsing failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BackendEndpointError {
    #[error("backend must not have surrounding whitespace")]
    SurroundingWhitespace,
    #[error("backend must include a port, for example `vpn.example.com:51820`")]
    MissingPort,
    #[error("backend host must not be empty")]
    EmptyHost,
    #[error("backend port `{0}` is invalid; expected an integer from 1 to 65535")]
    InvalidPort(String),
    #[error("backend port must be greater than zero")]
    ZeroPort,
    #[error("IPv6 backend addresses must be enclosed in brackets")]
    UnbracketedIpv6,
    #[error("backend contains an invalid bracketed IPv6 address")]
    InvalidBracketedIpv6,
    #[error("backend IP literal `{0}` is invalid")]
    InvalidIpLiteral(String),
    #[error("backend hostname `{host}` is invalid: {reason}")]
    InvalidHostname { host: String, reason: &'static str },
}

/// Configuration read, syntax, serialization, or semantic validation failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("failed to read configuration file `{path}`: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("malformed TOML configuration: {source}")]
    Parse {
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize normalized configuration: {source}")]
    Serialize {
        #[source]
        source: toml::ser::Error,
    },
    #[error("configuration must contain at least one `[[listeners]]` entry")]
    NoListeners,
    #[error("listener name `{name}` is configured more than once")]
    DuplicateListenerName { name: String },
    #[error(
        "listeners `{first_name}` ({first_bind}) and `{second_name}` ({second_bind}) \
         have conflicting effective bind addresses"
    )]
    ConflictingBindAddresses {
        first_name: String,
        first_bind: SocketAddr,
        second_name: String,
        second_bind: SocketAddr,
    },
    #[error("invalid configuration value `{field}`: {reason}")]
    InvalidValue { field: String, reason: String },
}

fn validate_config(
    service: &ServiceConfig,
    metrics: Option<&MetricsConfig>,
    listeners: &[ListenerConfig],
) -> Result<(), ConfigError> {
    validate_service(service)?;

    if let Some(metrics) = metrics {
        validate_nonzero_port("metrics.bind", metrics.bind)?;
        validate_bind_ip("metrics.bind", metrics.bind.ip())?;
    }

    if listeners.is_empty() {
        return Err(ConfigError::NoListeners);
    }
    if listeners.len() > MAX_LISTENERS {
        return Err(invalid_value(
            "listeners",
            format!("must contain no more than {MAX_LISTENERS} entries"),
        ));
    }

    for (index, listener) in listeners.iter().enumerate() {
        validate_listener(index, listener)?;

        for previous in &listeners[..index] {
            if previous.name == listener.name {
                return Err(ConfigError::DuplicateListenerName {
                    name: listener.name.clone(),
                });
            }
            if bind_addresses_conflict(previous.bind, listener.bind) {
                return Err(ConfigError::ConflictingBindAddresses {
                    first_name: previous.name.clone(),
                    first_bind: previous.bind,
                    second_name: listener.name.clone(),
                    second_bind: listener.bind,
                });
            }
        }
    }

    Ok(())
}

fn validate_service(service: &ServiceConfig) -> Result<(), ConfigError> {
    let control_socket = service
        .control_socket
        .to_str()
        .ok_or_else(|| invalid_value("service.control_socket", "path must be valid UTF-8"))?;
    if !control_socket.starts_with('/') {
        return Err(invalid_value(
            "service.control_socket",
            "path must be an absolute Unix path",
        ));
    }
    if control_socket.ends_with('/') {
        return Err(invalid_value(
            "service.control_socket",
            "path must name a socket, not a directory",
        ));
    }
    if control_socket
        .split('/')
        .any(|component| matches!(component, "." | ".."))
    {
        return Err(invalid_value(
            "service.control_socket",
            "path must not contain `.` or `..` components",
        ));
    }
    if control_socket.as_bytes().contains(&0) {
        return Err(invalid_value(
            "service.control_socket",
            "path must not contain a NUL byte",
        ));
    }
    if control_socket.len() > MAX_UNIX_SOCKET_PATH_BYTES {
        return Err(invalid_value(
            "service.control_socket",
            format!(
                "path is too long for a Linux Unix-domain socket (maximum \
                 {MAX_UNIX_SOCKET_PATH_BYTES} bytes)"
            ),
        ));
    }

    validate_duration(
        "service.idle_timeout",
        service.idle_timeout,
        MIN_IDLE_TIMEOUT,
        MAX_IDLE_TIMEOUT,
    )?;
    validate_duration(
        "service.dns_refresh_interval",
        service.dns_refresh_interval,
        MIN_DNS_REFRESH_INTERVAL,
        MAX_DNS_REFRESH_INTERVAL,
    )?;
    validate_duration(
        "service.shutdown_timeout",
        service.shutdown_timeout,
        MIN_SHUTDOWN_TIMEOUT,
        MAX_SHUTDOWN_TIMEOUT,
    )?;

    if !(1..=MAX_UDP_DATAGRAM_SIZE).contains(&service.max_datagram_size) {
        return Err(invalid_value(
            "service.max_datagram_size",
            format!("must be between 1 and {MAX_UDP_DATAGRAM_SIZE}, the maximum UDP payload size"),
        ));
    }
    validate_nonzero_limit("service.max_sessions", service.max_sessions)?;
    validate_nonzero_limit("service.max_sessions_per_ip", service.max_sessions_per_ip)?;
    if service.max_sessions > MAX_CONFIGURED_SESSIONS {
        return Err(invalid_value(
            "service.max_sessions",
            format!("must not exceed {MAX_CONFIGURED_SESSIONS}"),
        ));
    }
    if service.max_sessions_per_ip > service.max_sessions {
        return Err(invalid_value(
            "service.max_sessions_per_ip",
            "must not exceed service.max_sessions",
        ));
    }
    if service.new_sessions_per_second == 0 {
        return Err(invalid_value(
            "service.new_sessions_per_second",
            "must be greater than zero",
        ));
    }
    if service.new_sessions_per_second > MAX_NEW_SESSIONS_PER_SECOND {
        return Err(invalid_value(
            "service.new_sessions_per_second",
            format!("must not exceed {MAX_NEW_SESSIONS_PER_SECOND}"),
        ));
    }

    let per_session_datagram_memory = (service.max_datagram_size as u128)
        .saturating_mul(SESSION_DATAGRAM_SLOTS)
        .saturating_add(1);
    let worst_case_datagram_memory =
        (service.max_sessions as u128).saturating_mul(per_session_datagram_memory);
    if worst_case_datagram_memory > MAX_SESSION_DATAGRAM_MEMORY_BYTES {
        return Err(invalid_value(
            "service.max_sessions",
            format!(
                "combined with service.max_datagram_size, the worst-case per-session datagram \
                 buffers exceed the defensive {} GiB budget; reduce one of those values",
                MAX_SESSION_DATAGRAM_MEMORY_BYTES / (1024 * 1024 * 1024)
            ),
        ));
    }

    Ok(())
}

fn validate_listener(index: usize, listener: &ListenerConfig) -> Result<(), ConfigError> {
    let field = |suffix: &str| format!("listeners[{index}].{suffix}");
    if listener.name.is_empty() {
        return Err(invalid_value(field("name"), "must not be empty"));
    }
    if listener.name.trim() != listener.name {
        return Err(invalid_value(
            field("name"),
            "must not have leading or trailing whitespace",
        ));
    }
    if listener.name.len() > MAX_LISTENER_NAME_BYTES {
        return Err(invalid_value(
            field("name"),
            format!("must be at most {MAX_LISTENER_NAME_BYTES} bytes"),
        ));
    }
    if listener.name.chars().any(char::is_control) {
        return Err(invalid_value(
            field("name"),
            "must not contain control characters",
        ));
    }

    validate_nonzero_port(&field("bind"), listener.bind)?;
    validate_bind_ip(&field("bind"), listener.bind.ip())?;

    // BackendEndpoint's private fields make this invariant true for parsed and
    // programmatically constructed values. Retain the check defensively in case
    // its representation changes.
    if listener.backend.port() == 0 {
        return Err(invalid_value(
            field("backend"),
            "port must be greater than zero",
        ));
    }
    if let Some(ip) = listener.backend.ip_addr() {
        validate_backend_ip(&field("backend"), ip)?;
    }

    Ok(())
}

fn validate_duration(
    field: &str,
    value: Duration,
    minimum: Duration,
    maximum: Duration,
) -> Result<(), ConfigError> {
    if value < minimum {
        return Err(invalid_value(
            field,
            format!("must be at least {}", humantime::format_duration(minimum)),
        ));
    }
    if value > maximum {
        return Err(invalid_value(
            field,
            format!("must not exceed {}", humantime::format_duration(maximum)),
        ));
    }
    Ok(())
}

fn validate_bind_ip(field: &str, ip: IpAddr) -> Result<(), ConfigError> {
    if ip.is_multicast() || matches!(ip, IpAddr::V4(address) if address.is_broadcast()) {
        return Err(invalid_value(
            field,
            "multicast and broadcast addresses are not usable listener binds",
        ));
    }
    Ok(())
}

fn validate_backend_ip(field: &str, ip: IpAddr) -> Result<(), ConfigError> {
    if ip.is_unspecified()
        || ip.is_multicast()
        || matches!(ip, IpAddr::V4(address) if address.is_broadcast())
    {
        return Err(invalid_value(
            field,
            "backend IP must be a unicast, non-unspecified address",
        ));
    }
    Ok(())
}

fn validate_nonzero_limit(field: &str, value: usize) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(invalid_value(field, "must be greater than zero"));
    }
    Ok(())
}

fn validate_nonzero_port(field: &str, value: SocketAddr) -> Result<(), ConfigError> {
    if value.port() == 0 {
        return Err(invalid_value(field, "port must be greater than zero"));
    }
    Ok(())
}

fn invalid_value(field: impl Into<String>, reason: impl Into<String>) -> ConfigError {
    ConfigError::InvalidValue {
        field: field.into(),
        reason: reason.into(),
    }
}

/// Determine whether two UDP binds can claim any of the same local endpoint.
///
/// Linux IPv6 wildcard sockets are dual-stack unless `IPV6_V6ONLY` is set. The
/// listener layer does not promise to set that option, so `[::]:PORT` is
/// conservatively treated as conflicting with IPv4 binds on the same port.
fn bind_addresses_conflict(left: SocketAddr, right: SocketAddr) -> bool {
    if left.port() != right.port() {
        return false;
    }

    let left_ip = left.ip();
    let right_ip = right.ip();

    match (left_ip, right_ip) {
        (IpAddr::V4(left), IpAddr::V4(right)) => {
            left == right || left.is_unspecified() || right.is_unspecified()
        }
        (IpAddr::V6(left), IpAddr::V6(right)) => {
            left == right || left.is_unspecified() || right.is_unspecified()
        }
        (IpAddr::V6(v6), IpAddr::V4(v4)) | (IpAddr::V4(v4), IpAddr::V6(v6)) => {
            v6.is_unspecified()
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped == v4 || v4.is_unspecified())
        }
    }
}

fn parse_backend_port(port: &str) -> Result<u16, BackendEndpointError> {
    let parsed = port
        .parse::<u16>()
        .map_err(|_| BackendEndpointError::InvalidPort(port.to_owned()))?;
    if parsed == 0 {
        return Err(BackendEndpointError::ZeroPort);
    }
    Ok(parsed)
}

fn parse_backend_host(host: &str) -> Result<BackendHost, BackendEndpointError> {
    if host.is_empty() {
        return Err(BackendEndpointError::EmptyHost);
    }
    if host.trim() != host {
        return Err(BackendEndpointError::SurroundingWhitespace);
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(BackendHost::Ip(ip));
    }

    // A dotted numeric value is almost certainly a malformed IPv4 literal. Do
    // not silently send it through DNS, where platform-specific legacy numeric
    // parsing could produce a surprising destination.
    if host
        .chars()
        .all(|character| character.is_ascii_digit() || character == '.')
    {
        return Err(BackendEndpointError::InvalidIpLiteral(host.to_owned()));
    }
    if host.contains(['[', ']', ':']) {
        return Err(BackendEndpointError::InvalidHostname {
            host: host.to_owned(),
            reason: "must not contain brackets or colons",
        });
    }
    if !host.is_ascii() {
        return Err(BackendEndpointError::InvalidHostname {
            host: host.to_owned(),
            reason: "use an ASCII or IDNA (punycode) hostname",
        });
    }

    let canonical = host.to_ascii_lowercase();
    let without_root_dot = canonical.strip_suffix('.').unwrap_or(&canonical);
    if without_root_dot.is_empty() {
        return Err(BackendEndpointError::InvalidHostname {
            host: host.to_owned(),
            reason: "must contain at least one label",
        });
    }
    if without_root_dot.len() > 253 {
        return Err(BackendEndpointError::InvalidHostname {
            host: host.to_owned(),
            reason: "must be no more than 253 bytes",
        });
    }
    for label in without_root_dot.split('.') {
        if label.is_empty() {
            return Err(BackendEndpointError::InvalidHostname {
                host: host.to_owned(),
                reason: "labels must not be empty",
            });
        }
        if label.len() > 63 {
            return Err(BackendEndpointError::InvalidHostname {
                host: host.to_owned(),
                reason: "labels must be no more than 63 bytes",
            });
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err(BackendEndpointError::InvalidHostname {
                host: host.to_owned(),
                reason: "labels must not start or end with a hyphen",
            });
        }
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(BackendEndpointError::InvalidHostname {
                host: host.to_owned(),
                reason: "labels may contain only ASCII letters, digits, and hyphens",
            });
        }
    }

    Ok(BackendHost::Name(canonical))
}

mod socket_addr_serde {
    use std::net::SocketAddr;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(address: &SocketAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&address.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse::<SocketAddr>().map_err(|_| {
            serde::de::Error::custom(format!(
                "`{value}` is not a numeric IP socket address; hostnames are not allowed here"
            ))
        })
    }
}
