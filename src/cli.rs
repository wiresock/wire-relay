// SPDX-License-Identifier: AGPL-3.0-or-later

//! Clap command model, daemon queries, and stable human/JSON output.

use std::{
    fmt::Write as _,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::info;

use crate::{
    VERSION,
    config::{Config, DEFAULT_CONFIG_PATH, DEFAULT_CONTROL_SOCKET, LogLevel, NormalizedConfig},
    control::{
        ControlClient, ControlRequest, ControlResponse,
        protocol::{ReloadResult, SessionQuery, SessionSort, StatusSnapshot},
    },
    logging,
    metrics::MetricsSnapshot,
    relay::{ListenerSnapshot, SessionId, SessionSnapshot},
    runtime::Runtime,
    shutdown,
};

/// WireRelay command-line interface.
#[derive(Debug, Parser)]
#[command(name = "wire-relay", version = VERSION, about, long_about = None)]
pub struct Cli {
    /// Configuration file used by run/check-config. Daemon-query commands use
    /// it only to locate the control socket.
    #[arg(
        short,
        long,
        global = true,
        default_value = DEFAULT_CONFIG_PATH,
        value_name = "PATH"
    )]
    pub config: PathBuf,

    /// Override the control socket used by daemon-query commands.
    #[arg(long, global = true, value_name = "PATH")]
    pub control_socket: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the relay daemon in the foreground.
    Run,
    /// Parse and validate configuration without starting the daemon.
    CheckConfig,
    /// Show daemon and listener status.
    Show(JsonOutput),
    /// Show the active, daemon-owned normalized configuration.
    Config(ConfigOutput),
    /// List configured runtime listeners.
    Listeners(JsonOutput),
    /// List active client mappings.
    Sessions(SessionsArgs),
    /// Inspect or close one active session.
    Session(SessionArgs),
    /// Show cumulative runtime statistics.
    Stats(JsonOutput),
    /// Transactionally reload the daemon configuration.
    Reload(JsonOutput),
    /// Show the running daemon's application and control protocol versions.
    Version,
}

#[derive(Clone, Copy, Debug, Default, Args)]
pub struct JsonOutput {
    /// Emit stable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, Default, Args)]
pub struct ConfigOutput {
    /// Emit stable JSON.
    #[arg(long, conflicts_with = "toml")]
    pub json: bool,
    /// Emit normalized TOML.
    #[arg(long, conflicts_with = "json")]
    pub toml: bool,
}

#[derive(Debug, Args)]
pub struct SessionsArgs {
    /// Filter by listener name.
    #[arg(long)]
    pub listener: Option<String>,
    /// Filter by client source IP.
    #[arg(long)]
    pub client: Option<IpAddr>,
    /// Sort active mappings.
    #[arg(long, value_enum, default_value_t = SessionSortArg::Id)]
    pub sort: SessionSortArg,
    /// Emit stable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum SessionSortArg {
    #[default]
    Id,
    Bytes,
    Age,
    Idle,
}

impl From<SessionSortArg> for SessionSort {
    fn from(value: SessionSortArg) -> Self {
        match value {
            SessionSortArg::Id => Self::Id,
            SessionSortArg::Bytes => Self::Bytes,
            SessionSortArg::Age => Self::Age,
            SessionSortArg::Idle => Self::Idle,
        }
    }
}

#[derive(Debug, Args)]
pub struct SessionArgs {
    #[command(subcommand)]
    pub command: SessionCommand,
}

#[derive(Debug, Subcommand)]
pub enum SessionCommand {
    /// Show one mapping in detail.
    Show {
        session_id: SessionId,
        /// Emit stable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Close one mapping.
    Close { session_id: SessionId },
}

/// Parse and execute one command.
pub async fn execute(cli: Cli) -> Result<()> {
    if cli.control_socket.is_some() && matches!(&cli.command, Command::Run | Command::CheckConfig) {
        bail!("--control-socket is only valid with daemon-query commands");
    }

    match cli.command {
        Command::Run => run_daemon(&cli.config).await,
        Command::CheckConfig => check_config(&cli.config).await,
        command => {
            let client = discover_control_client(&cli.config, cli.control_socket.as_deref()).await;
            execute_control(command, client).await
        }
    }
}

async fn run_daemon(path: &Path) -> Result<()> {
    let config = load_config(path).await?;
    init_tracing(config.service.log_level)?;
    let normalized = config
        .into_normalized()
        .context("configuration validation failed")?;
    info!(
        version = VERSION,
        config = %path.display(),
        "starting WireRelay"
    );
    let runtime = Runtime::start(normalized, path.to_path_buf())
        .await
        .context("failed to start WireRelay")?;
    let signal = shutdown::wait_for_signal()
        .await
        .context("failed to install or receive shutdown signal")?;
    info!(signal, "shutdown requested");
    runtime.shutdown().await.context("graceful shutdown failed")
}

async fn check_config(path: &Path) -> Result<()> {
    let config = load_config(path).await?;
    let normalized = config.normalized()?;
    println!(
        "{}: configuration is valid ({} listener{})",
        path.display(),
        normalized.listeners.len(),
        if normalized.listeners.len() == 1 {
            ""
        } else {
            "s"
        }
    );
    Ok(())
}

async fn execute_control(command: Command, client: ControlClient) -> Result<()> {
    match command {
        Command::Show(output) => {
            let status = expect_status(client.request(ControlRequest::Status).await?)?;
            print_json_or(output.json, &status, || render_status(&status))
        }
        Command::Config(output) => {
            let config = expect_config(client.request(ControlRequest::ActiveConfig).await?)?;
            if output.json {
                println!("{}", serde_json::to_string_pretty(&config)?);
            } else {
                // Normalized TOML is also the most useful default human form.
                print!("{}", config.to_toml()?);
            }
            Ok(())
        }
        Command::Listeners(output) => {
            let listeners = expect_listeners(client.request(ControlRequest::Listeners).await?)?;
            print_json_or(output.json, &listeners, || render_listeners(&listeners))
        }
        Command::Sessions(arguments) => {
            let client = client.with_timeout(Duration::from_secs(30));
            let sessions = fetch_sessions(
                &client,
                SessionQuery {
                    listener: arguments.listener,
                    client_ip: arguments.client,
                    sort: arguments.sort.into(),
                    cursor: None,
                    offset: 0,
                    limit: 0,
                },
            )
            .await?;
            print_json_or(arguments.json, &sessions, || render_sessions(&sessions))
        }
        Command::Session(arguments) => match arguments.command {
            SessionCommand::Show { session_id, json } => {
                let session = expect_session(
                    client
                        .request(ControlRequest::Session { id: session_id })
                        .await?,
                )?;
                print_json_or(json, &session, || render_session(&session))
            }
            SessionCommand::Close { session_id } => {
                match client
                    .request(ControlRequest::CloseSession { id: session_id })
                    .await?
                {
                    ControlResponse::SessionClosed { id } => {
                        println!("closed session {id}");
                        Ok(())
                    }
                    other => Err(unexpected_response("session close", &other)),
                }
            }
        },
        Command::Stats(output) => {
            let stats = expect_stats(client.request(ControlRequest::Stats).await?)?;
            print_json_or(output.json, &stats, || render_stats(&stats))
        }
        Command::Reload(output) => {
            let result = expect_reload(
                client
                    .with_timeout(Duration::from_secs(30))
                    .request(ControlRequest::Reload)
                    .await?,
            )?;
            print_json_or(output.json, &result, || render_reload(&result))
        }
        Command::Version => match client.request(ControlRequest::Version).await? {
            ControlResponse::Version(version) => {
                println!(
                    "wire-relay {} (control protocol {})",
                    version.application, version.protocol
                );
                Ok(())
            }
            other => Err(unexpected_response("version", &other)),
        },
        Command::Run | Command::CheckConfig => {
            bail!("internal CLI dispatch error")
        }
    }
}

async fn discover_control_client(config_path: &Path, explicit: Option<&Path>) -> ControlClient {
    if let Some(path) = explicit {
        return ControlClient::new(path);
    }

    let default_path = PathBuf::from(DEFAULT_CONTROL_SOCKET);
    let configured_path = try_load_control_socket(config_path).await;
    let mut candidates = Vec::with_capacity(2);
    if config_path == Path::new(DEFAULT_CONFIG_PATH) {
        candidates.push(default_path.clone());
        if configured_path
            .as_ref()
            .is_some_and(|path| path != &default_path)
        {
            candidates.extend(configured_path);
        }
    } else {
        if let Some(path) = configured_path {
            candidates.push(path);
        }
        if !candidates.contains(&default_path) {
            candidates.push(default_path);
        }
    }

    for path in &candidates {
        let probe = ControlClient::new(path).with_timeout(Duration::from_secs(1));
        if probe.request(ControlRequest::Version).await.is_ok() {
            return ControlClient::new(path);
        }
    }

    ControlClient::new(
        candidates
            .into_iter()
            .next()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONTROL_SOCKET)),
    )
}

async fn try_load_control_socket(path: &Path) -> Option<PathBuf> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || Config::load(path))
        .await
        .ok()
        .and_then(Result::ok)
        .map(|config| config.service.control_socket)
}

async fn load_config(path: &Path) -> Result<Config> {
    let path = path.to_path_buf();
    let display_path = path.clone();
    tokio::task::spawn_blocking(move || Config::load(&path))
        .await
        .context("configuration reader task failed")?
        .with_context(|| format!("failed to load `{}`", display_path.display()))
}

fn init_tracing(level: LogLevel) -> Result<()> {
    logging::init(level).map_err(|error| anyhow!(error))
}

async fn fetch_sessions(
    client: &ControlClient,
    mut query: SessionQuery,
) -> Result<Vec<SessionSnapshot>> {
    let mut sessions = Vec::new();
    let mut snapshot_total = None;
    loop {
        let page = match client
            .request(ControlRequest::Sessions(query.clone()))
            .await?
        {
            ControlResponse::Sessions(page) => page,
            other => return Err(unexpected_response("sessions", &other)),
        };
        if snapshot_total.is_some_and(|total| total != page.total) {
            bail!("daemon changed the total within one session snapshot");
        }
        snapshot_total = Some(page.total);
        sessions.extend(page.sessions);
        match (page.next_cursor, page.next_offset) {
            (None, None) => break,
            (Some(cursor), Some(next_offset)) => {
                if next_offset <= query.offset {
                    bail!("daemon returned a non-advancing session cursor");
                }
                query.cursor = Some(cursor);
                query.offset = next_offset;
            }
            _ => bail!("daemon returned incomplete session cursor metadata"),
        }
    }
    Ok(sessions)
}

fn print_json_or<T, F>(json: bool, value: &T, human: F) -> Result<()>
where
    T: serde::Serialize,
    F: FnOnce() -> String,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        print!("{}", human());
    }
    Ok(())
}

fn render_status(status: &StatusSnapshot) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "wire-relay {}", status.version);
    let _ = writeln!(output, "  uptime: {}", format_duration(status.uptime_ms));
    let _ = writeln!(output, "  sessions: {}", status.active_sessions);
    let _ = writeln!(
        output,
        "  control socket: {}",
        status.control_socket.display()
    );
    if status.listeners.is_empty() {
        let _ = writeln!(output, "  listeners: none");
    } else {
        let _ = writeln!(output);
        for listener in &status.listeners {
            let _ = writeln!(output, "listener: {}", listener.name);
            let _ = writeln!(output, "  bind: {}", listener.bind);
            let _ = writeln!(output, "  backend: {}", listener.configured_backend);
            let _ = writeln!(
                output,
                "  resolved: {}",
                listener
                    .resolved_backend
                    .map_or_else(|| "unavailable".to_owned(), |address| address.to_string())
            );
            let _ = writeln!(output, "  status: {:?}", listener.status);
            let _ = writeln!(
                output,
                "  DNS age: {}",
                listener
                    .dns
                    .last_success_age_ms
                    .map_or_else(|| "never".to_owned(), format_duration)
            );
            let _ = writeln!(output, "  sessions: {}", listener.counters.active_sessions);
            let _ = writeln!(
                output,
                "  traffic: {} to backend, {} to client",
                format_bytes(listener.counters.bytes_to_backend),
                format_bytes(listener.counters.bytes_to_client)
            );
        }
    }
    output
}

fn render_listeners(listeners: &[ListenerSnapshot]) -> String {
    if listeners.is_empty() {
        return "no listeners\n".to_owned();
    }
    let mut output = String::from(
        "NAME                 BIND                     BACKEND                  RESOLVED                 STATUS       SESSIONS\n",
    );
    for listener in listeners {
        let resolved = listener
            .resolved_backend
            .map_or_else(|| "-".to_owned(), |address| address.to_string());
        let _ = writeln!(
            output,
            "{:<20} {:<24} {:<24} {:<24} {:<12?} {}",
            listener.name,
            listener.bind,
            listener.configured_backend,
            resolved,
            listener.status,
            listener.counters.active_sessions
        );
    }
    output
}

fn render_sessions(sessions: &[SessionSnapshot]) -> String {
    if sessions.is_empty() {
        return "no active sessions\n".to_owned();
    }
    let mut output = String::from(
        "SESSION ID                           LISTENER             CLIENT                    BACKEND                   AGE       IDLE      TO BACKEND  TO CLIENT\n",
    );
    for session in sessions {
        let _ = writeln!(
            output,
            "{:<36} {:<20} {:<25} {:<25} {:<9} {:<9} {:<11} {}",
            session.id,
            session.listener,
            session.client_addr,
            session.backend_addr,
            format_duration(session.age_ms),
            format_duration(session.idle_ms),
            format_bytes(session.bytes_to_backend),
            format_bytes(session.bytes_to_client)
        );
    }
    output
}

fn render_session(session: &SessionSnapshot) -> String {
    format!(
        "session: {}\n  listener: {}\n  client: {}\n  upstream local: {}\n  backend: {}\n  age: {}\n  idle: {}\n  packets: {} to backend, {} to client\n  bytes: {} to backend, {} to client\n",
        session.id,
        session.listener,
        session.client_addr,
        session.upstream_local_addr,
        session.backend_addr,
        format_duration(session.age_ms),
        format_duration(session.idle_ms),
        session.packets_to_backend,
        session.packets_to_client,
        format_bytes(session.bytes_to_backend),
        format_bytes(session.bytes_to_client)
    )
}

fn render_stats(stats: &MetricsSnapshot) -> String {
    format!(
        "active sessions: {}\nsessions created: {}\nsessions expired: {}\nsessions closed: {}\nsessions rejected: {}\npackets to backend: {}\npackets to client: {}\nbytes to backend: {}\nbytes to client: {}\ndatagrams dropped: {}\nrate limited: {}\nDNS errors: {}\nsocket errors: {}\n",
        stats.active_sessions,
        stats.sessions_created_total,
        stats.sessions_expired_total,
        stats.sessions_closed_total,
        stats.sessions_rejected_total,
        stats.packets_to_backend_total,
        stats.packets_to_client_total,
        stats.bytes_to_backend_total,
        stats.bytes_to_client_total,
        stats.datagrams_dropped_total,
        stats.rate_limited_total,
        stats.dns_errors_total,
        stats.socket_errors_total
    )
}

fn render_reload(result: &ReloadResult) -> String {
    format!(
        "{}\n  preserved: {}\n  added: {}\n  modified: {}\n  removed: {}\n  sessions closed: {}\n",
        result.message,
        display_names(&result.preserved),
        display_names(&result.added),
        display_names(&result.modified),
        display_names(&result.removed),
        result.sessions_closed
    )
}

fn display_names(names: &[String]) -> String {
    if names.is_empty() {
        "-".to_owned()
    } else {
        names.join(", ")
    }
}

fn format_duration(milliseconds: u64) -> String {
    humantime::format_duration(Duration::from_millis(milliseconds)).to_string()
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn expect_status(response: ControlResponse) -> Result<StatusSnapshot> {
    match response {
        ControlResponse::Status(value) => Ok(value),
        other => Err(unexpected_response("status", &other)),
    }
}

fn expect_config(response: ControlResponse) -> Result<NormalizedConfig> {
    match response {
        ControlResponse::ActiveConfig(value) => Ok(value),
        other => Err(unexpected_response("active config", &other)),
    }
}

fn expect_listeners(response: ControlResponse) -> Result<Vec<ListenerSnapshot>> {
    match response {
        ControlResponse::Listeners(value) => Ok(value),
        other => Err(unexpected_response("listeners", &other)),
    }
}

fn expect_session(response: ControlResponse) -> Result<SessionSnapshot> {
    match response {
        ControlResponse::Session(value) => Ok(value),
        other => Err(unexpected_response("session", &other)),
    }
}

fn expect_stats(response: ControlResponse) -> Result<MetricsSnapshot> {
    match response {
        ControlResponse::Stats(value) => Ok(value),
        other => Err(unexpected_response("stats", &other)),
    }
}

fn expect_reload(response: ControlResponse) -> Result<ReloadResult> {
    match response {
        ControlResponse::Reload(value) => Ok(value),
        other => Err(unexpected_response("reload", &other)),
    }
}

fn unexpected_response(operation: &str, response: &ControlResponse) -> anyhow::Error {
    anyhow!("daemon returned an unexpected response for {operation}: {response:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dns::DnsSnapshot,
        relay::{ListenerId, ListenerStatus, session::ListenerCounterSnapshot},
    };

    #[test]
    fn cli_models_accept_required_shapes() {
        Cli::try_parse_from(["wire-relay", "sessions", "--client", "198.51.100.20"])
            .expect("sessions command");
        Cli::try_parse_from([
            "wire-relay",
            "session",
            "show",
            "00000000-0000-0000-0000-000000000001",
            "--json",
        ])
        .expect("session show command");
        Cli::try_parse_from(["wire-relay", "config", "--toml"]).expect("config TOML command");
    }

    #[test]
    fn human_listener_output_contains_runtime_dns_state() {
        let listener = ListenerSnapshot {
            id: ListenerId::new(1),
            name: "germany".to_owned(),
            bind: "127.0.0.1:40001".parse().unwrap(),
            configured_backend: "de.example.com:51820".to_owned(),
            resolved_backend: Some("192.0.2.1:51820".parse().unwrap()),
            status: ListenerStatus::Available,
            dns: DnsSnapshot {
                configured_backend: "de.example.com:51820".to_owned(),
                resolved_backend: Some("192.0.2.1:51820".parse().unwrap()),
                available: true,
                last_success_age_ms: Some(1000),
                last_attempt_age_ms: 1000,
                last_error: None,
            },
            counters: ListenerCounterSnapshot::default(),
        };
        let rendered = render_listeners(&[listener]);
        assert!(rendered.contains("germany"));
        assert!(rendered.contains("192.0.2.1:51820"));
    }

    #[tokio::test]
    async fn local_commands_reject_a_control_socket_override() {
        for command in ["run", "check-config"] {
            let cli =
                Cli::try_parse_from(["wire-relay", command, "--control-socket", "ignored.sock"])
                    .expect("command shape must parse before semantic validation");
            let error = execute(cli)
                .await
                .expect_err("local command must reject a daemon-query option");
            assert!(
                error
                    .to_string()
                    .contains("only valid with daemon-query commands")
            );
        }
    }
}
