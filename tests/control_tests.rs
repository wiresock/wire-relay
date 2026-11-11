// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{io, net::IpAddr, str::FromStr, time::Duration};

use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use wire_relay::{
    CONTROL_PROTOCOL_VERSION, VERSION,
    control::{
        ControlClient, ControlError, ControlErrorCode, ControlRequest, ControlResponse,
        RequestEnvelope, ResponseEnvelope,
        protocol::{
            MAX_CONTROL_FRAME_SIZE, MAX_SESSION_PAGE_SIZE, SessionCursor, SessionQuery,
            SessionSort, VersionSnapshot, read_frame, write_frame,
        },
    },
    relay::SessionId,
};

#[test]
fn local_cli_version_matches_the_package_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_wire-relay"))
        .arg("--version")
        .output()
        .expect("version CLI must execute");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("wire-relay {VERSION}")
    );
}

#[test]
fn request_variants_round_trip_with_stable_json_tags() {
    let request_id =
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("valid test UUID");
    let session_id =
        SessionId::from_str("00000000-0000-0000-0000-000000000002").expect("valid session ID");
    let requests = [
        ControlRequest::Status,
        ControlRequest::ActiveConfig,
        ControlRequest::ActiveConfigExtended,
        ControlRequest::Listeners,
        ControlRequest::Sessions(SessionQuery {
            listener: Some("germany".to_owned()),
            client_ip: Some(IpAddr::from([198, 51, 100, 20])),
            sort: SessionSort::Bytes,
            cursor: None,
            offset: 7,
            limit: 25,
        }),
        ControlRequest::Session { id: session_id },
        ControlRequest::Stats,
        ControlRequest::Reload,
        ControlRequest::CloseSession { id: session_id },
        ControlRequest::Version,
    ];

    for request in requests {
        let envelope = RequestEnvelope {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request_id,
            request,
        };
        let encoded = serde_json::to_vec(&envelope).expect("request must serialize");
        let decoded =
            serde_json::from_slice::<RequestEnvelope>(&encoded).expect("request must deserialize");
        assert_eq!(decoded, envelope);
    }

    let sessions = RequestEnvelope {
        protocol_version: CONTROL_PROTOCOL_VERSION,
        request_id,
        request: ControlRequest::Sessions(SessionQuery::default()),
    };
    let json = serde_json::to_value(sessions).expect("request must serialize to JSON");
    assert_eq!(json["request"]["operation"], "sessions");
    assert_eq!(json["request"]["parameters"]["sort"], "id");
}

#[test]
fn response_success_and_failure_envelopes_round_trip() {
    let request_id =
        Uuid::parse_str("00000000-0000-0000-0000-000000000003").expect("valid test UUID");
    let success = ResponseEnvelope::success(
        request_id,
        ControlResponse::Version(VersionSnapshot {
            application: "0.1.0".to_owned(),
            protocol: CONTROL_PROTOCOL_VERSION,
        }),
    );
    let encoded = serde_json::to_vec(&success).expect("response must serialize");
    let decoded =
        serde_json::from_slice::<ResponseEnvelope>(&encoded).expect("response must deserialize");
    assert_eq!(decoded, success);
    assert!(matches!(
        decoded.into_result(),
        Ok(ControlResponse::Version(version))
            if version.application == "0.1.0"
                && version.protocol == CONTROL_PROTOCOL_VERSION
    ));

    let failure = ResponseEnvelope::failure(
        request_id,
        ControlError::new(ControlErrorCode::NotFound, "session was not found"),
    );
    let encoded = serde_json::to_value(&failure).expect("error response must serialize");
    assert_eq!(encoded["error"]["code"], "not_found");
    let decoded =
        serde_json::from_value::<ResponseEnvelope>(encoded).expect("error must deserialize");
    assert_eq!(
        decoded.into_result().expect_err("failure must be returned"),
        ControlError::new(ControlErrorCode::NotFound, "session was not found")
    );
}

#[test]
fn session_query_enforces_the_compiled_page_bound() {
    assert_eq!(
        SessionQuery::default().page_size(),
        usize::from(MAX_SESSION_PAGE_SIZE)
    );
    assert_eq!(
        SessionQuery {
            limit: u16::MAX,
            ..SessionQuery::default()
        }
        .page_size(),
        usize::from(MAX_SESSION_PAGE_SIZE)
    );
    assert_eq!(
        SessionQuery {
            limit: 17,
            ..SessionQuery::default()
        }
        .page_size(),
        17
    );
}

#[test]
fn session_cursor_has_stable_string_json_representation() {
    let cursor = SessionCursor::new();
    let query = SessionQuery {
        cursor: Some(cursor),
        offset: 1_024,
        ..SessionQuery::default()
    };
    let json = serde_json::to_value(&query).expect("cursor query must serialize");
    assert_eq!(json["cursor"], cursor.to_string());
    assert_eq!(json["offset"], 1_024);
    assert_eq!(
        serde_json::from_value::<SessionQuery>(json)
            .expect("cursor query must deserialize")
            .cursor,
        Some(cursor)
    );
}

#[tokio::test]
async fn bounded_frame_round_trips_cross_platform() {
    let expected = RequestEnvelope::new(ControlRequest::Stats);
    let (mut writer, mut reader) = tokio::io::duplex(4096);

    write_frame(&mut writer, &expected)
        .await
        .expect("bounded request must be written");
    let decoded = read_frame::<_, RequestEnvelope>(&mut reader)
        .await
        .expect("bounded request must be read");

    assert_eq!(decoded, expected);
}

#[tokio::test]
async fn invalid_frame_lengths_are_rejected_before_reading_a_payload() {
    for length in [
        0_u32,
        u32::try_from(MAX_CONTROL_FRAME_SIZE + 1).expect("frame bound fits u32"),
    ] {
        let (mut writer, mut reader) = tokio::io::duplex(4);
        writer
            .write_u32(length)
            .await
            .expect("test prefix must be written");

        let error = read_frame::<_, RequestEnvelope>(&mut reader)
            .await
            .expect_err("invalid length must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}

#[tokio::test]
async fn malformed_json_frame_is_rejected() {
    let (mut writer, mut reader) = tokio::io::duplex(8);
    writer
        .write_u32(1)
        .await
        .expect("test prefix must be written");
    writer
        .write_all(b"{")
        .await
        .expect("test payload must be written");

    let error = read_frame::<_, RequestEnvelope>(&mut reader)
        .await
        .expect_err("malformed JSON must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn oversized_serialized_frame_is_rejected_without_writing() {
    let oversized = "x".repeat(MAX_CONTROL_FRAME_SIZE);
    let error = write_frame(&mut tokio::io::sink(), &oversized)
        .await
        .expect_err("oversized encoded JSON must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_control_socket_queries_live_runtime_state_and_versions() {
    use std::{fs, net::UdpSocket as StdUdpSocket};

    use tempfile::tempdir;
    use tokio::net::UnixStream;
    use wire_relay::{
        config::Config,
        control::protocol::ActiveConfigSnapshot,
        runtime::{Runtime, RuntimeOptions},
    };

    let temporary = tempdir().expect("temporary directory must be created");
    let config_path = temporary.path().join("config.toml");
    let socket_path = temporary.path().join("control.sock");
    let listener_reservation =
        StdUdpSocket::bind("127.0.0.1:0").expect("listener port must be reserved");
    let listener_addr = listener_reservation
        .local_addr()
        .expect("reserved listener must have an address");
    let control_socket = socket_path.display();
    let config_text = format!(
        r#"
[service]
control_socket = "{control_socket}"
shutdown_timeout = "2s"

[[listeners]]
name = "control-test"
bind = "{listener_addr}"
backend = "127.0.0.1:9"
"#
    );
    fs::write(&config_path, &config_text).expect("configuration must be written");
    let config = Config::parse_str(&config_text)
        .expect("configuration must parse")
        .into_normalized()
        .expect("configuration must normalize");
    drop(listener_reservation);

    let runtime = Runtime::start_with_options(
        config,
        &config_path,
        RuntimeOptions {
            control: true,
            metrics: false,
        },
    )
    .await
    .expect("runtime must start");
    let client = ControlClient::new(&socket_path).with_timeout(Duration::from_secs(5));

    let status = client
        .request(ControlRequest::Status)
        .await
        .expect("status request must succeed");
    let ControlResponse::Status(status) = status else {
        panic!("status request returned the wrong response variant");
    };
    assert_eq!(status.protocol_version, CONTROL_PROTOCOL_VERSION);
    assert_eq!(status.control_socket, socket_path);
    assert_eq!(status.active_sessions, 0);
    assert_eq!(status.listeners.len(), 1);
    assert_eq!(status.listeners[0].name, "control-test");

    let active_config = client
        .request(ControlRequest::ActiveConfig)
        .await
        .expect("active-config request must succeed");
    // The plain request keeps the 0.1.6 shape that older CLIs decode.
    assert_eq!(
        active_config,
        ControlResponse::ActiveConfig(ActiveConfigSnapshot::legacy(&runtime.active_config()))
    );
    let ControlResponse::ActiveConfig(legacy) = &active_config else {
        unreachable!();
    };
    let legacy_service = serde_json::to_value(&legacy.service).expect("service must serialize");
    let mut legacy_keys: Vec<_> = legacy_service
        .as_object()
        .expect("service must be an object")
        .keys()
        .cloned()
        .collect();
    legacy_keys.sort();
    assert_eq!(
        legacy_keys,
        [
            "control_socket",
            "dns_refresh_interval",
            "idle_timeout",
            "log_level",
            "max_datagram_size",
            "max_sessions",
            "max_sessions_per_ip",
            "new_sessions_per_second",
            "shutdown_timeout",
        ]
    );
    let extended = client
        .request(ControlRequest::ActiveConfigExtended)
        .await
        .expect("extended active-config request must succeed");
    assert_eq!(
        extended,
        ControlResponse::ActiveConfigExtended(ActiveConfigSnapshot::extended(
            &runtime.active_config()
        ))
    );
    let config_toml = tokio::process::Command::new(env!("CARGO_BIN_EXE_wire-relay"))
        .args(["config", "--toml", "--control-socket"])
        .arg(&socket_path)
        .output()
        .await
        .expect("config CLI must execute");
    assert!(
        config_toml.status.success(),
        "{}",
        String::from_utf8_lossy(&config_toml.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&config_toml.stdout),
        runtime
            .active_config()
            .to_toml()
            .expect("active configuration must serialize")
    );

    let request_id =
        Uuid::parse_str("00000000-0000-0000-0000-000000000004").expect("valid test UUID");
    let unsupported = RequestEnvelope {
        protocol_version: CONTROL_PROTOCOL_VERSION.saturating_add(1),
        request_id,
        request: ControlRequest::Stats,
    };
    let mut stream = UnixStream::connect(&socket_path)
        .await
        .expect("raw control client must connect");
    write_frame(&mut stream, &unsupported)
        .await
        .expect("unsupported-version request must be written");
    let response = read_frame::<_, ResponseEnvelope>(&mut stream)
        .await
        .expect("unsupported-version response must be read");
    assert_eq!(response.request_id, request_id);
    assert_eq!(
        response
            .into_result()
            .expect_err("unsupported version must fail")
            .code,
        ControlErrorCode::UnsupportedVersion
    );

    let executable = env!("CARGO_BIN_EXE_wire-relay");
    let live_version = tokio::process::Command::new(executable)
        .args(["version", "--control-socket"])
        .arg(&socket_path)
        .output()
        .await
        .expect("version CLI must execute");
    assert!(
        live_version.status.success(),
        "{}",
        String::from_utf8_lossy(&live_version.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&live_version.stdout).trim(),
        format!("wire-relay {VERSION} (control protocol {CONTROL_PROTOCOL_VERSION})")
    );

    runtime.shutdown().await.expect("runtime must shut down");
    assert!(
        !socket_path.exists(),
        "runtime shutdown must remove the owned control socket"
    );

    let stopped_version = tokio::process::Command::new(executable)
        .args(["version", "--control-socket"])
        .arg(&socket_path)
        .output()
        .await
        .expect("version CLI must execute after shutdown");
    assert!(
        !stopped_version.status.success(),
        "version subcommand must query the daemon rather than report the local binary"
    );
}

#[cfg(not(unix))]
#[tokio::test]
async fn control_client_reports_unsupported_platform_without_connecting() {
    let error = ControlClient::new("unused-control.sock")
        .with_timeout(Duration::from_millis(10))
        .request(ControlRequest::Status)
        .await
        .expect_err("non-Unix control client must fail");
    assert_eq!(error.code, ControlErrorCode::Unavailable);
    assert!(error.message.contains("unsupported"));
}

/// A 0.1.6 daemon: it answers `active_config` with its real response and,
/// like any daemon that cannot decode a request, closes the connection
/// without a response to an operation it does not know.
#[cfg(unix)]
async fn serve_as_old_daemon(listener: tokio::net::UnixListener) {
    const RESPONSE: &str = r#"{"protocol_version":2,"request_id":"00000000-0000-0000-0000-000000000001","response":{"kind":"active_config","data":{"service":{"control_socket":"/run/wire-relay/control.sock","log_level":"info","idle_timeout":"3m","max_datagram_size":4096,"max_sessions":10000,"max_sessions_per_ip":64,"new_sessions_per_second":100,"dns_refresh_interval":"1m","shutdown_timeout":"10s"},"listeners":[{"name":"t","bind":"127.0.0.1:47811","backend":"127.0.0.1:47812"}]}}}"#;
    while let Ok((mut stream, _)) = listener.accept().await {
        let Ok(request) = read_frame::<_, serde_json::Value>(&mut stream).await else {
            continue;
        };
        if request["request"]["operation"] != "active_config" {
            continue;
        }
        let mut response: serde_json::Value = serde_json::from_str(RESPONSE).unwrap();
        response["request_id"] = request["request_id"].clone();
        write_frame(&mut stream, &response).await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn config_cli_falls_back_for_an_old_daemon_without_inventing_settings() {
    let temporary = tempfile::tempdir().expect("temporary directory must be created");
    let socket_path = temporary.path().join("old.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("fake daemon must bind");
    let daemon = tokio::spawn(serve_as_old_daemon(listener));

    let response = ControlClient::new(&socket_path)
        .request_with_fallback(
            ControlRequest::ActiveConfigExtended,
            ControlRequest::ActiveConfig,
        )
        .await
        .expect("the client must fall back to the plain request");
    let ControlResponse::ActiveConfig(config) = response else {
        panic!("unexpected response {response:?}");
    };
    assert_eq!(config.service.session_queue_depth, None);
    assert_eq!(config.service.max_queue_memory_bytes, None);

    let executable = env!("CARGO_BIN_EXE_wire-relay");
    for format in ["--toml", "--json"] {
        let output = tokio::process::Command::new(executable)
            .args(["config", format, "--control-socket"])
            .arg(&socket_path)
            .output()
            .await
            .expect("config CLI must execute");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{format}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout.contains("max_datagram_size"), "{format}: {stdout}");
        // The settings appear only in the comment that says they are absent.
        let settings: Vec<_> = stdout
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| {
                line.contains("session_queue_depth") || line.contains("max_queue_memory")
            })
            .collect();
        assert!(settings.is_empty(), "{format}: {stdout}");
        if format == "--toml" {
            assert!(
                stdout.starts_with(
                    "# The running daemon predates service.session_queue_depth and \
                     service.max_queue_memory_bytes; not shown.\n"
                ),
                "{stdout}"
            );
        }
    }

    // A daemon that is not there is an error, not a reason to fall back.
    let error = ControlClient::new(temporary.path().join("missing.sock"))
        .request_with_fallback(
            ControlRequest::ActiveConfigExtended,
            ControlRequest::ActiveConfig,
        )
        .await
        .expect_err("a missing daemon must fail");
    assert_eq!(error.code, ControlErrorCode::Unavailable);
    daemon.abort();
}

/// What a scripted daemon does with one connection.
#[cfg(unix)]
enum Reply {
    /// Close without a response, as a daemon that cannot decode a request.
    Close,
    /// Send this response, with the request's ID filled in.
    Respond(serde_json::Value),
    /// Send the start of a response frame and close in the middle of it.
    BreakOff,
}

/// A daemon that reads each request and answers as `script` says, given the
/// connection's index and the requested operation; it records every
/// operation it was asked for.
#[cfg(unix)]
fn serve_scripted(
    listener: tokio::net::UnixListener,
    script: impl Fn(usize, &str) -> Reply + Send + 'static,
) -> (
    tokio::task::JoinHandle<()>,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = std::sync::Arc::clone(&seen);
    let daemon = tokio::spawn(async move {
        let mut index = 0;
        while let Ok((mut stream, _)) = listener.accept().await {
            let Ok(request) = read_frame::<_, serde_json::Value>(&mut stream).await else {
                continue;
            };
            let operation = request["request"]["operation"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            log.lock().unwrap().push(operation.clone());
            match script(index, &operation) {
                Reply::Close => {}
                Reply::Respond(mut response) => {
                    response["request_id"] = request["request_id"].clone();
                    write_frame(&mut stream, &response).await.unwrap();
                }
                Reply::BreakOff => {
                    stream.write_u32(64).await.unwrap();
                    stream.write_all(b"{\"protocol_version\"").await.unwrap();
                    stream.flush().await.unwrap();
                }
            }
            index += 1;
        }
    });
    (daemon, seen)
}

/// The current daemon's `active_config_extended` (or, with `legacy`,
/// `active_config`) response for a configuration that is only valid with its
/// explicit queue settings.
#[cfg(unix)]
fn config_response(legacy: bool) -> serde_json::Value {
    use wire_relay::control::protocol::ActiveConfigSnapshot;

    let config = wire_relay::config::Config::parse_str(
        "[service]\nmax_datagram_size = 65507\nsession_queue_depth = 16\n\
         max_queue_memory_bytes = 2097152\n\n[[listeners]]\nname = \"t\"\n\
         bind = \"127.0.0.1:47811\"\nbackend = \"127.0.0.1:47812\"\n",
    )
    .expect("test configuration must be valid")
    .into_normalized()
    .expect("test configuration must normalize");
    // At the default queue depth the same budget would be invalid, so a CLI
    // that fell back would fail or show the wrong settings.
    let response = if legacy {
        ControlResponse::ActiveConfig(ActiveConfigSnapshot::legacy(&config))
    } else {
        ControlResponse::ActiveConfigExtended(ActiveConfigSnapshot::extended(&config))
    };
    serde_json::to_value(ResponseEnvelope::success(Uuid::nil(), response)).unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn config_retries_an_unanswered_request_before_falling_back() {
    let temporary = tempfile::tempdir().expect("temporary directory must be created");
    let socket_path = temporary.path().join("restarting.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("fake daemon must bind");
    // A current daemon that drops every other connection unanswered, as one
    // that is restarting might.
    let response = config_response(false);
    let (daemon, seen) = serve_scripted(listener, move |index, operation| {
        match (index % 2, operation) {
            (0, _) => Reply::Close,
            (_, "active_config_extended") => Reply::Respond(response.clone()),
            _ => Reply::Close,
        }
    });

    let reply = ControlClient::new(&socket_path)
        .request_with_fallback(
            ControlRequest::ActiveConfigExtended,
            ControlRequest::ActiveConfig,
        )
        .await
        .expect("the retried request must be answered");
    let ControlResponse::ActiveConfigExtended(config) = reply else {
        panic!("unexpected response {reply:?}");
    };
    assert_eq!(config.service.session_queue_depth, Some(16));
    assert_eq!(config.service.max_queue_memory_bytes, Some(2_097_152));
    assert_eq!(
        *seen.lock().unwrap(),
        ["active_config_extended", "active_config_extended"]
    );

    let executable = env!("CARGO_BIN_EXE_wire-relay");
    for format in ["--toml", "--json"] {
        let output = tokio::process::Command::new(executable)
            .args(["config", format, "--control-socket"])
            .arg(&socket_path)
            .output()
            .await
            .expect("config CLI must execute");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{format}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!stdout.contains("predates"), "{format}: {stdout}");
        assert!(stdout.contains("session_queue_depth"), "{format}: {stdout}");
        assert!(stdout.contains("2097152"), "{format}: {stdout}");
    }
    assert!(
        !seen
            .lock()
            .unwrap()
            .iter()
            .any(|operation| operation == "active_config"),
        "a daemon that answers the retry must not be asked for the legacy shape"
    );
    daemon.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn a_response_that_breaks_off_is_an_error_not_a_fallback() {
    let temporary = tempfile::tempdir().expect("temporary directory must be created");
    let socket_path = temporary.path().join("broken.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("fake daemon must bind");
    let response = config_response(false);
    let (daemon, seen) = serve_scripted(listener, move |_, operation| {
        if operation == "active_config_extended" {
            Reply::BreakOff
        } else {
            Reply::Respond(response.clone())
        }
    });

    let error = ControlClient::new(&socket_path)
        .request_with_fallback(
            ControlRequest::ActiveConfigExtended,
            ControlRequest::ActiveConfig,
        )
        .await
        .expect_err("a response cut off mid-frame must fail");
    assert_eq!(error.code, ControlErrorCode::Unavailable);
    assert_eq!(*seen.lock().unwrap(), ["active_config_extended"]);
    daemon.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn an_old_daemon_is_asked_twice_before_the_fallback() {
    let temporary = tempfile::tempdir().expect("temporary directory must be created");
    let socket_path = temporary.path().join("old-scripted.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("fake daemon must bind");
    let (daemon, seen) = serve_scripted(listener, |_, operation| {
        if operation == "active_config" {
            Reply::Respond(config_response(true))
        } else {
            Reply::Close
        }
    });
    let reply = ControlClient::new(&socket_path)
        .request_with_fallback(
            ControlRequest::ActiveConfigExtended,
            ControlRequest::ActiveConfig,
        )
        .await
        .expect("the client must fall back after two unanswered requests");
    assert!(matches!(reply, ControlResponse::ActiveConfig(_)));
    assert_eq!(
        *seen.lock().unwrap(),
        [
            "active_config_extended",
            "active_config_extended",
            "active_config"
        ]
    );
    daemon.abort();
}
