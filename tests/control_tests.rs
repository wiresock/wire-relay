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
    assert_eq!(
        active_config,
        ControlResponse::ActiveConfig(runtime.active_config())
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
