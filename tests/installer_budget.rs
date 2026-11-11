// SPDX-License-Identifier: AGPL-3.0-or-later

//! Keeps the installer's datagram-budget pre-check identical to the daemon.

use std::{collections::HashMap, fs, path::PathBuf};

use wire_relay::config::{
    Config, DEFAULT_MAX_QUEUE_MEMORY_BYTES, DEFAULT_SESSION_QUEUE_DEPTH, MAX_CONFIGURED_SESSIONS,
    MAX_SESSION_DATAGRAM_MEMORY_BYTES, MAX_SESSION_QUEUE_DEPTH, QUEUED_DATAGRAM_OVERHEAD_BYTES,
    SESSION_FIXED_DATAGRAM_BUFFERS,
};

fn installer_source() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("wire-relay-install.sh");
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn installer_constants(source: &str) -> HashMap<String, String> {
    source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("readonly "))
        .filter_map(|assignment| assignment.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.trim_matches('"').to_owned()))
        .collect()
}

#[test]
fn installer_budget_constants_match_the_daemon() {
    let constants = installer_constants(&installer_source());
    let expect = |name: &str, value: String| {
        assert_eq!(
            constants.get(name),
            Some(&value),
            "wire-relay-install.sh `{name}` must match the daemon"
        );
    };
    expect(
        "MAX_SESSION_DATAGRAM_MEMORY_BYTES",
        MAX_SESSION_DATAGRAM_MEMORY_BYTES.to_string(),
    );
    expect(
        "SESSION_FIXED_DATAGRAM_BUFFERS",
        SESSION_FIXED_DATAGRAM_BUFFERS.to_string(),
    );
    expect(
        "DEFAULT_SESSION_QUEUE_DEPTH",
        DEFAULT_SESSION_QUEUE_DEPTH.to_string(),
    );
    expect(
        "MAX_SESSION_QUEUE_DEPTH",
        MAX_SESSION_QUEUE_DEPTH.to_string(),
    );
    expect(
        "QUEUED_DATAGRAM_OVERHEAD_BYTES",
        QUEUED_DATAGRAM_OVERHEAD_BYTES.to_string(),
    );
    expect(
        "DEFAULT_MAX_QUEUE_MEMORY_BYTES",
        DEFAULT_MAX_QUEUE_MEMORY_BYTES.to_string(),
    );
    expect("MAX_SESSION_LIMIT", MAX_CONFIGURED_SESSIONS.to_string());
}

/// Runs the installer's own budget function against the daemon's validation
/// for boundary combinations.
#[cfg(unix)]
#[test]
fn installer_budget_function_agrees_with_config_validation() {
    use std::process::Command;

    let source = installer_source();
    let function = |name: &str| -> String {
        let start = source
            .find(&format!("\n{name}() {{\n"))
            .unwrap_or_else(|| panic!("installer must define {name}"));
        let body = &source[start + 1..];
        let end = body.find("\n}\n").expect("function must end") + 3;
        body[..end].to_owned()
    };
    let mut script: String = source
        .lines()
        .filter(|line| line.starts_with("readonly ") && !line.contains('$'))
        .map(|line| format!("{line}\n"))
        .collect();
    for name in [
        "is_uint_in_range",
        "is_max_datagram_size",
        "is_positive_limit",
        "is_session_queue_depth",
        "is_session_datagram_budget",
    ] {
        script.push_str(&function(name));
    }

    let cases: &[(usize, usize, usize)] = &[
        (10_000, 4_096, 512),
        (100_000, 4_096, 512),
        (100_000, 65_507, 512),
        (30_000, 65_507, 512),
        (32_000, 65_507, 512),
        (1, 65_507, 65_536),
        (1, 65_507, 65_535),
        (1, 1, 65_536),
        // One full queue against the default queue-memory limit, which the
        // installer does not write: 4094 * 65571 bytes fit, 4095 do not.
        (1, 65_507, 4_094),
        (1, 65_507, 4_095),
        (10_000, 4_096, 65_536),
    ];
    for &(sessions, datagram, depth) in cases {
        let input = format!(
            "[service]\nmax_sessions = {sessions}\nmax_sessions_per_ip = 1\n\
             max_datagram_size = {datagram}\nsession_queue_depth = {depth}\n\n\
             [[listeners]]\nname = \"test\"\nbind = \"127.0.0.1:40001\"\n\
             backend = \"127.0.0.1:51820\"\n"
        );
        let daemon = match Config::parse_str(&input) {
            Ok(_) => true,
            Err(error) => {
                let error = error.to_string();
                assert!(
                    error.contains("datagram buffers") || error.contains("max_queue_memory_bytes"),
                    "unexpected validation failure for {input}: {error}"
                );
                false
            }
        };
        let status = Command::new("bash")
            .arg("-c")
            .arg(format!(
                "{script}\nis_session_datagram_budget {sessions} {datagram} {depth}"
            ))
            .status();
        let Ok(status) = status else {
            eprintln!("bash is unavailable; skipping installer budget comparison");
            return;
        };
        assert_eq!(
            status.success(),
            daemon,
            "installer and daemon disagree for sessions={sessions} datagram={datagram} \
             depth={depth}"
        );
    }
}
