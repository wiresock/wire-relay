// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use wire_relay::config::{
    BackendEndpoint, BackendEndpointError, BackendHost, Config, ConfigError,
    DEFAULT_CONTROL_SOCKET, DEFAULT_MAX_QUEUE_MEMORY_BYTES, DEFAULT_SESSION_QUEUE_DEPTH,
    MAX_CONFIGURED_SESSIONS, MAX_LISTENERS, MAX_NEW_SESSIONS_PER_SECOND, MAX_SESSION_QUEUE_DEPTH,
    MAX_UDP_DATAGRAM_SIZE, ServiceConfig,
};
use wire_relay::relay::session::QueueBudget;

/// The whole defensive budget: a queue-memory limit that never applies, for
/// tests of the receive-buffer reservation behind it.
const UNCAPPED_QUEUE_MEMORY: u64 = 4 * 1024 * 1024 * 1024;

const MINIMAL: &str = r#"
[[listeners]]
name = "germany"
bind = "0.0.0.0:40001"
backend = "de.example.com:51820"
"#;

fn listener(name: &str, bind: &str, backend: &str) -> String {
    format!(
        r#"
[[listeners]]
name = "{name}"
bind = "{bind}"
backend = "{backend}"
"#
    )
}

#[test]
fn service_defaults_are_materialized() {
    let config = Config::parse_str(MINIMAL).expect("minimal configuration should be valid");

    assert_eq!(
        config.service.control_socket.to_string_lossy(),
        DEFAULT_CONTROL_SOCKET
    );
    assert_eq!(config.service.log_level.as_str(), "info");
    assert_eq!(config.service.idle_timeout.as_secs(), 180);
    assert_eq!(config.service.max_datagram_size, 4096);
    assert_eq!(
        config.service.session_queue_depth,
        DEFAULT_SESSION_QUEUE_DEPTH
    );
    assert_eq!(config.service.max_sessions, 10_000);
    assert_eq!(config.service.max_sessions_per_ip, 64);
    assert_eq!(config.service.new_sessions_per_second, 100);
    assert_eq!(config.service.dns_refresh_interval.as_secs(), 60);
    assert_eq!(config.service.shutdown_timeout, Duration::from_secs(10));
    assert!(config.metrics.is_none());
}

#[test]
fn complete_service_and_metrics_configuration_parses() {
    let input = r#"
[service]
control_socket = "/run/wire-relay/custom.sock"
log_level = "debug"
idle_timeout = "2m 30s"
max_datagram_size = 65507
max_sessions = 200
max_sessions_per_ip = 20
new_sessions_per_second = 50
dns_refresh_interval = "45s"
shutdown_timeout = "5s"

[metrics]
enabled = true
bind = "[::1]:9090"

[[listeners]]
name = "v6"
bind = "[2001:db8::1]:40001"
backend = "[2001:db8::20]:51820"
"#;

    let config = Config::parse_str(input).expect("complete configuration should be valid");
    assert_eq!(config.service.idle_timeout, Duration::from_secs(150));
    assert_eq!(config.service.max_datagram_size, MAX_UDP_DATAGRAM_SIZE);
    assert!(config.metrics.as_ref().expect("metrics section").enabled);
    assert_eq!(
        config.listeners[0].backend.ip_addr(),
        Some(IpAddr::V6(
            "2001:db8::20".parse().expect("test IPv6 literal")
        ))
    );
}

#[test]
fn malformed_toml_is_reported() {
    let error =
        Config::parse_str("[service\nlog_level = \"info\"").expect_err("malformed TOML must fail");
    assert!(matches!(&error, ConfigError::Parse { .. }));
    assert!(error.to_string().contains("malformed TOML"));
}

#[test]
fn unknown_fields_are_rejected_at_every_level() {
    let cases = [
        format!("unexpected = true\n{MINIMAL}"),
        format!("[service]\nunexpected = true\n{MINIMAL}"),
        format!("[metrics]\nunexpected = true\n{MINIMAL}"),
        listener("one", "127.0.0.1:40001", "example.com:51820") + "unexpected = true\n",
    ];

    for input in cases {
        let error = Config::parse_str(&input).expect_err("unknown field must fail");
        assert!(matches!(&error, ConfigError::Parse { .. }));
        assert!(error.to_string().contains("unknown field"));
    }
}

#[test]
fn required_listener_fields_and_nonempty_listener_set_are_enforced() {
    assert!(matches!(
        Config::parse_str("[service]\nlog_level = \"info\""),
        Err(ConfigError::NoListeners)
    ));

    let missing_backend = r#"
[[listeners]]
name = "missing"
bind = "127.0.0.1:40001"
"#;
    assert!(matches!(
        Config::parse_str(missing_backend),
        Err(ConfigError::Parse { .. })
    ));
}

#[test]
fn duplicate_listener_names_are_rejected() {
    let input = listener("same", "127.0.0.1:40001", "one.example:51820")
        + &listener("same", "127.0.0.2:40002", "two.example:51820");
    let error = Config::parse_str(&input).expect_err("duplicate name must fail");

    assert!(matches!(
        error,
        ConfigError::DuplicateListenerName { ref name } if name == "same"
    ));
}

#[test]
fn exact_and_wildcard_bind_conflicts_are_rejected() {
    let cases = [
        ("127.0.0.1:40001", "127.0.0.1:40001", "identical bind"),
        ("0.0.0.0:40001", "192.0.2.20:40001", "IPv4 wildcard"),
        ("[::]:40001", "[2001:db8::20]:40001", "IPv6 wildcard"),
        ("[::]:40001", "192.0.2.20:40001", "dual-stack IPv6 wildcard"),
    ];

    for (first, second, description) in cases {
        let input = listener("first", first, "one.example:51820")
            + &listener("second", second, "two.example:51820");
        let result = Config::parse_str(&input);
        assert!(
            matches!(&result, Err(ConfigError::ConflictingBindAddresses { .. })),
            "{description} must conflict: {result:?}"
        );
    }
}

#[test]
fn distinct_specific_addresses_can_share_a_port() {
    let input = listener("first", "192.0.2.1:40001", "one.example:51820")
        + &listener("second", "192.0.2.2:40001", "two.example:51820");
    Config::parse_str(&input).expect("specific addresses do not conflict");
}

#[test]
fn listener_bind_must_be_a_numeric_nonzero_socket_address() {
    for bind in [
        "relay.example.com:40001",
        "127.0.0.1",
        "127.0.0.1:0",
        "[2001:db8::1]",
        "2001:db8::1:40001",
    ] {
        let input = listener("bad-bind", bind, "backend.example:51820");
        let error = Config::parse_str(&input).expect_err("invalid bind must fail");
        assert!(
            matches!(
                &error,
                ConfigError::Parse { .. } | ConfigError::InvalidValue { .. }
            ),
            "{bind}: {error}"
        );
    }
}

#[test]
fn backend_endpoint_accepts_hostname_ipv4_and_bracketed_ipv6() {
    let hostname = BackendEndpoint::parse("VPN.Example.COM:51820").expect("hostname should parse");
    assert_eq!(hostname.hostname(), Some("vpn.example.com"));
    assert_eq!(hostname.port(), 51820);
    assert_eq!(hostname.to_string(), "vpn.example.com:51820");
    assert!(matches!(hostname.host(), BackendHost::Name(_)));

    let ipv4 = BackendEndpoint::parse("192.0.2.10:53").expect("IPv4 should parse");
    assert_eq!(
        ipv4.socket_addr(),
        Some("192.0.2.10:53".parse().expect("test socket address"))
    );

    let ipv6 = BackendEndpoint::parse("[2001:0db8:0:0::1]:51820").expect("IPv6 should parse");
    assert_eq!(ipv6.to_string(), "[2001:db8::1]:51820");
    assert_eq!(
        ipv6.ip_addr(),
        Some(IpAddr::V6(
            "2001:db8::1"
                .parse::<Ipv6Addr>()
                .expect("test IPv6 literal")
        ))
    );
}

#[test]
fn invalid_backend_authorities_are_rejected() {
    let cases = [
        "example.com",
        ":51820",
        "example.com:0",
        "example.com:65536",
        "2001:db8::1:51820",
        "[not-an-ip]:51820",
        "999.1.1.1:51820",
        "-bad.example:51820",
        "bad..example:51820",
    ];

    for backend in cases {
        assert!(
            BackendEndpoint::parse(backend).is_err(),
            "backend `{backend}` must be rejected"
        );
        let input = listener("bad-backend", "127.0.0.1:40001", backend);
        assert!(
            matches!(Config::parse_str(&input), Err(ConfigError::Parse { .. })),
            "{backend}"
        );
    }

    assert_eq!(
        BackendEndpoint::parse("example.com:0"),
        Err(BackendEndpointError::ZeroPort)
    );
}

#[test]
fn invalid_and_zero_durations_are_rejected() {
    for (field, value) in [
        ("idle_timeout", "definitely-not-a-duration"),
        ("idle_timeout", "0s"),
        ("idle_timeout", "1ms"),
        ("dns_refresh_interval", "0s"),
        ("dns_refresh_interval", "999ms"),
        ("shutdown_timeout", "0ms"),
        ("shutdown_timeout", "99ms"),
    ] {
        let input = format!("[service]\n{field} = \"{value}\"\n{MINIMAL}");
        let error = Config::parse_str(&input).expect_err("bad duration must fail");
        assert!(
            matches!(
                &error,
                ConfigError::Parse { .. } | ConfigError::InvalidValue { .. }
            ),
            "{field}={value}: {error}"
        );
    }
}

#[test]
fn zero_and_inconsistent_limits_are_rejected() {
    for field in [
        "max_datagram_size",
        "max_sessions",
        "max_sessions_per_ip",
        "new_sessions_per_second",
    ] {
        let input = format!("[service]\n{field} = 0\n{MINIMAL}");
        assert!(
            matches!(
                Config::parse_str(&input),
                Err(ConfigError::InvalidValue { .. })
            ),
            "{field}"
        );
    }

    let oversized = format!(
        "[service]\nmax_datagram_size = {}\n{MINIMAL}",
        MAX_UDP_DATAGRAM_SIZE + 1
    );
    assert!(matches!(
        Config::parse_str(&oversized),
        Err(ConfigError::InvalidValue { .. })
    ));

    let inconsistent = format!("[service]\nmax_sessions = 10\nmax_sessions_per_ip = 11\n{MINIMAL}");
    assert!(matches!(
        Config::parse_str(&inconsistent),
        Err(ConfigError::InvalidValue { .. })
    ));
}

#[test]
fn control_socket_and_metrics_bind_are_validated() {
    let relative = format!("[service]\ncontrol_socket = \"wire-relay.sock\"\n{MINIMAL}");
    assert!(matches!(
        Config::parse_str(&relative),
        Err(ConfigError::InvalidValue { .. })
    ));
    for ambiguous in [
        "/run/wire-relay/../control.sock",
        "/run/wire-relay/./control.sock",
    ] {
        let input = format!("[service]\ncontrol_socket = \"{ambiguous}\"\n{MINIMAL}");
        assert!(matches!(
            Config::parse_str(&input),
            Err(ConfigError::InvalidValue { .. })
        ));
    }

    let metrics_hostname =
        format!("[metrics]\nenabled = true\nbind = \"metrics.example:9090\"\n{MINIMAL}");
    assert!(matches!(
        Config::parse_str(&metrics_hostname),
        Err(ConfigError::Parse { .. })
    ));

    let metrics_zero = format!("[metrics]\nenabled = true\nbind = \"127.0.0.1:0\"\n{MINIMAL}");
    assert!(matches!(
        Config::parse_str(&metrics_zero),
        Err(ConfigError::InvalidValue { .. })
    ));
}

#[test]
fn normalized_configuration_round_trips_as_toml() {
    let input = r#"
[service]
idle_timeout = "180s"

[metrics]
enabled = true

[[listeners]]
name = "canonical"
bind = "[::1]:40001"
backend = "VPN.Example.COM:51820"
"#;
    let normalized = Config::parse_str(input)
        .expect("configuration should parse")
        .normalized()
        .expect("configuration should normalize");
    let encoded = normalized
        .to_toml()
        .expect("normalized TOML should serialize");
    assert!(encoded.contains("vpn.example.com:51820"));

    let reparsed = Config::parse_str(&encoded)
        .expect("serialized configuration should parse")
        .into_normalized()
        .expect("serialized configuration should normalize");
    assert_eq!(reparsed, normalized);
}

#[test]
fn directly_constructed_backend_parts_are_validated() {
    assert_eq!(
        BackendEndpoint::from_parts("example.com", 0),
        Err(BackendEndpointError::ZeroPort)
    );
    assert_eq!(
        BackendEndpoint::from_parts("[2001:db8::1]", 51820),
        Err(BackendEndpointError::InvalidHostname {
            host: "[2001:db8::1]".to_owned(),
            reason: "must not contain brackets or colons",
        })
    );

    let endpoint =
        BackendEndpoint::from_parts("2001:db8::1", 51820).expect("raw IPv6 host part is valid");
    assert_eq!(
        endpoint.socket_addr(),
        Some(SocketAddr::new(
            "2001:db8::1".parse().expect("test IPv6 literal"),
            51820
        ))
    );
}

#[test]
fn defensive_resource_ceilings_are_enforced() {
    let sessions = format!(
        "[service]\nmax_sessions = {}\nmax_sessions_per_ip = 1\n{MINIMAL}",
        MAX_CONFIGURED_SESSIONS + 1
    );
    assert!(matches!(
        Config::parse_str(&sessions),
        Err(ConfigError::InvalidValue { .. })
    ));

    let rate = format!(
        "[service]\nnew_sessions_per_second = {}\n{MINIMAL}",
        MAX_NEW_SESSIONS_PER_SECOND + 1
    );
    assert!(matches!(
        Config::parse_str(&rate),
        Err(ConfigError::InvalidValue { .. })
    ));

    let excessive_combined_buffers = format!(
        "[service]\nmax_sessions = 100000\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\n{MINIMAL}"
    );
    let error = Config::parse_str(&excessive_combined_buffers)
        .expect_err("combined session buffer budget must be enforced");
    assert!(
        error.to_string().contains("datagram buffers"),
        "unexpected error: {error}"
    );

    for depth in [0, MAX_SESSION_QUEUE_DEPTH + 1] {
        let input = format!("[service]\nsession_queue_depth = {depth}\n{MINIMAL}");
        let error = Config::parse_str(&input).expect_err("queue depth must be range checked");
        assert!(
            error.to_string().contains("session_queue_depth"),
            "unexpected error: {error}"
        );
    }

    let mut listeners = String::new();
    for index in 0..=MAX_LISTENERS {
        listeners.push_str(&listener(
            &format!("listener-{index}"),
            &format!("127.0.0.1:{}", 10_000 + index),
            "example.com:51820",
        ));
    }
    assert!(matches!(
        Config::parse_str(&listeners),
        Err(ConfigError::InvalidValue { .. })
    ));
}

#[test]
fn unusable_multicast_broadcast_and_unspecified_backends_are_rejected() {
    for bind in [
        "224.0.0.1:40001",
        "255.255.255.255:40001",
        "[ff02::1]:40001",
    ] {
        assert!(matches!(
            Config::parse_str(&listener("bad-bind", bind, "example.com:51820")),
            Err(ConfigError::InvalidValue { .. })
        ));
    }

    for backend in [
        "0.0.0.0:51820",
        "[::]:51820",
        "224.0.0.1:51820",
        "255.255.255.255:51820",
        "[ff02::1]:51820",
    ] {
        assert!(matches!(
            Config::parse_str(&listener("bad-backend", "127.0.0.1:40001", backend)),
            Err(ConfigError::InvalidValue { .. })
        ));
    }
}

#[test]
fn excessive_durations_are_rejected_before_runtime_timer_arithmetic() {
    for (field, value) in [
        ("idle_timeout", "366days"),
        ("dns_refresh_interval", "8days"),
        ("shutdown_timeout", "6min"),
    ] {
        let input = format!("[service]\n{field} = \"{value}\"\n{MINIMAL}");
        assert!(
            matches!(
                Config::parse_str(&input),
                Err(ConfigError::InvalidValue { .. })
            ),
            "{field}={value}"
        );
    }
}

#[test]
fn session_queue_depth_parses_and_shares_a_runtime_memory_budget() {
    let input = format!("[service]\nsession_queue_depth = 1024\n{MINIMAL}");
    let config = Config::parse_str(&input).expect("queue depth should be configurable");
    assert_eq!(config.service.session_queue_depth, 1024);

    // Deep default queues do not require every session to own a full queue
    // of maximum-sized datagrams; all queues share one limit, which by
    // default is far below what the receive buffers leave of the 4 GiB.
    let defaults = ServiceConfig::default();
    assert_eq!(
        defaults.max_queue_memory_bytes,
        DEFAULT_MAX_QUEUE_MEMORY_BYTES
    );
    assert_eq!(
        QueueBudget::new(&defaults).limit(),
        usize::try_from(DEFAULT_MAX_QUEUE_MEMORY_BYTES).unwrap()
    );
    let fixed = defaults.max_sessions * (defaults.max_datagram_size * 2 + 1);
    let uncapped = ServiceConfig {
        max_queue_memory_bytes: UNCAPPED_QUEUE_MEMORY,
        ..defaults
    };
    assert_eq!(
        QueueBudget::new(&uncapped).limit(),
        4 * 1024 * 1024 * 1024 - fixed
    );
}

#[test]
fn queue_memory_limit_parses_and_bounds_the_shared_budget() {
    let input = format!("[service]\nmax_queue_memory_bytes = 1048576\n{MINIMAL}");
    let error = Config::parse_str(&input).expect_err("one default queue must fit in the limit");
    assert!(
        matches!(
            &error,
            ConfigError::InvalidValue { field, .. } if field == "service.max_queue_memory_bytes"
        ),
        "unexpected error: {error}"
    );

    // 16 queued 4096-byte datagrams with their overhead fit in 1 MiB.
    let input =
        format!("[service]\nsession_queue_depth = 16\nmax_queue_memory_bytes = 1048576\n{MINIMAL}");
    let config = Config::parse_str(&input).expect("a limit that holds one full queue is valid");
    assert_eq!(config.service.max_queue_memory_bytes, 1_048_576);
    let budget = QueueBudget::new(&config.service);
    assert_eq!(budget.limit(), 1_048_576);
    assert_eq!(budget.max_queue_memory(), 1_048_576);

    // A reload applies a new limit to later reservations.
    budget.configure(&ServiceConfig {
        max_queue_memory_bytes: 2_097_152,
        ..config.service
    });
    assert_eq!(budget.limit(), 2_097_152);

    // The limit cannot exceed the defensive ceiling.
    let input = format!("[service]\nmax_queue_memory_bytes = 4294967297\n{MINIMAL}");
    let error = Config::parse_str(&input).expect_err("limit above the ceiling must be rejected");
    assert!(
        error.to_string().contains("max_queue_memory_bytes"),
        "unexpected error: {error}"
    );
    let input = format!("[service]\nmax_queue_memory_bytes = 4294967296\n{MINIMAL}");
    Config::parse_str(&input).expect("the ceiling itself is valid");
}

#[test]
fn a_queue_deeper_than_the_default_limit_needs_a_higher_limit() {
    // 65536 * (4096 + 64) bytes is about 272 MB, above the 256 MiB default.
    let deep = "[service]\nsession_queue_depth = 65536\n";
    let error = Config::parse_str(&format!("{deep}{MINIMAL}"))
        .expect_err("a full queue above the default limit must be rejected");
    assert!(
        matches!(
            &error,
            ConfigError::InvalidValue { field, reason }
                if field == "service.max_queue_memory_bytes"
                    && reason.contains("service.session_queue_depth")
        ),
        "unexpected error: {error}"
    );
    Config::parse_str(&format!(
        "{deep}max_queue_memory_bytes = 536870912\n{MINIMAL}"
    ))
    .expect("raising the limit makes the deep queue valid");
}

#[test]
fn the_default_queue_memory_limit_holds_every_previously_valid_default_queue() {
    // Whatever limits an earlier release accepted, one default-depth queue
    // of the largest datagrams fits in the default limit.
    let input = format!(
        "[service]\nmax_sessions = 6000\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\n{MINIMAL}"
    );
    let config = Config::parse_str(&input).expect("previously valid limits must stay valid");
    assert_eq!(
        QueueBudget::new(&config.service).limit(),
        usize::try_from(DEFAULT_MAX_QUEUE_MEMORY_BYTES).unwrap()
    );
}

#[test]
fn configurations_accepted_by_the_previous_eight_slot_budget_remain_valid() {
    // The old guard accepted sessions * (datagram * 10 + 1) <= 4 GiB.
    let input = format!(
        "[service]\nmax_sessions = 100000\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 4096\n{MINIMAL}"
    );
    Config::parse_str(&input).expect("previously valid limits must stay valid");
}

#[test]
fn one_full_queue_must_fit_beside_fixed_session_buffers() {
    let input = format!(
        "[service]\nmax_sessions = 30000\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\nsession_queue_depth = 65536\n{MINIMAL}"
    );
    let error = Config::parse_str(&input).expect_err("oversized queue budget must be rejected");
    assert!(
        error.to_string().contains("session_queue_depth"),
        "unexpected error: {error}"
    );
}

#[test]
fn one_full_queue_is_charged_with_per_datagram_overhead() {
    // 65536 * 65507 plus one session's receive buffers fits in 4 GiB, but not
    // once each queued datagram is charged its fixed overhead.
    let input = format!(
        "[service]\nmax_sessions = 1\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\nsession_queue_depth = 65536\n{MINIMAL}"
    );
    let error = Config::parse_str(&input).expect_err("queue overhead must be budgeted");
    assert!(
        error.to_string().contains("session_queue_depth"),
        "unexpected error: {error}"
    );
    let input = format!(
        "[service]\nmax_sessions = 1\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\nsession_queue_depth = 65000\n\
         max_queue_memory_bytes = 4294967296\n{MINIMAL}"
    );
    Config::parse_str(&input).expect("a queue that fits with overhead must be accepted");
}

#[test]
fn reload_budget_reserves_buffers_for_live_sessions_above_the_limit() {
    let service = ServiceConfig {
        max_sessions: 10,
        max_sessions_per_ip: 10,
        max_queue_memory_bytes: UNCAPPED_QUEUE_MEMORY,
        ..ServiceConfig::default()
    };
    let per_session = service.max_datagram_size * 2 + 1;
    let total = 4 * 1024 * 1024 * 1024;
    let budget = QueueBudget::new(&service);
    assert_eq!(budget.limit(), total - 10 * per_session);
    let mut live: Vec<_> = (0..3).map(|_| budget.session_started()).collect();
    assert_eq!(budget.limit(), total - 10 * per_session);
    live.extend((3..500).map(|_| budget.session_started()));
    assert_eq!(budget.limit(), total - 500 * per_session);
    drop(live);
    assert_eq!(budget.limit(), total - 10 * per_session);
}

#[test]
fn the_runtime_budget_keeps_the_largest_validated_limits_exact() {
    // The runtime packs both values into one atomic word; validated maxima
    // must survive that unchanged.
    let service = ServiceConfig {
        max_sessions: 30_000,
        max_sessions_per_ip: 1,
        max_datagram_size: MAX_UDP_DATAGRAM_SIZE,
        max_queue_memory_bytes: UNCAPPED_QUEUE_MEMORY,
        ..ServiceConfig::default()
    };
    let budget = QueueBudget::new(&service);
    assert_eq!(budget.configured(), (30_000, MAX_UDP_DATAGRAM_SIZE));
    assert_eq!(budget.max_datagram_size(), MAX_UDP_DATAGRAM_SIZE);
    let per_session = MAX_UDP_DATAGRAM_SIZE * 2 + 1;
    assert_eq!(
        budget.limit(),
        4 * 1024 * 1024 * 1024 - 30_000 * per_session
    );

    let sessions = ServiceConfig {
        max_sessions: MAX_CONFIGURED_SESSIONS,
        max_sessions_per_ip: 1,
        max_queue_memory_bytes: UNCAPPED_QUEUE_MEMORY,
        ..ServiceConfig::default()
    };
    budget.configure(&sessions);
    assert_eq!(
        budget.configured(),
        (MAX_CONFIGURED_SESSIONS, sessions.max_datagram_size)
    );
    assert_eq!(
        budget.limit(),
        4 * 1024 * 1024 * 1024 - MAX_CONFIGURED_SESSIONS * (sessions.max_datagram_size * 2 + 1)
    );
}

#[test]
fn datagram_budget_errors_name_the_field_to_change() {
    let field_of = |input: &str| match Config::parse_str(input) {
        Err(ConfigError::InvalidValue { field, reason }) => {
            assert!(reason.contains("datagram buffers"), "{reason}");
            field
        }
        other => panic!("expected a budget validation error, got {other:?}"),
    };

    // The queue depth alone overflows the budget with a single session.
    let deep_queue = format!(
        "[service]\nmax_sessions = 1\nmax_sessions_per_ip = 1\nmax_datagram_size = 65507\n\
         session_queue_depth = 65536\n{MINIMAL}"
    );
    assert_eq!(field_of(&deep_queue), "service.session_queue_depth");

    // The receive buffers alone overflow it, whatever the queue depth.
    let many_sessions = format!(
        "[service]\nmax_sessions = 100000\nmax_sessions_per_ip = 1\n\
         max_datagram_size = 65507\nsession_queue_depth = 1\n{MINIMAL}"
    );
    assert_eq!(field_of(&many_sessions), "service.max_sessions");
}
