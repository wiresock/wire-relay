# Security policy

## Supported versions

WireRelay has not yet declared a production release in `CHANGELOG.md`.
Security fixes are made on the current development line until release notes
identify maintained tagged versions.

After releases begin, the release notes will state which version lines remain
supported. Operators should normally run the latest patch release in a
maintained line; older development snapshots should not be assumed to receive
security updates.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue, discussion,
pull request, log excerpt, or chat.

Use GitHub's private vulnerability reporting flow:

<https://github.com/Wiresock-Foundation/wire-relay/security/advisories/new>

Include, where possible:

- the affected version, commit, platform, and configuration shape;
- a concise impact statement and the trust boundary crossed;
- reproducible steps or a minimal proof of concept;
- whether the report involves response misdelivery, an unintended
  destination, resource exhaustion, payload disclosure, control-socket access,
  reload inconsistency, or service escape;
- relevant logs with payloads, credentials, public IPs, and other sensitive
  data removed;
- any known mitigations or evidence of active exploitation.

Maintainers will acknowledge and triage reports as soon as practical, but this
policy does not promise a fixed response time. Please allow time to reproduce,
prepare a fix, test regressions, and coordinate a release before public
disclosure. The project will credit reporters when requested and legally
permitted.

If GitHub private reporting is unavailable, do not fall back to a public
issue. Ask a repository owner for a private reporting channel without
including vulnerability details in that initial request.

## Security boundaries

WireRelay forwards opaque UDP payloads to fixed operator-configured backends.
It does not:

- authenticate relay clients;
- encrypt or authenticate carried data;
- inspect WireGuard or AmneziaWG packets;
- preserve a client's source address at the backend;
- decide whether carried traffic is benign;
- provide an unrestricted client-selected UDP proxy.

A public listener therefore accepts datagrams from any sender that can reach
it unless the operator adds firewall or network access control. Resource
limits bound abuse; they do not identify an authorized WireGuard peer.

Reports are especially useful when they demonstrate:

- forwarding to a destination not fixed by active configuration;
- payload bytes appearing in logs, metrics, or control output;
- one session receiving another session's response;
- bypass of global, per-IP, frame-size, queue, or rate bounds;
- unauthorized control operations despite expected filesystem permissions;
- partial runtime mutation after a rejected reload;
- panic, deadlock, memory growth, descriptor exhaustion, or task leakage from
  untrusted input;
- control-socket path replacement or symlink attacks;
- escape from the packaged service's intended OS restrictions.

The expected ability to send UDP to a configured listener, DNS changes
affecting newly created sessions, packet loss under bounded queue pressure,
and lack of synthetic backend health probes are design behavior rather than
vulnerabilities by themselves.

## Deployment guidance

- Run the packaged service as the dedicated unprivileged `wire-relay` user.
- Keep `/etc/wire-relay/config.toml` writable only by trusted administrators.
- Keep `/run/wire-relay/control.sock` restricted to trusted operators.
  Control access permits state inspection, reload, and session closure.
- Keep a custom control socket's immediate parent non-writable by other users,
  and every path ancestor owned by root or the WireRelay service user.
  WireRelay rejects symlinked or untrusted ancestors; only root-owned sticky
  intermediate directories such as `/tmp` may be writable by other users.
- Bind Prometheus metrics to loopback unless an authenticated, access-controlled
  monitoring path is provided.
- Firewall listener ports and metrics endpoints according to deployment
  policy.
- Tune `max_sessions`, `max_sessions_per_ip`,
  `new_sessions_per_second`, `max_datagram_size`, and systemd's descriptor
  limit to the host.
- Monitor session rejections, drops, resolver errors, and socket errors.
- Protect DNS resolution and the configuration supply chain: WireRelay uses
  the latest successfully resolved backend for new sessions.
- Validate configuration before restart or reload and retain a rollback path.
- Apply operating-system and WireRelay security updates promptly.

WireRelay never needs UDP payloads for support or diagnosis. Do not attach
payload captures to a report unless maintainers explicitly request them and a
safe private transfer method has been agreed.
