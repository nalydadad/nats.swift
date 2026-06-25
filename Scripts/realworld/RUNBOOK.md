# Real-world transport test runbook

How to verify the new transport layer (`Sources/Nats/Transport/`) actually
works against a live server — not with mocks or unit tests, but end-to-end over
a real socket, and for the headline case, through a real HTTP proxy.

## Why this exists

The branch replaces NIO socket I/O with `URLSession` / `Network.framework`
transports. Transport selection (`Sources/Nats/NatsConnection.swift:506`):

| URL scheme        | macOS 14+ / iOS 17+        | older Darwin                  | Linux                 |
| ----------------- | -------------------------- | ----------------------------- | --------------------- |
| `wss://` / `ws://`| `NWWebSocketTransport`     | `URLSessionWebSocketTransport`| `URLSessionWebSocket…`|
| `nats://`/`tls://`| `URLSessionStreamTransport`| `URLSessionStreamTransport`   | `NIOStreamTransport`  |

The thing that **cannot** be unit-tested is the reason the branch exists:
`wss://` tunneled through a corporate HTTP/PAC proxy via `NWWebSocketTransport`
+ `ProxyResolver`. `ProxyResolver` reads the **system** proxy configuration
(`CFNetworkCopySystemProxySettings`), so proving it works means pointing a real
machine at a real proxy and watching the tunnel form. That is what this runbook
does.

> Run on **macOS** (or a real iOS device/sim). The `URLSession`/`Network`
> transports are Darwin-only and do not compile on Linux; the Linux CI path
> uses `NIOStreamTransport` and `URLSessionWebSocketTask` (the latter is known
> broken on the Linux Swift toolchain — see `RECON.md`).

## Prerequisites

- `nats-server` on `PATH`
  (`curl --fail https://binaries.nats.dev/nats-io/nats-server/v2@latest | PREFIX=/usr/local/bin sh`)
- A Swift toolchain (Xcode or the macOS toolchain) — run commands from the repo root.
- `python3` (only for `proxy` mode's bundled CONNECT proxy).

## Test 1 — direct WebSocket round-trip

Proves the WebSocket transport connects and pub/sub works end-to-end.

```bash
./Scripts/realworld/realworld-test.sh direct
```

Expected: probe prints `connected to: …`, `round-trip OK`, `PASS`, exit 0.

## Test 2 — WebSocket through a real HTTP proxy (the headline case)

Proves `ProxyResolver` + `NWWebSocketTransport` tunnel `ws://`/`wss://` through
an HTTP CONNECT proxy that the system is configured to use.

```bash
./Scripts/realworld/realworld-test.sh proxy
```

This starts `nats-server`, starts a local CONNECT proxy on `:8888`, flips the
**system** Secure Web Proxy to it (needs `sudo`; restored on exit), and runs the
probe. Success criteria:

1. The proxy log prints `CONNECT 127.0.0.1:8080` — the tunnel was established
   **through the proxy**, not directly.
2. The probe prints `PASS` (exit 0) — frames flowed over that tunnel.

If the probe passes but you see **no** `CONNECT` line, the client bypassed the
proxy (a real bug — `ProxyResolver` failed to pick up the system config).

## Test 3 — manual / against your own server

The probe is fully env-driven, so point it anywhere:

```bash
NATS_URL=wss://demo.nats.io:8443 \
NATS_SUBJECT=probe.$(uuidgen) \
swift run RealWorldProbe
```

| Env var            | Meaning                                  |
| ------------------ | ---------------------------------------- |
| `NATS_URL`         | server URL (default `ws://127.0.0.1:8080`) |
| `NATS_SUBJECT`     | subject to round-trip on                 |
| `NATS_TIMEOUT`     | per-step timeout in seconds (default 15) |
| `NATS_USER`/`NATS_PASS` | username/password auth              |
| `NATS_TOKEN`       | token auth                               |
| `NATS_ROOT_CA`     | PEM root CA for `wss://`/`tls://` pinning |
| `NATS_CLIENT_CERT` + `NATS_CLIENT_KEY` | mTLS client identity |

### mTLS / pinned-CA real-world check

Reuse the test certificates already in the repo
(`Tests/NatsTests/Integration/Resources/`) with a TLS WebSocket server, e.g.:

```bash
NATS_URL=wss://localhost:8080 \
NATS_ROOT_CA=Tests/NatsTests/Integration/Resources/rootCA.pem \
NATS_CLIENT_CERT=Tests/NatsTests/Integration/Resources/client-cert.pem \
NATS_CLIENT_KEY=Tests/NatsTests/Integration/Resources/client-key.pem \
swift run RealWorldProbe
```

This exercises `TLSIdentity` / `TLSChallengeDelegate` (server-trust pinning +
client identity) against a live TLS handshake.

## What a green run proves

- The transport actually opens a connection to a real server (no mock).
- A NATS publish/subscribe round-trips over it with byte-exact payload.
- In `proxy` mode: the connection was relayed through an HTTP CONNECT proxy
  resolved from system settings — the corporate-proxy scenario the branch
  targets.
