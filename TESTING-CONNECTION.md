# Verifying a real NATS connection (and credentials)

This guide describes how to prove that NATS.swift can **establish a real
connection** and **carry credentials** in your actual environment — including
the corporate-proxy case that produces the WebSocket `-1005`
("network connection lost") error.

It is deliberately layered. Each layer rules out a different class of problem,
so when something fails you know *where* it failed.

---

## Background: what `-1005` actually is

`-1005` is `NSURLErrorNetworkConnectionLost`. The socket **opened and was then
dropped**. For `wss://` through a TLS-terminating corporate proxy, the HTTP
`101` upgrade succeeds but the upgraded stream is torn down immediately. The
client resolves the system proxy/PAC into an explicit `ProxyConfiguration`
(iOS 17 / macOS 14+) so the connection is carried by Network.framework's modern
relay stack, which tunnels WebSocket-over-proxy correctly. See
`Sources/Nats/Transport/URLSessionWebSocketTransport.swift`.

Because `-1005` is a *Darwin + real network* phenomenon, it **cannot** be
reproduced on a Linux CI box (`URLSessionWebSocketTask` is not functional
there). The verification below therefore targets a real macOS/iOS runtime.

---

## Layer 1 — Automated credential test (CI-friendly)

These prove the credential path end-to-end against a local `nats-server`:

| Test | Proves |
|------|--------|
| `CoreNatsTests/testCredentialsAuth` | JWT `.creds` file → connect → pub/sub round-trip |
| `CoreNatsTests/testNkeyAuthFile`    | nkey seed file → connect → pub/sub round-trip |
| `CoreNatsTests/testUsernameAndPassword`, `testTokenAuth` | user/pass + token auth, incl. rejection of bad creds |
| `JwtTests/testParseCredentialsFile` | `.creds` parsing (JWT + nkey extraction) |

Run them (requires `nats-server` on `PATH`):

```bash
swift test --filter NatsTests.CoreNatsTests/testCredentialsAuth
swift test --filter NatsTests.CoreNatsTests/testNkeyAuthFile
swift test --filter NatsTests.JwtTests/testParseCredentialsFile
```

> **Note:** credential/nkey files are now read with `Data(contentsOf:)` instead
> of `URLSession.shared.data(from:)`. A local file read no longer goes through
> the networking/proxy stack at all, which (a) makes these tests pass on Linux
> CI, and (b) removes the proxy layer as a possible cause of credential-load
> failures on device.

Layer 1 confirms the **library logic** is correct. It does **not** exercise
your real broker, your real network, or `wss://`-over-proxy.

---

## Layer 2 — `nats-smoke`: real-environment connectivity check

`nats-smoke` connects with the same `NatsClient` your app uses, optionally with
credentials, and performs a **publish/subscribe round-trip** — so a green result
means data actually flowed, not merely that the handshake completed. Run it on
the real target (your Mac, or an iOS host app) and on the real network path
(VPN/proxy on) where `-1005` appears.

### Run

```bash
# JWT credentials over wss (the proxy/-1005 scenario)
swift run nats-smoke --url wss://your-broker.example:443 --creds ./user.creds

# Plain TCP with user/password
swift run nats-smoke --url nats://localhost:4222 --user alice --pass secret --count 5

# nkey seed file, with verbose client logs
swift run nats-smoke --url tls://your-broker:4222 --nkey-file ./user.nk --verbose
```

`--help` lists every flag (TLS, mutual TLS, token, subject, count, timeout).

### What you see

- A timestamped line for **every** connection event
  (`connected` / `disconnected` / `suspended` / `closed` / `lameDuckMode` /
  `error`).
- Measured RTT after connect.
- The pub/sub round-trip result.
- **On failure**, the raw `NSError` `domain`/`code` is printed and `-1005` is
  called out explicitly with remediation hints.

Exit code is `0` on success, `1` on failure — so it drops straight into a
macOS CI job or a pre-release gate:

```bash
swift run nats-smoke --url "$NATS_URL" --creds "$NATS_CREDS" || exit 1
```

### Reproducing the proxy case deliberately

To confirm the `-1005` fix specifically, run `nats-smoke` against a `wss://`
broker **with the corporate proxy / VPN active** (the configuration where it
previously failed). A successful round-trip there is the real proof. The
lower-level `Scripts/verify-ws-proxy.swift` exercises the same
`ProxyConfiguration` tunnelling mechanism in isolation against a local CONNECT
proxy.

---

## Recommended workflow

1. **Layer 1** in CI on every change — guards the credential/parse logic.
2. **Layer 2 (`nats-smoke`)** on a real Mac against your real broker + creds,
   first on a plain network, then with the proxy/VPN on, before shipping.

If Layer 1 passes but `nats-smoke` fails with `-1005`, the problem is the
network transport/proxy path (not credentials). If `nats-smoke` fails with
an `authorizationViolation` or a credentials-read error, the problem is the
credentials, not the transport.
