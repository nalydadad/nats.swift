# Design: NWConnection-based WebSocket transport (fix per-app VPN drop after 101)

Date: 2026-06-25
Branch: `claude/nifty-hopper-sg0m9n`
Status: Approved direction (Approach A, full switch to NWConnection)

## Problem

`wss://` connections fail when the client is on a corporate intranet that
applies a **tunnel-type per-app VPN** (`NEAppProxyProvider` / packet tunnel,
**not** a system HTTP proxy). The observed symptom: the TLS + HTTP `101
Switching Protocols` upgrade succeeds, then the connection is immediately
dropped before any WebSocket frame flows.

## Root cause (confirmed)

1. `ConnectionHandler.connectToServer` selects `URLSessionWebSocketTransport`
   for every `ws`/`wss` URL (`Sources/Nats/NatsConnection.swift:505-506`).
2. `URLSessionWebSocketTransport` only routes through Network.framework's
   modern relay stack when `ProxyResolver.resolve(for:)` returns a system
   **HTTP/HTTPS proxy** (`Sources/Nats/Transport/URLSessionWebSocketTransport.swift:55-59`).
3. A tunnel-type per-app VPN is not a system HTTP proxy, so `resolve()`
   returns `nil`, no `proxyConfigurations` is applied, and the connection
   falls back to a plain `URLSessionWebSocketTask`.
4. `URLSessionWebSocketTask` has a known defect when traversing a VPN
   tunnel / network middlebox: the HTTP 101 upgrade succeeds, but the
   upgraded stream is torn down before any frame flows, surfacing as
   `NSURLErrorNetworkConnectionLost` (-1005). This is exactly the failure the
   in-tree comment at `URLSessionWebSocketTransport.swift:49-54` already
   documents — but the existing workaround only covers the HTTP-proxy case,
   not the tunnel-VPN case.

The repository already contained the correct fix once: `NWWebSocketTransport`
(`NWConnection` + `NWProtocolWebSocket`), added in commit `b3040bb` and
removed in commit `efc7f97`. It was removed because `NWParameters` could not
attach the resolved **HTTP proxy** (`proxyConfigurations`) on the macOS build
at that time. That removal reason is specific to HTTP-proxy traversal and is
**irrelevant to a tunnel VPN**, which needs no proxy attachment at all.

## Why NWConnection fixes it

`NWConnection` + `NWProtocolWebSocket` performs the TLS handshake, the
WebSocket upgrade, and frame I/O **on a single connection**. There is no
internal "hand the upgraded stream to a different object" step — which is the
step that `URLSessionWebSocketTask` mishandles through a tunnel. The
connection is also transparently captured by the per-app VPN flow/packet
interception, because `NWConnection` is the modern Apple networking path that
`NEAppProxyProvider` is designed around.

`NWProtocolWebSocket` is available from macOS 10.15 / iOS 13. The package
minimum targets are macOS 13 / iOS 17.2, both well above that floor, so —
once `proxyConfigurations` is dropped — **no `@available` gate is required**.

## Decisions (locked)

- **Approach A**: revive the `NWConnection`-based WebSocket transport.
- **Full switch**: on Apple platforms, `ws`/`wss` always uses the
  NWConnection transport. The previous URLSession + `ProxyConfiguration`
  proxy path is **not** retained. Explicit system-HTTP-proxy WebSocket
  traversal is dropped as a deliberate, user-approved scope reduction;
  `NWConnection` still covers tunnel VPN, direct connections, and
  transparent/intercepting proxies.

## Design

### 1. Re-add `Sources/Nats/Transport/NWWebSocketTransport.swift`

Restore the implementation from commit `b3040bb` with these changes:

- **Remove all proxy resolution.** Delete the `ProxyResolver.resolve(...)`
  call and the `parameters.proxyConfigurations = [proxy]` assignment in
  `connect(url:tls:)`. (This is the line that caused the original removal.)
- **Drop the `@available(iOS 17.0, macOS 14.0, …)` gate.** It was only needed
  for `ProxyConfiguration`. Guard the file with `#if canImport(Network)`
  only.
- Keep unchanged: binary-frame-only send/receive, the text-frame drop with a
  log line, native ping auto-reply (`autoReplyPing = true`), the bounded
  connect timeout, and the custom-root-CA / mTLS TLS configuration via
  `sec_protocol_options_set_verify_block` /
  `sec_protocol_options_set_local_identity`.

Public/internal surface is the existing `NatsTransport` protocol
(`incomingMessages`, `connect`, `startSecureConnection`, `send`, `close`) —
no protocol changes.

### 2. Transport selection — `Sources/Nats/NatsConnection.swift`

In `connectToServer` (currently lines 504-513), change the `ws`/`wss` branch:

```swift
if s.scheme == "ws" || s.scheme == "wss" {
    #if canImport(Network)
        newTransport = NWWebSocketTransport()
    #else
        newTransport = URLSessionWebSocketTransport()  // Linux fallback
    #endif
} else {
    // unchanged: URLSessionStreamTransport / NIOStreamTransport
}
```

On Apple platforms `NWWebSocketTransport` is always used for WebSocket. On
Linux (`!canImport(Network)`), `URLSessionWebSocketTransport` remains the
fallback (its `#if canImport(Network)` proxy block already compiles to
nothing there).

### 3. Error mapping — `Sources/Nats/NatsConnection.swift:431-468`

`NWConnection` surfaces `NWError`, not `URLError`. Add an `NWError` case to
the connect-failure mapping so connection-level failures map to
`NatsError.ConnectError.io` (and DNS failures to `.dns`) instead of falling
through to the `default` branch that mislabels them as
`ConnectError.tlsFailure` on `wss://` / `requireTls`. Mirror the existing
`URLError` handling:

- `.posix(.ETIMEDOUT)` / timeout → `ConnectError.timeout`
- DNS resolution failures → `ConnectError.dns`
- other connection-level NWErrors → `ConnectError.io`

Guard with `#if canImport(Network)`.

### 4. Cleanup of now-dead code (scoped, optional within this change)

After the switch, on Apple platforms `URLSessionWebSocketTransport`'s
`#if canImport(Network)` proxy block and `ProxyResolver` are no longer
reached. Because `URLSessionWebSocketTransport` is still used as the Linux
fallback (where that block is already inert), the lowest-risk path is:

- Remove the dead `#if canImport(Network)` proxy block (and the
  `ProxyResolver.resolve` call) from `URLSessionWebSocketTransport.swift`,
  leaving it a plain `URLSessionWebSocketTask` transport for Linux.
- Delete `Sources/Nats/Transport/ProxyResolver.swift` (now unreferenced) and
  its `CFNetwork` dependency.

This cleanup keeps the change honest ("full switch") and removes ~150 lines
of PAC/proxy machinery that no longer runs. It is included in scope but can
be split into a follow-up commit if it complicates review.

## Testing & verification

- **Existing integration tests** (`Tests/NatsTests/Integration/ConnectionTests.swift`
  `testWebsocket` / `testWebsocketTLS`, with `Resources/ws.conf` / `wss.conf`)
  continue to exercise the WebSocket data path; on Apple targets they now run
  through `NWWebSocketTransport`. (Per `RECON.md`, the WS data path is not
  exercisable on the Linux CI toolchain regardless of transport.)
- **Runtime verification script**: update / add a script alongside
  `Scripts/verify-ws-proxy.swift` that connects `wss://` against a local
  `nats-server` WS listener with **no** system proxy configured, asserting a
  frame round-trips after the 101 upgrade (the exact scenario that fails
  today with `URLSessionWebSocketTask`).
- **On-device acceptance** (must be done by the requester): build the app on
  a real device with the corporate per-app VPN active and confirm a `wss://`
  connection establishes and stays up past the 101 upgrade. This is the only
  environment that reproduces the original `-1005` drop; CI cannot.

## Out of scope

- Raw `nats://` / `tls://` transports (unchanged).
- Explicit system-HTTP-proxy WebSocket traversal (intentionally dropped).
- JetStream (depends on `Nats`, not on the transport).
- Linux WebSocket functionality (pre-existing toolchain limitation per
  `RECON.md`; not regressed by this change).
```
