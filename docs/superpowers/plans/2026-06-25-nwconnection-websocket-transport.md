# NWConnection WebSocket Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `wss://` connections dropping after the HTTP 101 upgrade on tunnel-type per-app VPNs by switching the Apple-platform WebSocket transport from `URLSessionWebSocketTask` to `NWConnection` + `NWProtocolWebSocket`.

**Architecture:** Revive the previously-removed `NWWebSocketTransport` (commit `b3040bb`) minus its proxy-resolution machinery, make it the sole `ws`/`wss` transport on Apple platforms, add `NWError` connect-error mapping, and remove the now-dead `ProxyResolver` and URLSession proxy block.

**Tech Stack:** Swift, Network.framework (`NWConnection`, `NWProtocolWebSocket`, `NWProtocolTLS`), Security (`sec_protocol_options_*`), `NIOConcurrencyHelpers` (`NIOLockedValueBox`).

## Global Constraints

- Package minimum targets: macOS 13.0, iOS 17.2 (verbatim from `Package.swift`).
- `NWProtocolWebSocket` floor is macOS 10.15 / iOS 13 → **no `@available` gate needed** once `proxyConfigurations` is dropped.
- Apple-platform WebSocket uses `NWConnection` exclusively; the URLSession + `ProxyConfiguration` proxy path is removed (user-approved scope reduction).
- WebSocket is **binary-frame only** — never construct or accept text frames.
- This dev environment is **Linux**: all `#if canImport(Network)` code does not compile here. `swift build` / `swift test` on Linux validate only the non-Network paths. Darwin compilation is validated by reusing known-good `b3040bb` source and surgical edits.
- Commit identity must be `Claude <noreply@anthropic.com>`.

---

### Task 1: Re-add `NWWebSocketTransport` without proxy resolution

**Files:**
- Create: `Sources/Nats/Transport/NWWebSocketTransport.swift`

**Interfaces:**
- Consumes: `NatsTransport` protocol (`incomingMessages`, `connect(url:tls:)`, `startSecureConnection()`, `send(_:)`, `close()`); `TransportTLSOptions`; `TLSIdentity.loadIdentity` / `loadCertificate`; `NatsError.ConnectError.timeout`; `NatsError.ClientError`.
- Produces: `internal final class NWWebSocketTransport: NatsTransport, @unchecked Sendable` with a no-arg `init()`.

- [ ] **Step 1: Create the file from the known-good `b3040bb` source, with two changes**

Restore `git show b3040bb:Sources/Nats/Transport/NWWebSocketTransport.swift`, but:
1. Drop the `@available(iOS 17.0, macOS 14.0, tvOS 17.0, watchOS 10.0, *)` attribute on the class.
2. In `connect(url:tls:)`, delete the proxy block so it reads:

```swift
func connect(url: URL, tls: TransportTLSOptions?) async throws {
    let parameters = try makeParameters(url: url, tls: tls)

    let connection = NWConnection(to: .url(url), using: parameters)
    connectionBox.withLockedValue { $0 = connection }
    connection.stateUpdateHandler = { [weak self] state in
        self?.handleState(state)
    }
    // ... unchanged connect/timeout task group ...
    startReceiveLoop(connection)
}
```

Everything else (binary-only send/receive, text-frame drop + log, `autoReplyPing = true`, `connectTimeout`, `configureTLS` with `sec_protocol_options_set_verify_block` / `sec_protocol_options_set_local_identity`, `handleState`, `resumeConnect`, `receiveNext`, `yield`, `finish`, `send`, `close`) stays verbatim. Keep the `#if canImport(Network)` file guard and `import Network` / `import NIOConcurrencyHelpers` / `#if canImport(Security)`.

- [ ] **Step 2: Verify the file no longer references `ProxyResolver`**

Run: `grep -n ProxyResolver Sources/Nats/Transport/NWWebSocketTransport.swift`
Expected: no output.

- [ ] **Step 3: Verify Linux build still compiles (Network code excluded there)**

Run: `swift build`
Expected: build succeeds (the new file compiles to nothing on Linux).

- [ ] **Step 4: Commit**

```bash
git add Sources/Nats/Transport/NWWebSocketTransport.swift
git commit -m "Re-add NWConnection WebSocket transport (no proxy resolution)"
```

---

### Task 2: Select `NWWebSocketTransport` for ws/wss on Apple platforms

**Files:**
- Modify: `Sources/Nats/NatsConnection.swift:504-513`

**Interfaces:**
- Consumes: `NWWebSocketTransport()` (Task 1) on Apple platforms; `URLSessionWebSocketTransport()` on Linux.

- [ ] **Step 1: Replace the ws/wss transport selection**

Change the `if s.scheme == "ws" || s.scheme == "wss"` branch to:

```swift
let newTransport: any NatsTransport
if s.scheme == "ws" || s.scheme == "wss" {
    #if canImport(Network)
        newTransport = NWWebSocketTransport()
    #else
        newTransport = URLSessionWebSocketTransport()
    #endif
} else {
    #if canImport(FoundationNetworking)
        newTransport = NIOStreamTransport()
    #else
        newTransport = URLSessionStreamTransport()
    #endif
}
```

- [ ] **Step 2: Verify Linux build (selects URLSessionWebSocketTransport branch)**

Run: `swift build`
Expected: build succeeds.

- [ ] **Step 3: Commit**

```bash
git add Sources/Nats/NatsConnection.swift
git commit -m "Use NWConnection WebSocket transport on Apple platforms"
```

---

### Task 3: Map `NWError` connect failures correctly

**Files:**
- Modify: `Sources/Nats/NatsConnection.swift` (connect-failure mapping, ~lines 431-468)

**Interfaces:**
- Consumes: `NWError` (Network.framework); `NatsError.ConnectError.timeout` / `.dns` / `.io`.

- [ ] **Step 1: Add an `NWError` case to the error switch**

Immediately before the `#if canImport(FoundationNetworking)` block in the
`catch`/mapping switch, insert (so connection-level NWErrors do not fall to
`default` and get mislabelled as `tlsFailure`):

```swift
#if canImport(Network)
    case let error as NWError:
        switch error {
        case .posix(.ETIMEDOUT):
            throw NatsError.ConnectError.timeout
        case .dns:
            throw NatsError.ConnectError.dns(error)
        default:
            throw NatsError.ConnectError.io(error)
        }
#endif
```

Add `#if canImport(Network)` `import Network` `#endif` to the file's imports
if not already present.

- [ ] **Step 2: Verify Linux build (NWError case excluded there)**

Run: `swift build`
Expected: build succeeds.

- [ ] **Step 3: Commit**

```bash
git add Sources/Nats/NatsConnection.swift
git commit -m "Map NWError connect failures to ConnectError.io/.dns/.timeout"
```

---

### Task 4: Remove dead proxy machinery

**Files:**
- Delete: `Sources/Nats/Transport/ProxyResolver.swift`
- Modify: `Sources/Nats/Transport/URLSessionWebSocketTransport.swift` (remove the `#if canImport(Network)` proxy block + comment in `connect`)

**Interfaces:**
- After this task `ProxyResolver` is unreferenced anywhere in `Sources/`.

- [ ] **Step 1: Confirm `ProxyResolver` is only referenced by the URLSession transport**

Run: `grep -rn ProxyResolver Sources/`
Expected: matches only in `URLSessionWebSocketTransport.swift` (and the file itself).

- [ ] **Step 2: Remove the proxy block from `URLSessionWebSocketTransport.connect`**

Delete the `#if canImport(Network) ... #endif` block (the `if #available ... ProxyResolver.resolve ... proxyConfigurations` lines and their comment) inside `connect(url:tls:)`, leaving a plain `URLSessionWebSocketTask` setup. Update the type-doc comment at the top of the file to drop the proxy-tunnelling sentence.

- [ ] **Step 3: Delete `ProxyResolver.swift`**

```bash
git rm Sources/Nats/Transport/ProxyResolver.swift
```

- [ ] **Step 4: Verify no dangling references**

Run: `grep -rn ProxyResolver Sources/`
Expected: no output.

- [ ] **Step 5: Verify Linux build**

Run: `swift build`
Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add Sources/Nats/Transport/URLSessionWebSocketTransport.swift
git commit -m "Remove dead ProxyResolver and URLSession WS proxy block"
```

---

### Task 5: Update runtime verification script & docs

**Files:**
- Modify/Create: a no-proxy `wss://` round-trip verification under `Scripts/`
- Modify: `RECON.md` (note the transport change) — optional, low priority

**Interfaces:** none (standalone script).

- [ ] **Step 1: Inspect the existing proxy verification script**

Run: `git show HEAD:Scripts/verify-ws-proxy.swift | head -40`

- [ ] **Step 2: Add `Scripts/verify-ws-nwtransport.swift`**

A standalone script that starts (or assumes) a local `nats-server` WS
listener, connects `wss://` (or `ws://`) with no system proxy, publishes and
receives one message, and asserts a frame round-trips after the 101 upgrade.
Print PASS/FAIL. (Mirror the structure/util of `verify-ws-proxy.swift`.)

- [ ] **Step 3: Commit**

```bash
git add Scripts/verify-ws-nwtransport.swift
git commit -m "Add no-proxy wss round-trip verification script"
```

---

### Task 6: Full verification & lint

- [ ] **Step 1: Build**

Run: `swift build`
Expected: clean.

- [ ] **Step 2: Run the test suite (Linux baseline)**

Run: `swift test`
Expected: only the pre-existing `RECON.md`-documented Linux failures
(`file://` URLSession gaps, and WS data-path tests that need a Darwin
toolchain). No NEW failures attributable to this change in the non-WS paths.

- [ ] **Step 3: Lint**

Run: `swift-format lint --configuration .swift-format -r --strict Sources` (if `swift-format` available)
Expected: no violations. If unavailable, skip and note it.

- [ ] **Step 4: Push & open draft PR**

```bash
git push -u origin claude/nifty-hopper-sg0m9n
```
Then open a draft PR describing the root cause and fix.

---

## Self-Review

- **Spec coverage:** Task 1 = re-add transport (spec §1); Task 2 = selection (spec §2); Task 3 = error mapping (spec §3); Task 4 = cleanup (spec §4); Task 5 = verification (spec Testing); Task 6 = build/test/lint/PR. All spec sections covered.
- **Placeholder scan:** None — each step has concrete commands/code. The only judgement step is the verification script body (Task 5), inherently bespoke; structure and assertion criteria are specified.
- **Type consistency:** `NWWebSocketTransport()` no-arg init used in Task 2 matches Task 1's definition. `ConnectError.io/.dns/.timeout` names match existing usage in `NatsConnection.swift`.
- **Caveat:** Darwin-only code cannot be compiled on this Linux box; correctness rests on reusing known-good `b3040bb` source + surgical diffs. On-device acceptance by the requester remains the final gate (per spec).
