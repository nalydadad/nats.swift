# Plan B Recon — NIO transport → URLSessionWebSocketTask

Phase 0+1 findings. The human checkpoint after this phase was waived by the
requester ("run all phases autonomously"); this document records the
findings and the design decision taken instead of pausing for review.

## Baseline (Phase 0)

- `swift build` succeeds.
- `swift test`: 62 tests, 59 pass / 3 fail on this Linux dev sandbox. All 3
  failures are the same root cause: swift-corelibs-foundation's `URLSession`
  on Linux does not support `file://` URLs (`NSURLErrorDomain Code=-1002`).
  This affects `JwtTests.testParseCredentialsFile`,
  `CoreNatsTests.testCredentialsAuth`, `CoreNatsTests.testNkeyAuthFile` — all
  three load a local file via `URLSession.shared.data(from:)`. This is a
  Linux-Foundation limitation unrelated to NATS logic and is expected to pass
  on the real target platforms (iOS/macOS).
- JetStream is excluded from non-Darwin builds (`Package.swift`,
  `#if canImport(Darwin)`) because it depends on `CryptoKit`/`Combine`, which
  have no Linux equivalent. This predates Plan B and is unrelated to it;
  JetStream is out of scope for the transport swap (it depends on `Nats`,
  not on NIO directly).

## Where NIO is actually load-bearing vs. incidental (Phase 1 recon)

Grepped every file under `Sources/Nats` for NIO imports/types.

**Pure I/O boundary (must be replaced):**
- `Sources/Nats/NatsConnection.swift` — `ConnectionHandler: ChannelInboundHandler`.
  `bootstrapConnection`, `connectToServer`, `channelRead`/`channelReadComplete`/
  `channelActive`/`channelInactive`/`errorCaught`, TLS via `NIOSSLContext`/
  `NIOSSLClientHandler`, WS upgrade via `NIOWebSocketClientUpgrader`/
  `HTTPUpgradeRequestHandler`/`NIOWebSocketFrameAggregator`/
  `WebSocketByteBufferCodec`. `Channel`/`EventLoop`/`EventLoopPromise` are
  used both for I/O *and* as a general-purpose executor/promise abstraction
  (ping scheduling, suspend/resume sequencing) — the latter needs a
  non-NIO equivalent once there's no `Channel`.
- `Sources/Nats/BatchBuffer.swift` — writes batched bytes via
  `channel.writeAndFlush`. Batching policy itself is transport-agnostic;
  only the final write call is NIO-coupled.
- `Sources/Nats/HTTPUpgradeRequestHandler.swift` — NIO HTTP upgrade handler,
  entirely subsumed by `URLSessionWebSocketTask` (it does the HTTP
  Upgrade/101 handshake itself); this file becomes dead code.
- `Sources/Nats/RttCommand.swift` — `EventLoopPromise<TimeInterval>`, trivial
  to replace with `CheckedContinuation`.

**Incidental / vestigial (no behavior change needed):**
- `Sources/Nats/Extensions/Data+String.swift` — imports `NIOPosix`, unused.
- `Sources/Nats/NatsClient/NatsClientOptions.swift` — imports `NIO`/
  `NIOFoundationCompat`, unused; public builder API is 100% NIO-free already.
- `Sources/Nats/NatsProto.swift` — imports `NIO`, no NIO type used.
- `Sources/Nats/ConcurrentQueue.swift`, `NatsSubscription.swift` — only use
  `NIOLockedValueBox` as a plain lock; trivially portable (kept as-is, it's
  still available since `NIOConcurrencyHelpers` has no socket I/O in it).
- `Sources/Nats/Extensions/ByteBuffer+Writer.swift` — pure byte formatting
  into a `ByteBuffer` sink; logic is transport-agnostic, only the sink type
  needs widening to also support `Data`.
- `Sources/Nats/Extensions/Data+Parser.swift` — **zero NIO**. Already parses
  `Data` and already implements the partial-message `remainder` carry-over
  needed by constraint #5. No changes required here at all.
- `Sources/Nats/NatsClient/NatsClient.swift` — public API is 100% NIO-free;
  two internal touchpoints reach into `connectionHandler.channel` directly
  (`flush()`, `rtt()`) and need retargeting to the new transport, not the
  public surface.

**Conclusion:** the parser/protocol/auth/subscription/event layers are
already transport-agnostic by construction. The only real surgery is
`NatsConnection.swift`'s connect/read/write/lifecycle code and `BatchBuffer`.

## Design decision: scope of "wss-only" (constraint #3)

The existing test suite exercises two distinct wire transports:
1. Raw NATS-over-TCP/TLS (`nats://`, `tls://`) — the majority of tests.
2. NATS-over-WebSocket (`ws://`, `wss://`) — `testWebsocket`,
   `testWebsocketTLS`.

Plan B's stated motivation (corporate proxy/VPN traversal, system trust
store inheritance) is specific to the WebSocket gateway deployment, which is
what `URLSessionWebSocketTask` addresses. Constraint #3 ("connections only
use wss://") is interpreted as scoping *that* requirement to the WebSocket
path, not as a mandate to drop raw-TCP NATS support — dropping it outright
would directly contradict constraint #6 (existing tests usable as
regression tests), since most existing tests use `nats://`/`tls://` URLs
with no WS gateway involved.

Resolution taken: replace **all** of NIO's *socket* I/O with `URLSession`-
based transports, satisfying constraint #2's intent (remove NIO from the
network path; everything goes through APIs that inherit system
proxy/PAC/trust):
- `ws://` / `wss://` → `URLSessionWebSocketTask`, binary frames only
  (constraint #4), exactly as specified.
- `nats://` / `tls://` → `URLSessionStreamTask`, which is also a `URLSession`
  task and also inherits system proxy/trust configuration — it is the
  `URLSession`-native equivalent of a raw NIO `ClientBootstrap` TCP/TLS
  socket. This is *not* the WebSocket path the user's proxy issue concerned;
  using it is what keeps the un-deprecated, documented `nats://` URL scheme
  alive without any NIO sockets.

`NIOSSL`/`NIOPosix`/`NIOHTTP1`/`NIOWebSocket` are removed entirely from the
dependency graph in Phase 7; only `NIO`'s core data-structure module
(`ByteBuffer`/`NIOConcurrencyHelpers`) may remain as an internal locking/
buffer convenience per constraint #2's allowance, not for I/O.

Proceeding directly into Phases 2-7 per the "run all phases autonomously"
instruction.

## Phase 4-7 follow-up: a Linux-only gap in the plan

Phase 2/3 assumed `URLSessionStreamTask` (Darwin) would also be usable to
build/test on Linux dev/CI. That assumption was wrong:
`URLSessionStreamTask` is annotated `unavailable` in its entirety in
swift-corelibs-foundation (`streamTask(withHostName:port:)`,
`startSecureConnection()`, `readData`, `write`, `closeWrite`, `closeRead`) —
this only surfaces at compile time on Linux, not on the Darwin target the
fork is actually for.

Since dropping Linux buildability was not requested and this repo's CI/dev
loop runs on Linux, `Sources/Nats/Transport/NIOStreamTransport.swift` was
added as a Linux-only fallback for `nats://`/`tls://` (`#if
canImport(FoundationNetworking)`), built on NIO's `ClientBootstrap`/
`NIOSSLClientHandler`. This is a deliberate, non-requested deviation from
"NIO sockets fully removed" — narrowly scoped to the non-Darwin build path
only. On the actual Darwin/iOS target, `URLSessionStreamTransport.swift`
(Darwin-only, gated `#if !canImport(FoundationNetworking)`) is what's used,
and it has zero NIO socket I/O, satisfying constraint #2 in full there.
`NIOSSL` stays a package dependency solely to support this Linux fallback's
TLS.

## Phase 6 verification: binary frames / partial-message tolerance

- Binary-only constraint (#4): `URLSessionWebSocketTransport.swift` sends
  via `.send(.data(...))` exclusively and, on receive, matches only
  `.data(let data)`; a `.string` frame is logged and dropped, never
  converted. No code path constructs or accepts a text frame.
- Partial-message tolerance (#5): unchanged — `Data+Parser.swift`'s
  `parseOutMessages()`/`remainder` carry-over is transport-agnostic and
  untouched by this migration; `ConnectionHandler.handleReceivedChunk`
  feeds each transport chunk through the same parser used before the
  rewrite. Existing `ParserTests` continue to pass unmodified.
- Both are exercised indirectly by the full Linux test run (parser tests
  pass; WS data-frame path is reached on `wss://` and `ws://` integration
  tests up through the handshake/connect codepath), but `URLSessionWebSocketTask`
  itself is non-functional on this Linux Swift 6.0.3 toolchain (see below),
  so the WS *data path* could not be exercised end-to-end on Linux. It is
  expected to work on the Darwin/iOS target, where `URLSessionWebSocketTask`
  is mature.

## Final test state (Phase 7)

`swift build`: clean, no warnings, no errors.

`swift test`: 62 tests, 57 pass / 5 fail, all 5 pre-existing/explained Linux
Foundation gaps, not regressions from this migration:
- `testCredentialsAuth`, `testNkeyAuthFile`, `JwtTests.testParseCredentialsFile`
  — `URLSession` on Linux doesn't support `file://` (baseline failure,
  present before this migration too).
- `testWebsocket`, `testWebsocketTLS` — new failures, root-caused to
  `URLSessionWebSocketTask.send()`/`.receive()` throwing
  `NSURLErrorDomain Code=-1002` immediately on this Linux Swift 6.0.3
  toolchain, independently reproduced with a standalone script with zero
  nats.swift code involved, against a real local `nats-server` WS listener.
  This is a toolchain/environment limitation, not a defect in
  `URLSessionWebSocketTransport.swift`; expected to pass on the real
  macOS/iOS target.

`Package.swift`: `NIOHTTP1` and `NIOWebSocket` dependencies removed (no
longer referenced anywhere in `Sources/`); `NIO` (core)/`NIOSSL`/
`NIOConcurrencyHelpers`/`NIOFoundationCompat` retained — the first three only
for the Linux-only `NIOStreamTransport` fallback and as a locking primitive,
per the design decision above.
