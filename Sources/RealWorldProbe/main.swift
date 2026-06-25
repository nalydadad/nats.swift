// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// RealWorldProbe — an end-to-end smoke test for the new transport layer,
// meant to be run by hand against a *real* nats-server (and, for the headline
// case, a *real* HTTP proxy). Unlike the XCTest suite this is not hermetic:
// it dials whatever `NATS_URL` points at, so it can exercise the things that
// can only be checked in the real world — `wss://` over a corporate/HTTP
// proxy (NWWebSocketTransport + ProxyResolver), system trust-store inheritance,
// and mTLS — none of which a unit test can stand in for.
//
// Everything is configured by environment variables so no recompile is needed
// to point it somewhere else. See Scripts/realworld/ for a ready-made local
// nats-server + proxy setup, and RUNBOOK.md for the full procedure.
//
// Exit code is 0 on a successful publish/subscribe round-trip, non-zero on any
// failure — so it drops straight into a shell script or CI step.

import Foundation
import Nats

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

// MARK: - Configuration (all via environment)

func env(_ key: String) -> String? {
    guard let value = ProcessInfo.processInfo.environment[key], !value.isEmpty else { return nil }
    return value
}

let urlString = env("NATS_URL") ?? "ws://127.0.0.1:8080"
guard let url = URL(string: urlString) else {
    FileHandle.standardError.write(Data("FAIL: NATS_URL is not a valid URL: \(urlString)\n".utf8))
    exit(2)
}

let subject = env("NATS_SUBJECT") ?? "probe.roundtrip"
let timeoutSeconds = Double(env("NATS_TIMEOUT") ?? "") ?? 15.0

// A unique payload so a stray retained message can't make a broken run look
// like a pass.
let nonce = UUID().uuidString
let payload = Data("probe:\(nonce)".utf8)

// MARK: - Build the client from the environment

var options = NatsClientOptions().url(url)

if let user = env("NATS_USER"), let pass = env("NATS_PASS") {
    options = options.usernameAndPassword(user, pass)
}
if let token = env("NATS_TOKEN") {
    options = options.token(token)
}
if let rootCA = env("NATS_ROOT_CA") {
    options = options.rootCertificates(URL(fileURLWithPath: rootCA))
}
if let cert = env("NATS_CLIENT_CERT"), let key = env("NATS_CLIENT_KEY") {
    options = options.clientCertificate(URL(fileURLWithPath: cert), URL(fileURLWithPath: key))
}

let client = options.build()

// MARK: - Helpers

/// Runs `operation`, but throws `ProbeError.timedOut` if it doesn't finish
/// within `seconds`. Real-world hangs (a proxy that accepts the TCP connection
/// but never relays a frame) are exactly the failure mode we must not wait on
/// forever.
struct ProbeError: Error, CustomStringConvertible {
    let description: String
}

func withTimeout<T: Sendable>(
    _ seconds: Double, _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw ProbeError(description: "timed out after \(seconds)s")
        }
        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}

func log(_ message: String) {
    print("[probe] \(message)")
}

// MARK: - Run

func run() async -> Int32 {
    #if os(macOS)
        let platform = "macOS"
    #elseif os(iOS)
        let platform = "iOS"
    #elseif os(Linux)
        let platform = "Linux"
    #else
        let platform = "other"
    #endif
    log("platform: \(platform)")
    log("target:   \(urlString)")
    log("subject:  \(subject)")
    if url.scheme == "ws" || url.scheme == "wss" {
        log(
            "scheme is WebSocket -> NWWebSocketTransport (macOS 14+/iOS 17+, proxy-aware) "
                + "or URLSessionWebSocketTransport (older / non-Darwin)")
    } else {
        log("scheme is raw NATS -> URLSessionStreamTransport (Darwin) or NIOStreamTransport (Linux)")
    }
    if let proxy = ProcessInfo.processInfo.environment["https_proxy"]
        ?? ProcessInfo.processInfo.environment["HTTPS_PROXY"]
    {
        log("note: https_proxy env=\(proxy) (Network.framework reads the *system* proxy, not this)")
    }

    do {
        log("connecting...")
        try await withTimeout(timeoutSeconds) { try await client.connect() }
        log("connected to: \(client.connectedUrl?.absoluteString ?? "<unknown>")")

        log("subscribing to \(subject)...")
        let sub = try await client.subscribe(subject: subject)

        log("publishing \(payload.count)-byte payload...")
        try await client.publish(payload, subject: subject)

        log("awaiting round-trip (timeout \(timeoutSeconds)s)...")
        let message = try await withTimeout(timeoutSeconds) {
            var iterator = sub.makeAsyncIterator()
            return try await iterator.next()
        }

        guard let received = message?.payload else {
            log("FAIL: subscription ended without delivering a message")
            return 1
        }
        guard received == payload else {
            log("FAIL: payload mismatch")
            log("  expected: \(String(decoding: payload, as: UTF8.self))")
            log("  received: \(String(decoding: received, as: UTF8.self))")
            return 1
        }
        log("round-trip OK: \(String(decoding: received, as: UTF8.self))")

        if let rtt = try? await withTimeout(timeoutSeconds, { try await client.rtt() }) {
            log(String(format: "rtt: %.2f ms", rtt * 1000))
        }

        try? await client.close()
        log("PASS")
        return 0
    } catch {
        log("FAIL: \(error)")
        try? await client.close()
        return 1
    }
}

exit(await run())
