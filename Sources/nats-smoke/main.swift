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

// nats-smoke: a real-environment connectivity checker for NATS.swift.
//
// It connects to a live NATS server using the same `NatsClient` your app uses
// — optionally with credentials — and performs a full publish/subscribe
// round-trip to prove the connection actually carries data (not just that the
// handshake succeeded). Every connection event is logged with a timestamp, and
// on failure the underlying `NSError` domain/code is printed so transport-level
// problems such as the WebSocket `-1005` ("network connection lost") seen
// behind corporate proxies are unambiguous.
//
// Exit code is 0 on success, 1 on failure — so it can be wired into CI on a
// macOS runner or run by hand on a device.
//
// Example:
//   swift run nats-smoke --url wss://demo.example:443 --creds ./user.creds
//   swift run nats-smoke --url nats://localhost:4222 --user a --pass b --count 5

import Foundation
import Logging
import Nats

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

// MARK: - Output helpers

let startInstant = Date()

func stamp() -> String {
    String(format: "%7.3fs", Date().timeIntervalSince(startInstant))
}

func note(_ message: String) {
    FileHandle.standardError.write(Data("[\(stamp())] \(message)\n".utf8))
}

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data("\n❌ FAIL: \(message)\n".utf8))
    exit(1)
}

/// Recursively unwraps an error and prints the most useful diagnostics,
/// surfacing the raw `NSError` domain/code so transport failures like `-1005`
/// are visible regardless of how the client wrapped them.
func diagnose(_ error: Error) {
    note("error (swift): \(error)")

    // NatsError wraps the transport error; pull the inner one out when present.
    var underlying: Error? = nil
    switch error {
    case let connect as NatsError.ConnectError:
        note("classified as NatsError.ConnectError: \(connect.description)")
        switch connect {
        case .io(let inner), .dns(let inner), .tlsFailure(let inner):
            underlying = inner
        default:
            break
        }
    case let server as NatsError.ServerError:
        note("classified as NatsError.ServerError: \(server.description)")
    case let client as NatsError.ClientError:
        note("classified as NatsError.ClientError: \(client.description)")
    default:
        break
    }

    let ns = (underlying ?? error) as NSError
    note("NSError: domain=\(ns.domain) code=\(ns.code) — \(ns.localizedDescription)")
    if ns.domain == NSURLErrorDomain {
        note("URLError code name: \(urlErrorName(ns.code))")
    }
    if ns.code == -1005 {
        note(
            "↳ -1005 is NSURLErrorNetworkConnectionLost: the socket was dropped after it "
                + "opened. Behind a corporate proxy this is the WebSocket-upgrade-lost "
                + "failure; confirm the system proxy/PAC is reachable and that wss:// (not "
                + "ws://) is used through TLS-terminating proxies.")
    }
}

func urlErrorName(_ code: Int) -> String {
    switch code {
    case -1001: return "timedOut"
    case -1003: return "cannotFindHost"
    case -1004: return "cannotConnectToHost"
    case -1005: return "networkConnectionLost"
    case -1006: return "dnsLookupFailed"
    case -1009: return "notConnectedToInternet"
    case -1200: return "secureConnectionFailed"
    case -1202: return "serverCertificateUntrusted"
    default: return "code \(code)"
    }
}

// MARK: - Arguments

struct Options {
    var urls: [URL] = []
    var creds: URL?
    var nkeyFile: URL?
    var nkey: String?
    var user: String?
    var pass: String?
    var token: String?
    var requireTls = false
    var tlsFirst = false
    var rootCA: URL?
    var clientCert: URL?
    var clientKey: URL?
    var subject = "_nats_smoke.\(UUID().uuidString.prefix(8))"
    var count = 1
    var timeout: TimeInterval = 10
    var verbose = false
}

func printUsage() {
    let usage = """
        nats-smoke — verify a real NATS connection (optionally with credentials)

        USAGE:
          swift run nats-smoke --url <url> [options]

        REQUIRED:
          --url <url>             Server URL. Repeatable. Schemes: nats, tls, ws, wss.

        CREDENTIALS (choose at most one auth method):
          --creds <path>          JWT credentials file (user JWT + nkey seed).
          --nkey-file <path>      File containing an nkey seed.
          --nkey <seed>           nkey seed string.
          --user <u> --pass <p>   Username/password.
          --token <t>             Auth token.

        TLS:
          --tls                   Require TLS.
          --tls-first             Perform TLS before INFO (server must support it).
          --root-ca <path>        Root CA certificate (PEM).
          --client-cert <path>    Client certificate (PEM) for mutual TLS.
          --client-key <path>     Client private key (PEM) for mutual TLS.

        TEST SHAPE:
          --subject <s>           Round-trip subject (default: random _nats_smoke.*).
          --count <n>             Messages to publish/expect (default: 1).
          --timeout <seconds>     Per-step timeout (default: 10).
          --verbose               Enable debug-level client logging.
          -h, --help              Show this help.

        EXIT: 0 on success, 1 on failure.
        """
    print(usage)
}

func parseArgs() -> Options {
    var opts = Options()
    var args = Array(CommandLine.arguments.dropFirst())
    var index = 0

    func value(for flag: String) -> String {
        index += 1
        guard index < args.count else { fail("missing value for \(flag)") }
        return args[index]
    }
    func fileURL(for flag: String) -> URL {
        URL(fileURLWithPath: value(for: flag))
    }

    while index < args.count {
        let arg = args[index]
        switch arg {
        case "--url":
            guard let url = URL(string: value(for: arg)) else { fail("invalid --url") }
            opts.urls.append(url)
        case "--creds": opts.creds = fileURL(for: arg)
        case "--nkey-file": opts.nkeyFile = fileURL(for: arg)
        case "--nkey": opts.nkey = value(for: arg)
        case "--user": opts.user = value(for: arg)
        case "--pass": opts.pass = value(for: arg)
        case "--token": opts.token = value(for: arg)
        case "--tls": opts.requireTls = true
        case "--tls-first": opts.tlsFirst = true
        case "--root-ca": opts.rootCA = fileURL(for: arg)
        case "--client-cert": opts.clientCert = fileURL(for: arg)
        case "--client-key": opts.clientKey = fileURL(for: arg)
        case "--subject": opts.subject = value(for: arg)
        case "--count":
            guard let n = Int(value(for: arg)), n > 0 else { fail("--count must be > 0") }
            opts.count = n
        case "--timeout":
            guard let t = TimeInterval(value(for: arg)), t > 0 else {
                fail("--timeout must be > 0")
            }
            opts.timeout = t
        case "--verbose": opts.verbose = true
        case "-h", "--help":
            printUsage()
            exit(0)
        default:
            fail("unknown argument: \(arg)")
        }
        index += 1
    }

    if opts.urls.isEmpty {
        printUsage()
        fail("--url is required")
    }
    return opts
}

// MARK: - Timeout wrapper

struct TimeoutError: Error {}

func withTimeout<T: Sendable>(
    _ seconds: TimeInterval, _ label: String, operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw TimeoutError()
        }
        defer { group.cancelAll() }
        do {
            let result = try await group.next()!
            return result
        } catch is TimeoutError {
            fail("timed out after \(seconds)s while: \(label)")
        }
    }
}

// MARK: - Main

let opts = parseArgs()

logger.logLevel = opts.verbose ? .debug : .info

note("nats-smoke starting")
note("urls: \(opts.urls.map { $0.absoluteString }.joined(separator: ", "))")
note(
    "auth: "
        + (opts.creds != nil
            ? "credentials file"
            : opts.nkeyFile != nil
                ? "nkey file"
                : opts.nkey != nil
                    ? "nkey"
                    : opts.user != nil
                        ? "user/pass" : opts.token != nil ? "token" : "none"))
note("subject: \(opts.subject), count: \(opts.count), timeout: \(opts.timeout)s")

var builder = NatsClientOptions().urls(opts.urls)
if let creds = opts.creds { builder = builder.credentialsFile(creds) }
if let nkeyFile = opts.nkeyFile { builder = builder.nkeyFile(nkeyFile) }
if let nkey = opts.nkey { builder = builder.nkey(nkey) }
if let user = opts.user, let pass = opts.pass { builder = builder.usernameAndPassword(user, pass) }
if let token = opts.token { builder = builder.token(token) }
if opts.requireTls { builder = builder.requireTls() }
if opts.tlsFirst { builder = builder.withTlsFirst() }
if let rootCA = opts.rootCA { builder = builder.rootCertificates(rootCA) }
if let cert = opts.clientCert, let key = opts.clientKey {
    builder = builder.clientCertificate(cert, key)
}

let client = builder.build()

client.on([.connected, .disconnected, .closed, .suspended, .lameDuckMode, .error]) { event in
    switch event {
    case .error(let err):
        note("event: error — \(err.description)")
    default:
        note("event: \(event.kind().rawValue)")
    }
}

do {
    note("connecting...")
    try await withTimeout(opts.timeout, "connect") { try await client.connect() }
    note("connected ✔ (\(client.connectedUrl?.absoluteString ?? "unknown url"))")

    let rtt = try await client.rtt()
    note(String(format: "rtt: %.1f ms", rtt * 1000))

    note("subscribing to \(opts.subject)...")
    let sub = try await client.subscribe(subject: opts.subject)
    let iterator = sub.makeAsyncIterator()

    // Drain expected messages on a child task while we publish.
    let expected = opts.count
    let subject = opts.subject
    let receiver = Task { () -> Int in
        var received = 0
        while received < expected {
            guard let msg = try await iterator.next() else { break }
            let body = msg.payload.flatMap { String(data: $0, encoding: .utf8) } ?? ""
            note("received [\(msg.subject)] \(body)")
            received += 1
        }
        return received
    }

    note("publishing \(opts.count) message(s)...")
    for i in 1...opts.count {
        try await client.publish("nats-smoke #\(i)".data(using: .utf8)!, subject: subject)
    }
    try await client.flush()

    let received = try await withTimeout(opts.timeout, "receive round-trip") {
        try await receiver.value
    }
    guard received == expected else {
        fail("round-trip incomplete: received \(received)/\(expected) messages")
    }
    note("round-trip ✔ received \(received)/\(expected) message(s)")

    try await sub.unsubscribe()
    try await client.close()
    note("closed ✔")

    print(
        "\n✅ PASS: real NATS connection established and "
            + "credential-carrying round-trip verified.")
    exit(0)
} catch {
    note("connection/round-trip failed")
    diagnose(error)
    fail("see diagnostics above")
}
