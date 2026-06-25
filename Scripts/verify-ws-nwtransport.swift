// Runtime verification for the NWConnection WebSocket transport: connects an
// `NWConnection` + `NWProtocolWebSocket` to a NATS websocket (TLS) listener
// directly (no proxy) and asserts the server's INFO protocol message is
// received after the HTTP 101 upgrade.
//
// This exercises the exact mechanism `NWWebSocketTransport` uses to defeat the
// `URLSessionWebSocketTask` "-1005 connection lost after upgrade" failure: the
// TLS handshake, the WebSocket upgrade, and the first frame all flow on a
// single `NWConnection`. A trust-all verify block is used because the server
// presents a throwaway self-signed cert.
//
// Exits 0 if INFO is received, 1 otherwise.
// Run with: swift verify-ws-nwtransport.swift
import Foundation
import Network

let natsWebSocket = URL(string: "wss://127.0.0.1:8443")!

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data("verify-ws-nwtransport: \(message)\n".utf8))
    exit(1)
}

let queue = DispatchQueue(label: "verify-ws-nwtransport")

let tlsOptions = NWProtocolTLS.Options()
// Trust-all: the server uses a throwaway self-signed cert.
sec_protocol_options_set_verify_block(
    tlsOptions.securityProtocolOptions,
    { _, _, complete in complete(true) },
    queue)

let parameters = NWParameters(tls: tlsOptions, tcp: NWProtocolTCP.Options())
let wsOptions = NWProtocolWebSocket.Options()
wsOptions.autoReplyPing = true
parameters.defaultProtocolStack.applicationProtocols.insert(wsOptions, at: 0)

let connection = NWConnection(to: .url(natsWebSocket), using: parameters)

let semaphore = DispatchSemaphore(value: 0)
var receivedInfo = false

func receiveNext() {
    connection.receiveMessage { content, _, _, error in
        if let content, !content.isEmpty {
            let text = String(decoding: content, as: UTF8.self)
            FileHandle.standardError.write(Data("recv: \(text.prefix(80))\n".utf8))
            if text.hasPrefix("INFO") {
                receivedInfo = true
                semaphore.signal()
                return
            }
        }
        if let error {
            FileHandle.standardError.write(Data("recv error: \(error)\n".utf8))
            semaphore.signal()
            return
        }
        receiveNext()
    }
}

connection.stateUpdateHandler = { state in
    switch state {
    case .ready:
        FileHandle.standardError.write(Data("connection ready (101 upgrade complete)\n".utf8))
        receiveNext()
    case .failed(let error):
        FileHandle.standardError.write(Data("connection failed: \(error)\n".utf8))
        semaphore.signal()
    case .waiting(let error):
        FileHandle.standardError.write(Data("connection waiting: \(error)\n".utf8))
    default:
        break
    }
}

connection.start(queue: queue)

if semaphore.wait(timeout: .now() + 15) == .timedOut {
    fail("timed out waiting for INFO")
}
if !receivedInfo {
    fail("did not receive INFO")
}
FileHandle.standardError.write(
    Data("verify-ws-nwtransport: received INFO over NWConnection WebSocket\n".utf8))
exit(0)
