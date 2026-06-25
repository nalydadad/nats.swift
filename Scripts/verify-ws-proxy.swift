// Runtime verification for WebSocket-over-proxy: connects a
// URLSessionWebSocketTask to a NATS websocket listener through an explicit
// ProxyConfiguration (the same construction ProxyResolver produces) and
// asserts the server's INFO protocol message is received. This exercises the
// exact mechanism the proxy fix relies on to defeat the -1005
// "connection lost after upgrade" failure seen through corporate proxies.
//
// Exits 0 if INFO is received, 1 otherwise. Run with: swift verify-ws-proxy.swift
import Foundation
import Network

let proxyHost = "127.0.0.1"
let proxyPort: UInt16 = 8888
let natsWebSocket = URL(string: "ws://127.0.0.1:8080")!

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data("verify-ws-proxy: \(message)\n".utf8))
    exit(1)
}

if #available(macOS 14.0, *) {
    let endpoint = NWEndpoint.hostPort(
        host: NWEndpoint.Host(proxyHost),
        port: NWEndpoint.Port(rawValue: proxyPort)!)
    let proxyConfig = ProxyConfiguration(httpCONNECTProxy: endpoint)

    let configuration = URLSessionConfiguration.default
    configuration.proxyConfigurations = [proxyConfig]
    let session = URLSession(configuration: configuration)
    let task = session.webSocketTask(with: natsWebSocket)
    task.resume()

    let semaphore = DispatchSemaphore(value: 0)
    var receivedInfo = false

    func receiveNext() {
        task.receive { result in
            switch result {
            case .success(let message):
                let text: String
                switch message {
                case .data(let data): text = String(decoding: data, as: UTF8.self)
                case .string(let string): text = string
                @unknown default: text = ""
                }
                FileHandle.standardError.write(Data("recv: \(text.prefix(80))\n".utf8))
                if text.hasPrefix("INFO") {
                    receivedInfo = true
                    semaphore.signal()
                    return
                }
                receiveNext()
            case .failure(let error):
                FileHandle.standardError.write(Data("recv error: \(error)\n".utf8))
                semaphore.signal()
            }
        }
    }

    receiveNext()
    if semaphore.wait(timeout: .now() + 15) == .timedOut {
        fail("timed out waiting for INFO")
    }
    if !receivedInfo {
        fail("did not receive INFO")
    }
    FileHandle.standardError.write(Data("verify-ws-proxy: received INFO through proxy\n".utf8))
    exit(0)
} else {
    fail("requires macOS 14+")
}
