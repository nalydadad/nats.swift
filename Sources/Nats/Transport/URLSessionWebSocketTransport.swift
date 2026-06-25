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

import Foundation
import NIOConcurrencyHelpers

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// `ws://`/`wss://` transport backed by `URLSessionWebSocketTask`, which
/// (unlike NIO's WebSocket client) inherits the system/PAC proxy
/// configuration and certificate trust store. Always sends and expects
/// binary frames; the NATS protocol is carried as raw bytes inside them.
internal final class URLSessionWebSocketTransport: NSObject, NatsTransport, @unchecked Sendable {
    private let continuationBox = NIOLockedValueBox<AsyncThrowingStream<Data, Error>.Continuation?>(
        nil)
    let incomingMessages: AsyncThrowingStream<Data, Error>

    private let taskBox = NIOLockedValueBox<URLSessionWebSocketTask?>(nil)
    private let sessionBox = NIOLockedValueBox<URLSession?>(nil)
    private let receiveLoopBox = NIOLockedValueBox<Task<Void, Never>?>(nil)

    override init() {
        var continuation: AsyncThrowingStream<Data, Error>.Continuation!
        self.incomingMessages = AsyncThrowingStream { continuation = $0 }
        super.init()
        continuationBox.withLockedValue { $0 = continuation }
    }

    func connect(url: URL, tls: TransportTLSOptions?) async throws {
        let configuration = URLSessionConfiguration.default
        #if canImport(Security)
            let delegate: TLSChallengeDelegate? = try TLSChallengeDelegate(tls: tls)
        #else
            let delegate: URLSessionDelegate? = nil
        #endif
        let session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        let task = session.webSocketTask(with: url)
        sessionBox.withLockedValue { $0 = session }
        taskBox.withLockedValue { $0 = task }
        task.resume()
        startReceiveLoop()
    }

    func startSecureConnection() throws {
        // No-op: TLS for the WS path is fully decided by the URL scheme
        // (ws vs wss) at `connect()` time; there is no in-place upgrade.
    }

    private func startReceiveLoop() {
        let loop = Task { [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                guard let task = self.taskBox.withLockedValue({ $0 }) else { break }
                do {
                    let message = try await task.receive()
                    switch message {
                    case .data(let data):
                        self.yield(data)
                    case .string:
                        logger.error(
                            "received a text WebSocket frame; nats.swift only sends/accepts binary frames, dropping it"
                        )
                    @unknown default:
                        break
                    }
                } catch {
                    let nsError = error as NSError
                    let reason = task.closeReason.flatMap { String(data: $0, encoding: .utf8) }
                    logger.error(
                        "websocket receive failed: \(error) [domain=\(nsError.domain) code=\(nsError.code)] closeCode=\(task.closeCode.rawValue) reason=\(reason ?? "<none>")"
                    )
                    self.finish(throwing: error)
                    return
                }
            }
        }
        receiveLoopBox.withLockedValue { $0 = loop }
    }

    private func yield(_ data: Data) {
        continuationBox.withLockedValue { _ = $0?.yield(data) }
    }

    private func finish(throwing error: Error?) {
        continuationBox.withLockedValue {
            $0?.finish(throwing: error)
            $0 = nil
        }
    }

    func send(_ data: Data) async throws {
        guard let task = taskBox.withLockedValue({ $0 }) else {
            throw NatsError.ClientError.invalidConnection("not connected")
        }
        try await task.send(.data(data))
    }

    func close() {
        receiveLoopBox.withLockedValue { $0?.cancel() }
        taskBox.withLockedValue { $0?.cancel(with: .normalClosure, reason: nil) }
        sessionBox.withLockedValue { $0?.finishTasksAndInvalidate() }
        finish(throwing: nil)
    }
}
