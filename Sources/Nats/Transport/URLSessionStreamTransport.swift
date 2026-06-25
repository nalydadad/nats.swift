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

// `URLSessionStreamTask` is unavailable in swift-corelibs-foundation; this
// transport is Darwin-only. Linux uses `NIOStreamTransport` instead (see
// Transport/NIOStreamTransport.swift).
#if !canImport(FoundationNetworking)

    import Foundation
    import NIOConcurrencyHelpers

    /// `nats://`/`tls://` transport backed by `URLSessionStreamTask` — the
    /// `URLSession`-native equivalent of a raw TCP/TLS socket. Like
    /// `URLSessionWebSocketTask`, it inherits the system proxy/PAC and trust
    /// store, so plain (non-WebSocket) NATS connections keep working without
    /// any NIO socket I/O. TLS can be requested upfront or upgraded in place
    /// mid-stream via `startSecureConnection()`, mirroring NATS's own
    /// `tlsFirst` vs. INFO-driven TLS negotiation.
    internal final class URLSessionStreamTransport: NSObject, NatsTransport, @unchecked Sendable {
        private let continuationBox = NIOLockedValueBox<
            AsyncThrowingStream<Data, Error>.Continuation?
        >(nil)
        let incomingMessages: AsyncThrowingStream<Data, Error>

        private let taskBox = NIOLockedValueBox<URLSessionStreamTask?>(nil)
        private let sessionBox = NIOLockedValueBox<URLSession?>(nil)
        private let receiveLoopBox = NIOLockedValueBox<Task<Void, Never>?>(nil)

        override init() {
            var continuation: AsyncThrowingStream<Data, Error>.Continuation!
            self.incomingMessages = AsyncThrowingStream { continuation = $0 }
            super.init()
            continuationBox.withLockedValue { $0 = continuation }
        }

        func connect(url: URL, tls: TransportTLSOptions?) async throws {
            guard let host = url.host, let port = url.port else {
                throw NatsError.ConnectError.invalidConfig("no url")
            }
            let configuration = URLSessionConfiguration.default
            #if canImport(Security)
                let delegate: TLSChallengeDelegate? = try TLSChallengeDelegate(tls: tls)
            #else
                let delegate: URLSessionDelegate? = nil
            #endif
            let session = URLSession(
                configuration: configuration, delegate: delegate, delegateQueue: nil)
            let task = session.streamTask(withHostName: host, port: port)
            sessionBox.withLockedValue { $0 = session }
            taskBox.withLockedValue { $0 = task }
            task.resume()
            startReceiveLoop()
        }

        func startSecureConnection() throws {
            guard let task = taskBox.withLockedValue({ $0 }) else {
                throw NatsError.ClientError.invalidConnection("not connected")
            }
            task.startSecureConnection()
        }

        private func startReceiveLoop() {
            let loop = Task { [weak self] in
                guard let self else { return }
                while !Task.isCancelled {
                    guard let task = self.taskBox.withLockedValue({ $0 }) else { break }
                    do {
                        let (data, atEOF) = try await Self.readChunk(task: task)
                        if let data, !data.isEmpty {
                            self.yield(data)
                        }
                        if atEOF {
                            self.finish(throwing: nil)
                            return
                        }
                    } catch {
                        self.finish(throwing: error)
                        return
                    }
                }
            }
            receiveLoopBox.withLockedValue { $0 = loop }
        }

        private static func readChunk(task: URLSessionStreamTask) async throws -> (Data?, Bool) {
            try await withCheckedThrowingContinuation { continuation in
                task.readData(ofMinLength: 1, maxLength: 64 * 1024, timeout: 0) {
                    data, atEOF, error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume(returning: (data, atEOF))
                    }
                }
            }
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
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, Error>) in
                task.write(data, timeout: 0) { error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }

        func close() {
            receiveLoopBox.withLockedValue { $0?.cancel() }
            if let task = taskBox.withLockedValue({ $0 }) {
                task.closeWrite()
                task.closeRead()
            }
            sessionBox.withLockedValue { $0?.finishTasksAndInvalidate() }
            finish(throwing: nil)
        }
    }

#endif
