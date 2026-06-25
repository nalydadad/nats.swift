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

// `wss://`/`ws://` transport backed by Network.framework's `NWConnection` +
// `NWProtocolWebSocket`, routed through an explicit `ProxyConfiguration`
// resolved from the system proxy/PAC.
//
// This exists because `URLSessionWebSocketTask` cannot reliably tunnel a
// WebSocket through an HTTP/PAC proxy: the upgrade (HTTP 101) succeeds but the
// upgraded stream is dropped before any frame flows, surfacing as
// `NSURLErrorNetworkConnectionLost` (-1005) / "Connection not set before
// response is received". Network.framework's modern relay path handles
// WebSocket-over-proxy correctly, and `NWProtocolWebSocket` provides native
// framing/handshake/control-frame handling, so there is no hand-rolled
// WebSocket code to maintain. Requires iOS 17 / macOS 14 (`ProxyConfiguration`
// + `NWParameters.proxyConfigurations`); older OSes fall back to
// `URLSessionWebSocketTransport`.
#if canImport(Network)

    import Foundation
    import Network
    import NIOConcurrencyHelpers

    #if canImport(Security)
        import Security
    #endif

    @available(iOS 17.0, macOS 14.0, tvOS 17.0, watchOS 10.0, *)
    internal final class NWWebSocketTransport: NatsTransport, @unchecked Sendable {
        private let continuationBox = NIOLockedValueBox<
            AsyncThrowingStream<Data, Error>.Continuation?
        >(nil)
        let incomingMessages: AsyncThrowingStream<Data, Error>

        private let connectionBox = NIOLockedValueBox<NWConnection?>(nil)
        private let connectContinuation = NIOLockedValueBox<CheckedContinuation<Void, Error>?>(nil)
        private let queue = DispatchQueue(label: "io.nats.swift.nw-websocket")

        // Bound an initial connect so a stuck proxy/handshake doesn't hang the
        // NATS connect attempt (the NATS layer retries on failure).
        private let connectTimeout: TimeInterval = 30

        init() {
            var continuation: AsyncThrowingStream<Data, Error>.Continuation!
            self.incomingMessages = AsyncThrowingStream { continuation = $0 }
            continuationBox.withLockedValue { $0 = continuation }
        }

        func connect(url: URL, tls: TransportTLSOptions?) async throws {
            let parameters = try makeParameters(url: url, tls: tls)
            if let proxy = await ProxyResolver.resolve(for: url) {
                // Proxies are attached to an NWParameters via a PrivacyContext,
                // not set on NWParameters directly.
                let context = NWParameters.PrivacyContext(description: "io.nats.swift.proxy")
                context.proxyConfigurations = [proxy]
                parameters.setPrivacyContext(context)
            }

            let connection = NWConnection(to: .url(url), using: parameters)
            connectionBox.withLockedValue { $0 = connection }
            connection.stateUpdateHandler = { [weak self] state in
                self?.handleState(state)
            }

            do {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    group.addTask {
                        try await withTaskCancellationHandler {
                            try await withCheckedThrowingContinuation {
                                (cont: CheckedContinuation<Void, Error>) in
                                self.connectContinuation.withLockedValue { $0 = cont }
                                connection.start(queue: self.queue)
                            }
                        } onCancel: {
                            // Cancelling the connection drives `.cancelled`,
                            // which resumes the continuation above.
                            connection.cancel()
                        }
                    }
                    group.addTask {
                        try await Task.sleep(
                            nanoseconds: UInt64(self.connectTimeout * 1_000_000_000))
                        throw NatsError.ConnectError.timeout
                    }
                    // Wait for whichever finishes first, then cancel the rest.
                    _ = try await group.next()
                    group.cancelAll()
                }
            } catch {
                connection.cancel()
                throw error
            }

            startReceiveLoop(connection)
        }

        func startSecureConnection() throws {
            // No-op: TLS for the WS path is decided by the URL scheme (ws vs
            // wss) at `connect()` time; there is no in-place upgrade.
        }

        private func makeParameters(url: URL, tls: TransportTLSOptions?) throws -> NWParameters {
            let tcpOptions = NWProtocolTCP.Options()
            let parameters: NWParameters
            if url.scheme == "wss" {
                let tlsOptions = NWProtocolTLS.Options()
                try configureTLS(tlsOptions, tls: tls)
                parameters = NWParameters(tls: tlsOptions, tcp: tcpOptions)
            } else {
                parameters = NWParameters(tls: nil, tcp: tcpOptions)
            }

            let wsOptions = NWProtocolWebSocket.Options()
            wsOptions.autoReplyPing = true
            parameters.defaultProtocolStack.applicationProtocols.insert(wsOptions, at: 0)
            return parameters
        }

        private func configureTLS(
            _ options: NWProtocolTLS.Options, tls: TransportTLSOptions?
        ) throws {
            #if canImport(Security)
                let secOptions = options.securityProtocolOptions

                if let clientCertURL = tls?.clientCertificate, let clientKeyURL = tls?.clientKey {
                    let identity = try TLSIdentity.loadIdentity(
                        certificate: clientCertURL, key: clientKeyURL)
                    if let secIdentity = sec_identity_create(identity) {
                        sec_protocol_options_set_local_identity(secOptions, secIdentity)
                    }
                }

                if let rootCertURL = tls?.rootCertificate {
                    let rootCertificate = try TLSIdentity.loadCertificate(pem: rootCertURL)
                    sec_protocol_options_set_verify_block(
                        secOptions,
                        { _, secTrust, complete in
                            let trust = sec_trust_copy_ref(secTrust).takeRetainedValue()
                            SecTrustSetAnchorCertificates(trust, [rootCertificate] as CFArray)
                            SecTrustSetAnchorCertificatesOnly(trust, true)
                            complete(SecTrustEvaluateWithError(trust, nil))
                        }, queue)
                }
            #endif
        }

        private func handleState(_ state: NWConnection.State) {
            switch state {
            case .ready:
                resumeConnect(throwing: nil)
            case .failed(let error):
                if !resumeConnect(throwing: error) {
                    finish(throwing: error)
                }
            case .cancelled:
                if !resumeConnect(throwing: NatsError.ClientError.connectionClosed) {
                    finish(throwing: nil)
                }
            case .waiting(let error):
                // Transient: the connect timeout will fail the attempt if this
                // never resolves to `.ready`.
                logger.debug("nw websocket waiting: \(error)")
            default:
                break
            }
        }

        @discardableResult
        private func resumeConnect(throwing error: Error?) -> Bool {
            let cont = connectContinuation.withLockedValue {
                (stored: inout CheckedContinuation<Void, Error>?)
                    -> CheckedContinuation<Void, Error>? in
                let toResume = stored
                stored = nil
                return toResume
            }
            guard let cont else { return false }
            if let error {
                cont.resume(throwing: error)
            } else {
                cont.resume()
            }
            return true
        }

        private func startReceiveLoop(_ connection: NWConnection) {
            receiveNext(connection)
        }

        private func receiveNext(_ connection: NWConnection) {
            connection.receiveMessage { [weak self] content, context, _, error in
                guard let self else { return }

                if let context,
                    let metadata = context.protocolMetadata(
                        definition: NWProtocolWebSocket.definition) as? NWProtocolWebSocket.Metadata
                {
                    switch metadata.opcode {
                    case .binary:
                        if let content, !content.isEmpty { self.yield(content) }
                    case .text:
                        logger.error(
                            "received a text WebSocket frame; nats.swift only sends/accepts binary frames, dropping it"
                        )
                    case .close:
                        self.finish(throwing: nil)
                        return
                    case .ping, .pong, .cont:
                        break
                    @unknown default:
                        break
                    }
                } else if let content, !content.isEmpty {
                    self.yield(content)
                }

                if let error {
                    self.finish(throwing: error)
                    return
                }
                self.receiveNext(connection)
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
            guard let connection = connectionBox.withLockedValue({ $0 }) else {
                throw NatsError.ClientError.invalidConnection("not connected")
            }
            let metadata = NWProtocolWebSocket.Metadata(opcode: .binary)
            let context = NWConnection.ContentContext(
                identifier: "binaryMessage", metadata: [metadata])
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<Void, Error>) in
                connection.send(
                    content: data, contentContext: context, isComplete: true,
                    completion: .contentProcessed { error in
                        if let error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume()
                        }
                    })
            }
        }

        func close() {
            connectionBox.withLockedValue { $0?.cancel() }
            finish(throwing: nil)
        }
    }

#endif
