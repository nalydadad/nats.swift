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

import Atomics
import Dispatch
import Foundation
import NIO
import NIOConcurrencyHelpers
import NIOFoundationCompat
import NKeys

#if canImport(Network)
    import Network
#endif

#if canImport(FoundationNetworking)
    import FoundationNetworking
    import NIOSSL
#endif

final class ConnectionHandler: Sendable {
    let lang = "Swift"
    let version = "0.0.1"

    private let _connectedUrl = NIOLockedValueBox<URL?>(nil)
    internal var connectedUrl: URL? {
        get { _connectedUrl.withLockedValue { $0 } }
        set { _connectedUrl.withLockedValue { $0 = newValue } }
    }
    private let _transport = NIOLockedValueBox<(any NatsTransport)?>(nil)
    internal var transport: (any NatsTransport)? {
        get { _transport.withLockedValue { $0 } }
        set { _transport.withLockedValue { $0 = newValue } }
    }
    private let readLoopTask = NIOLockedValueBox<Task<Void, Never>?>(nil)

    private let eventHandlerStore = NIOLockedValueBox<[NatsEventKind: [NatsEventHandler]]>([:])

    // Connection options
    internal let retryOnFailedConnect: Bool
    private let _urls: NIOLockedValueBox<[URL]>
    private var urls: [URL] {
        get { _urls.withLockedValue { $0 } }
        set { _urls.withLockedValue { $0 = newValue } }
    }
    // nanoseconds representation of TimeInterval
    private let reconnectWait: UInt64
    private let maxReconnects: Int?
    private let retainServersOrder: Bool
    private let pingInterval: TimeInterval
    private let requireTls: Bool
    private let tlsFirst: Bool
    private let rootCertificate: URL?
    private let clientCertificate: URL?
    private let clientKey: URL?

    private let state = NIOLockedValueBox(NatsState.pending)
    private let subscriptions = NIOLockedValueBox([UInt64: NatsSubscription]())

    // Helper methods for state access
    internal var currentState: NatsState {
        state.withLockedValue { $0 }
    }

    internal func setState(_ newState: NatsState) {
        state.withLockedValue { $0 = newState }
    }

    private let subscriptionCounter = ManagedAtomic<UInt64>(0)
    private let _serverInfo = NIOLockedValueBox<ServerInfo?>(nil)
    private var serverInfo: ServerInfo? {
        get { _serverInfo.withLockedValue { $0 } }
        set { _serverInfo.withLockedValue { $0 = newValue } }
    }

    private let auth: Auth?
    private let parseRemainder = NIOLockedValueBox<Data?>(nil)
    private let _pingTask = NIOLockedValueBox<Task<Void, Never>?>(nil)
    private var pingTask: Task<Void, Never>? {
        get { _pingTask.withLockedValue { $0 } }
        set { _pingTask.withLockedValue { $0 = newValue } }
    }
    private let outstandingPings = ManagedAtomic<UInt8>(0)
    private let _reconnectAttempts = ManagedAtomic<Int>(0)
    private var reconnectAttempts: Int {
        get { _reconnectAttempts.load(ordering: .relaxed) }
        set { _reconnectAttempts.store(newValue, ordering: .relaxed) }
    }
    private let capturedConnectionError = NIOLockedValueBox<Error?>(nil)

    private let _reconnectTask = NIOLockedValueBox<Task<(), Error>?>(nil)
    private var reconnectTask: Task<(), Error>? {
        get { _reconnectTask.withLockedValue { $0 } }
        set { _reconnectTask.withLockedValue { $0 = newValue } }
    }

    private let serverInfoContinuation = NIOLockedValueBox<CheckedContinuation<ServerInfo, Error>?>(
        nil)
    private let connectionEstablishedContinuation = NIOLockedValueBox<
        CheckedContinuation<Void, Error>?
    >(nil)

    private let pingQueue = ConcurrentQueue<RttCommand>()
    private let _batchBuffer = NIOLockedValueBox<BatchBuffer?>(nil)
    private(set) var batchBuffer: BatchBuffer? {
        get { _batchBuffer.withLockedValue { $0 } }
        set { _batchBuffer.withLockedValue { $0 = newValue } }
    }

    init(
        urls: [URL], reconnectWait: TimeInterval, maxReconnects: Int?,
        retainServersOrder: Bool,
        pingInterval: TimeInterval, auth: Auth?, requireTls: Bool, tlsFirst: Bool,
        clientCertificate: URL?, clientKey: URL?,
        rootCertificate: URL?, retryOnFailedConnect: Bool
    ) {
        self._urls = NIOLockedValueBox(urls)
        self.reconnectWait = UInt64(reconnectWait * 1_000_000_000)
        self.maxReconnects = maxReconnects
        self.retainServersOrder = retainServersOrder
        self.auth = auth
        self.pingInterval = pingInterval
        self.requireTls = requireTls
        self.tlsFirst = tlsFirst
        self.clientCertificate = clientCertificate
        self.clientKey = clientKey
        self.rootCertificate = rootCertificate
        self.retryOnFailedConnect = retryOnFailedConnect
    }

    private func handleReceivedChunk(_ data: Data) {
        let state: NatsState = self.currentState

        guard state == .connected || state == .pending || state == .connecting else {
            parseRemainder.withLockedValue { $0 = nil }
            return
        }

        var inputChunk = data

        let remainder = parseRemainder.withLockedValue { value in
            let current = value
            value = nil
            return current
        }

        if let remainder = remainder, !remainder.isEmpty {
            inputChunk.prepend(remainder)
        }

        let parseResult: (ops: [ServerOp], remainder: Data?)
        do {
            parseResult = try inputChunk.parseOutMessages()
        } catch {
            // if parsing throws an error, clear remainder, then reconnect
            parseRemainder.withLockedValue { $0 = nil }

            if self.currentState != .closed && self.currentState != .suspended {
                triggerDisconnectDueToError(error)
            }

            return
        }
        if let remainder = parseResult.remainder {
            parseRemainder.withLockedValue { $0 = remainder }
        }
        for op in parseResult.ops {
            // Only resume the server info continuation when we actually receive
            // an INFO or -ERR op. Do NOT clear it for unrelated ops.
            switch op {
            case .error(let err):
                if let continuation = serverInfoContinuation.withLockedValue({ cont in
                    let toResume = cont
                    cont = nil
                    return toResume
                }) {
                    logger.debug("server info error")
                    continuation.resume(throwing: err)
                    continue
                }
            case .info(let info):
                if let continuation = serverInfoContinuation.withLockedValue({ cont in
                    let toResume = cont
                    cont = nil
                    return toResume
                }) {
                    logger.debug("server info")
                    continuation.resume(returning: info)
                    continue
                }
            default:
                break
            }

            let connEstablishedCont = connectionEstablishedContinuation.withLockedValue { cont in
                let toResume = cont
                cont = nil
                return toResume
            }

            if let continuation = connEstablishedCont {
                logger.debug("conn established")
                switch op {
                case .error(let err):
                    continuation.resume(throwing: err)
                default:
                    continuation.resume()
                }
                continue
            }

            switch op {
            case .ping:
                logger.debug("ping")
                Task {
                    do {
                        try await self.write(operation: .pong)
                    } catch let err as NatsError.ClientError {
                        logger.error("error sending pong: \(err)")
                        self.fire(
                            .error(err))
                    } catch {
                        logger.error("unexpected error sending pong: \(error)")
                    }
                }
            case .pong:
                logger.debug("pong")
                self.outstandingPings.store(0, ordering: AtomicStoreOrdering.relaxed)
                self.pingQueue.dequeue()?.setRoundTripTime()
            case .error(let err):
                logger.debug("error \(err)")

                switch err {
                case .staleConnection, .maxConnectionsExceeded:
                    parseRemainder.withLockedValue { $0 = nil }
                    triggerDisconnectDueToError(err)
                case .permissionsViolation(let operation, let subject, _):
                    switch operation {
                    case .subscribe:
                        subscriptions.withLockedValue { subs in
                            for (_, s) in subs {
                                if s.subject == subject {
                                    s.receiveError(NatsError.SubscriptionError.permissionDenied)
                                }
                            }
                        }
                    case .publish:
                        self.fire(.error(err))
                    }
                default:
                    self.fire(.error(err))
                }

                let normalizedError = err.normalizedError
                // on some errors, force reconnect
                if normalizedError == "stale connection"
                    || normalizedError == "maximum connections exceeded"
                {
                    parseRemainder.withLockedValue { $0 = nil }
                    triggerDisconnectDueToError(err)
                } else {
                    self.fire(.error(err))
                }
            case .message(let msg):
                self.handleIncomingMessage(msg)
            case .hMessage(let msg):
                self.handleIncomingMessage(msg)
            case .info(let serverInfo):
                logger.debug("info \(op)")
                self.serverInfo = serverInfo
                if serverInfo.lameDuckMode {
                    self.fire(.lameDuckMode)
                }
                updateServersList(info: serverInfo)
            default:
                logger.debug("unknown operation type: \(op)")
            }
        }
    }

    private func handleIncomingMessage(_ message: MessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: nil, status: nil, description: nil)
        subscriptions.withLockedValue { subs in
            if let sub = subs[message.sid] {
                sub.receiveMessage(natsMsg)
            }
        }
    }

    private func handleIncomingMessage(_ message: HMessageInbound) {
        let natsMsg = NatsMessage(
            payload: message.payload, subject: message.subject, replySubject: message.reply,
            length: message.length, headers: message.headers, status: message.status,
            description: message.description)
        subscriptions.withLockedValue { subs in
            if let sub = subs[message.sid] {
                sub.receiveMessage(natsMsg)
            }
        }
    }

    private func startReadLoop(transport: any NatsTransport) {
        parseRemainder.withLockedValue { $0 = nil }
        let task = Task {
            var thrownError: Error? = nil
            do {
                for try await chunk in transport.incomingMessages {
                    self.handleReceivedChunk(chunk)
                }
            } catch {
                thrownError = error
            }
            self.handleReadLoopEnded(error: thrownError)
        }
        readLoopTask.withLockedValue { $0 = task }
    }

    private func triggerDisconnectDueToError(_ error: Error) {
        logger.debug("Encountered error on the transport: \(error)")

        let isConnecting = state.withLockedValue { $0 == .pending || $0 == .connecting }
        if isConnecting {
            capturedConnectionError.withLockedValue { $0 = error }
        }

        if let natsErr = error as? NatsErrorProtocol {
            self.fire(.error(natsErr))
        } else {
            logger.error("unexpected error: \(error)")
        }

        transport?.close()
    }

    private func handleReadLoopEnded(error: Error?) {
        logger.debug("transport read loop ended")

        // If we lost the transport before we delivered server INFO or connection
        // establishment, make sure to fail any pending continuations to avoid leaks.
        // Use captured error if available (e.g., TLS failure), otherwise use connectionClosed.
        let capturedError = capturedConnectionError.withLockedValue { captured in
            let c = captured
            captured = nil
            return c
        }
        let errorToUse: Error
        if let capturedError {
            errorToUse = NatsError.ConnectError.tlsFailure(capturedError)
        } else if let error {
            errorToUse = error
        } else {
            errorToUse = NatsError.ClientError.connectionClosed
        }

        if let continuation = serverInfoContinuation.withLockedValue({ cont in
            let toResume = cont
            cont = nil
            return toResume
        }) {
            continuation.resume(throwing: errorToUse)
        }

        if let continuation = connectionEstablishedContinuation.withLockedValue({ cont in
            let toResume = cont
            cont = nil
            return toResume
        }) {
            continuation.resume(throwing: errorToUse)
        }

        let shouldHandleDisconnect = state.withLockedValue { $0 == .connected }
        if shouldHandleDisconnect {
            handleDisconnect()
        }
    }

    func connect() async throws {
        self.setState(.connecting)
        var servers = self.urls
        if !self.retainServersOrder {
            servers = self.urls.shuffled()
        }
        var lastErr: Error?

        // if there are more reconnect attempts than the number of servers,
        // we are after the initial connect, so sleep between servers
        let shouldSleep = self.reconnectAttempts >= self.urls.count
        for s in servers {
            if let maxReconnects {
                if reconnectAttempts > 0 && reconnectAttempts >= maxReconnects {
                    throw NatsError.ClientError.maxReconnects
                }
            }
            self.reconnectAttempts += 1
            if shouldSleep {
                try await Task.sleep(nanoseconds: self.reconnectWait)
            }

            do {
                try await connectToServer(s: s)
            } catch let error as NatsError.ConnectError {
                if case .invalidConfig(_) = error {
                    throw error
                }
                logger.debug("error connecting to server: \(error)")
                lastErr = error
                continue
            } catch {
                logger.debug("error connecting to server: \(error)")
                lastErr = error
                continue
            }
            lastErr = nil
            break
        }
        if let lastErr {
            self.state.withLockedValue { $0 = .disconnected }
            switch lastErr {
            case let err as NatsError.ServerError:
                throw err
            case let err as NatsError.ConnectError:
                throw err
            case let error as URLError:
                switch error.code {
                case .timedOut:
                    throw NatsError.ConnectError.timeout
                case .cannotFindHost, .dnsLookupFailed:
                    throw NatsError.ConnectError.dns(error)
                case .networkConnectionLost, .cannotConnectToHost, .notConnectedToInternet:
                    // The connection itself failed (e.g. the peer/proxy dropped
                    // the socket); this is not a TLS handshake failure even on a
                    // wss:// / requireTls connection, so don't mislabel it.
                    throw NatsError.ConnectError.io(error)
                default:
                    if self.requireTls || self.tlsFirst || self.rootCertificate != nil
                        || self.clientCertificate != nil
                    {
                        throw NatsError.ConnectError.tlsFailure(error)
                    }
                    throw NatsError.ConnectError.io(error)
                }
            #if canImport(Network)
                // The Apple WebSocket transport (`NWWebSocketTransport`)
                // surfaces `NWError`, not `URLError`. Map connection-level
                // failures to `.io`/`.dns`/`.timeout` so they aren't mislabelled
                // as `tlsFailure` on a wss:// / requireTls connection.
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
            #if canImport(FoundationNetworking)
                // Linux fallback (`NIOStreamTransport`) surfaces NIO's own
                // connection/TLS error types instead of `URLError`.
                case let error as NIOConnectionError:
                    if let dnsAAAAError = error.dnsAAAAError {
                        throw NatsError.ConnectError.dns(dnsAAAAError)
                    } else if let dnsAError = error.dnsAError {
                        throw NatsError.ConnectError.dns(dnsAError)
                    } else {
                        throw NatsError.ConnectError.io(error)
                    }
                case let err as NIOSSLError:
                    throw NatsError.ConnectError.tlsFailure(err)
                case let err as BoringSSLError:
                    throw NatsError.ConnectError.tlsFailure(err)
            #endif
            default:
                throw NatsError.ConnectError.io(lastErr)
            }
        }
        self.reconnectAttempts = 0
        guard self.transport != nil else {
            throw NatsError.ClientError.internalError("empty transport")
        }
        startPingTask()
        logger.debug("connection established")
        return
    }

    private func startPingTask() {
        let interval = self.pingInterval
        let task = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(interval * 1_000_000_000))
                if Task.isCancelled { break }
                await self.sendPing()
            }
        }
        self.pingTask = task
    }

    private func connectToServer(s: URL) async throws {
        var infoTask: Task<(), Never>? = nil
        // this continuation can throw NatsError.ServerError if server responds with
        // -ERR to client connect (e.g. auth error)
        let info: ServerInfo = try await withCheckedThrowingContinuation { continuation in
            serverInfoContinuation.withLockedValue { $0 = continuation }
            infoTask = Task {
                await withTaskCancellationHandler {
                    do {
                        guard s.host != nil, s.port != nil else {
                            throw NatsError.ConnectError.invalidConfig("no url")
                        }

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

                        let tls = TransportTLSOptions(
                            rootCertificate: self.rootCertificate,
                            clientCertificate: self.clientCertificate,
                            clientKey: self.clientKey)
                        try await newTransport.connect(url: s, tls: tls)

                        if self.requireTls && self.tlsFirst {
                            try newTransport.startSecureConnection()
                        }

                        self.transport = newTransport
                        self.batchBuffer = BatchBuffer(transport: newTransport)
                        self.startReadLoop(transport: newTransport)
                    } catch {
                        let continuationToResume: CheckedContinuation<ServerInfo, Error>? = self
                            .serverInfoContinuation.withLockedValue { cont in
                                guard let c = cont else { return nil }
                                cont = nil
                                return c
                            }
                        if let continuation = continuationToResume {
                            continuation.resume(throwing: error)
                        }
                    }
                } onCancel: {
                    logger.debug("Connection task cancelled")
                    // Clean up resources
                    self.transport?.close()
                    self.transport = nil
                    self.batchBuffer = nil

                    let continuationToResume: CheckedContinuation<ServerInfo, Error>? = self
                        .serverInfoContinuation.withLockedValue { cont in
                            guard let c = cont else { return nil }
                            cont = nil
                            return c
                        }
                    if let continuation = continuationToResume {
                        continuation.resume(throwing: NatsError.ClientError.cancelled)
                    }
                }
            }
        }

        await infoTask?.value
        self.serverInfo = info
        if (info.tlsRequired ?? false || self.requireTls) && !self.tlsFirst
            && s.scheme != "wss" && s.scheme != "ws"
        {
            try self.transport?.startSecureConnection()
        }

        try await sendClientConnectInit()
        self.connectedUrl = s
    }

    private func sendClientConnectInit() async throws {
        var initialConnect = ConnectInfo(
            verbose: false, pedantic: false, userJwt: nil, nkey: "", name: "", echo: true,
            lang: self.lang, version: self.version, natsProtocol: .dynamic, tlsRequired: false,
            user: self.auth?.user ?? "", pass: self.auth?.password ?? "",
            authToken: self.auth?.token ?? "", headers: true, noResponders: true)

        if self.auth?.nkey != nil && self.auth?.nkeyPath != nil {
            throw NatsError.ConnectError.invalidConfig("cannot use both nkey and nkeyPath")
        }
        if let auth = self.auth, let credentialsPath = auth.credentialsPath {
            let credentials = try await URLSession.shared.data(from: credentialsPath).0
            guard let jwt = JwtUtils.parseDecoratedJWT(contents: credentials) else {
                throw NatsError.ConnectError.invalidConfig(
                    "failed to extract JWT from credentials file")
            }
            guard let nkey = JwtUtils.parseDecoratedNKey(contents: credentials) else {
                throw NatsError.ConnectError.invalidConfig(
                    "failed to extract NKEY from credentials file")
            }
            guard let nonce = self.serverInfo?.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let keypair = try KeyPair(seed: String(data: nkey, encoding: .utf8)!)
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.userJwt = String(data: jwt, encoding: .utf8)!
        }
        if let nkey = self.auth?.nkeyPath {
            let nkeyData = try await URLSession.shared.data(from: nkey).0

            guard let nkeyContent = String(data: nkeyData, encoding: .utf8) else {
                throw NatsError.ConnectError.invalidConfig("failed to read NKEY file")
            }
            let whitespace: CharacterSet = .whitespacesAndNewlines
            let keypair = try KeyPair(
                seed: nkeyContent.trimmingCharacters(in: whitespace)
            )

            guard let nonce = self.serverInfo?.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let sig = try keypair.sign(input: nonce.data(using: .utf8)!)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        if let nkey = self.auth?.nkey {
            let keypair = try KeyPair(seed: nkey)
            guard let nonce = self.serverInfo?.nonce else {
                throw NatsError.ConnectError.invalidConfig("missing nonce")
            }
            let nonceData = nonce.data(using: .utf8)!
            let sig = try keypair.sign(input: nonceData)
            let base64sig = sig.base64EncodedURLSafeNotPadded()
            initialConnect.signature = base64sig
            initialConnect.nkey = keypair.publicKeyEncoded
        }
        let connect = initialConnect
        // this continuation can throw NatsError.ServerError if server responds with
        // -ERR to client connect (e.g. auth error)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                connectionEstablishedContinuation.withLockedValue { $0 = continuation }
                Task.detached {
                    do {
                        try await self.write(operation: ClientOp.connect(connect))
                        try await self.write(operation: ClientOp.ping)
                    } catch {
                        let continuationToResume: CheckedContinuation<Void, Error>? = self
                            .connectionEstablishedContinuation.withLockedValue { cont in
                                guard let c = cont else { return nil }
                                cont = nil
                                return c
                            }
                        if let continuation = continuationToResume {
                            continuation.resume(throwing: error)
                        }
                    }
                }
            }
        } onCancel: {
            logger.debug("Client connect initialization cancelled")
            // Clean up resources
            self.transport?.close()
            self.transport = nil
            self.batchBuffer = nil

            let continuationToResume: CheckedContinuation<Void, Error>? = self
                .connectionEstablishedContinuation.withLockedValue { cont in
                    guard let c = cont else { return nil }
                    cont = nil
                    return c
                }
            if let continuation = continuationToResume {
                continuation.resume(throwing: NatsError.ClientError.cancelled)
            }
        }
    }

    private func updateServersList(info: ServerInfo) {
        if let connectUrls = info.connectUrls {
            for connectUrl in connectUrls {
                guard let url = URL(string: connectUrl) else {
                    continue
                }
                if !self.urls.contains(url) {
                    urls.append(url)
                }
            }
        }
    }

    func close() async throws {
        self.reconnectTask?.cancel()
        try await self.reconnectTask?.value

        self.state.withLockedValue { $0 = .closed }
        self.pingTask?.cancel()
        readLoopTask.withLockedValue { $0?.cancel() }
        self.transport?.close()

        self.fire(.closed)
    }

    private func disconnect() async throws {
        self.pingTask?.cancel()
        self.transport?.close()
    }

    func suspend() async throws {
        self.reconnectTask?.cancel()
        _ = try await self.reconnectTask?.value

        let shouldClose = self.state.withLockedValue { currentState in
            let wasConnected = currentState == .connected
            currentState = .suspended
            return wasConnected
        }

        if shouldClose {
            self.pingTask?.cancel()
            self.transport?.close()
        }

        self.fire(.suspended)
    }

    func resume() async throws {
        let canResume = self.state.withLockedValue { $0 == .suspended }
        guard canResume else {
            throw NatsError.ClientError.invalidConnection(
                "unable to resume connection - connection is not in suspended state")
        }
        self.handleReconnect()
    }

    func reconnect() async throws {
        try await suspend()
        try await resume()
    }

    internal func sendPing(_ rttCommand: RttCommand? = nil) async {
        let pingsOut = self.outstandingPings.wrappingIncrementThenLoad(
            ordering: AtomicUpdateOrdering.relaxed)
        if pingsOut > 2 {
            handleDisconnect()
            return
        }
        let ping = ClientOp.ping
        do {
            self.pingQueue.enqueue(rttCommand ?? RttCommand.makeFrom())
            try await self.write(operation: ping)
            logger.debug("sent ping: \(pingsOut)")
        } catch {
            logger.error("Unable to send ping: \(error)")
        }

    }

    func handleDisconnect() {
        state.withLockedValue { $0 = .disconnected }
        if self.transport != nil {
            Task {
                do {
                    try await self.disconnect()
                    self.fire(.disconnected)
                } catch {
                    logger.error("Error closing connection: \(error)")
                }
            }
        }

        handleReconnect()
    }

    func handleReconnect() {

        let isAlreadyReconnecting = _reconnectTask.withLockedValue { task -> Bool in
            guard let activeTask = task else { return false }
            return !activeTask.isCancelled
        }

        guard !isAlreadyReconnecting else {
            logger.debug("Reconnect already in progress. Ignoring duplicate trigger.")
            return
        }

        reconnectTask = Task {

            defer {
                _reconnectTask.withLockedValue { $0 = nil }
            }

            var connected = false
            while !Task.isCancelled
                && (maxReconnects == nil || self.reconnectAttempts < maxReconnects!)
            {
                do {
                    try await self.connect()
                    connected = true
                    break  // Successfully connected
                } catch is CancellationError {
                    logger.debug("Reconnect task cancelled")
                    return
                } catch {
                    logger.debug("Could not reconnect: \(error)")
                    if !Task.isCancelled {
                        try await Task.sleep(nanoseconds: self.reconnectWait)
                    }
                }
            }

            // Early return if cancelled
            if Task.isCancelled {
                logger.debug("Reconnect task cancelled after connection attempts")
                return
            }

            // If we got here without connecting and weren't cancelled, we hit max reconnects
            if !connected {
                logger.error("Could not reconnect; maxReconnects exceeded")
                try await self.close()
                return
            }

            // Recreate subscriptions - safely copy first
            let subsToRestore = subscriptions.withLockedValue { Array($0) }
            for (sid, sub) in subsToRestore {
                do {
                    try await write(operation: ClientOp.subscribe((sid, sub.subject, nil)))
                } catch {
                    logger.error("Error recreating subscription \(sid): \(error)")
                }
            }

            self.state.withLockedValue { $0 = .connected }
            self.fire(.connected)
        }
    }

    func write(operation: ClientOp) async throws {
        guard let buffer = self.batchBuffer else {
            throw NatsError.ClientError.invalidConnection("not connected")
        }
        do {
            try await buffer.writeMessage(operation)
        } catch {
            throw NatsError.ClientError.io(error)
        }
    }

    internal func subscribe(
        _ subject: String, queue: String? = nil
    ) async throws -> NatsSubscription {
        let sid = self.subscriptionCounter.wrappingIncrementThenLoad(
            ordering: AtomicUpdateOrdering.relaxed)
        let sub = try NatsSubscription(sid: sid, subject: subject, queue: queue, conn: self)

        // Add subscription BEFORE sending command to avoid race condition
        subscriptions.withLockedValue { $0[sid] = sub }

        do {
            try await write(operation: ClientOp.subscribe((sid, subject, queue)))
        } catch {
            // Remove subscription if subscribe command fails
            _ = subscriptions.withLockedValue { $0.removeValue(forKey: sid) }
            throw error
        }

        return sub
    }

    internal func unsubscribe(sub: NatsSubscription, max: UInt64?) async throws {
        if let max, sub.delivered < max {
            // if max is set and the sub has not yet reached it, send unsub with max set
            // and do not remove the sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: max)))
            sub.max = max
        } else {
            // if max is not set or the subscription received at least as many
            // messages as max, send unsub command without max and remove sub from connection
            try await write(operation: ClientOp.unsubscribe((sid: sub.sid, max: nil)))
            self.removeSub(sub: sub)
        }
    }

    internal func removeSub(sub: NatsSubscription) {
        _ = subscriptions.withLockedValue { $0.removeValue(forKey: sub.sid) }
        sub.complete()
    }
}

extension ConnectionHandler {

    internal func fire(_ event: NatsEvent) {
        let eventKind = event.kind()
        let handlerStore = self.eventHandlerStore.withLockedValue { $0[eventKind] }
        guard let handlerStore = handlerStore else { return }

        for handler in handlerStore {
            handler.handler(event)
        }
    }

    internal func addListeners(
        for events: [NatsEventKind], using handler: @escaping @Sendable (NatsEvent) -> Void
    ) -> String {

        let id = String.hash()

        for event in events {
            self.eventHandlerStore.withLockedValue { store in
                if store[event] == nil {
                    store[event] = []
                }
                store[event]?.append(NatsEventHandler(lid: id, handler: handler))
            }
        }

        return id

    }

    internal func removeListener(_ id: String) {

        for event in NatsEventKind.all {
            self.eventHandlerStore.withLockedValue { store in
                if let handlerStore = store[event] {
                    store[event] = handlerStore.filter { $0.listenerId != id }
                }
            }
        }

    }

}

/// Nats events
public enum NatsEventKind: String, Sendable {
    case connected = "connected"
    case disconnected = "disconnected"
    case closed = "closed"
    case suspended = "suspended"
    case lameDuckMode = "lameDuckMode"
    case error = "error"
    static let all = [connected, disconnected, closed, lameDuckMode, error]
}

public enum NatsEvent: Sendable {
    case connected
    case disconnected
    case suspended
    case closed
    case lameDuckMode
    case error(NatsErrorProtocol)

    public func kind() -> NatsEventKind {
        switch self {
        case .connected:
            return .connected
        case .disconnected:
            return .disconnected
        case .suspended:
            return .suspended
        case .closed:
            return .closed
        case .lameDuckMode:
            return .lameDuckMode
        case .error(_):
            return .error
        }
    }
}

internal struct NatsEventHandler: Sendable {
    let listenerId: String
    let handler: @Sendable (NatsEvent) -> Void
    init(lid: String, handler: @escaping @Sendable (NatsEvent) -> Void) {
        self.listenerId = lid
        self.handler = handler
    }
}
