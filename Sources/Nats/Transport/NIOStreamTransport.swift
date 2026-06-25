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

#if canImport(FoundationNetworking)

    import FoundationNetworking
    import Foundation
    import NIO
    import NIOConcurrencyHelpers
    import NIOFoundationCompat
    import NIOSSL

    /// Linux-only fallback transport for `nats://`/`tls://`.
    ///
    /// `URLSessionStreamTask` (used by `URLSessionStreamTransport` on Darwin)
    /// is explicitly unavailable in swift-corelibs-foundation, so it cannot be
    /// used to build/test this package on Linux. This NIO-socket-based
    /// implementation exists purely to keep `swift build`/`swift test` working
    /// in non-Darwin environments; the actual iOS/macOS target uses
    /// `URLSessionStreamTransport` instead, which is what inherits the
    /// system proxy/trust store this fork was built to support.
    internal final class NIOStreamTransport: NatsTransport, @unchecked Sendable {
        private final class InboundHandler: ChannelInboundHandler {
            typealias InboundIn = ByteBuffer

            let onData: @Sendable (Data) -> Void
            let onClose: @Sendable (Error?) -> Void

            init(
                onData: @escaping @Sendable (Data) -> Void,
                onClose: @escaping @Sendable (Error?) -> Void
            ) {
                self.onData = onData
                self.onClose = onClose
            }

            func channelRead(context: ChannelHandlerContext, data: NIOAny) {
                let buffer = unwrapInboundIn(data)
                onData(Data(buffer: buffer))
            }

            func channelInactive(context: ChannelHandlerContext) {
                onClose(nil)
            }

            func errorCaught(context: ChannelHandlerContext, error: Error) {
                onClose(error)
                context.close(promise: nil)
            }
        }

        private let continuationBox = NIOLockedValueBox<
            AsyncThrowingStream<Data, Error>.Continuation?
        >(nil)
        let incomingMessages: AsyncThrowingStream<Data, Error>

        private let channelBox = NIOLockedValueBox<Channel?>(nil)
        private let hostBox = NIOLockedValueBox<String?>(nil)
        private let tlsOptionsBox = NIOLockedValueBox<TransportTLSOptions?>(nil)

        init() {
            var continuation: AsyncThrowingStream<Data, Error>.Continuation!
            self.incomingMessages = AsyncThrowingStream { continuation = $0 }
            continuationBox.withLockedValue { $0 = continuation }
        }

        func connect(url: URL, tls: TransportTLSOptions?) async throws {
            guard let host = url.host, let port = url.port else {
                throw NatsError.ConnectError.invalidConfig("no url")
            }
            hostBox.withLockedValue { $0 = host }
            tlsOptionsBox.withLockedValue { $0 = tls }

            let group = MultiThreadedEventLoopGroup.singleton
            let bootstrap = ClientBootstrap(group: group)
                .channelOption(
                    ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1
                )
                .channelInitializer { channel in
                    let handler = InboundHandler(
                        onData: { [weak self] data in self?.yield(data) },
                        onClose: { [weak self] error in self?.finish(throwing: error) })
                    return channel.pipeline.addHandler(handler)
                }
                .connectTimeout(.seconds(5))

            let channel = try await bootstrap.connect(host: host, port: port).get()
            channelBox.withLockedValue { $0 = channel }
        }

        func startSecureConnection() throws {
            guard let channel = channelBox.withLockedValue({ $0 }),
                let host = hostBox.withLockedValue({ $0 })
            else {
                throw NatsError.ClientError.invalidConnection("not connected")
            }

            let tlsOptions = tlsOptionsBox.withLockedValue { $0 }
            var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
            if let rootCertificate = tlsOptions?.rootCertificate {
                tlsConfiguration.trustRoots = .file(rootCertificate.path)
            }
            if let clientCertificate = tlsOptions?.clientCertificate,
                let clientKey = tlsOptions?.clientKey
            {
                let certificate = try NIOSSLCertificate.fromPEMFile(clientCertificate.path).map {
                    NIOSSLCertificateSource.certificate($0)
                }
                tlsConfiguration.certificateChain = certificate
                let privateKey = try NIOSSLPrivateKey(file: clientKey.path, format: .pem)
                tlsConfiguration.privateKey = .privateKey(privateKey)
            }

            let sslContext = try NIOSSLContext(configuration: tlsConfiguration)
            let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: host)
            try channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandler(sslHandler, position: .first)
            }.wait()
        }

        func send(_ data: Data) async throws {
            guard let channel = channelBox.withLockedValue({ $0 }) else {
                throw NatsError.ClientError.invalidConnection("not connected")
            }
            var buffer = channel.allocator.buffer(capacity: data.count)
            buffer.writeBytes(data)
            try await channel.writeAndFlush(buffer)
        }

        func close() {
            channelBox.withLockedValue { $0?.close(mode: .all, promise: nil) }
            finish(throwing: nil)
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
    }

#endif
