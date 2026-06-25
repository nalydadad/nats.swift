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

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

internal struct TransportTLSOptions: Sendable {
    let rootCertificate: URL?
    let clientCertificate: URL?
    let clientKey: URL?
}

/// Abstraction over the wire transport used by `ConnectionHandler`.
///
/// Implementations are responsible for all socket/TLS/WebSocket I/O; the
/// caller only ever sees `Data` chunks in and out, leaving NATS protocol
/// parsing (`Data.parseOutMessages()`) untouched and transport-agnostic.
internal protocol NatsTransport: AnyObject, Sendable {
    /// Inbound byte chunks, one element per underlying read/frame. The
    /// stream finishes (optionally with an error) when the connection is
    /// closed, locally or by the peer.
    var incomingMessages: AsyncThrowingStream<Data, Error> { get }

    func connect(url: URL, tls: TransportTLSOptions?) async throws

    /// Upgrades an already-open plaintext connection to TLS in place
    /// (`tlsFirst` / server-requested-TLS negotiation for raw NATS). A no-op
    /// for transports where TLS is already fully decided by the URL scheme
    /// (e.g. WebSocket, which is always `wss://`).
    func startSecureConnection() throws

    func send(_ data: Data) async throws

    func close()
}
