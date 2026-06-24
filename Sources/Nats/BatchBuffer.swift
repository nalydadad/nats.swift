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
import NIO
import NIOFoundationCompat

/// Batches `ClientOp` writes into a single `transport.send()` call when
/// possible, so concurrent publishers don't each pay a full write round-trip.
/// `ByteBuffer` is kept here purely as an in-memory accumulation buffer
/// (never touches a socket directly) before being copied out as `Data` for
/// the transport to send.
internal actor BatchBuffer {
    private let batchSize: Int
    private let transport: any NatsTransport
    private let allocator = ByteBufferAllocator()
    private var buffer: ByteBuffer
    private var waitingMessages: [(ClientOp, UnsafeContinuation<Void, Error>)] = []
    private var isWriteInProgress = false

    init(transport: any NatsTransport, batchSize: Int = 16 * 1024) {
        self.transport = transport
        self.batchSize = batchSize
        self.buffer = allocator.buffer(capacity: batchSize)
    }

    func writeMessage(_ message: ClientOp) async throws {
        guard buffer.readableBytes < batchSize else {
            try await withUnsafeThrowingContinuation { continuation in
                waitingMessages.append((message, continuation))
            }
            return
        }

        buffer.writeClientOp(message)
        await flushWhenIdle()
    }

    private func flushWhenIdle() async {
        // The idea is to keep writing to the buffer while a send() is in
        // progress, so we can batch as many messages as possible.
        guard !isWriteInProgress else {
            return
        }

        var writeBuffer = allocator.buffer(capacity: buffer.readableBytes)
        writeBuffer.writeBytes(buffer.readableBytesView)
        buffer.clear()
        isWriteInProgress = true

        do {
            try await transport.send(Data(buffer: writeBuffer))
            isWriteInProgress = false
            let pending = waitingMessages
            waitingMessages.removeAll()
            for (message, continuation) in pending {
                buffer.writeClientOp(message)
                continuation.resume()
            }
        } catch {
            isWriteInProgress = false
            let pending = waitingMessages
            waitingMessages.removeAll()
            for (_, continuation) in pending {
                continuation.resume(throwing: error)
            }
            buffer.clear()
        }

        // Check if there are any pending flushes
        if buffer.readableBytes > 0 {
            await flushWhenIdle()
        }
    }
}
