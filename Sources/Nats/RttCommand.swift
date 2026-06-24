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

internal final class RttCommand: Sendable {
    private enum State {
        case pending
        case ready(TimeInterval)
        case waiting(CheckedContinuation<TimeInterval, Never>)
    }

    let startTime = DispatchTime.now()
    private let state = NIOLockedValueBox<State>(.pending)

    static func makeFrom() -> RttCommand {
        RttCommand()
    }

    private init() {}

    func setRoundTripTime() {
        let now = DispatchTime.now()
        let nanoTime = now.uptimeNanoseconds - startTime.uptimeNanoseconds
        let rtt = TimeInterval(nanoTime) / 1_000_000_000  // Convert nanos to seconds

        let continuation = state.withLockedValue { current -> CheckedContinuation<TimeInterval, Never>? in
            if case .waiting(let continuation) = current {
                current = .ready(rtt)
                return continuation
            }
            current = .ready(rtt)
            return nil
        }
        continuation?.resume(returning: rtt)
    }

    func getRoundTripTime() async throws -> TimeInterval {
        return await withCheckedContinuation { (continuation: CheckedContinuation<TimeInterval, Never>) in
            let immediateResult = state.withLockedValue { current -> TimeInterval? in
                if case .ready(let rtt) = current {
                    return rtt
                }
                current = .waiting(continuation)
                return nil
            }
            if let immediateResult {
                continuation.resume(returning: immediateResult)
            }
        }
    }
}
