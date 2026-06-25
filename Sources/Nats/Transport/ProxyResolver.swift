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

// Network.framework's `ProxyConfiguration` (iOS 17 / macOS 14) is the modern,
// WebSocket-capable way to traverse an HTTP proxy. Unlike `URLSession`,
// `NWConnection` does *not* consult the system proxy automatically, so we
// resolve the system configuration (including PAC / proxy-auto-config) for a
// given URL ourselves and hand it back as an explicit `ProxyConfiguration`.
#if canImport(Network)

    import CFNetwork
    import Foundation
    import Network

    @available(iOS 17.0, macOS 14.0, tvOS 17.0, watchOS 10.0, *)
    internal enum ProxyResolver {
        /// Resolves the system proxy for `url` into a Network.framework
        /// `ProxyConfiguration`, or `nil` for a direct connection. PAC /
        /// auto-config URLs are evaluated to a concrete CONNECT proxy.
        static func resolve(for url: URL) async -> ProxyConfiguration? {
            guard let cfSettings = CFNetworkCopySystemProxySettings()?.takeRetainedValue() else {
                return nil
            }
            let proxies =
                (CFNetworkCopyProxiesForURL(url as CFURL, cfSettings).takeRetainedValue() as NSArray)
                as? [[String: Any]] ?? []
            return await firstSupportedProxy(from: proxies, targetURL: url)
        }

        private static func firstSupportedProxy(
            from proxies: [[String: Any]], targetURL: URL
        ) async -> ProxyConfiguration? {
            for proxy in proxies {
                guard let type = proxy[kCFProxyTypeKey as String] as? String else { continue }

                if type == kCFProxyTypeNone as String {
                    // Explicit "go direct" entry — honour it and stop looking.
                    return nil
                }

                if type == kCFProxyTypeHTTPS as String || type == kCFProxyTypeHTTP as String {
                    if let config = makeConfiguration(from: proxy) {
                        return config
                    }
                }

                if type == kCFProxyTypeAutoConfigurationURL as String {
                    guard let pacURL = pacURL(from: proxy) else { continue }
                    let resolved = await executePAC(pacURL: pacURL, targetURL: targetURL)
                    if let config = await firstSupportedProxy(
                        from: resolved, targetURL: targetURL)
                    {
                        return config
                    }
                }

                // SOCKS and inline-JavaScript PAC are intentionally not handled;
                // fall through to the next entry (or a direct connection).
            }
            return nil
        }

        private static func pacURL(from proxy: [String: Any]) -> URL? {
            let value = proxy[kCFProxyAutoConfigurationURLKey as String]
            if let url = value as? URL { return url }
            if let string = value as? String { return URL(string: string) }
            return nil
        }

        private static func makeConfiguration(from proxy: [String: Any]) -> ProxyConfiguration? {
            guard
                let host = proxy[kCFProxyHostNameKey as String] as? String,
                let portNumber = proxy[kCFProxyPortNumberKey as String] as? Int,
                let port = NWEndpoint.Port(rawValue: UInt16(truncatingIfNeeded: portNumber))
            else { return nil }

            let endpoint = NWEndpoint.hostPort(host: NWEndpoint.Host(host), port: port)
            var config = ProxyConfiguration(httpCONNECTProxy: endpoint)
            if let username = proxy[kCFProxyUsernameKey as String] as? String,
                let password = proxy[kCFProxyPasswordKey as String] as? String
            {
                config.applyCredential(username: username, password: password)
            }
            return config
        }

        /// Carries the resolved proxy list and the awaiting continuation across
        /// the dedicated run-loop thread. `@unchecked Sendable` because access is
        /// confined to that single thread (the C callback runs on its run loop).
        private final class PACContext: @unchecked Sendable {
            var proxies: [[String: Any]]?
            let continuation: CheckedContinuation<[[String: Any]], Never>
            init(_ continuation: CheckedContinuation<[[String: Any]], Never>) {
                self.continuation = continuation
            }
        }

        /// Evaluates a PAC file for `targetURL`. `CFNetworkExecuteProxyAuto-
        /// ConfigurationURL` delivers its result via a run-loop source, so we
        /// drive a dedicated thread's run loop until the callback fires (or a
        /// timeout elapses, so a stuck PAC fetch can't hang connect()).
        private static func executePAC(pacURL: URL, targetURL: URL) async -> [[String: Any]] {
            await withCheckedContinuation {
                (continuation: CheckedContinuation<[[String: Any]], Never>) in
                let pacContext = PACContext(continuation)
                let thread = Thread {
                    var streamContext = CFStreamClientContext(
                        version: 0,
                        info: Unmanaged.passUnretained(pacContext).toOpaque(),
                        retain: nil, release: nil, copyDescription: nil)

                    // Non-capturing so it converts to a C function pointer; the
                    // `PACContext` is recovered from `client` (the `info` pointer).
                    let callback: CFProxyAutoConfigurationResultCallback = {
                        client, proxyList, error in
                        guard let client else { return }
                        let ctx = Unmanaged<PACContext>.fromOpaque(client).takeUnretainedValue()
                        if error == nil {
                            ctx.proxies = (proxyList as NSArray) as? [[String: Any]] ?? []
                        } else {
                            ctx.proxies = []
                        }
                        CFRunLoopStop(CFRunLoopGetCurrent())
                    }

                    let source = CFNetworkExecuteProxyAutoConfigurationURL(
                        pacURL as CFURL, targetURL as CFURL, callback, &streamContext
                    ).takeRetainedValue()

                    let mode = CFRunLoopMode.defaultMode
                    CFRunLoopAddSource(CFRunLoopGetCurrent(), source, mode)
                    // Bound the wait so a stuck PAC fetch can't hang connect().
                    let deadline = Date().addingTimeInterval(10)
                    while pacContext.proxies == nil && Date() < deadline {
                        CFRunLoopRunInMode(mode, 0.25, true)
                    }
                    CFRunLoopRemoveSource(CFRunLoopGetCurrent(), source, mode)

                    pacContext.continuation.resume(returning: pacContext.proxies ?? [])
                }
                thread.start()
            }
        }
    }

#endif
