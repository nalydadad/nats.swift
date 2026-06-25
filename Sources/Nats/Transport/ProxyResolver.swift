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
#if canImport(Network) && canImport(CFNetwork)

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
                CFNetworkCopyProxiesForURL(url as CFURL, cfSettings).takeRetainedValue()
                as? [[String: Any]] ?? []
            return await firstSupportedProxy(from: proxies, targetURL: url)
        }

        private static func firstSupportedProxy(
            from proxies: [[String: Any]], targetURL: URL
        ) async -> ProxyConfiguration? {
            for proxy in proxies {
                guard let type = proxy[kCFProxyTypeKey as String] as? CFString else { continue }

                if type == kCFProxyTypeNone {
                    // Explicit "go direct" entry — honour it and stop looking.
                    return nil
                }

                if type == kCFProxyTypeHTTPS || type == kCFProxyTypeHTTP {
                    if let config = makeConfiguration(from: proxy) {
                        return config
                    }
                }

                if type == kCFProxyTypeAutoConfigurationURL {
                    guard let pacURL = proxy[kCFProxyAutoConfigurationURLKey as String] as? URL
                    else { continue }
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

        private static func makeConfiguration(from proxy: [String: Any]) -> ProxyConfiguration? {
            guard
                let host = proxy[kCFProxyHostNameKey as String] as? String,
                let portNumber = proxy[kCFProxyPortNumberKey as String] as? Int,
                let port = NWEndpoint.Port(rawValue: UInt16(truncatingIfNeeded: portNumber))
            else { return nil }

            let endpoint = NWEndpoint.hostPort(host: NWEndpoint.Host(host), port: port)
            let config = ProxyConfiguration(httpCONNECTProxy: endpoint)
            if let username = proxy[kCFProxyUsernameKey as String] as? String,
                let password = proxy[kCFProxyPasswordKey as String] as? String
            {
                config.applyCredential(username: username, password: password)
            }
            return config
        }

        /// Result holder so the C result callback can deliver the resolved
        /// proxy list back out of the run-loop source.
        private final class PACResult {
            var proxies: [[String: Any]]?
        }

        /// Evaluates a PAC file for `targetURL`. `CFNetworkExecuteProxyAuto-
        /// ConfigurationURL` delivers its result via a run-loop source, so we
        /// drive a dedicated thread's run loop until the callback fires (or a
        /// timeout elapses, so a stuck PAC fetch can't hang connect()).
        private static func executePAC(pacURL: URL, targetURL: URL) async -> [[String: Any]] {
            await withCheckedContinuation {
                (continuation: CheckedContinuation<[[String: Any]], Never>) in
                let thread = Thread {
                    let result = PACResult()
                    var context = CFStreamClientContext(
                        version: 0,
                        info: Unmanaged.passUnretained(result).toOpaque(),
                        retain: nil, release: nil, copyDescription: nil)

                    // Must be non-capturing so it converts to a C function
                    // pointer; the `PACResult` is recovered from `client`
                    // (the `info` pointer set in `context`).
                    let callback: CFProxyAutoConfigurationResultCallback = {
                        (client, proxyList, error) in
                        guard let client else { return }
                        let holder = Unmanaged<PACResult>.fromOpaque(client)
                            .takeUnretainedValue()
                        if error == nil {
                            holder.proxies = (proxyList as? [[String: Any]]) ?? []
                        } else {
                            holder.proxies = []
                        }
                        CFRunLoopStop(CFRunLoopGetCurrent())
                    }

                    let source = CFNetworkExecuteProxyAutoConfigurationURL(
                        pacURL as CFURL, targetURL as CFURL, callback, &context
                    )

                    let mode = CFRunLoopMode.defaultMode
                    CFRunLoopAddSource(CFRunLoopGetCurrent(), source, mode)
                    // Bound the wait so a stuck PAC fetch can't hang connect().
                    let deadline = Date().addingTimeInterval(10)
                    while result.proxies == nil && Date() < deadline {
                        CFRunLoopRunInMode(mode, 0.25, true)
                    }
                    CFRunLoopRemoveSource(CFRunLoopGetCurrent(), source, mode)

                    continuation.resume(returning: result.proxies ?? [])
                }
                thread.start()
            }
        }
    }

#endif
