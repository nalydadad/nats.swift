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

#if canImport(Security)

    import Foundation
    import Security

    internal enum TLSIdentityError: Error {
        case invalidPEM
        case keychainError(OSStatus)
    }

    /// Loads PEM-encoded certificates/keys into `Security` framework types so
    /// they can be handed to `URLSessionDelegate` TLS challenges.
    ///
    /// Apple has no public API to build a `SecIdentity` directly from a raw
    /// certificate + private key pair (the private `SecIdentityCreateWithCertificate`
    /// is not usable from the public SDK). The supported route on both iOS and
    /// macOS is to add the certificate and key to the keychain tagged so the
    /// system pairs them, then look the pairing up as a `SecIdentity`.
    internal enum TLSIdentity {
        static func loadCertificate(pem url: URL) throws -> SecCertificate {
            let pemString = try String(contentsOf: url, encoding: .utf8)
            guard let der = derData(fromPEM: pemString, marker: "CERTIFICATE") else {
                throw TLSIdentityError.invalidPEM
            }
            guard let cert = SecCertificateCreateWithData(nil, der as CFData) else {
                throw TLSIdentityError.invalidPEM
            }
            return cert
        }

        static func loadIdentity(certificate certURL: URL, key keyURL: URL) throws -> SecIdentity {
            let cert = try loadCertificate(pem: certURL)

            let keyPEM = try String(contentsOf: keyURL, encoding: .utf8)
            guard
                let keyDER = derData(fromPEM: keyPEM, marker: "PRIVATE KEY")
                    ?? derData(fromPEM: keyPEM, marker: "RSA PRIVATE KEY")
            else {
                throw TLSIdentityError.invalidPEM
            }
            let pkcs1 = pkcs1Data(fromPotentialPKCS8: keyDER)

            let keyAttributes: [CFString: Any] = [
                kSecAttrKeyType: kSecAttrKeyTypeRSA,
                kSecAttrKeyClass: kSecAttrKeyClassPrivate,
            ]
            var keyError: Unmanaged<CFError>?
            guard
                let secKey = SecKeyCreateWithData(
                    pkcs1 as CFData, keyAttributes as CFDictionary, &keyError)
            else {
                throw TLSIdentityError.invalidPEM
            }

            // Tag pairs this key/cert in the keychain so SecItemCopyMatching
            // can retrieve them back out as a single SecIdentity.
            let tag = "io.nats.swift.transport.\(UUID().uuidString)".data(using: .utf8)!

            let keyAddAttributes: [CFString: Any] = [
                kSecClass: kSecClassKey,
                kSecValueRef: secKey,
                kSecAttrApplicationTag: tag,
            ]
            let keyStatus = SecItemAdd(keyAddAttributes as CFDictionary, nil)
            guard keyStatus == errSecSuccess || keyStatus == errSecDuplicateItem else {
                throw TLSIdentityError.keychainError(keyStatus)
            }

            let certAddAttributes: [CFString: Any] = [
                kSecClass: kSecClassCertificate,
                kSecValueRef: cert,
                kSecAttrApplicationTag: tag,
            ]
            let certStatus = SecItemAdd(certAddAttributes as CFDictionary, nil)
            guard certStatus == errSecSuccess || certStatus == errSecDuplicateItem else {
                throw TLSIdentityError.keychainError(certStatus)
            }

            let identityQuery: [CFString: Any] = [
                kSecClass: kSecClassIdentity,
                kSecAttrApplicationTag: tag,
                kSecReturnRef: true,
            ]
            var identityResult: CFTypeRef?
            let identityStatus = SecItemCopyMatching(identityQuery as CFDictionary, &identityResult)
            guard identityStatus == errSecSuccess, let identityResult else {
                throw TLSIdentityError.keychainError(identityStatus)
            }
            return identityResult as! SecIdentity
        }

        private static func derData(fromPEM pem: String, marker: String) -> Data? {
            let beginMarker = "-----BEGIN \(marker)-----"
            let endMarker = "-----END \(marker)-----"
            guard let beginRange = pem.range(of: beginMarker),
                let endRange = pem.range(of: endMarker)
            else {
                return nil
            }
            let base64 =
                pem[beginRange.upperBound..<endRange.lowerBound]
                .replacingOccurrences(of: "\n", with: "")
                .replacingOccurrences(of: "\r", with: "")
            return Data(base64Encoded: base64)
        }

        /// `SecKeyCreateWithData` expects PKCS#1 for RSA keys, but PEM private
        /// keys are commonly PKCS#8-wrapped (`-----BEGIN PRIVATE KEY-----`).
        /// A PKCS#8 blob is `SEQUENCE { version INTEGER, AlgorithmIdentifier
        /// SEQUENCE, OCTET STRING }` where the OCTET STRING holds the inner
        /// PKCS#1 key. This walks just far enough to extract it; if the
        /// shape doesn't match (already PKCS#1), the input is returned as-is.
        private static func pkcs1Data(fromPotentialPKCS8 der: Data) -> Data {
            guard let octetString = findTrailingOctetString(der) else {
                return der
            }
            return octetString
        }

        private static func findTrailingOctetString(_ der: Data) -> Data? {
            var index = der.startIndex
            guard index < der.endIndex, der[index] == 0x30 else { return nil }
            index = der.index(after: index)
            guard let (_, afterOuterLength) = readLength(der, from: index) else { return nil }
            index = afterOuterLength

            // version INTEGER
            guard index < der.endIndex, der[index] == 0x02 else { return nil }
            index = der.index(after: index)
            guard let (versionLength, afterVersionLength) = readLength(der, from: index) else {
                return nil
            }
            guard
                let afterVersion = der.index(
                    afterVersionLength, offsetBy: versionLength, limitedBy: der.endIndex)
            else { return nil }
            index = afterVersion

            // AlgorithmIdentifier SEQUENCE
            guard index < der.endIndex, der[index] == 0x30 else { return nil }
            index = der.index(after: index)
            guard let (algorithmLength, afterAlgorithmLength) = readLength(der, from: index) else {
                return nil
            }
            guard
                let afterAlgorithm = der.index(
                    afterAlgorithmLength, offsetBy: algorithmLength, limitedBy: der.endIndex)
            else { return nil }
            index = afterAlgorithm

            // OCTET STRING containing the PKCS#1 key
            guard index < der.endIndex, der[index] == 0x04 else { return nil }
            index = der.index(after: index)
            guard let (octetLength, afterOctetLength) = readLength(der, from: index) else {
                return nil
            }
            guard
                let end = der.index(
                    afterOctetLength, offsetBy: octetLength, limitedBy: der.endIndex)
            else { return nil }
            return der[afterOctetLength..<end]
        }

        private static func readLength(
            _ der: Data, from index: Data.Index
        ) -> (
            length: Int, next: Data.Index
        )? {
            guard index < der.endIndex else { return nil }
            let first = der[index]
            if first & 0x80 == 0 {
                return (Int(first), der.index(after: index))
            }
            let numBytes = Int(first & 0x7F)
            var next = der.index(after: index)
            var length = 0
            for _ in 0..<numBytes {
                guard next < der.endIndex else { return nil }
                length = (length << 8) | Int(der[next])
                next = der.index(after: next)
            }
            return (length, next)
        }
    }

    /// `URLSessionDelegate` that pins server trust to a custom root CA (when
    /// provided) and/or presents a client identity for mTLS challenges, used
    /// by both `URLSessionWebSocketTransport` and `URLSessionStreamTransport`.
    internal final class TLSChallengeDelegate: NSObject, URLSessionDelegate, @unchecked Sendable {
        private let rootCertificate: SecCertificate?
        private let identity: SecIdentity?

        init(tls: TransportTLSOptions?) throws {
            if let rootCertificateURL = tls?.rootCertificate {
                self.rootCertificate = try TLSIdentity.loadCertificate(pem: rootCertificateURL)
            } else {
                self.rootCertificate = nil
            }
            if let clientCertificateURL = tls?.clientCertificate, let clientKeyURL = tls?.clientKey
            {
                self.identity = try TLSIdentity.loadIdentity(
                    certificate: clientCertificateURL, key: clientKeyURL)
            } else {
                self.identity = nil
            }
        }

        func urlSession(
            _ session: URLSession,
            didReceive challenge: URLAuthenticationChallenge,
            completionHandler:
                @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
        ) {
            switch challenge.protectionSpace.authenticationMethod {
            case NSURLAuthenticationMethodServerTrust:
                guard let serverTrust = challenge.protectionSpace.serverTrust else {
                    completionHandler(.performDefaultHandling, nil)
                    return
                }
                guard let rootCertificate else {
                    completionHandler(.performDefaultHandling, nil)
                    return
                }
                SecTrustSetAnchorCertificates(serverTrust, [rootCertificate] as CFArray)
                SecTrustSetAnchorCertificatesOnly(serverTrust, true)
                if SecTrustEvaluateWithError(serverTrust, nil) {
                    completionHandler(.useCredential, URLCredential(trust: serverTrust))
                } else {
                    completionHandler(.cancelAuthenticationChallenge, nil)
                }
            case NSURLAuthenticationMethodClientCertificate:
                guard let identity else {
                    completionHandler(.performDefaultHandling, nil)
                    return
                }
                let credential = URLCredential(
                    identity: identity, certificates: nil, persistence: .forSession)
                completionHandler(.useCredential, credential)
            default:
                completionHandler(.performDefaultHandling, nil)
            }
        }
    }

#endif
