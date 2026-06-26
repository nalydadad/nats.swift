# 改動明細 — Fork vs. 上游 NATS.swift（逐檔、逐項）

> 這份文件詳細說明本 fork 相對於上游原版的 **每一個改動**,深入到程式碼層級。
> 高層摘要與維護/排查指南請見 [`HANDOVER.md`](./HANDOVER.md);這份是給需要逐行理解的人看的。

- **上游基準點**：`a9031f1`（上游 PR #118）
- **本 fork HEAD**：`claude/version-comparison-changes-jy9uvv`
- **規模**：20 檔,+2246 / −873
- **重現任一檔 diff**：`git diff a9031f1 HEAD -- <path>`

---

## 目錄

1. [核心概念：傳輸層抽象](#1-核心概念傳輸層抽象)
2. [新增檔案：傳輸層](#2-新增檔案傳輸層)
3. [既有檔案改動](#3-既有檔案改動)
4. [Package.swift 與平台](#4-packageswift-與平台)
5. [CI / Scripts / 文件](#5-ci--scripts--文件)
6. [完整檔案清單與每檔摘要](#6-完整檔案清單與每檔摘要)

---

## 1. 核心概念：傳輸層抽象

上游用 Swift NIO 的 `ChannelInboundHandler` 直接做 socket / TLS / WebSocket I/O。本 fork 在 NATS 協定邏輯與底層 I/O 之間插入一層 **`NatsTransport` 協定**,把兩者解耦。

```swift
// Sources/Nats/Transport/NatsTransport.swift
internal struct TransportTLSOptions: Sendable {
    let rootCertificate: URL?
    let clientCertificate: URL?
    let clientKey: URL?
}

internal protocol NatsTransport: AnyObject, Sendable {
    var incomingMessages: AsyncThrowingStream<Data, Error> { get }   // 入站位元組,一個 chunk 一個元素
    func connect(url: URL, tls: TransportTLSOptions?) async throws
    func startSecureConnection() throws        // 就地升級 TLS(tlsFirst / INFO 後協商);WebSocket 為 no-op
    func send(_ data: Data) async throws
    func close()
}
```

**關鍵設計**:介面只進出 `Data`。NATS 協定解析(`Data.parseOutMessages()`)完全不知道底下是 NIO、URLSession 還是 NWConnection。這讓「換傳輸」這件事不需要碰任何協定/訂閱/認證程式碼。

**傳輸選擇邏輯**(位於 `NatsConnection.swift::connectToServer`,依 URL scheme + 編譯平台):

| scheme | Apple 平台 | 非 Apple(Linux) |
|--------|-----------|------------------|
| `ws://` / `wss://` | `NWWebSocketTransport`(`canImport(Network)`)| `URLSessionWebSocketTransport` |
| `nats://` / `tls://` | `URLSessionStreamTransport`(`!canImport(FoundationNetworking)`)| `NIOStreamTransport`(`canImport(FoundationNetworking)`)|

> 條件式判斷的邏輯:Linux 才有 `FoundationNetworking`(走 NIO);Apple 才有 `Network`(走 NW)。Apple 沒有 `FoundationNetworking`,所以 raw 路徑落到 URLSession。

---

## 2. 新增檔案：傳輸層

`Sources/Nats/Transport/` 共 6 個新檔。

### 2.1 `NatsTransport.swift`(48 行）
傳輸抽象協定 + `TransportTLSOptions` 結構。見上節。

### 2.2 `NWWebSocketTransport.swift`(265 行,Apple,`canImport(Network)`)— **本 fork 的核心**

用 Network.framework 的 `NWConnection` + `NWProtocolWebSocket` 實作 `ws/wss`。

**為什麼存在**(檔頭註解講得很清楚):`URLSessionWebSocketTask` 無法可靠地讓 WebSocket 穿過 tunnel-type 的 per-app VPN —— HTTP 101 upgrade 會成功,但升級後的 stream 在任何 frame 流動前就被丟掉,表現為 `NSURLErrorNetworkConnectionLost (-1005)`。`NWConnection` 把 TLS 握手、WebSocket upgrade、frame I/O 全部在同一條連線上做,沒有「把升級後的 stream 交接出去」這個會掉封包的步驟,且會被 per-app VPN 的 flow/packet 攔截透明捕捉。

重點實作:
- `connect()`:用 `withThrowingTaskGroup` 同時跑「建立連線」與「30 秒 timeout」,誰先完成就取消另一個,避免握手卡死拖垮 NATS connect。
- `makeParameters()`:`wss` 時掛 `NWProtocolTLS.Options`(走 `configureTLS`),`ws` 不掛;再把 `NWProtocolWebSocket.Options`(`autoReplyPing = true`)插到 protocol stack 最前。
- `configureTLS()`:client cert → `TLSIdentity.loadIdentity` → `sec_protocol_options_set_local_identity`;root CA → `sec_protocol_options_set_verify_block` 自訂信任評估。
- `receiveNext()`:遞迴接收;只處理 binary frame(text frame 記 error 並丟棄,因為 NATS 只走 binary);`.close` opcode → 結束 stream。
- `send()`:用 binary opcode 的 `NWProtocolWebSocket.Metadata` 包裝後送出。
- `startSecureConnection()`:no-op(TLS 已由 scheme 在 connect 時決定)。

### 2.3 `URLSessionWebSocketTransport.swift`(117 行,非 Apple fallback)

用 `URLSessionWebSocketTask` 實作 `ws/wss`,給沒有 Network.framework 的平台(如 Linux)用。
- `connect()`:建 `URLSession`(Apple 上掛 `TLSChallengeDelegate` 處理自訂 TLS),`webSocketTask(with:)`,`resume()`,開接收 loop。
- 接收 loop:`task.receive()`;只收 binary,text 記 error 丟棄;失敗時把 `closeCode`/`closeReason`/`NSError domain+code` 一起記 log 後結束。
- `send()`:`task.send(.data(data))`。

### 2.4 `URLSessionStreamTransport.swift`(147 行,Apple,`!canImport(FoundationNetworking)`)

用 `URLSessionStreamTask` 做 raw `nats://`/`tls://` —— URLSession 原生的 TCP/TLS socket 等價物,繼承系統 proxy/PAC 與信任庫。
- `connect()`:`session.streamTask(withHostName:port:)`,Apple 上掛 `TLSChallengeDelegate`。
- `startSecureConnection()`:呼叫 `task.startSecureConnection()` —— 真正支援「就地升級 TLS」(對應 NATS 的 INFO 後 TLS 協商)。
- 接收 loop:`readData(ofMinLength:1, maxLength:64KB)`,EOF 時結束。
- `send()`:`task.write(data,...)`。

### 2.5 `NIOStreamTransport.swift`(158 行,Linux only,`canImport(FoundationNetworking)`)

Linux 的 raw `nats://`/`tls://` fallback。`URLSessionStreamTask` 在 swift-corelibs-foundation 不存在,所以 Linux 用這個 NIO socket 實作 **純粹是為了讓 `swift build`/`swift test` 在 Linux 能跑**;真正的 iOS/macOS target 用的是 `URLSessionStreamTransport`。
- 內含一個簡單的 `InboundHandler: ChannelInboundHandler`,把 `ByteBuffer` 轉 `Data` yield 進 stream。
- `startSecureConnection()`:用 NIOSSL 就地加 `NIOSSLClientHandler` 到 pipeline(支援 root CA / client cert)。
- 這是唯一還在用 NIO socket I/O 的傳輸。

### 2.6 `TLSIdentity.swift`(258 行,Apple,`canImport(Security)`)

把 PEM 憑證/金鑰載入成 Security framework 型別,供 URLSession/NWConnection 的 TLS 挑戰使用。
- `loadCertificate(pem:)`:PEM → DER → `SecCertificate`。
- `loadIdentity(certificate:key:)`:**因為 Apple 沒有公開 API 直接從 cert+key 建 `SecIdentity`**,所以走「把 cert 與 key 用同一個 tag 加進 keychain,再用 `SecItemCopyMatching` 把配對撈回成 `SecIdentity`」的官方路徑。
- `pkcs1Data(fromPotentialPKCS8:)` + `findTrailingOctetString()`:手寫 DER 解析,把 PKCS#8 包裝(`-----BEGIN PRIVATE KEY-----`)剝成 `SecKeyCreateWithData` 需要的 PKCS#1。
- `TLSChallengeDelegate`:`URLSessionDelegate`,處理 server trust(pin 自訂 root CA)與 client certificate(mTLS)挑戰。給 `URLSessionWebSocketTransport` 與 `URLSessionStreamTransport` 共用。

---

## 3. 既有檔案改動

### 3.1 `Sources/Nats/NatsConnection.swift`（617 行變動 — 最大改動）

這是整個 fork 的核心手術。

**類別宣告**:
```diff
-final class ConnectionHandler: ChannelInboundHandler, Sendable {
+final class ConnectionHandler: Sendable {
```
不再是 NIO 的 `ChannelInboundHandler`。移除 `import NIOHTTP1 / NIOSSL / NIOWebSocket`;改成條件式 `import Network`(Apple)與 `import FoundationNetworking + NIOSSL`(Linux)。

**狀態欄位替換**:
- 移除 `allocator` / `_inputBuffer`(ByteBuffer)/ `_channel`(`Channel?`)/ `group`(`MultiThreadedEventLoopGroup`)/ `typealias InboundIn`。
- 新增 `_transport`(`(any NatsTransport)?`)與 `readLoopTask`(`Task<Void, Never>?`)。
- `pingTask` 型別:`RepeatedTask?`(NIO)→ `Task<Void, Never>?`(Swift concurrency)。

**讀取路徑改寫**:
- 移除 NIO 的 `channelRead` / `channelReadComplete`(它們把 inbound `ByteBuffer` 寫進 `_inputBuffer` 再批次 parse)。
- 改成 `handleReceivedChunk(_ data: Data)`:直接拿傳輸 yield 出來的 `Data` chunk,接上 `parseRemainder`,呼叫 `parseOutMessages()`。**協定解析邏輯本身沒變**,只是輸入來源從 ByteBuffer 改成 Data。
- 新增 `startReadLoop(transport:)`:起一個 `Task`,`for try await chunk in transport.incomingMessages { handleReceivedChunk(chunk) }`,結束時呼叫 `handleReadLoopEnded(error:)`。

**生命週期事件改寫**(從 NIO callback 改成自有方法):
- `channelInactive` → `handleReadLoopEnded(error:)`:read loop 結束時,失敗任何 pending 的 `serverInfoContinuation` / `connectionEstablishedContinuation`(避免 continuation 洩漏),已連線則 `handleDisconnect()`。**保留了原本「用 capturedError 判斷是否 TLS 失敗」的邏輯**。
- `errorCaught` → `triggerDisconnectDueToError(_:)`:記錄連線中的錯誤、fire `.error`、關閉傳輸。
- `channelActive` 移除(初始化 inputBuffer 的工作沒了)。

**連線建立 `connectToServer`**:
- 移除 `bootstrapConnection`(整個 ~120 行的 NIO `ClientBootstrap` + WebSocket upgrader + HTTP upgrade handler + SSL handler 設定全部刪掉)。
- 改成依 scheme/平台 `new...Transport()` → `transport.connect(url:tls:)` → 若 `requireTls && tlsFirst` 則 `startSecureConnection()` → 建 `BatchBuffer(transport:)` → `startReadLoop`。

**TLS 協商**:
- 移除 `makeTLSConfig()`(NIO `TLSConfiguration` / `NIOSSLContext` / `NIOSSLClientHandler`)。
- INFO 後的 TLS 升級改成一行 `try self.transport?.startSecureConnection()`,且條件多排除 `ws`(原本只排除 `wss`)。

**Ping 排程**:
- 移除 `channel.eventLoop.scheduleRepeatedTask`。
- 新增 `startPingTask()`:`Task { while !isCancelled { try? await Task.sleep(...); await sendPing() } }`。

**close / disconnect / suspend / resume**:全部從「`channel.eventLoop.execute` + `EventLoopPromise`」改寫成「直接操作 `transport?.close()` + `state` 鎖」。`resume()` 不再需要 eventLoop,直接檢查 suspended state。

**其他**:
- `RttCommand.makeFrom(channel:)` → `RttCommand.makeFrom()`(不再需要 channel)。
- `flush()` 相關:`self.channel?.flush()` 拿掉(BatchBuffer 每次 write 都已 flush)。

### 3.2 `Sources/Nats/BatchBuffer.swift`（149 行）

把批次寫入從 NIO channel 改成傳輸抽象,並改寫並行模型。

- **class → actor**:`final class BatchBuffer: Sendable`(用 `NIOLockedValueBox<State>` 手動鎖)→ `actor BatchBuffer`(用 actor 隔離)。移除巢狀的 `State` struct。
- **建構子**:`init(channel: Channel)` → `init(transport: any NatsTransport)`。
- **寫入**:`channel.writeAndFlush(writeBuffer, promise:)` + `EventLoopPromise` 回呼 → `try await transport.send(Data(buffer: writeBuffer))`。
- `ByteBuffer` 仍保留,但**只當記憶體累積緩衝**(從不直接碰 socket),最後複製成 `Data` 交給傳輸。
- 批次邏輯(超過 batchSize 時用 continuation 等待 flush、flush 完成後處理 waiting messages)保留,只是改用 actor + async/await 重寫,移除 `#if SWIFT_NATS_BATCH_BUFFER_DISABLED` 分支。

### 3.3 `Sources/Nats/RttCommand.swift`（44 行）

把 RTT 量測從 NIO promise 改成 Swift continuation。
- 移除 `import NIOCore`,改 `import NIOConcurrencyHelpers`。
- `EventLoopPromise<TimeInterval>?` → 一個 `NIOLockedValueBox<State>` 狀態機(`.pending` / `.ready(rtt)` / `.waiting(continuation)`)。
- `makeFrom(channel:)` → `makeFrom()`。
- `setRoundTripTime()`:依狀態 resume continuation 或記下 ready 值(處理「結果先到 vs. 等待先到」的競態)。
- `getRoundTripTime()`:`promise.futureResult.get()` → `withCheckedContinuation`,若已 ready 立即回傳。

### 3.4 `Sources/Nats/NatsClient/NatsClient.swift`（5 行）

兩個內部觸點不再直接抓 channel:
- `flush()`:`connectionHandler.channel?.flush()` → 移除(改為註解說明 BatchBuffer 每次 write 都已 flush)。
- `rtt()`:`RttCommand.makeFrom(channel: connectionHandler.channel)` → `RttCommand.makeFrom()`。

公開 API 表面完全沒變。

### 3.5 `Sources/Nats/HTTPUpgradeRequestHandler.swift`（−182 行,整檔刪除）

NIO 的 HTTP Upgrade(101)handler。WebSocket 握手現在交給 `URLSessionWebSocketTask` / `NWConnection` 自己做,此檔變成死碼,刪除。

### 3.6 `Sources/NatsServer/NatsServer.swift`（318 行變動,但無邏輯改動）

整檔包進 `#if os(macOS) || os(Linux)` 並重新縮排。原因:`NatsServer` 會 spawn `nats-server` 子行程,依賴 `Process`/`XCTest`,在 iOS/tvOS/watchOS 不可用 —— 包起來讓 app 在 iOS link 此 package 時仍能編譯。**功能邏輯一行未改**,diff 大是因為整段縮排位移。

### 3.7 `Tests/NatsTests/Unit/JwtTests.swift`（+4 行）

加 `#if canImport(FoundationNetworking) import FoundationNetworking #endif`,讓 Linux 上測試能編譯(URLSession 相關 API 在 Linux 位於 FoundationNetworking)。

---

## 4. Package.swift 與平台

- **iOS 最低部署目標**:`.iOS(.v13)` → `.iOS("17.2")`。
- **移除 NIO 依賴**:`NIOHTTP1`、`NIOWebSocket`(WebSocket 握手不再用 NIO)。保留 `swift-nio` / `NIOSSL` / `NIOFoundationCompat`(Linux raw 路徑與工具仍用)。
- **JetStream 改成只在 Apple 平台編譯**:把 JetStream 的 product / target / testTarget 抽成
  ```swift
  #if canImport(Darwin)
      let jetStreamProducts: [Product] = [ .library(name: "JetStream", targets: ["JetStream"]) ]
      let jetStreamTargets: [Target] = [ .target(name: "JetStream", ...), .testTarget(...) ]
  #else
      let jetStreamProducts: [Product] = []
      let jetStreamTargets: [Target] = []
  #endif
  ```
  再用 `products: [...] + jetStreamProducts` 與 `targets: [...] + jetStreamTargets` 串接。原因:JetStream 依賴 CryptoKit/Combine,Linux 無對應;這樣 Nats 核心仍能在 Linux CI 建置/測試。

> ⚠️ 這是未來 sync 上游最容易出錯的地方:上游是平鋪陣列,我們改成條件式組裝。

---

## 5. CI / Scripts / 文件

### `.github/workflows/ci.yml`
- **移除 macOS build/test job**(原本在 macos-15 跑 xcodebuild + swift test)。
- iOS job 改成同時 build `Nats` 與 `NatsServer`。
- 保留 `check-linter` job。

### `.github/workflows/build-check.yml`（新增,54 行）
針對 Darwin-only 的 WebSocket 程式碼(`canImport(Network)` gated):
- `build` job:macos-15 上 `swift build` 編譯檢查。
- `ws-nwtransport-verify` job:裝 nats-server、用 openssl 產臨時 TLS cert、啟動 websocket+TLS 的 nats-server、跑 `swift Scripts/verify-ws-nwtransport.swift` 驗證 NWConnection WebSocket 能直連(無 proxy)並在 101 upgrade 後收到 server INFO。
- 觸發:push 到 `claude/**` 分支 或 手動 `workflow_dispatch`。

### `Scripts/verify-ws-nwtransport.swift`（新增,88 行）
NWConnection WebSocket 傳輸的執行期驗證腳本(被上面的 CI job 使用)。

### 文件（新增）
- `RECON.md`(170 行):NIO → URLSession 的偵查與設計決策紀錄。Phase 0/1 findings,記錄了「哪些 NIO 用法是 load-bearing、哪些是 vestigial」、Linux `file://` 測試限制、wss-only 範圍的設計決定。
- `docs/superpowers/specs/2026-06-25-nwconnection-websocket-transport-design.md`(163 行):NWConnection WebSocket 設計規格。
- `docs/superpowers/plans/2026-06-25-nwconnection-websocket-transport.md`(264 行):實作計畫。

---

## 6. 完整檔案清單與每檔摘要

| 檔案 | 增/減 | 類型 | 一句話 |
|------|-------|------|--------|
| `Sources/Nats/Transport/NatsTransport.swift` | +48 | 新增 | 傳輸抽象協定 |
| `Sources/Nats/Transport/NWWebSocketTransport.swift` | +265 | 新增 | **Apple WS,NWConnection,per-app VPN 穿透** |
| `Sources/Nats/Transport/URLSessionWebSocketTransport.swift` | +117 | 新增 | 非 Apple WS fallback |
| `Sources/Nats/Transport/URLSessionStreamTransport.swift` | +147 | 新增 | Apple raw TCP/TLS |
| `Sources/Nats/Transport/NIOStreamTransport.swift` | +158 | 新增 | Linux raw TCP/TLS fallback |
| `Sources/Nats/Transport/TLSIdentity.swift` | +258 | 新增 | PEM→Security 型別 + URLSession TLS delegate |
| `Sources/Nats/NatsConnection.swift` | ~617 | 改寫 | 去 NIO,改走傳輸抽象(核心手術)|
| `Sources/Nats/BatchBuffer.swift` | ~149 | 改寫 | class→actor,寫入改走 transport.send |
| `Sources/Nats/RttCommand.swift` | ~44 | 改寫 | EventLoopPromise→CheckedContinuation |
| `Sources/Nats/NatsClient/NatsClient.swift` | ~5 | 微改 | flush/rtt 去 channel |
| `Sources/Nats/HTTPUpgradeRequestHandler.swift` | −182 | 刪除 | NIO HTTP upgrade,死碼 |
| `Sources/NatsServer/NatsServer.swift` | ~318 | 包平台條件 | `#if os(macOS)||os(Linux)`,無邏輯改動 |
| `Tests/NatsTests/Unit/JwtTests.swift` | +4 | 微改 | Linux import |
| `Package.swift` | ~49 | 改寫 | iOS 17.2、去 NIOHTTP1/WS、JetStream Apple-only |
| `.github/workflows/ci.yml` | ~19 | 改寫 | 移除 macOS job |
| `.github/workflows/build-check.yml` | +54 | 新增 | macOS NW WebSocket 編譯+執行驗證 |
| `Scripts/verify-ws-nwtransport.swift` | +88 | 新增 | NW WebSocket 執行期驗證腳本 |
| `RECON.md` | +170 | 新增 | 偵查/設計決策紀錄 |
| `docs/superpowers/specs/...design.md` | +163 | 新增 | NWConnection WS 設計規格 |
| `docs/superpowers/plans/...transport.md` | +264 | 新增 | NWConnection WS 實作計畫 |

---

## 附:什麼**沒有**改(這很重要)

以下層級本來就 transport-agnostic,完全沒動,sync 上游時可放心:
- `Data+Parser.swift` — NATS 協定解析,零 NIO。
- `NatsSubscription.swift` / `ConcurrentQueue.swift` — 只把 `NIOLockedValueBox` 當鎖用。
- `NatsProto.swift`、`NatsClientOptions.swift`、`NatsEvents.swift`、認證/錯誤型別等 — 公開 API 與協定邏輯。

所以這次手術的「爆炸半徑」幾乎只集中在 `NatsConnection.swift` + `BatchBuffer.swift` + `RttCommand.swift`。
