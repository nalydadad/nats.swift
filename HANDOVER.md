# 交接文件 — Fork 與上游 NATS.swift 的差異與維護指南

> 目的：讓任何接手者能快速知道 **這個 fork 相對於上游原版改了什麼、為什麼改、未來要 sync 上游時哪裡會衝突、出問題時怎麼排查**。

- **撰寫日期**：2026-06-25
- **上游基準點（fork 分歧點）**：commit `a9031f1`（上游 PR #118，"Update Claude workflow permissions"）
- **本 fork HEAD**：`claude/version-comparison-changes-jy9uvv`
- **規模**：`a9031f1..HEAD` 共 **20 個檔案，+2246 / −873**，橫跨已合併的 PR #1、#2、#3、#5 與目前分支（#7）

---

## 1. 一句話總結

> **核心工作：把網路傳輸層從 Swift NIO 換成 URLSession / Network.framework 為基礎的可插拔傳輸。**
> 動機是「企業 Proxy/VPN 穿透」與「繼承系統信任憑證庫（system trust store）」，這是 NIO 直接開 socket 做不到的。
> NATS 協定解析、訂閱、認證、事件層 **幾乎沒動**（它們本來就 transport-agnostic）。附帶把 JetStream 限縮到 Apple 平台、iOS 最低版本提到 17.2、並調整 CI 與補上設計文件。

---

## 2. 為什麼要改（背景動機）

原版用 NIO 的 `ChannelInboundHandler` 直接管理 socket / TLS / WebSocket。問題：

1. **無法走系統 Proxy / per-app VPN**：NIO 自己開 raw socket，不會套用 OS 層的 proxy 設定，也不走 per-app VPN tunnel。在企業環境（強制 proxy / VPN）連不出去。
2. **不繼承系統信任庫**：NIOSSL 需要自帶 CA，無法直接用 iOS/macOS 內建的信任鏈。

`URLSessionWebSocketTask` 與 `NWConnection`（Network.framework）天生會套用系統 proxy 設定、per-app VPN 與信任庫，所以改用它們。詳細偵查紀錄見 `RECON.md`。

---

## 3. 架構改動：新增可插拔傳輸層

新增 `Sources/Nats/Transport/`，把「NATS 協定」與「底層 I/O」解耦。介面只進出 `Data`，協定解析（`Data.parseOutMessages()`）完全不碰底層傳輸。

### 傳輸抽象協定 `NatsTransport`（`Transport/NatsTransport.swift`）

```swift
internal protocol NatsTransport: AnyObject, Sendable {
    var incomingMessages: AsyncThrowingStream<Data, Error> { get }
    func connect(url: URL, tls: TransportTLSOptions?) async throws
    func startSecureConnection() throws   // 原 NATS 的 tlsFirst / 伺服器要求 TLS 的就地升級；WebSocket 為 no-op
    func send(_ data: Data) async throws
    func close()
}
```

### 五個傳輸實作

| 檔案 | 用途 | 平台 |
|------|------|------|
| `NatsTransport.swift` | 抽象協定 + `TransportTLSOptions` | 全平台 |
| `NWWebSocketTransport.swift` | Network.framework (NWConnection) WebSocket，支援 per-app VPN / 系統 proxy | Apple（`canImport(Network)`）|
| `URLSessionWebSocketTransport.swift` | `URLSessionWebSocketTask` WebSocket | 非 Apple fallback |
| `NIOStreamTransport.swift` | 保留 NIO 的 raw TCP/TLS 路徑 | Linux（`canImport(FoundationNetworking)`）|
| `URLSessionStreamTransport.swift` | URLSession 做 raw NATS TCP/TLS | Apple |
| `TLSIdentity.swift` | TLS 憑證 / 身分（client cert / key）處理 | — |

### 傳輸選擇邏輯（關鍵！位於 `NatsConnection.swift::connectToServer`）

依 **URL scheme** + **編譯平台** 決定用哪個傳輸：

```
ws:// 或 wss://  →  Apple: NWWebSocketTransport      / 其他: URLSessionWebSocketTransport
nats:// / tls:// →  Linux: NIOStreamTransport         / Apple: URLSessionStreamTransport
```

> ⚠️ 注意這個編譯條件的判斷：raw 路徑用 `canImport(FoundationNetworking)`（Linux 有 → 用 NIO），WebSocket 路徑用 `canImport(Network)`（Apple 有 → 用 NW）。Apple 平台沒有 `FoundationNetworking`，所以 raw 走 URLSession。

---

## 4. 既有檔案的改動（與上游對應）

| 檔案 | 行數 | 改了什麼 | 未來 sync 上游的衝突風險 |
|------|------|----------|--------------------------|
| `Sources/Nats/NatsConnection.swift` | 617 | **最大改動**。`ConnectionHandler` 不再是 `ChannelInboundHandler`；connect / read / write / 生命週期改走 `NatsTransport`；read loop 改用 `AsyncThrowingStream`（`startReadLoop`）；ping 排程、suspend/resume 改用非 NIO 等價物 | **極高** — 上游任何對連線邏輯的修改都會在這裡衝突 |
| `Sources/Nats/BatchBuffer.swift` | 149 | 批次寫入不再呼叫 `channel.writeAndFlush`，改走 `BatchBuffer(transport:)` → `transport.send()` | 中高 |
| `Sources/Nats/HTTPUpgradeRequestHandler.swift` | −182 | **整檔刪除**。WebSocket 的 HTTP Upgrade/101 握手交給 URLSession / NWConnection 自己做 | 若上游改此檔，sync 時需確認是否仍要刪 |
| `Sources/Nats/RttCommand.swift` | 44 | `EventLoopPromise<TimeInterval>` → `CheckedContinuation` | 中 |
| `Sources/Nats/NatsClient/NatsClient.swift` | 5 | `flush()` / `rtt()` 兩個內部點不再直接抓 `connectionHandler.channel`，改走 transport | 低 |
| `Sources/NatsServer/NatsServer.swift` | 318 | 用 `#if os(macOS) || os(Linux)` 包起來（依賴 `Process`/`XCTest`，iOS/tvOS 無）；iOS 上只 build 不跑 | 中 |
| `Tests/NatsTests/Unit/JwtTests.swift` | +4 | 小幅調整 | 低 |

---

## 5. Package.swift 與平台變更

- **iOS 最低部署目標：`13.0` → `17.2`**（NWConnection WebSocket 與相關 API 需要）。
- **移除 NIO 依賴**：`NIOHTTP1`、`NIOWebSocket`（WebSocket 握手不再用 NIO）。`swift-nio` / `NIOSSL` / `NIOFoundationCompat` 仍保留（Linux raw 路徑與部分工具仍用）。
- **JetStream 改成只在 Apple 平台編譯**（`#if canImport(Darwin)` 包住 product + target + testTarget）。原因：JetStream 依賴 CryptoKit / Combine，Linux 沒有對應。這讓 Nats 核心仍能在 Linux CI 建置/測試。

> ⚠️ **sync 上游時最容易出錯的點**：上游的 `Package.swift` 是平鋪的 product/target 陣列；我們改成 `let jetStreamProducts/jetStreamTargets` 用 `#if canImport(Darwin)` 條件組裝，再用 `+ jetStreamProducts` / `+ jetStreamTargets` 串接。上游若改依賴，需手動把變更搬進這個條件結構。

---

## 6. CI / 工具 / 文件

- `.github/workflows/ci.yml`：**移除 macOS build/test job**；iOS job 改成同時 build `Nats` 與 `NatsServer`。
- `.github/workflows/build-check.yml`：新增 / 調整（Linux 建置檢查）。
- `Scripts/verify-ws-nwtransport.swift`：WebSocket NWTransport 的執行期驗證腳本。
- `RECON.md`：NIO → URLSession 的偵查與設計決策紀錄（**強烈建議先讀**）。
- `docs/superpowers/specs/2026-06-25-nwconnection-websocket-transport-design.md`：NWConnection WebSocket 設計規格。
- `docs/superpowers/plans/2026-06-25-nwconnection-websocket-transport.md`：實作計畫。

---

## 7. 未來 sync 上游版本的步驟

1. 加上游 remote（若未加）：
   ```bash
   git remote add upstream https://github.com/nats-io/nats.swift.git
   git fetch upstream
   ```
2. 先看上游從 `a9031f1` 之後動了哪些檔案：
   ```bash
   git log --oneline a9031f1..upstream/main
   git diff --stat a9031f1 upstream/main
   ```
3. **重點檢查清單（衝突熱點，依風險排序）**：
   - `Sources/Nats/NatsConnection.swift` — 幾乎一定衝突，需逐段比對協定邏輯 vs. 我們的 transport 改寫。
   - `Sources/Nats/BatchBuffer.swift` — 寫入路徑。
   - `Package.swift` — 條件式 JetStream / 依賴清單 / iOS 版本。
   - `Sources/Nats/HTTPUpgradeRequestHandler.swift` — 我們已刪，上游若改要決定是否保留刪除。
   - `Sources/Nats/RttCommand.swift` — promise → continuation。
4. **不要把上游的 NIO 連線邏輯直接吃回來** — 那會退回我們刻意換掉的東西。原則：**保留我們的 transport 抽象，只把上游的「協定 / bug fix / 新功能」搬進來。**
5. sync 後驗證（見下節）。

---

## 8. 問題排查指南（Troubleshooting）

### 連線連不上 / 立刻斷線
- 先確認 **scheme 與傳輸的對應**（第 3 節表）。`ws/wss` 走 WebSocket 傳輸；`nats/tls` 走 stream 傳輸。
- 在 `NatsConnection.swift::startReadLoop` 與 `connectToServer` 加 log；read loop 用 `AsyncThrowingStream`，斷線會以 stream 結束 / 拋錯呈現。

### TLS 失敗
- `NatsConnection.swift` 約 444–490 行有針對不同傳輸的錯誤標記邏輯：NWWebSocketTransport 與 NIOStreamTransport 各自會把底層錯誤映射成 `tlsFailure`。若錯誤被誤標，先看這段。
- client cert / key / root CA 走 `TransportTLSOptions` → `TLSIdentity.swift`。

### Proxy / VPN 沒生效
- 這正是改用 URLSession / NWConnection 的目的。若沒走 proxy，確認是否走到 `NWWebSocketTransport`（Apple）而非 fallback。可用 `Scripts/verify-ws-nwtransport.swift` 驗證。

### Linux 上測試失敗（檔案類認證）
- 已知：`JwtTests.testParseCredentialsFile`、`CoreNatsTests.testCredentialsAuth`、`testNkeyAuthFile` 在 Linux 會因 swift-corelibs-foundation 的 `URLSession` 不支援 `file://` 而失敗（`NSURLError -1002`）。**這是 Linux Foundation 限制，非 NATS 邏輯問題**，在 iOS/macOS 上會通過。見 `RECON.md`。

### JetStream 在 Linux 編不過
- 預期行為。JetStream 只在 `canImport(Darwin)` 編譯。要在 Linux 動它需自行提供 CryptoKit/Combine 替代。

---

## 9. 驗證指令

```bash
swift build                      # 核心建置
swift test --filter NatsTests    # 核心測試（需 nats-server 在 PATH）
swift-format lint --configuration .swift-format -r --strict Sources Tests   # 嚴格 lint
swift Scripts/verify-ws-nwtransport.swift   # WebSocket NW 傳輸執行期驗證
```

> CI（`.github/workflows/`）：iOS 在 macOS-15 runner build `Nats` + `NatsServer`；另有 linter 與 Linux build check。

---

## 10. 快速指令備忘（重現本文件的差異分析）

```bash
# 看所有相對上游的改動
git diff --stat a9031f1 HEAD
git log --oneline a9031f1..HEAD

# 看單一檔案逐行差異
git diff a9031f1 HEAD -- Sources/Nats/NatsConnection.swift
```
