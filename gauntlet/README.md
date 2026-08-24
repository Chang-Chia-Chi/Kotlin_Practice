# Gauntlet — AI Agent 寫碼、驗證網把關的參考專案

Kotlin 2.0.21 + Quarkus 3.2.x + JDBI + SQLite（OLTP）+ DuckDB（OLAP），Maven 構建。

這個專案要示範的不是「怎麼寫訂單系統」，而是一件事：

> **人類不逐行讀 Agent 寫的實作碼，改成讀 SPEC、讀 EVIDENCE，剩下的交給一道跑得動的驗證網。**

前提是驗證網真的擋得住。擋不住的話，這整套只是自我安慰。

---

## 工作流

```
人類寫需求
   ↓
Agent 產出 SPEC.md ──── 人類批准（唯一一次人類仔細讀的地方）
   ↓
Agent 寫實作 + 測試（人類不逐行讀）
   ↓
./run-gauntlet.sh  →  mvn clean verify
   ↓
綠燈 → Agent 產出 EVIDENCE.md（數字從報告抄）
紅燈 → Agent 自己修，不准動測試
   ↓
人類讀 EVIDENCE.md，特別是「Unverified 清單」
```

### 三條給 Agent 的硬規矩

1. **不准削弱測試。** 不刪、不註解、不改斷言、不加 `@Disabled`、不放寬門檻。門檻要改，回到 SPEC 重新談。
2. **mock 不准碰資料庫。** 規則不是「不准 mock」，是 mock 與 JDBI 不准出現在同一個類別。
   要驗 SQL 就打真的 SQLite / DuckDB；要 mock 就只 mock 自己定義的 port。
3. **沒驗證的事要自己招。** EVIDENCE.md 的 Unverified 清單寫得越誠實，這套流程越有用。空的 Unverified 清單通常代表 Agent 在騙人。

---

## 五層驗證網

| 層 | 擋什麼 | 工具 | 門檻 |
|---|---|---|---|
| 1 | 業務行為錯 | Kotest BehaviorSpec，真實 SQLite + DuckDB | 全綠 |
| 2 | 架構腐化、型別偷懶 | Detekt + ArchUnit + Arrow Either + `-Werror` | 零 issue、零警告 |
| 3 | 資料庫踩雷 | PRAGMA / duckdb_settings() 實際回讀 | 設定值相符 |
| 4 | 測試有跑但沒斷言 | JaCoCo + Pitest | branch ≥ 85%、mutation ≥ 80 |
| 5 | 「我本機是好的」 | `mvn clean verify` 一條命令 | 任一項失敗即 fail |

第四層是這套的重點。JaCoCo 只證明「這行被執行過」，Pitest 才證明「這行壞掉時測試會叫」。
AI 最常見的缺陷就是產出跑得過但什麼都沒斷言的測試，那種測試在 JaCoCo 是綠的，在 Pitest 是紅的。

---

## 專案結構

```
com.example.gauntlet
├── domain          純業務。不准出現 Quarkus / JDBI / SQL / 例外
│   ├── Order.kt            建構走 create()，不合法的資料進不了型別
│   ├── DailySummary.kt     彙整邏輯，Pitest 的主要打擊目標
│   ├── DomainError.kt      8 種具名失敗
│   └── Ports.kt            OrderRepository / AnalyticsRepository 介面
├── application     use case 編排
├── infrastructure  JDBI DAO、SQLite / DuckDB 連線與設定
└── adapter         Quarkus REST + RFC 9457 風格錯誤輸出
```

依賴方向只有一個：外層指向內層。ArchUnit 會驗這件事，不是靠自律。

---

## 兩個資料庫的坑（這是第三層存在的理由）

**SQLite**
- `busy_timeout` 是 per-connection 的設定，不是資料庫層級。連線工廠沒跑它，併發寫入就直接 `SQLITE_BUSY`。
- WAL 不支援 `:memory:`，所以測試也用暫存檔，不然測到的跟正式跑的不是同一套行為。

**DuckDB**
- 記憶體是 C++ native heap，JVM 的 `-Xmx` 完全管不到。不設 `memory_limit`，容器裡就是被 OOM Killer 整個砍掉，連 heap dump 都沒有。
- 記憶體模式下「一條連線一個資料庫」。用 `DriverManager.getConnection("jdbc:duckdb:")` 開第二條，會拿到一個空的新資料庫。所以留住 root connection 再 `duplicate()`。

---

## 跑起來

```bash
./run-gauntlet.sh          # 等同 mvn clean verify
mvn quarkus:dev            # 開發模式
```

報告：

```
target/detekt/detekt.html
target/site/jacoco/index.html
target/pit-reports/index.html
target/surefire-reports/
```

### 版本線：Quarkus 3.2.x + Kotlin 2.0.21

這個組合的重點是「Kotlin 往上蓋」。Quarkus 3.2 BOM 本身帶 Kotlin 1.9.x，
我們把 `kotlin.version` 蓋到 2.0.21。**這是安全方向**：新編譯器讀得懂舊函式庫的
metadata，反過來（拿舊編譯器讀新函式庫）才會報 "compiled with a newer version of Kotlin"。

但光改屬性不夠。匯入的 BOM 不吃你本地的 property，`kotlin-stdlib` 會繼續停在 1.9.x，
變成「用 2.0 編譯器配 1.9 stdlib」。所以 `dependencyManagement` 裡
**kotlin-bom 排在 quarkus-bom 之前**（Maven 是先宣告者勝），把整組 Kotlin 產物拉到 2.0.21。

確認有生效：

```bash
mvn dependency:tree -Dincludes=org.jetbrains.kotlin
```

其他跟著 3.2 走的地方：

- `quarkus-resteasy-reactive-jackson` 用舊名。改名成 `quarkus-rest-jackson` 是 3.9 才發生的事。
- `maven.compiler.release` 設 17。3.2 的基準是 JDK 17，要用 21 先確認你那條 3.2.x 撐不撐。
- detekt 1.23.8 對 Kotlin 2.0.21 編譯，正好對上，不必碰 detekt 2.0 alpha。
- Arrow 2.0.1 需要 Kotlin 2.0+，符合。編得過再往上試，看到 metadata 錯就退一階。

順帶一提，Quarkus 3.2 這條線社群端早就 EOL 了，不再收 CVE 修補。
這件事跟這個 gauntlet 沒關係，但既然是要當團隊的參考架構，值得在別的地方單獨排一次。

### mock 的界線

專案有 MockK，可以用。硬性規則只有一條，由 `MockBoundaryTest` 強制：

> 依賴 mock 函式庫的類別，不准同時依賴 JDBI / JDBC / SQLite / DuckDB。

看 `application/` 底下那兩個測試就懂分工：

- `ProcessOrderUseCaseTest` — 打真實 SQLite，驗行為與 SQL。整個 class 不出現 mock。
- `ProcessOrderFailurePathTest` — mock `OrderRepository`（我們自己的 port），
  逼出「磁碟壞掉」這種真 DB 重現不了的失敗路徑。整個 class 不出現 JDBI。

順帶一提：**擋假綠燈真正靠的是 Pitest，不是禁 mock。**
mock 用得再兇，只要 mutation score 撐得住，那些測試就是有在斷言；
反過來全都不 mock 但斷言寫得虛，Pitest 一樣會紅。禁 mock 頂多是個粗糙的代理指標。

### Detekt 型別解析

`UnnecessarySafeCall`、`UnnecessaryNotNullOperator`、`UnsafeCallOnNullableType`
這三條需要 type resolution 才會真的報。detekt-maven-plugin 要另外傳 `classPath` 與 `jvmTarget`；
沒設的話它們不會誤報，但也等於沒開。這件事已經記在 EVIDENCE.md 的 Unverified 清單。

如果 detekt 啟動時抱怨某個規則名稱不存在（各版本規則有搬家過），
先把 `detekt.yml` 的 `config.validation` 改成 `false` 讓 build 過，再回頭對照該版本的規則清單修正。

---

## 調門檻的地方

全部集中在 `pom.xml` 的 properties：

```xml
<coverage.branch.min>0.85</coverage.branch.min>
<mutation.threshold>80</mutation.threshold>
```

調鬆之前先想清楚：門檻調鬆的那一刻，這道網就從「擋得住」變成「看起來擋得住」。

---

## 已知限制

見 `EVIDENCE.md` 的 Unverified 清單。那份清單就是這個專案的誠實度指標。
