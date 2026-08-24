# EVIDENCE — 完工後證據

> 協定：這份文件的每個數字都必須從 **真實跑過的報告** 抄過來。
> 沒跑過的、跑失敗的、猜的，一律標記 `Unverified`，不准填看起來合理的數字。
>
> **本檔目前的狀態：全部 Unverified。**
> 產出這份骨架的環境沒有 Maven，也連不到 Maven Central，所以一行 build 都沒跑過。
> 請在你自己的環境執行 `./run-gauntlet.sh`，用真實輸出覆蓋下面每一格。

執行環境：`Unverified`（請記錄實際的 JDK / Kotlin / Quarkus 版本）
執行時間：`Unverified`
Git commit：`Unverified`
指令：`mvn clean verify`

---

## 第一層 — BDD 業務驗收

| 項目 | 值 | 來源 |
|---|---|---|
| Tests run | `Unverified` | `target/surefire-reports/` |
| Failures / Errors | `Unverified` | 同上 |
| Skipped | `Unverified` | 同上 |
| `OrderProcessingSpec` 全綠 | `Unverified` | 同上 |

## 第二層 — 靜態與架構

| 項目 | 值 | 來源 |
|---|---|---|
| Kotlin 編譯警告數（`-Werror`） | `Unverified` | Maven console |
| Detekt issues | `Unverified` | `target/detekt/detekt.html` |
| Detekt type resolution 是否啟用 | `Unverified` | 見 README「Detekt 型別解析」 |
| ArchUnit 規則數 / 通過數 | `Unverified` | `target/surefire-reports/` |
| mock 邊界規則（MockBoundaryTest） | `Unverified` | 同上 |

## 第三層 — 資料庫安全設定（實際回讀值）

| 設定 | 期望 | 實測 | 來源 |
|---|---|---|---|
| SQLite `journal_mode` | `wal` | `Unverified` | `DatabaseGuardrailTest` |
| SQLite `busy_timeout` | `5000` | `Unverified` | 同上 |
| DuckDB `threads` | `2` | `Unverified` | 同上 |
| DuckDB `memory_limit` | 已設定（字串格式依版本而異） | `Unverified` | 同上 |
| 併發寫入 100 筆全成功 | 是 | `Unverified` | `HostileInputSpec` |

## 第四層 — 覆蓋率與變異

| 項目 | 門檻 | 實測 | 來源 |
|---|---|---|---|
| JaCoCo branch coverage — `domain` | ≥ 85% | `Unverified` | `target/site/jacoco/index.html` |
| JaCoCo branch coverage — `application` | ≥ 85% | `Unverified` | 同上 |
| Pitest mutation score | ≥ 80 | `Unverified` | `target/pit-reports/index.html` |
| Pitest 存活變異數 | — | `Unverified` | 同上 |

## 第五層 — 一鍵驗證

| 項目 | 值 |
|---|---|
| `mvn clean verify` 結果 | `Unverified` |
| 總耗時 | `Unverified` |

---

## Unverified 清單（誠實區）

這一節是整份文件最重要的部分。任何沒被自動化驗證的東西都要列在這裡。

1. **整個 build 從未執行過。** 產出環境無 Maven、無外網，所有版本相容性都只是紙上相容，未經編譯驗證。
   Kotlin 2.0.21 × detekt 1.23.8 這組是對得上的。剩下要親手確認的是：
   **kotlin-bom 排在 quarkus-bom 之前有沒有真的把 kotlin-stdlib 拉到 2.0.21**
   （`mvn dependency:tree -Dincludes=org.jetbrains.kotlin`），
   以及 Arrow 2.0.1 / MockK 1.13.13 / Pitest kotlin plugin 在你的 repository 抓不抓得到。
   另外 `quarkus.platform.version` 目前是佔位的 3.2.12.Final，要換成你實際那個版本。
2. **Pitest 的 Kotlin plugin 座標與版本未驗證。** `com.groupcdg.pitest:pitest-kotlin-plugin` 需要確認在你的 repository 抓得到；抓不到就先移除該 dependency 與 `+KOTLIN` feature，mutation 仍可跑，只是會多一些 Kotlin 樣板造成的等價變異。
3. **Detekt `UnnecessarySafeCall` / `UnnecessaryNotNullOperator` 需要 type resolution。** 未設 classpath 時它們不會報錯，也等於沒作用。
4. **DuckDB `memory_limit` 只驗證「有被設定」，沒有驗證真的觸發記憶體上限。** 要真的證明，需要跑一個會爆記憶體的查詢並斷言它失敗，目前沒做。
5. **JaCoCo 對 Kotlin 的 inline function 與 `data class` 自動產生的方法會有偏差。** 85% 這個數字要對照報告細看，不要只看總數。
6. **沒有做 Quarkus 端對端 HTTP 測試。** `OrderResource` 目前只有編譯期保證，沒有 `@QuarkusTest` 打過真實 endpoint。
7. **`domainNeverThrows` 這條 ArchUnit 規則的行為未驗證。** 它依賴 ArchUnit 如何統計 dependency；
   若因為 Arrow inline 展開的 try/catch 而誤報，改用「禁止呼叫 Throwable 建構子」的寫法。
8. **沒有跨程序（multi-process）併發測試。** 目前的併發測試都在同一個 JVM 內，WAL 的跨程序行為未驗證。
