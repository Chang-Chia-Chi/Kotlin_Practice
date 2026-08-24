# SPEC — 開工前文件

> 協定：Agent 在寫任何 Kotlin 實作或測試之前，先產出這份文件並取得人類批准。
> 沒有批准就開始寫碼，等於這份 SPEC 不存在。
> 本檔同時是這個參考專案的實際 SPEC，以及後續任務可以照抄的範本。

## 1. 我理解的需求

用兩個嵌入式資料庫示範 OLTP / OLAP 分工：

- 新訂單寫進 SQLite（交易型），欄位為 id、customer_id、amount_cents、order_date。
- 每日彙整把當日訂單轉成一列統計（筆數、總額、最大值、平均值），寫進 DuckDB（分析型）。
- 兩條路徑的成功與失敗都要能被斷言，失敗必須是具名的業務錯誤，不是例外。

## 2. 邊界條件

| 項目 | 決定 | 理由 |
|---|---|---|
| id | 非空白、trim 後長度 ≤ 36 | 對齊 UUID 長度上限 |
| customer_id | 非空白、trim 後長度 ≤ 64 | 擋超長字串攻擊 |
| amount_cents | 整數、> 0、≤ 100,000,000 | 用分為單位避免浮點數；上限擋溢位測試 |
| order_date | 不可為 null | 彙整以日期為 key |
| 平均值 | 整數除法無條件捨去 | 金額用分，不引入 BigDecimal |
| 空日期彙整 | 回傳 `NoDataForDate` | 不寫一列全 0 進 DuckDB，避免下游誤判 |
| 重跑彙整 | 同日期覆蓋，不新增列 | delete + insert 包在同一交易 |
| 混入他日訂單 | 回傳 `StorageFailure` | 資料來源不一致時寧可停下 |

## 3. 失敗模型

`DomainError` 為 sealed interface，共 8 種：
`InvalidOrderId`、`InvalidCustomer`、`InvalidAmount`、`InvalidDate`、
`OrderNotFound`、`DuplicateOrder`、`NoDataForDate`、`StorageFailure`。

Domain 與 application 層一律回傳 `Either<DomainError, T>`，不 throw。
JDBC 例外只能在 infrastructure 的 `guard { }` 收斂成 `StorageFailure`。

## 4. 預計安裝與使用的工具

| 用途 | 工具 | 版本 |
|---|---|---|
| 語言 / 框架 | Kotlin / Quarkus | 2.0.21（覆寫 BOM）/ 3.2.x |
| 函數式錯誤處理 | Arrow-kt | 2.0.1（要與 Kotlin 2.0.x 對得上） |
| DB 存取 | JDBI 3 | 3.45.4 |
| OLTP | SQLite (xerial) | 3.46.1.3 |
| OLAP | DuckDB JDBC | 1.1.3 |
| BDD | Kotest | 5.9.1 |
| 單元測試 | JUnit 5 | 由 Quarkus BOM 管理 |
| 靜態分析 | Detekt | 1.23.8（正好對 Kotlin 2.0.21 編譯） |
| 架構檢查 | ArchUnit | 1.3.0 |
| 覆蓋率 | JaCoCo | 0.8.12 |
| 變異測試 | Pitest (+ junit5 / kotlin plugin) | 1.17.0 |

| Mock | MockK | 1.13.13 |

**mock 的邊界**：可以 mock，但 mock 與 JDBI 不准出現在同一個類別（`MockBoundaryTest` 強制）。
SQLite / DuckDB 開個暫存檔就是真資料庫，mock 掉只會丟失 SQL 正確性與 PRAGMA 驗證；
但 mock 自己定義的 port 來逼出真 DB 重現不了的失敗路徑，是正當用法。

## 5. 資料庫安全設定

- SQLite：每條連線執行 `journal_mode=WAL`、`busy_timeout=5000`、`foreign_keys=ON`、`synchronous=NORMAL`。
  WAL 不支援 `:memory:`，所以測試也走暫存檔。
- DuckDB：每條連線執行 `memory_limit='1GB'`、`threads=2`。
  DuckDB 的記憶體是 C++ native heap，`-Xmx` 管不到，不設上限會被 OS OOM Killer 砍掉整個行程。
  另外 DuckDB 的記憶體資料庫是「一條連線一個 DB」，所以保留 root connection 再 `duplicate()`。

## 6. 驗證計畫

| 層 | 手段 | 門檻 |
|---|---|---|
| 1 | Kotest BehaviorSpec，真實 SQLite + DuckDB | 全數通過 |
| 2 | Detekt + ArchUnit + `-Werror` | 零 issue、零警告 |
| 3 | PRAGMA / duckdb_settings() 實際回讀 | 設定值必須相符 |
| 4 | JaCoCo 分支覆蓋率（domain、application） | ≥ 85% |
| 4 | Pitest mutation score（domain、application） | ≥ 80 |
| 5 | `mvn clean verify` 一鍵跑完上述全部 | 任何一項失敗即 build fail |

## 7. 明確不做的事

- 不做 Quarkus native image：SQLite 與 DuckDB 都是 JNI，native 不支援。
- 不做 DB migration 工具（Flyway/Liquibase）：範例規模用 `CREATE TABLE IF NOT EXISTS` 即可。
- 不做認證授權、不做分頁、不做 OpenAPI 文件。

## 8. 待人類確認

- [ ] 平均值採整數捨去，可接受？
- [ ] DuckDB 用純記憶體（重啟即消失）示範，或要換成檔案模式？
- [ ] Mutation score 門檻 80 是否要提高到 85？
