package com.example.gauntlet.architecture

import com.tngtech.archunit.junit.AnalyzeClasses
import com.tngtech.archunit.junit.ArchTest
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * mock 的邊界規則。這一份「要」分析測試碼，所以不加 DoNotIncludeTests。
 *
 * 規則不是「不准 mock」，而是「mock 跟資料庫不准出現在同一個類別」。
 *
 * 理由：SQLite 與 DuckDB 都是嵌入式的，開個暫存檔就有真資料庫。
 * 這種情況下 mock 掉它，等於把 SQL 正確性、型別對映、PRAGMA 有沒有生效
 * 一起丟掉，什麼都沒換到。但 mock 自己定義的 port（OrderRepository）
 * 來逼出「磁碟壞掉」這類真 DB 難以重現的失敗路徑，是完全正當的用法。
 */
@AnalyzeClasses(packages = ["com.example.gauntlet"])
class MockBoundaryTest {

    @ArchTest
    @JvmField
    val mocksNeverTouchTheDatabase: ArchRule =
        noClasses().that().dependOnClassesThat()
            .resideInAnyPackage("io.mockk..", "org.mockito..", "org.easymock..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("org.jdbi..", "java.sql..", "org.sqlite..", "org.duckdb..")
            .because("要驗 SQL 就打真的資料庫；要 mock 就只 mock 自己的介面")
}
