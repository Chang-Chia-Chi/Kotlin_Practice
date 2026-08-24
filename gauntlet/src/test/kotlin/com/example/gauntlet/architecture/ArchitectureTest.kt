package com.example.gauntlet.architecture

import com.tngtech.archunit.base.DescribedPredicate
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests
import com.tngtech.archunit.junit.AnalyzeClasses
import com.tngtech.archunit.junit.ArchTest
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.jdbi.v3.sqlobject.statement.SqlQuery
import org.jdbi.v3.sqlobject.statement.SqlUpdate

/**
 * 第二層護欄：架構邊界。
 * 這些規則不是風格建議，是硬性邊界；踩到就 build fail。
 */
@AnalyzeClasses(
    packages = ["com.example.gauntlet"],
    importOptions = [DoNotIncludeTests::class],
)
class ArchitectureTest {

    @ArchTest
    @JvmField
    val domainDependsOnNothingInside: ArchRule =
        noClasses().that().resideInAPackage("..domain..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("..infrastructure..", "..adapter..", "..application..")
            .because("domain 是最內層，不准往外看")

    @ArchTest
    @JvmField
    val domainHasNoFrameworkOrDriver: ArchRule =
        noClasses().that().resideInAPackage("..domain..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("io.quarkus..", "org.jdbi..", "org.sqlite..", "org.duckdb..", "java.sql..")
            .because("domain 不綁框架也不綁資料庫")

    // 注意：Arrow 的 either { } 是 inline，會把 try/catch 展開到 domain 的 bytecode，
    // 但它 catch 的是 arrow 自己的型別，所以下面只比對 java/javax/jakarta 的例外。
    // 若某版本 ArchUnit 把 catch 型別也算成 dependency 而誤報，改成只擋
    // 「呼叫 Throwable 建構子」即可（見 EVIDENCE.md Unverified 第 8 點）。
    @ArchTest
    @JvmField
    val domainNeverThrows: ArchRule =
        noClasses().that().resideInAPackage("..domain..")
            .should().dependOnClassesThat(JVM_EXCEPTIONS)
            .because("domain 的失敗一律用 Either 表達，不用例外")

    @ArchTest
    @JvmField
    val applicationTalksToPortsOnly: ArchRule =
        noClasses().that().resideInAPackage("..application..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("..infrastructure..", "..adapter..")
            .because("use case 只認 domain 介面，不認實作")

    @ArchTest
    @JvmField
    val sqlObjectsStayInInfrastructure: ArchRule =
        noClasses().that().resideOutsideOfPackage("..infrastructure..")
            .should().containAnyMethodsThat(
                com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates
                    .annotatedWith(SqlQuery::class.java)
                    .or(
                        com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates
                            .annotatedWith(SqlUpdate::class.java),
                    ),
            )
            .because("SQL 只能出現在 infrastructure")

    @ArchTest
    @JvmField
    val productionCodeHasNoMocks: ArchRule =
        noClasses().that().resideInAPackage("com.example.gauntlet..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("io.mockk..", "org.mockito..", "org.easymock..")
            .because("mock 只屬於測試碼（這個 class 帶 DoNotIncludeTests，只看正式碼）")

    private companion object {
        val JVM_EXCEPTIONS: DescribedPredicate<JavaClass> =
            JavaClass.Predicates.assignableTo(Throwable::class.java)
                .and(JavaClass.Predicates.resideInAnyPackage("java..", "javax..", "jakarta.."))
                .`as`("JVM exception types")
    }
}
