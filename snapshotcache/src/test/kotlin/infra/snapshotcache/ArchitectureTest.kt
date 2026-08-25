package infra.snapshotcache

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.junit.jupiter.api.Test

/**
 * The five boundary rules of plan 2.2. These are the module's static-analysis gate;
 * they run on every build from P0 onward rather than being retrofitted later.
 */
class ArchitectureTest {

    private val framework: JavaClasses = ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("infra.snapshotcache")

    @Test
    fun `framework does not depend on business packages`() {
        noClasses().that().resideInAPackage("infra.snapshotcache..")
            .should().dependOnClassesThat().resideInAnyPackage("etl..", "source..")
            .because("the framework knows nothing about the service it runs inside (plan 2.2)")
            .check(framework)
    }

    @Test
    fun `core layers do not depend on the duckdb adapter or on frameworks`() {
        noClasses().that().resideInAnyPackage(
            "infra.snapshotcache.api..",
            "infra.snapshotcache.spi..",
            "infra.snapshotcache.core..",
        )
            .should().dependOnClassesThat().resideInAnyPackage(
                "infra.snapshotcache.duckdb..",
                "org.duckdb..",
                "io.quarkus..",
                "io.micrometer..",
            )
            .because("adapters point inward; the core knows nothing about DuckDB, Quarkus or Micrometer (plan 2.2)")
            .check(framework)
    }

    @Test
    fun `api does not depend on spi, core or duckdb`() {
        noClasses().that().resideInAPackage("infra.snapshotcache.api..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "infra.snapshotcache.spi..",
                "infra.snapshotcache.core..",
                "infra.snapshotcache.duckdb..",
            )
            .because("the public surface is the innermost layer (plan 2.2)")
            .check(framework)
    }

    @Test
    fun `java sql is confined to api signatures, spi and duckdb`() {
        noClasses().that().resideInAPackage("infra.snapshotcache.core..")
            .should().dependOnClassesThat().resideInAPackage("java.sql..")
            .because("java.sql is permitted only in api signatures, spi and duckdb (plan 2.2)")
            .check(framework)
    }

    @Test
    fun `nothing outside core reaches into core`() {
        noClasses().that().resideOutsideOfPackage("infra.snapshotcache.core..")
            .should().dependOnClassesThat().resideInAPackage("infra.snapshotcache.core..")
            .because("core internals are reached only through api and spi (plan 2.2)")
            .check(framework)
    }
}
