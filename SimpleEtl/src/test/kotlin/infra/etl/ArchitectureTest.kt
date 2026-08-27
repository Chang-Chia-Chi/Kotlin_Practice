package infra.etl

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.junit.jupiter.api.Test

/**
 * The package split is a contract, not a filing scheme. Each rule below is the machine-readable
 * form of a boundary the design depends on; without them the split degrades to convention and
 * the first import that crosses a layer goes unnoticed.
 */
class ArchitectureTest {

    private val classes: JavaClasses =
        ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("infra.etl")

    @Test
    fun `pipe does not depend on task`() {
        noClasses()
            .that().resideInAPackage("infra.etl.pipe..")
            .should().dependOnClassesThat().resideInAPackage("infra.etl.task..")
            .because("Layer 1 ships to the snapshot cache without the task engine (spec 2.1)")
            .check(classes)
    }

    @Test
    fun `pipe does not depend on its adapters`() {
        noClasses()
            .that().resideInAPackage("infra.etl.pipe..")
            .should().dependOnClassesThat().resideInAnyPackage("infra.etl.duckdb..", "infra.etl.jdbc..")
            .because("Layer 1 defines the RowWriter seam; adapters implement it, not the reverse")
            .check(classes)
    }

    @Test
    fun `only the duckdb adapter depends on org duckdb`() {
        noClasses()
            .that().resideInAPackage("infra.etl..")
            .and().resideOutsideOfPackage("infra.etl.duckdb..")
            .should().dependOnClassesThat().resideInAPackage("org.duckdb..")
            .because("one adapter, named for its technology")
            .check(classes)
    }

    @Test
    fun `adapters do not depend on task`() {
        noClasses()
            .that().resideInAnyPackage("infra.etl.duckdb..", "infra.etl.jdbc..")
            .should().dependOnClassesThat().resideInAPackage("infra.etl.task..")
            .because("adapters are leaves")
            .check(classes)
    }

    @Test
    fun `the adapters do not depend on each other`() {
        noClasses()
            .that().resideInAPackage("infra.etl.jdbc..")
            .should().dependOnClassesThat().resideInAPackage("infra.etl.duckdb..")
            .because("two independent adapters; a JDBC target must not drag DuckDB in behind it")
            .check(classes)
        noClasses()
            .that().resideInAPackage("infra.etl.duckdb..")
            .should().dependOnClassesThat().resideInAPackage("infra.etl.jdbc..")
            .because("two independent adapters, in both directions")
            .check(classes)
    }

    @Test
    fun `only task may depend on the snapshot cache`() {
        noClasses()
            .that().resideInAPackage("infra.etl..")
            .and().resideOutsideOfPackage("infra.etl.task..")
            .should().dependOnClassesThat().resideInAPackage("infra.snapshotcache..")
            .because(
                "spec 7.3's cache read step is a Layer 2 concern; Layer 1 ships to the cache " +
                    "and must not depend on it in return (spec 9.5)",
            )
            .check(classes)
    }

    @Test
    fun `packages are free of cycles`() {
        SlicesRuleDefinition.slices()
            .matching("infra.etl.(*)..")
            .should().beFreeOfCycles()
            .because("a cycle makes the split unenforceable: every package would depend on every other")
            .check(classes)
    }
}
