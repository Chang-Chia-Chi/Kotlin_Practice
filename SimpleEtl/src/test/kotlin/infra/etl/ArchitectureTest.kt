package infra.etl

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

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

    // ------------------------------------------------------------------------------------
    // P8b, contract 5. The three rules that constrain the metrics binding. The existing
    // `adapters do not depend on task` rule names duckdb and jdbc literally and therefore says
    // nothing about micrometer, and micrometer is not like them: it implements `TaskMetrics`, a
    // seam defined in `task`, so it names `task` on purpose (SimpleEtl/CLAUDE.md).
    //
    // All three read a class graph imported with DO_NOT_INCLUDE_TESTS, which filters by *path*.
    // The label contract test lives in `package infra.etl.micrometer` under src/test and is
    // excluded from every rule here because of it - correct today, and worth stating: dropping
    // that import option would make the first rule fail against clean production code, because a
    // test may legitimately hold both a MeterRegistry and an engine.
    // ------------------------------------------------------------------------------------

    @Test
    fun `only the micrometer adapter depends on io micrometer`() {
        noClasses()
            .that().resideInAPackage("infra.etl..")
            .and().resideOutsideOfPackage("infra.etl.micrometer..")
            .should().dependOnClassesThat().resideInAPackage("io.micrometer..")
            .because(
                "micrometer-core is a `provided` dependency so that Layer 1 ships to the snapshot " +
                    "cache without it (spec 2.1); one import outside this package would put the " +
                    "whole module's compilation at the mercy of a host that has no MeterRegistry",
            )
            .check(classes)
    }

    @Test
    fun `the micrometer adapter is a leaf and nothing depends on it`() {
        noClasses()
            .that().resideInAPackage("infra.etl.micrometer..")
            .should().dependOnClassesThat()
            .resideInAnyPackage(
                "infra.etl.pipe..",
                "infra.etl.duckdb..",
                "infra.etl.jdbc..",
                "infra.snapshotcache..",
            )
            .because("an adapter depends only on the package defining the seam it implements - here, task")
            .check(classes)
        noClasses()
            .that().resideInAPackage("infra.etl..")
            .and().resideOutsideOfPackage("infra.etl.micrometer..")
            .should().dependOnClassesThat().resideInAPackage("infra.etl.micrometer..")
            .because("the engine talks to the TaskMetrics interface; only a host names the binding")
            .check(classes)
    }

    /**
     * The canary the other two rules need, and it is a plain assertion over [classes] on purpose.
     *
     * Both rules above are `noClasses().that()...` sentences, and a `noClasses` sentence whose
     * *selected* set is non-empty passes whenever nothing in it violates the `should` - which is
     * exactly what happens if the binding is never written, or is written in some other package.
     * `failOnEmptyShould` does not save them either: the selected set here (everything in
     * `infra.etl` outside `infra.etl.micrometer`) is non-empty whether or not the micrometer
     * package exists at all, so the option never fires. Collapsing this into another `that()`
     * clause would rest the whole confinement contract on a default configuration property; there
     * is no `archunit.properties` anywhere in this repo, and there should not need to be.
     */
    @Test
    fun `the micrometer adapter exists and is the thing those rules constrain`() {
        val adapter = classes.filter { it.packageName.startsWith("infra.etl.micrometer") }
        assertAll(
            {
                assertTrue(adapter.isNotEmpty()) {
                    "no production class resides in infra.etl.micrometer, so the two rules above " +
                        "pass vacuously; imported packages were " +
                        classes.map { it.packageName }.toSortedSet()
                }
            },
            {
                val binding = adapter.filter { candidate ->
                    candidate.directDependenciesFromSelf.any {
                        it.targetClass.packageName.startsWith("io.micrometer")
                    }
                }
                assertTrue(binding.isNotEmpty()) {
                    "infra.etl.micrometer exists but no class in it names io.micrometer, so the " +
                        "confinement rule is confining nothing; classes were " +
                        adapter.map { it.name }
                }
            },
        )
    }

    @Test
    fun `packages are free of cycles`() {
        SlicesRuleDefinition.slices()
            .matching("infra.etl.(*)..")
            .should().beFreeOfCycles()
            .because("a cycle makes the split unenforceable: every package would depend on every other")
            .check(classes)
    }

    // ------------------------------------------------------------------------------------
    // The non-suspend invariant CLAUDE.md's Concurrency idiom section records: no frame in
    // `run -> execute -> pipe -> ScratchDb.connection()` is `suspend`. That is what makes a
    // single DuckDB Connection safe under `synchronized` rather than `Mutex` - DuckDB's hazard
    // is a *thread* constraint, and a suspend fun can resume its continuation on a
    // different thread. A `suspend fun` compiles to an extra `kotlin.coroutines.Continuation`
    // parameter, so banning that parameter type bans `suspend` without needing bytecode-level
    // coroutine inspection. Both the duckdb leaf and the task-layer entry point are checked:
    // banning only the leaf would let a suspend `TaskEngine.execute` shuffle threads above it
    // and still reach `ScratchDb.connection()` from something other than the calling thread.
    // ------------------------------------------------------------------------------------

    @Test
    fun `no method in duckdb is suspend`() {
        noClasses()
            .that().resideInAPackage("infra.etl.duckdb..")
            .should().dependOnClassesThat().haveFullyQualifiedName("kotlin.coroutines.Continuation")
            .because(
                "a single DuckDB Connection used from two threads crashes the JVM (spec 7.2); a " +
                    "suspend fun's continuation can resume on another thread, so ScratchDb's " +
                    "synchronized guard is only sound if nothing in this package is suspend",
            )
            .check(classes)
    }

    /**
     * The canary the rule below needs, on the P8b micrometer block's own precedent: its
     * `noClasses().that().haveNameMatching(...)` selects by FQN regex, so a rename empties the
     * selection and the rule passes vacuously - held off today only by ArchUnit's
     * failOnEmptyShould default, which this repo pins nowhere. A plain assertion that the
     * selection is non-empty fails loudly on the rename instead (depth review, 2026-08-30).
     */
    @Test
    fun `the TaskEngine classes the suspend rule constrains actually exist`() {
        val matched = classes.filter { it.name.matches(Regex("""infra\.etl\.task\.TaskEngine(\$.*)?""")) }
        org.junit.jupiter.api.Assertions.assertTrue(matched.size >= 2) {
            "the non-suspend rule's FQN regex matched ${matched.size} classes - a rename has " +
                "emptied its selection and the rule is constraining nothing; matched: " +
                matched.map { it.name }
        }
    }

    @Test
    fun `TaskEngine is not suspend`() {
        // Matched by FQN prefix, not simple name: the crash path runs through the *inner* class
        // (`TaskEngine${'$'}Run.execute -> pipe -> ScratchDb.connection()`), whose simple name is
        // `Run`. A simple-name match would guard the shell and miss the executor.
        noClasses()
            .that().haveNameMatching("""infra\.etl\.task\.TaskEngine(\$.*)?""")
            .should().dependOnClassesThat().haveFullyQualifiedName("kotlin.coroutines.Continuation")
            .because(
                "TaskEngine.run is frozen as an ordinary function (spec 11.2); the crash path to " +
                    "ScratchDb.connection() runs through the engine, so a suspend TaskEngine could " +
                    "reach it from a different thread even with the duckdb package itself clean",
            )
            .check(classes)
    }
}
