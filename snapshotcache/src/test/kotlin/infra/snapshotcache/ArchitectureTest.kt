package infra.snapshotcache

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.junit.jupiter.api.Test

/**
 * The boundary rules of plan 2.2 - its original five plus the bootstrap leaf rule its
 * 2026-08-30 amendment added - the two archive-layer rules of plan 3c, and D33's ban on a
 * bucket listing. These are the module's static-analysis gate; they run on every build from
 * P0 onward rather than being retrofitted later.
 */
class ArchitectureTest {

    /**
     * Both package trees, not just the framework's. `infra.snapshotarchive` has to be
     * imported for the plan 3c rules below to see anything at all - a rule whose subject
     * package was never imported passes vacuously, which is worse than no rule because it
     * reads as enforcement.
     */
    private val framework: JavaClasses = ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("infra.snapshotcache", "infra.snapshotarchive")

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

    /**
     * Plan 3c, first archive rule. D30 puts the archive layer outside the framework
     * precisely so the framework's five-interface budget and its D10/D22/D24 decisions stay
     * untouched; a single edge in this direction would undo that silently.
     */
    @Test
    fun `the framework does not depend on the archive layer`() {
        noClasses().that().resideInAPackage("infra.snapshotcache..")
            .should().dependOnClassesThat().resideInAPackage("infra.snapshotarchive..")
            .because("the archive layer is a consumer of the framework, never part of it (D30, plan 3c)")
            .check(framework)
    }

    /**
     * Plan 3c, second archive rule. The archive layer is a consumer like any other, so it
     * reaches the framework through `api` only. Without this, it could bind itself to
     * internals that carry no compatibility promise - and the DuckDB adapter in particular
     * is pinned to 1.1.3 for a CI constraint the archive layer does not share.
     */
    @Test
    fun `the archive layer reaches the framework through api only`() {
        noClasses().that().resideInAPackage("infra.snapshotarchive..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "infra.snapshotcache.spi..",
                "infra.snapshotcache.core..",
                "infra.snapshotcache.duckdb..",
            )
            .because("the archive layer consumes the public API only (D30, plan 3c)")
            .check(framework)
    }

    /**
     * D33's negative space, and the one rule here that guards an absence rather than a
     * boundary.
     *
     * The publish protocol commits a manifest row carrying the complete inventory before the
     * first object is uploaded, which makes an object without a covering row impossible to
     * create. Everything downstream - the watchdog's verification, the purge's reclaim -
     * therefore reads the inventory, never the bucket. A LIST call is how that guarantee
     * rots: a sweep looks like defence in depth, but it is a second, weaker source of truth
     * that the ordering would slowly be trusted to instead of itself, and it cannot tell a
     * genuinely orphaned object from one whose upload is still in flight. Listing is
     * unreachable without naming its argument type, so naming it fails the build.
     */
    @Test
    fun `no LIST-based orphan sweep exists`() {
        noClasses().should().dependOnClassesThat().haveNameMatching("io\\.minio\\.ListObjectsArgs.*")
            .because(
                "a dangling object is impossible by construction (D33), so it is asserted, never " +
                    "scanned for; the inventory is the only list of what a version contains",
            )
            .check(framework)
    }

    /**
     * Plan 2.2's rule, with the one named exception the 2026-08-30 amendment added.
     * `bootstrap` is the composition root: it is the single place the object graph is
     * assembled, and assembling it means naming the `internal` classes that implement the
     * `api` interfaces. Everything else still reaches `core` only through `api` and `spi`.
     */
    @Test
    fun `nothing outside core reaches into core, except the bootstrap composition root`() {
        noClasses().that().resideOutsideOfPackage("infra.snapshotcache.core..")
            .and().resideOutsideOfPackage("infra.snapshotcache.bootstrap..")
            .should().dependOnClassesThat().resideInAPackage("infra.snapshotcache.core..")
            .because(
                "core internals are reached only through api and spi, and only the bootstrap " +
                    "composition root is excepted (plan 2.2, amended 2026-08-30)",
            )
            .check(framework)
    }

    /**
     * The other half of the amendment, and the reason the exception above costs nothing.
     * A composition root that something depends on is no longer a composition root - it has
     * become a layer, and the inward dependency it is allowed to carry would then be a path
     * from anywhere to `core`. Stating the leaf property positively is what keeps the
     * exception one edge wide instead of a hole.
     */
    @Test
    fun `nothing depends on the bootstrap composition root`() {
        noClasses().that().resideOutsideOfPackage("infra.snapshotcache.bootstrap..")
            .should().dependOnClassesThat().resideInAPackage("infra.snapshotcache.bootstrap..")
            .because(
                "bootstrap assembles the object graph and nothing assembles it; it is a leaf, " +
                    "so its permission to reach core and duckdb reaches no further (plan 2.2, amended 2026-08-30)",
            )
            .check(framework)
    }
}
