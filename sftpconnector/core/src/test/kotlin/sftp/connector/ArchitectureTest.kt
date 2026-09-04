package sftp.connector

import com.tngtech.archunit.base.DescribedPredicate
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck

/**
 * The boundaries the connector cannot be allowed to lose, checked by the build rather than by
 * review. They run from the first ticket onward, because a boundary is only cheap to keep while
 * nothing has crossed it yet.
 */
class ArchitectureTest {

    private val core: JavaClasses = ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("sftp.connector")

    /**
     * A rule whose subject was never imported passes without checking anything, which reads
     * like enforcement and is worse than no rule at all. This is what proves the two below have
     * something to look at.
     */
    @Test
    fun `the rules below have the connector's classes to check`() {
        assertThat(core.map { it.name })
            .contains(
                "sftp.connector.transport.jsch.JschTransport",
                "sftp.connector.config.SftpConnectorBuilder",
                "sftp.connector.error.PoolExhausted",
            )
    }

    @Test
    fun `no application framework reaches into the connector`() {
        noClasses().that().resideInAPackage("sftp.connector..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "io.quarkus..",
                "jakarta.enterprise..",
                "jakarta.inject..",
            )
            .because(
                "the connector has to outlive whichever framework hosts it, so the framework " +
                    "is named in an adapter module and never here",
            )
            .check(core)
    }

    @Test
    fun `JSch stays inside the adapter built around it`() {
        noClasses().that().resideOutsideOfPackage("sftp.connector.transport.jsch..")
            .should().dependOnClassesThat().resideInAPackage("com.jcraft..")
            .because(
                "the pool, the client and the source hold sessions they cannot inspect; letting " +
                    "a JSch type past the transport seam would make replacing the SSH library a " +
                    "rewrite of all three",
            )
            .check(core)
    }

    @Test
    fun `the DSL names the readiness checks it configures and nothing else beneath it`() {
        classes().that().resideInAPackage("$CONFIG..")
            .should().onlyDependOnClassesThat(whatTheDslMayName)
            .because(
                "configuration is what everything else is built from, so a knob that reaches " +
                    "down into the client or the pool to say what it means makes those two " +
                    "unreadable without each other. The readiness checks are the exception " +
                    "because choosing which of them runs is the point of the polling block",
            )
            .check(core)
    }

    @Test
    fun `the failure vocabulary depends on nothing, so every layer is free to name it`() {
        noClasses().that().resideInAPackage("$ERROR..")
            .should().dependOnClassesThat(anywhereElseInTheConnector)
            .because(
                "every layer raises these classes and so imports this package; the moment it " +
                    "imports one back, the two can only be read together, and the failure a " +
                    "caller catches drags a pool or a transport type into its signature",
            )
            .check(core)
    }

    /**
     * Anything outside the connector, anything inside the package itself, the failure vocabulary
     * - which by the rule above owes nothing to anyone - and the readiness checks.
     */
    private val whatTheDslMayName = object : DescribedPredicate<JavaClass>(
        "outside the connector, inside $CONFIG, in $ERROR, or a readiness check",
    ) {
        override fun test(candidate: JavaClass): Boolean =
            !candidate.isUnder(CONNECTOR) ||
                candidate.isUnder(CONFIG) ||
                candidate.isUnder(ERROR) ||
                candidate.isAssignableTo(ReadinessCheck::class.java) ||
                candidate.isAssignableTo(Readiness::class.java) ||
                // The `+` that composes two checks is a top-level function, so it answers to the
                // name the compiler gives its file rather than to any of the types above.
                candidate.name == "${ReadinessCheck::class.java.packageName}.ReadinessKt"
    }

    private val anywhereElseInTheConnector = object : DescribedPredicate<JavaClass>(
        "inside the connector but outside $ERROR",
    ) {
        override fun test(candidate: JavaClass): Boolean = candidate.isUnder(CONNECTOR) && !candidate.isUnder(ERROR)
    }

    /** The package itself or one below it, and not a package that merely starts with its name. */
    private fun JavaClass.isUnder(pkg: String) = packageName == pkg || packageName.startsWith("$pkg.")

    private companion object {
        private const val CONNECTOR = "sftp.connector"
        private const val CONFIG = "$CONNECTOR.config"
        private const val ERROR = "$CONNECTOR.error"
    }
}
