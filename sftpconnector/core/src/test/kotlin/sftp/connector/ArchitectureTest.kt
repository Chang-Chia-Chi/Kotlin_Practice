package sftp.connector

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The two boundaries the connector cannot be allowed to lose, checked by the build rather than
 * by review. They run from the first ticket onward, because a boundary is only cheap to keep
 * while nothing has crossed it yet.
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
}
