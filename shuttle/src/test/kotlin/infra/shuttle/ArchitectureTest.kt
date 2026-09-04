package infra.shuttle

import com.tngtech.archunit.core.domain.JavaClasses
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Plan 2.2, sentence by sentence, checked by the build from the first ticket onward. */
class ArchitectureTest {

    private val all: JavaClasses = ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("infra.shuttle")

    private val coreAllowed = arrayOf(
        "infra.shuttle.core..", "java..", "kotlin..", "kotlinx.coroutines..", "org.jetbrains.annotations..",
        "io.micrometer.core..", "org.jboss.logging..", "com.fasterxml.jackson.databind..", "com.fasterxml.jackson.core..",
    )

    /** A rule whose subject was never imported passes without checking anything. */
    @Test
    fun `the rules below have the module's classes to check`() {
        assertTrue(all.contain("infra.shuttle.core.StateStore"))
    }

    @Test
    fun `core depends on no other package of the module and on no technology`() {
        classes().that().resideInAPackage("infra.shuttle.core..")
            .should().onlyDependOnClassesThat().resideInAnyPackage(*coreAllowed)
            .because("the pipeline, the seams and the notifier outlive every adapter")
            .check(all)
    }

    @Test
    fun `each adapter depends on core and its own technology only`() {
        adapter("yaml", "YamlLoader", "com.fasterxml.jackson..")
        adapter("sftp", "SftpPollSource", "sftp.connector..")
        adapter("s3", "S3Target", "software.amazon.awssdk..")
        adapter("http", "HttpChannel", "java.net.http..")
        adapter("nats", "NatsChannel", "io.nats..")
        adapter("jdbi", "JdbiStateStore", "org.jdbi..", "java.sql..", "javax.sql..")
    }

    /** [subject] is a class the package must still hold: a renamed or emptied package would otherwise pass unchecked. */
    private fun adapter(name: String, subject: String, vararg technology: String) {
        assertTrue(all.contain("infra.shuttle.$name.$subject"), "the $name adapter's sentence has a subject")
        classes().that().resideInAPackage("infra.shuttle.$name..")
            .should().onlyDependOnClassesThat().resideInAnyPackage("infra.shuttle.$name..", *coreAllowed, *technology)
            .because("an adapter names one technology and core; nothing else")
            .check(all)
    }

    /** Spec 3.2: `quarkus` is the composition root; it may import everything above it and Quarkus, and nothing imports it. */
    @Test
    fun `quarkus is depended on by nothing and is the only package that imports Quarkus`() {
        assertTrue(all.contain("infra.shuttle.quarkus.ShuttleHost"), "the sentence has a subject")
        noClasses().that().resideOutsideOfPackage("infra.shuttle.quarkus..")
            .should().dependOnClassesThat().resideInAnyPackage("infra.shuttle.quarkus..", "io.quarkus..", "jakarta..")
            .check(all)
    }

    @Test
    fun `java sql and jdbi appear nowhere outside the jdbi package and the composition root`() {
        noClasses().that().resideOutsideOfPackages("infra.shuttle.jdbi..", "infra.shuttle.quarkus..")
            .should().dependOnClassesThat().resideInAnyPackage("java.sql..", "javax.sql..", "org.jdbi..")
            .allowEmptyShould(true)
            .check(all)
    }

    /** Spec 3.2: only `sftp` and `quarkus` import the connector. */
    @Test
    fun `the sftp connector appears nowhere outside the sftp package and the composition root`() {
        assertTrue(all.contain("infra.shuttle.sftp.SftpPollSource"), "the sentence above has a subject")
        noClasses().that().resideOutsideOfPackages("infra.shuttle.sftp..", "infra.shuttle.quarkus..")
            .should().dependOnClassesThat().resideInAnyPackage("sftp.connector..")
            .allowEmptyShould(true)
            .check(all)
    }

    @Test
    fun `jnats appears nowhere outside the nats package and the composition root`() {
        noClasses().that().resideOutsideOfPackages("infra.shuttle.nats..", "infra.shuttle.quarkus..")
            .should().dependOnClassesThat().resideInAnyPackage("io.nats..")
            .allowEmptyShould(true)
            .check(all)
    }

    @Test
    fun `logging is jboss logging directly and no context object carries a logger`() {
        noClasses().that().resideInAPackage("infra.shuttle..")
            .should().dependOnClassesThat().resideInAnyPackage("org.slf4j..", "java.util.logging..", "org.apache.logging..")
            .allowEmptyShould(true)
            .check(all)
        noFields().that().haveRawType("org.jboss.logging.Logger")
            .should().beDeclaredInClassesThat().haveSimpleNameEndingWith("Context")
            .because("D34: correlation is the MDC, not a logger handed around")
            .allowEmptyShould(true)
            .check(all)
        noClasses().that().haveSimpleNameEndingWith("Context")
            .should().dependOnClassesThat().haveFullyQualifiedName("org.jboss.logging.Logger")
            .allowEmptyShould(true)
            .check(all)
    }

    /** The composition root is the one place a clock is born; everything below it is handed one. */
    @Test
    fun `time is java time Clock injected`() {
        noClasses().that().resideInAPackage("infra.shuttle..").and().resideOutsideOfPackage("infra.shuttle.quarkus..")
            .should().callMethod(java.time.Instant::class.java, "now")
            .orShould().callMethod(java.time.Clock::class.java, "systemUTC")
            .orShould().callMethod(java.time.Clock::class.java, "systemDefaultZone")
            .orShould().callMethod(System::class.java, "currentTimeMillis")
            .because("a clock nobody injected cannot be advanced by a test")
            .allowEmptyShould(true)
            .check(all)
    }
}
