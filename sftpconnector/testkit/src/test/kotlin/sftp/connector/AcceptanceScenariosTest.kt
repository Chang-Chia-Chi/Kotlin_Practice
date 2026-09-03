package sftp.connector

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.platform.suite.api.SelectMethod
import org.junit.platform.suite.api.Suite
import sftp.connector.testkit.JschErrorMappingTest
import sftp.connector.testkit.JschTransportTest

/**
 * The acceptance suite is a list of names, and a list of names can rot: a scenario renamed, a
 * method that was never a test, an ID picked twice or not at all. This is what keeps the list
 * honest on every build, since the suite itself runs only when asked for by name.
 */
class AcceptanceScenariosTest {

    private val selected = AcceptanceScenarios::class.java.getAnnotationsByType(SelectMethod::class.java)

    @Test
    fun `the suite is a suite, and every method it selects exists and is a test`() {
        assertThat(AcceptanceScenarios::class.java.isAnnotationPresent(Suite::class.java)).isTrue()
        selected.forEach { selection ->
            val method = selection.type.java.methods.singleOrNull { it.name == selection.name }
            assertThat(method).describedAs("${selection.type.simpleName}.${selection.name}").isNotNull()
            assertThat(method!!.isAnnotationPresent(Test::class.java)).describedAs("$method is a test").isTrue()
        }
    }

    @Test
    fun `the suite covers every scenario from S1 to S12 exactly once, each named by its ID`() {
        val ids = selected.map { selection ->
            SCENARIO_ID.find(selection.name)?.groupValues?.get(1)?.toInt()
                ?: error("${selection.name} is not named by a scenario ID")
        }

        assertThat(ids).containsExactlyInAnyOrderElementsOf(1..12)
    }

    /**
     * S2 and S10 are also proven at the transport layer, where what is asserted is what the
     * adapter raises. A scenario is about what the connector does with that, so those proofs
     * are not the ones the suite may count.
     */
    @Test
    fun `no scenario is counted at the transport layer`() {
        assertThat(selected.map { it.type }).doesNotContain(JschErrorMappingTest::class, JschTransportTest::class)
    }

    private companion object {
        private val SCENARIO_ID = Regex("^S(\\d+)_")
    }
}
