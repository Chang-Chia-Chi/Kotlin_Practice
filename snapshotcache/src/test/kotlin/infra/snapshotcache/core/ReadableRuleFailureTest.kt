package infra.snapshotcache.core

import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.spi.GenerationStore
import infra.snapshotcache.spi.OpenGeneration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.sql.Connection

/**
 * P5 rider (assigned by the P4b progress note): the `readable` rule's failure path - the
 * one verify-rule row previously unasserted. The rule's observable: "a candidate that cannot
 * be reopened and queried never publishes."
 *
 * What production actually does (pinned here): [infra.snapshotcache.spi.VerifyGate]
 * catches both failure shapes - the verify connection failing to open, and the opened
 * connection failing its first query - and classifies them as a `readable` rule failure,
 * so the round ends [RefreshResult.VERIFY_FAILED], NOT `disk_error`. That is consistent
 * with the rule's own definition (`readable` = "candidate file can be reopened and queried").
 * A store-level
 * open() failure, by contrast, classifies as disk_error before the gate runs (P4b note).
 */
internal class ReadableRuleFailureTest : RefreshCycleTestBase() {

    private val refusingStore = VerifyConnectionRefusingStore(stubStore)

    private val riderCycle = RefreshCycle(
        group = group,
        registry = registry,
        store = refusingStore,
        source = source,
        config = config,
        events = events,
        clock = clock,
        hooks = hooks,
    )

    @Test
    fun readableRule_rejectsCandidate_whenVerifyConnectionCannotBeObtained() {
        val gen1 = runSuccess(riderCycle)
        refusingStore.mode = VerifyConnectionRefusingStore.Mode.REFUSE_CONNECTION

        val rejected = riderCycle.runOnce()

        assertThat(rejected.result)
            .describedAs("an unobtainable verify connection is a verify failure, got %s (%s)", rejected.result, rejected.detail)
            .isEqualTo(RefreshResult.VERIFY_FAILED)
        val (rule, detail) = events.verifyFailures.single()
        assertThat(rule).describedAs("spec 8.1 rule label, verbatim").isEqualTo("readable")
        assertThat(detail).describedAs("spec 8.5: never just 'verification failed'").isNotBlank()
        assertThat(registry.current())
            .describedAs("a candidate that cannot be reopened never publishes (spec 8.1)")
            .isEqualTo(gen1)
        assertThat(store.generationsOnDisk())
            .describedAs("the rejected candidate is cleaned up (I7)")
            .containsExactly(gen1)

        refusingStore.mode = VerifyConnectionRefusingStore.Mode.NONE
        runSuccess(riderCycle) // return to a usable state
    }

    @Test
    fun readableRule_rejectsCandidate_whenVerifyConnectionCannotBeQueried() {
        val gen1 = runSuccess(riderCycle)
        refusingStore.mode = VerifyConnectionRefusingStore.Mode.REFUSE_QUERY

        val rejected = riderCycle.runOnce()

        assertThat(rejected.result)
            .describedAs("an unqueryable candidate is a verify failure, got %s (%s)", rejected.result, rejected.detail)
            .isEqualTo(RefreshResult.VERIFY_FAILED)
        val (rule, detail) = events.verifyFailures.single()
        assertThat(rule).describedAs("spec 8.1 rule label, verbatim").isEqualTo("readable")
        assertThat(detail).isNotBlank()
        assertThat(registry.current())
            .describedAs("a candidate that cannot be queried never publishes (spec 8.1)")
            .isEqualTo(gen1)
        assertThat(store.generationsOnDisk()).containsExactly(gen1)

        refusingStore.mode = VerifyConnectionRefusingStore.Mode.NONE
        runSuccess(riderCycle)
    }
}

/**
 * Delegating store whose opened generations can refuse the verify connection: [open] itself
 * succeeds (a store-level open failure is disk_error territory, out of the readable rule's
 * scope), but the returned [OpenGeneration.connection] either throws or yields a connection
 * that fails every query. close/isClosed still reach the tracked delegate connection, so the
 * JVM-side leak detector keeps seeing every connection.
 */
internal class VerifyConnectionRefusingStore(
    private val delegate: GenerationStore,
) : GenerationStore by delegate {

    enum class Mode { NONE, REFUSE_CONNECTION, REFUSE_QUERY }

    @Volatile
    var mode: Mode = Mode.NONE

    override fun open(gen: Long): OpenGeneration {
        val real = delegate.open(gen)
        return object : OpenGeneration {
            override val generation: Long = real.generation

            override fun connection(): Connection = when (mode) {
                Mode.NONE -> real.connection()
                Mode.REFUSE_CONNECTION ->
                    throw IllegalStateException("scripted: verify connection refused for gen $generation")
                Mode.REFUSE_QUERY -> queryRefusing(real.connection())
            }

            override fun fileBytes(): Long = real.fileBytes()
        }
    }

    /** A connection that closes normally but refuses every query. */
    private fun queryRefusing(real: Connection): Connection =
        Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Connection::class.java)) { proxy, method, args ->
            when (method.name) {
                "close", "isClosed" -> method.invoke(real, *(args ?: emptyArray()))
                "toString" -> "QueryRefusing($real)"
                "hashCode" -> System.identityHashCode(proxy)
                "equals" -> proxy === args!![0]
                else -> throw IllegalStateException("scripted: verify connection cannot be queried (${method.name})")
            }
        } as Connection
}
