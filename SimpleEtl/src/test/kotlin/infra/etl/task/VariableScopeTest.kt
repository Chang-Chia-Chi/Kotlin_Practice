package infra.etl.task

import infra.etl.task.VariableScope
import java.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows

/**
 * `VariableScope` on its own, because the plan names it as P5 public surface and a public type
 * that no test touches is how P3 shipped a real defect behind 134 green tests.
 *
 * What this type promises is narrower than the variable rules as a whole. It holds names and
 * values and refuses a second definition; it does not know what a built-in is, does not read a
 * database, and does **not** reject an unknown name on read - [get] answers null, and [contains]
 * is how a caller tells "defined as null" from "never defined". Enforcing the use-before-export
 * rule is therefore the engine's job at bind time, not this class's, and it is asserted there in
 * [TaskEngineVariableTest].
 *
 * That split is worth pinning down, because the tempting simplification - make `get` throw - is
 * what would break a zero-row export, where null is a legitimate value.
 */
class VariableScopeTest {

    @Test
    fun aDefinedValueIsReadableAndReportedAsPresent() {
        val scope = VariableScope()

        scope.define("lastTs", "2026-08-01")

        assertAll(
            { assertEquals("2026-08-01", scope["lastTs"]) },
            { assertTrue(scope.contains("lastTs")) { "contains said false; names were ${scope.names}" } },
            { assertTrue("lastTs" in scope.names) { "names were ${scope.names}" } },
        )
    }

    /**
     * Any canonical value, not only strings: the `triggerTime` built-in is an `Instant` and an
     * export of a numeric watermark is a `BigDecimal` or a `Long`.
     */
    @Test
    fun aValueKeepsItsType() {
        val scope = VariableScope()
        val triggerTime = Instant.parse("2026-08-27T03:00:00Z")

        scope.define("triggerTime", triggerTime)
        scope.define("lastLot", 42L)

        assertAll(
            { assertEquals(triggerTime, scope["triggerTime"]) },
            { assertEquals(42L, scope["lastLot"]) },
        )
    }

    /**
     * A zero-row export yields null, so null is a value the scope must be able to hold -
     * and it must stay distinguishable from a name that was never defined, or the redefinition
     * rule cannot be enforced for exactly the variable most likely to be null on a first run.
     */
    @Test
    fun anExportedNullIsAValueAndNotAnAbsence() {
        val scope = VariableScope()

        scope.define("lastTs", null)

        assertAll(
            { assertNull(scope["lastTs"]) { "lastTs was defined as null, but read back as ${scope["lastTs"]}" } },
            {
                assertTrue(scope.contains("lastTs")) {
                    "defined as null is still defined; names were ${scope.names}"
                }
            },
            {
                assertNull(scope["neverExported"]) {
                    "neverExported was never defined, but read back as ${scope["neverExported"]}"
                }
            },
            {
                assertFalse(scope.contains("neverExported")) {
                    "neverExported was never defined; names were ${scope.names}"
                }
            },
        )
    }

    /**
     * A variable may not be redefined once set. The scope has one namespace, so this
     * covers an export overwriting an earlier export, a literal var, or a built-in alike - and
     * the original value has to survive the rejection, or a failed task file would leave a run
     * reading a half-applied scope.
     */
    @Test
    fun definingTheSameNameTwiceIsRejectedAndTheFirstValueSurvives() {
        val scope = VariableScope()
        scope.define("lastTs", "2026-08-01")

        val rejected = assertThrows<IllegalArgumentException> { scope.define("lastTs", "2026-08-02") }
        assertTrue(rejected.message?.contains("lastTs") == true) { "message was: ${rejected.message}" }

        assertEquals("2026-08-01", scope["lastTs"])
        assertEquals(setOf("lastTs"), scope.names) { "no ghost entry was added" }
    }

    /** A null value is still a definition, so redefining it is still rejected. */
    @Test
    fun aNameDefinedAsNullCannotBeRedefinedEither() {
        val scope = VariableScope()
        scope.define("lastTs", null)

        val rejected = assertThrows<IllegalArgumentException> { scope.define("lastTs", "2026-08-01") }
        assertTrue(rejected.message?.contains("lastTs") == true) { "message was: ${rejected.message}" }
    }

    /** [VariableScope.names] is a view for diagnostics, not a handle on the scope's insides. */
    @Test
    fun namesCannotBeUsedToMutateTheScope() {
        val scope = VariableScope()
        scope.define("siteCode", "F12")

        @Suppress("UNCHECKED_CAST")
        val names = scope.names as MutableSet<String>
        assertThrows<UnsupportedOperationException> { names.add("smuggled") }
        assertFalse(scope.contains("smuggled")) { "the scope was mutated; names were ${scope.names}" }
    }
}
