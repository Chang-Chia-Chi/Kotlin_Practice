package infra.etl

import infra.etl.task.VariableScope
import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `VariableScope` on its own, because the plan names it as P5 public surface and a public type
 * that no test touches is how P3 shipped a real defect behind 134 green tests.
 *
 * What this type promises is narrower than spec 6 as a whole. It holds names and values and
 * refuses a second definition; it does not know what a built-in is, does not read a database,
 * and does **not** reject an unknown name on read - [get] answers null, and [contains] is how a
 * caller tells "defined as null" from "never defined". Enforcing spec 6.2's use-before-export
 * rule is therefore the engine's job at bind time, not this class's, and it is asserted there in
 * [TaskEngineVariableTest].
 *
 * That split is worth pinning down, because the tempting simplification - make `get` throw - is
 * what would break the zero-row export of spec 6.3, where null is a legitimate value.
 */
class VariableScopeTest {

    @Test
    fun aDefinedValueIsReadableAndReportedAsPresent() {
        val scope = VariableScope()

        scope.define("lastTs", "2026-08-01")

        assertThat(scope["lastTs"]).isEqualTo("2026-08-01")
        assertThat(scope.contains("lastTs")).isTrue()
        assertThat(scope.names).contains("lastTs")
    }

    /**
     * Any canonical value, not only strings: spec 6.1's `triggerTime` is an `Instant` and an
     * export of a numeric watermark is a `BigDecimal` or a `Long` (spec 4.1).
     */
    @Test
    fun aValueKeepsItsType() {
        val scope = VariableScope()
        val triggerTime = Instant.parse("2026-08-27T03:00:00Z")

        scope.define("triggerTime", triggerTime)
        scope.define("lastLot", 42L)

        assertThat(scope["triggerTime"]).isEqualTo(triggerTime)
        assertThat(scope["lastLot"]).isEqualTo(42L)
    }

    /**
     * Spec 6.3's zero-row export yields null, so null is a value the scope must be able to hold -
     * and it must stay distinguishable from a name that was never defined, or the redefinition
     * rule cannot be enforced for exactly the variable most likely to be null on a first run.
     */
    @Test
    fun anExportedNullIsAValueAndNotAnAbsence() {
        val scope = VariableScope()

        scope.define("lastTs", null)

        assertThat(scope["lastTs"]).isNull()
        assertThat(scope.contains("lastTs")).describedAs("defined as null is still defined").isTrue()
        assertThat(scope["neverExported"]).isNull()
        assertThat(scope.contains("neverExported")).isFalse()
    }

    /**
     * Spec 6.2: a variable may not be redefined once set. The scope has one namespace, so this
     * covers an export overwriting an earlier export, a literal var, or a built-in alike - and
     * the original value has to survive the rejection, or a failed task file would leave a run
     * reading a half-applied scope.
     */
    @Test
    fun definingTheSameNameTwiceIsRejectedAndTheFirstValueSurvives() {
        val scope = VariableScope()
        scope.define("lastTs", "2026-08-01")

        assertThatThrownBy { scope.define("lastTs", "2026-08-02") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("lastTs")

        assertThat(scope["lastTs"]).isEqualTo("2026-08-01")
        assertThat(scope.names).describedAs("no ghost entry was added").containsExactly("lastTs")
    }

    /** A null value is still a definition, so redefining it is still rejected. */
    @Test
    fun aNameDefinedAsNullCannotBeRedefinedEither() {
        val scope = VariableScope()
        scope.define("lastTs", null)

        assertThatThrownBy { scope.define("lastTs", "2026-08-01") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("lastTs")
    }

    /** [VariableScope.names] is a view for diagnostics, not a handle on the scope's insides. */
    @Test
    fun namesCannotBeUsedToMutateTheScope() {
        val scope = VariableScope()
        scope.define("siteCode", "F12")

        @Suppress("UNCHECKED_CAST")
        val names = scope.names as MutableSet<String>
        assertThatThrownBy { names.add("smuggled") }.isInstanceOf(UnsupportedOperationException::class.java)
        assertThat(scope.contains("smuggled")).isFalse()
    }
}
