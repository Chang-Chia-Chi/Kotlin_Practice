package infra.snapshotcache.testkit

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.ExtensionContext

/**
 * Shared fixture asserting the four accounting equations, plus the unclosed-connection
 * check, automatically at the end of every test. Register with
 * `@RegisterExtension`, or call [verify] directly.
 *
 * The equations, FIXED verbatim:
 *
 *     count(createCandidate) == count(promote) + count(delete of candidates)
 *     per generation: count(open) == count(close)      // except still-live ones
 *     at test end: opened generations == { current } U { gens with refcount > 0 }
 *     at test end: generations on disk == opened generations
 *
 * "current" and "refcount" are registry-side facts the store cannot know; tests that end
 * with generations legitimately open register the [currentGeneration] / [refCounts]
 * suppliers (how the fixture learns them is FREE per the plan). The defaults mean
 * "no registry in play": nothing may remain opened or on disk.
 *
 * Every violation names the exact generation and operation that leaked.
 */
class AccountingFixture(
    val store: InMemoryGenerationStore = InMemoryGenerationStore(),
) : AfterEachCallback {

    var currentGeneration: () -> Long? = { null }
    var refCounts: () -> Map<Long, Int> = { emptyMap() }

    override fun afterEach(context: ExtensionContext?) = verify()

    fun verify() {
        // Scripted-failure calls did not mutate state; the equations count effects.
        val ok = store.calls().filterNot { it.failed }
        val failures = mutableListOf<String>()

        // Equation 1: count(createCandidate) == count(promote) + count(delete of candidates)
        val created = ok.count { it.op == StoreOp.CREATE_CANDIDATE }
        val promoted = ok.count { it.op == StoreOp.PROMOTE }
        val candidateDeletes = ok.count { it.op == StoreOp.DELETE && it.detail == "candidate" }
        if (created != promoted + candidateDeletes) {
            val leaked = ok.gens(StoreOp.CREATE_CANDIDATE) -
                ok.gens(StoreOp.PROMOTE) -
                ok.filter { it.op == StoreOp.DELETE && it.detail == "candidate" }.mapNotNull { it.gen }.toSet()
            failures += "equation 1: createCandidate x$created != promote x$promoted + candidate deletes x$candidateDeletes" +
                "; createCandidate leaked for generations $leaked (never promoted, never deleted)"
        }

        // Equation 2: per generation count(open) == count(close), except still-live ones
        val stillOpened = store.openedGenerations()
        for (g in ok.mapNotNull { it.gen }.toSortedSet()) {
            if (g in stillOpened) continue
            val opens = ok.count { it.op == StoreOp.OPEN && it.gen == g }
            val closes = ok.count { it.op == StoreOp.CLOSE && it.gen == g }
            if (opens != closes) {
                failures += "equation 2: generation $g: open x$opens != close x$closes; close leaked for generation $g"
            }
        }

        // Equation 3: at test end, opened generations == { current } U { gens with refcount > 0 }
        val expectedOpen = buildSet {
            currentGeneration()?.let { add(it) }
            addAll(refCounts().filterValues { it > 0 }.keys)
        }
        if (stillOpened != expectedOpen) {
            val extra = stillOpened - expectedOpen
            val missing = expectedOpen - stillOpened
            failures += "equation 3: opened $stillOpened != current-or-leased $expectedOpen" +
                extra.ifNotEmpty { "; close leaked for generations $it (opened but neither current nor leased)" } +
                missing.ifNotEmpty { "; generations $it are current/leased but were never opened" }
        }

        // Equation 4: at test end, generations on disk == opened generations
        val onDisk = store.generationsOnDisk()
        if (onDisk != stillOpened) {
            val extra = onDisk - stillOpened
            val missing = stillOpened - onDisk
            failures += "equation 4: on disk $onDisk != opened $stillOpened" +
                extra.ifNotEmpty { "; delete leaked for generations $it (file on disk, not opened)" } +
                missing.ifNotEmpty { "; generations $it opened but their file is gone (deleted while opened)" }
        }

        // The JVM-side leak detector: every issued connection must have been closed.
        for (leak in store.tracker.unclosed()) {
            failures += "unclosed connection [${leak.label}], created at:\n" +
                leak.creationStack.stackTraceToString().trimEnd()
        }

        if (failures.isNotEmpty()) {
            throw AssertionError("Accounting violations (spec 17.3 / 17.6):\n" + failures.joinToString("\n"))
        }
    }

    private fun List<StoreCall>.gens(op: StoreOp): Set<Long> =
        filter { it.op == op }.mapNotNull { it.gen }.toSet()

    private fun Set<Long>.ifNotEmpty(message: (Set<Long>) -> String): String =
        if (isEmpty()) "" else message(this)
}
