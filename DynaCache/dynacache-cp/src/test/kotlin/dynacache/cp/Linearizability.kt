package dynacache.cp

/**
 * A sequential specification: the single-threaded model a linearization must reproduce. [initial]
 * is the state before any operation; [apply] runs one operation, returning the new state and the
 * output the model produces for it.
 */
interface SequentialSpec<S, In, Out> {
    val initial: S
    fun apply(state: S, input: In): Pair<S, Out>
}

/**
 * One operation as it happened (CP spec 11, Herlihy & Wing): who ran it, when it was called and
 * when it returned, its input and its output. [ret] is [Long.MAX_VALUE] and [output] is null for an
 * operation that never got a definite answer (a timeout under chaos) - it may have taken effect or
 * not, so the checker is free to place it anywhere after its call or to leave it out.
 */
data class Op<In, Out>(
    val client: Int,
    val call: Long,
    val ret: Long,
    val input: In,
    val output: Out?,
)

/**
 * A small linearizability checker (CP spec 10.9, `invariant_linearizable_ops`): a Wing-Gong search
 * over the orders a history allows for one the sequential model accepts. Meant for histories of a
 * few dozen operations, the "simple case, no full Jepsen" the spec asks for.
 */
object Linearizability {

    /**
     * True when [ops] has a linearization: an order in which every operation appears to take effect
     * at one instant between its call and its return, respecting real time (an operation that
     * returned before another was called comes first) and reproducing every known output under
     * [spec]. An operation with an unknown output may take effect with any output or not at all.
     */
    fun <S, In, Out> check(ops: List<Op<out In, Out>>, spec: SequentialSpec<S, In, Out>): Boolean {
        // The frontier is the set of operations not yet linearized. An operation may go next only
        // when nothing still to be placed must precede it (its call is at or before the earliest
        // return among the rest). Memoized on (model state, frontier), so a state reached two ways
        // is explored once.
        val dead = HashSet<Pair<S, Set<Int>>>()
        fun search(state: S, frontier: Set<Int>): Boolean {
            if (frontier.isEmpty()) return true
            if (!dead.add(state to frontier)) return false
            val earliestReturn = frontier.minOf { ops[it].ret }
            for (i in frontier) {
                val op = ops[i]
                if (op.call > earliestReturn) continue
                val (next, output) = spec.apply(state, op.input)
                val known = op.output != null
                if ((!known || output == op.output) && search(next, frontier - i)) return true
                // An unknown operation may also simply never have happened.
                if (!known && search(state, frontier - i)) return true
            }
            return false
        }
        return search(spec.initial, ops.indices.toSet())
    }
}

/** A counter operation for the checker: the AtomicLong verbs a chaos run records on one key. */
sealed interface CounterOp {
    data class IncrBy(val delta: Long) : CounterOp
    data class GetAdd(val delta: Long) : CounterOp
    data class Cas(val expected: Long, val new: Long) : CounterOp
    data object Get : CounterOp
}

/** The AtomicLong sequential spec (CP spec 3.2): a counter from 0, CAS answering 1 on a match else 0. */
object CounterSpec : SequentialSpec<Long, CounterOp, Long> {
    override val initial = 0L
    override fun apply(state: Long, input: CounterOp): Pair<Long, Long> = when (input) {
        is CounterOp.IncrBy -> (state + input.delta).let { it to it }
        // GETADD answers the old value, so the model's output is the state it came in with.
        is CounterOp.GetAdd -> (state + input.delta) to state
        is CounterOp.Get -> state to state
        is CounterOp.Cas -> if (state == input.expected) input.new to 1L else state to 0L
    }
}
