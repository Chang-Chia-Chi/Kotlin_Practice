package dynacache.cluster

import dynacache.engine.Value
import dynacache.engine.ds.SkipList
import dynacache.engine.fieldBytes

/** One key's value with the [Dvv] that version carries: what replicas exchange and what [merge] reconciles. */
class Versioned(val value: Value, val dvv: Dvv)

/**
 * Spec 5.3: the version to keep when [local] and [remote] hold one key. A dominated side is
 * discarded, equal versions keep [local], and concurrent versions combine by their type's rule
 * (spec 2.5) under a DVV descending from both, with one fresh dot from [counter].
 *
 * Pure: neither input is touched, and the caller writes the result through the engine. The
 * last writer of two concurrent versions is the one with the higher dot, node name first.
 */
fun merge(local: Versioned, remote: Versioned, counter: DotCounter): Versioned = when {
    remote.dvv.dominates(local.dvv) -> remote
    !local.dvv.isConcurrent(remote.dvv) -> local
    else -> {
        val dvv = local.dvv.merge(remote.dvv, counter)
        Versioned(combine(minOf(local, remote, byDot).value, maxOf(local, remote, byDot).value, dvv.dot.counter), dvv)
    }
}

/** Spec 2.5's tiebreak between concurrent versions, made total: the higher dot, node name first. */
internal val lastWriter: Comparator<Dvv> = compareBy({ it.dot.node }, { it.dot.counter })

private val byDot = compareBy(lastWriter, Versioned::dvv)

/**
 * The table of spec 2.5 for two concurrent values, [later] being the last writer's. A string,
 * or two values of different kinds, is the last writer's outright. [seed] levels a new skip list.
 */
private fun combine(earlier: Value, later: Value, seed: Long): Value = when {
    earlier is Value.Hash && later is Value.Hash -> union(earlier, later)
    earlier is Value.List && later is Value.List -> union(earlier, later)
    earlier is Value.ZSet && later is Value.ZSet -> union(earlier, later, seed)
    else -> later
}

/**
 * Field-level last-writer-wins: every field of either side survives, and a field both wrote
 * takes the last writer's bytes. The one DVV is the whole hash's, so "last" is decided per
 * value and applied per field; a concurrent `HDEL` is undone by the side still holding the field.
 */
private fun union(earlier: Value.Hash, later: Value.Hash): Value.Hash {
    val merged = Value.Hash()
    for (hash in listOf(earlier, later)) for ((name, bytes) in hash.fields.entries()) merged.fields.put(name, bytes)
    return merged
}

/**
 * Concurrent appends: the shared prefix, then what each side pushed after it, the last writer's
 * push last. When one side is a prefix of the other, somebody popped, and the pop is
 * last-writer-wins: the later side stands as it is. Without the common ancestor, two sides
 * that share no prefix at all look like two appends onto nothing and are concatenated.
 */
private fun union(earlier: Value.List, later: Value.List): Value.List {
    val shared = earlier.items.zip(later.items).takeWhile { (a, b) -> a.contentEquals(b) }.size
    if (shared == earlier.items.size || shared == later.items.size) return Value.List(later.items)
    return Value.List(earlier.items + later.items.drop(shared))
}

/** Every member of either side, at the higher of its two scores. */
private fun union(earlier: Value.ZSet, later: Value.ZSet, seed: Long): Value.ZSet {
    val merged = Value.ZSet(SkipList(seed))
    for (zset in listOf(earlier, later)) for ((member, score) in zset.scores.entries()) {
        merged.writeScore(maxOf(score, merged.scores.get(member) ?: Double.NEGATIVE_INFINITY), fieldBytes(member))
    }
    return merged
}
