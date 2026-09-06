package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import kotlin.random.Random

/**
 * A seeded history of writes, reads and faults over an [InProcessCluster]: every step is drawn
 * from one `Random`, so a seed is a reproducible run. A key's first letter is its type (`s`
 * string, `h` hash, `l` list, `z` sorted set, `c` counter), so a write always fits the key.
 * At most one node is down and one network partition open at a time, and [run] restarts the
 * one and heals the other before it returns. [acked] is the last acknowledged version of each
 * key: the coordinator's version right after a reply that was not an error, since an error
 * (a quorum that did not form, or a coordinator out of reach) promises nothing to the client.
 */
class ChaosDriver(private val cluster: InProcessCluster, seed: Long, private val keys: List<Key> = keys(4)) {

    private val random = Random(seed)
    private val incarnations = HashMap<NodeId, Long>()
    private var split = false
    private var down: NodeId? = null
    private var serial = 0

    val acked = LinkedHashMap<Key, Dvv>()

    suspend fun run(steps: Int) {
        repeat(steps) {
            when (random.nextInt(10)) {
                in 0..5 -> write()
                6, 7 -> read()
                8 -> if (split) heal() else partition()
                else -> down?.let { restart(it) } ?: kill()
            }
        }
        if (split) heal()
        down?.let { restart(it) }
    }

    private suspend fun write() {
        val key = keys.random(random)
        val value = (serial++).toString().toByteArray()
        val command: Command.Keyed = if (random.nextInt(8) == 0) Command.Del(key) else when (kind(key)) {
            'h' -> Command.HSet(key, listOf("f${random.nextInt(3)}".toByteArray() to value))
            'l' -> Command.Push(key, listOf(value), Command.End.HEAD)
            'z' -> Command.ZAdd(key, listOf(value to "m${random.nextInt(3)}".toByteArray()))
            'c' -> Command.IncrBy(key, random.nextLong(1, 10))
            else -> Command.Set(key, value)
        }
        val reply = cluster.submitVia(cluster.nodes.filter { it != down }.random(random), command)
        if (reply !is Reply.Error) acked[key] = cluster.replication(cluster.ring.preferenceList(key, cluster.n).first()).version(key)!!
    }

    private suspend fun read() {
        cluster.submitVia(cluster.nodes.random(random), readOf(keys.random(random)))
    }

    private fun partition() {
        val shuffled = cluster.nodes.shuffled(random)
        val cut = random.nextInt(1, shuffled.size)
        cluster.network.networkPartition(listOf(shuffled.take(cut).toSet(), shuffled.drop(cut).toSet()))
        split = true
    }

    private fun heal() {
        cluster.network.heal()
        split = false
    }

    private fun kill() {
        val node = cluster.nodes.random(random)
        cluster.network.kill(node)
        cluster.membership.set(node, MemberState.DEAD, incarnations[node] ?: 0)
        down = node
    }

    /** Back with a fresh incarnation, as SWIM would see a rejoining node; the hints held for it replay on the alive event. */
    private fun restart(node: NodeId) {
        val incarnation = (incarnations[node] ?: 0) + 1
        incarnations[node] = incarnation
        cluster.network.restart(node)
        cluster.membership.set(node, MemberState.ALIVE, incarnation)
        down = null
    }

    companion object {
        /** [perType] keys of each type: `s1..sN`, `h1..hN`, and so on. */
        fun keys(perType: Int): List<Key> = "shlzc".flatMap { type -> (1..perType).map { Key("$type$it") } }

        private fun kind(key: Key): Char = key.bytes[0].toInt().toChar()

        /** The whole of [key] as a client reads it, by its type. */
        fun readOf(key: Key): Command.Keyed = when (kind(key)) {
            'h' -> Command.HGetAll(key)
            'l' -> Command.LRange(key, 0, -1)
            'z' -> Command.ZRange(key, 0, -1, withScores = true)
            else -> Command.Get(key)
        }
    }
}
