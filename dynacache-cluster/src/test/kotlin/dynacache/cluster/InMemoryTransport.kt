package dynacache.cluster

import dynacache.cluster.proto.Envelope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.yield
import kotlin.random.Random

/**
 * The test kit's adapter of the [Transport] seam: one hub shared by every node, one
 * [endpoint] per node, and the faults a distributed test needs.
 *
 * Nothing moves until [drain] runs. A drain delivers everything in flight in delivery
 * rounds: each envelope is due a seeded number of rounds after it was sent ([delay]), and a
 * round hands each sender-to-receiver pair its due envelopes in send order, so [delay]
 * reorders delivery across pairs and never within one. Between rounds the drain yields so
 * receivers run and their replies join the next round; it returns when nothing is in flight.
 * Everything is deterministic under `runTest`: seeded randomness, no time.
 */
class InMemoryTransport {

    private class InFlight(val envelope: Envelope, val due: Long)

    private inner class Endpoint(val self: NodeId) : Transport {
        val channel = Channel<Envelope>(Channel.UNLIMITED)
        override val inbound: ReceiveChannel<Envelope> get() = channel
        override suspend fun send(to: NodeId, envelope: Envelope) = enqueue(self, to, envelope)
        override fun close() { channel.close() }
    }

    private val endpoints = LinkedHashMap<NodeId, Endpoint>()
    private val pairs = LinkedHashMap<Pair<NodeId, NodeId>, ArrayDeque<InFlight>>()
    private var round = 0L
    private var delayRounds: IntRange = 0..0
    private var delayRandom: Random = Random(0)
    private var dropRate = 0.0
    private var dropRandom: Random = Random(0)
    private var sides: List<Set<NodeId>> = emptyList()
    private val dead = HashSet<NodeId>()

    fun endpoint(node: NodeId): Transport = endpoints.getOrPut(node) { Endpoint(node) }

    /** Every envelope sent from now on is lost with probability [rate], decided by [seed]. */
    fun drop(rate: Double, seed: Long) {
        dropRate = rate
        dropRandom = Random(seed)
    }

    /** Every envelope sent from now on is held a seeded number of rounds in [rounds]. */
    fun delay(rounds: IntRange, seed: Long) {
        delayRounds = rounds
        delayRandom = Random(seed)
    }

    /**
     * Splits the network: an envelope crosses only when one side holds both its ends, judged
     * at delivery, so what is in flight when the split forms is lost too.
     */
    fun networkPartition(sides: List<Set<NodeId>>) {
        this.sides = sides
    }

    /** Ends the network partition. */
    fun heal() {
        sides = emptyList()
    }

    /** Takes [node] down: nothing reaches it and nothing it sends leaves, judged at delivery. */
    fun kill(node: NodeId) {
        dead.add(node)
    }

    /** Brings [node] back; what was sent to it while down is gone, its inbox is untouched. */
    fun restart(node: NodeId) {
        dead.remove(node)
    }

    private fun reachable(from: NodeId, to: NodeId): Boolean =
        from !in dead && to !in dead && (sides.isEmpty() || sides.any { from in it && to in it })

    /** Whether any envelope is still waiting to be delivered. */
    val inFlight: Boolean get() = pairs.values.any { it.isNotEmpty() }

    /** Runs delivery rounds until nothing is in flight, yielding between rounds. */
    suspend fun drain() {
        while (inFlight) {
            round++
            for ((pair, queue) in pairs) {
                while (queue.isNotEmpty() && queue.first().due <= round) {
                    val next = queue.removeFirst()
                    if (reachable(pair.first, pair.second)) {
                        endpoints.getValue(pair.second).channel.trySend(next.envelope)
                    }
                }
            }
            yield()
        }
    }

    private fun enqueue(from: NodeId, to: NodeId, envelope: Envelope) {
        if (dropRandom.nextDouble() < dropRate) return
        val due = round + delayRandom.nextInt(delayRounds.first, delayRounds.last + 1)
        pairs.getOrPut(from to to) { ArrayDeque() }.addLast(InFlight(envelope, due))
    }
}
