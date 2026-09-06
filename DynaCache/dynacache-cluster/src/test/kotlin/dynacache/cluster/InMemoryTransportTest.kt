package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Ping
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** The in-memory adapter of the [Transport] seam: the fault-injecting network every P2 test runs on. */
class InMemoryTransportTest {

    private val alpha = NodeId("alpha")
    private val bravo = NodeId("bravo")

    private fun ping(from: NodeId, to: NodeId, seq: Long): Envelope =
        Envelope.newBuilder().setFrom(from.name).setTo(to.name).setPing(Ping.newBuilder().setSeq(seq)).build()

    private fun ReceiveChannel<Envelope>.seqs(): List<Long> =
        generateSequence { tryReceive().getOrNull() }.map { it.ping.seq }.toList()

    @Test
    fun transport_delivers_in_order_per_pair() = runTest {
        val network = InMemoryTransport()
        network.delay(0..3, seed = 7)
        val a = network.endpoint(alpha)
        val b = network.endpoint(bravo)

        for (seq in 1L..20L) {
            a.send(bravo, ping(alpha, bravo, seq))
            b.send(alpha, ping(bravo, alpha, seq))
        }
        network.drain()

        assertEquals((1L..20L).toList(), b.inbound.seqs())
        assertEquals((1L..20L).toList(), a.inbound.seqs())
    }

    @Test
    fun network_partition_blocks_both_directions() = runTest {
        val network = InMemoryTransport()
        val a = network.endpoint(alpha)
        val b = network.endpoint(bravo)
        network.networkPartition(listOf(setOf(alpha), setOf(bravo)))

        a.send(bravo, ping(alpha, bravo, 1))
        b.send(alpha, ping(bravo, alpha, 1))
        network.drain()

        assertEquals(emptyList<Long>(), b.inbound.seqs())
        assertEquals(emptyList<Long>(), a.inbound.seqs())
    }

    @Test
    fun heal_restores_delivery() = runTest {
        val network = InMemoryTransport()
        val a = network.endpoint(alpha)
        val b = network.endpoint(bravo)
        network.networkPartition(listOf(setOf(alpha), setOf(bravo)))
        a.send(bravo, ping(alpha, bravo, 1))
        network.drain()

        network.heal()
        a.send(bravo, ping(alpha, bravo, 2))
        b.send(alpha, ping(bravo, alpha, 2))
        network.drain()

        assertEquals(listOf(2L), b.inbound.seqs())
        assertEquals(listOf(2L), a.inbound.seqs())
    }

    @Test
    fun kill_stops_delivery_and_restart_resumes() = runTest {
        val network = InMemoryTransport()
        val a = network.endpoint(alpha)
        val b = network.endpoint(bravo)
        network.kill(bravo)
        a.send(bravo, ping(alpha, bravo, 1))
        b.send(alpha, ping(bravo, alpha, 1))
        network.drain()
        assertEquals(emptyList<Long>(), b.inbound.seqs())
        assertEquals(emptyList<Long>(), a.inbound.seqs())

        network.restart(bravo)
        a.send(bravo, ping(alpha, bravo, 2))
        b.send(alpha, ping(bravo, alpha, 2))
        network.drain()

        assertEquals(listOf(2L), b.inbound.seqs())
        assertEquals(listOf(2L), a.inbound.seqs())
    }

    @Test
    fun drop_is_reproducible_by_seed() = runTest {
        suspend fun deliveredUnder(seed: Long): List<Long> {
            val network = InMemoryTransport()
            network.drop(rate = 0.5, seed = seed)
            val a = network.endpoint(alpha)
            val b = network.endpoint(bravo)
            for (seq in 1L..100L) a.send(bravo, ping(alpha, bravo, seq))
            network.drain()
            return b.inbound.seqs()
        }

        val first = deliveredUnder(seed = 11)

        assertEquals(first, deliveredUnder(seed = 11))
        assertNotEquals(first, deliveredUnder(seed = 12))
        assertTrue(first.size in 1..99, "half the envelopes should be lost, not all or none")
    }
}
