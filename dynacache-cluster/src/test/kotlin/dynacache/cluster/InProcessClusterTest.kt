package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Ping
import dynacache.engine.Key
import dynacache.engine.Reply
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** The test kit's cluster: N nodes, one ring, one engine and one transport endpoint each. */
class InProcessClusterTest {

    @Test
    fun cluster_boots_three_nodes_sharing_one_ring() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2)
        val key = Key("orders:4711")
        val (first, second) = cluster.nodes

        assertEquals(3, cluster.nodes.size)
        assertEquals(Ring.of(cluster.nodes.toSet()).preferenceList(key, 3), cluster.ring.preferenceList(key, 3))

        assertEquals(Reply.Simple("OK"), cluster.writeVia(first, key, "v1".toByteArray()))
        assertEquals(Reply.Bulk("v1".toByteArray()), cluster.readVia(first, key))
        assertEquals(cluster.ring.preferenceList(key, 3).toSet(), cluster.readAllReplicas(key).keys)

        val ping = Envelope.newBuilder().setFrom(first.name).setTo(second.name).setPing(Ping.newBuilder().setSeq(1)).build()
        cluster.transport(first).send(second, ping)
        cluster.drainMessages()
        assertEquals(ping, cluster.transport(second).inbound.tryReceive().getOrNull())

        cluster.close()
    }
}
