package dynacache.cluster

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * I1 and I2 (spec 4) as executable checkers over the test kit's cluster: after every network
 * partition heals, every message drains and one full anti-entropy cycle runs, every replica of
 * every key holds the same value and version; and a minority crash loses no acknowledged write.
 */
class ConvergenceTest {

    private val one = NodeId("node-1")
    private val two = NodeId("node-2")

    /**
     * Spec 6.7: one key of each type is written, the network splits, both sides write the same
     * keys concurrently, the split heals, and every replica converges to the type merge of
     * spec 2.5. W = 1 so each side acknowledges its own write; the far side writes through its
     * replication layer directly, standing in for the coordinator failover of spec 5.1 step 7
     * that the router does not do yet (progress T22 deviation 7).
     */
    @Test
    fun convergence_after_partition() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 1, r = 3, scope = backgroundScope, partitionsPerNode = 2)
        val (string, hash, list, zset) = listOf("s", "h", "l", "z").map(::Key)
        cluster.submitVia(one, Command.Set(string, bytes("base")))
        cluster.submitVia(one, Command.HSet(hash, listOf(bytes("f0") to bytes("base"))))
        cluster.submitVia(one, Command.Push(list, listOf(bytes("x")), Command.End.TAIL))
        cluster.submitVia(one, Command.ZAdd(zset, listOf(bytes("0") to bytes("m0"))))
        cluster.drainMessages()

        cluster.network.networkPartition(listOf(setOf(one), setOf(two, NodeId("node-3"))))
        for ((side, tag) in listOf(one to "a", two to "b")) {
            assertEquals(Reply.Simple("OK"), cluster.submitOn(side, Command.Set(string, bytes(tag))))
            assertEquals(Reply.Integer(1), cluster.submitOn(side, Command.HSet(hash, listOf(bytes("f$tag") to bytes(tag)))))
            assertEquals(Reply.Integer(2), cluster.submitOn(side, Command.Push(list, listOf(bytes(tag)), Command.End.TAIL)))
            val score = if (tag == "a") "1" else "2"
            assertEquals(Reply.Integer(1), cluster.submitOn(side, Command.ZAdd(zset, listOf(bytes(score) to bytes("m1")))))
        }
        cluster.drainMessages()

        cluster.assertConverged()

        // The merged state of spec 2.5, on any replica since they are all equal now.
        val engine = cluster.engine(one)
        assertEquals(Reply.Bulk(bytes("b")), engine.submit(Command.Get(string)).await(), "the last writer's string")
        assertEquals(mapOf("f0" to "base", "fa" to "a", "fb" to "b"), fields(engine.submit(Command.HGetAll(hash)).await()))
        assertEquals(strings("x", "a", "b"), engine.submit(Command.LRange(list, 0, -1)).await(), "shared prefix, then both tails")
        assertEquals(strings("m0", "0", "m1", "2"), engine.submit(Command.ZRange(zset, 0, -1, withScores = true)).await(), "union at the higher score")
        cluster.close()
    }

    /**
     * Five seeded chaos runs (writes of every type, reads, network partitions, kills, restarts),
     * each followed by the checker: whatever the faults did, the cluster converges. Four nodes
     * and N = 3, so sloppy quorum and hints (T25) are in play too.
     */
    @Test
    fun I1_all_replicas_equal_after_heal_drain_sync() = runTest {
        for (seed in 1L..5L) {
            val cluster = InProcessCluster(nodeCount = 4, n = 3, w = 2, r = 2, scope = backgroundScope, partitionsPerNode = 2)
            ChaosDriver(cluster, seed).run(steps = 60)
            try {
                cluster.assertConverged()
            } catch (e: AssertionError) {
                throw AssertionError("seed $seed: ${e.message}", e)
            }
            cluster.close()
        }
    }

    /**
     * I2: N = 3, W = 2, R = 2, so fewer than N-W+1 = 2 crashes, that is one, leaves every
     * acknowledged write on a survivor. After a chaos run, node-3 crashes for good; every key
     * the run acknowledged is read at quorum through a survivor and answers without error, and
     * the version the read answered with (which read repair leaves on both survivors) dominates
     * or equals the acknowledged one: "at least as new" is the read side dominating, never the
     * acknowledged side. The keys are coordinated by node-1 and node-2 because a key whose
     * coordinator is down is unreachable through the router (progress T22 deviation 7), which
     * is availability, not loss.
     */
    @Test
    fun I2_minority_crash_loses_no_acked_write() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 2, r = 2, scope = backgroundScope, partitionsPerNode = 2)
        val victim = NodeId("node-3")
        val survivors = cluster.nodes - victim
        val keys = ChaosDriver.keys(perType = 6).filter { cluster.ring.preferenceList(it, 3).first() != victim }
        val driver = ChaosDriver(cluster, seed = 30, keys = keys)
        driver.run(steps = 60)
        assertTrue(driver.acked.size >= 10, "the run acknowledged only ${driver.acked.size} keys")

        cluster.network.kill(victim)
        cluster.membership.set(victim, MemberState.DEAD)
        for ((key, acked) in driver.acked) {
            val reply = cluster.submitVia(survivors[key.hashCode() and 1], ChaosDriver.readOf(key))
            assertFalse(reply is Reply.Error, "$key read after the crash: $reply")
            cluster.drainMessages()
            for (node in survivors) {
                val read = cluster.replication(node).version(key)!!
                assertTrue(read == acked || read.dominates(acked), "$key on $node: read $read is older than acknowledged $acked")
            }
        }
        cluster.close()
    }

    private fun bytes(text: String) = text.toByteArray()

    private fun strings(vararg items: String) = Reply.Array(items.map { Reply.Bulk(it.toByteArray()) })

    private fun fields(reply: Reply): Map<String, String> =
        (reply as Reply.Array).items.map { String((it as Reply.Bulk).bytes!!) }.chunked(2).associate { (field, value) -> field to value }
}
