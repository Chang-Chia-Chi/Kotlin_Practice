package dynacache.cluster

import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.MembershipEntry
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.math.ceil
import kotlin.math.log2
import kotlin.random.Random

/**
 * SWIM gossip (spec 2.4, I8): every node is a [Swim] on one [InMemoryTransport], stepped by
 * rounds. What a node receives reaches its [Swim] the one way a real node's does, through the
 * gossip handler of an [InboundLoop] (T68).
 */
class SwimTest {

    /** One round is a tick on every live node followed by a full drain of the network. */
    private class Gossip(n: Int, val k: Int = 2, val rtt: Int = 2, val t: Int = 3, val seed: Long = 1) {
        val network = InMemoryTransport()
        val nodes = List(n) { NodeId("node-${it + 1}") }
        val swims = LinkedHashMap<NodeId, Swim>()
        val inbounds = LinkedHashMap<NodeId, InboundLoop>()
        var rounds = 0

        init { nodes.forEach { start(it, incarnation = 0) } }

        fun start(node: NodeId, incarnation: Long) {
            val swim = Swim(
                self = node, peers = nodes.toSet() - node, transport = network.endpoint(node),
                random = Random(seed * 31 + nodes.indexOf(node)), incarnation = incarnation,
                k = k, rttTicks = rtt, suspectTicks = t,
            )
            swims[node] = swim
            inbounds[node] = InboundLoop(network.endpoint(node).inbound, gossip = swim::deliver)
        }

        fun kill(node: NodeId) {
            network.kill(node)
            swims.remove(node)
            inbounds.remove(node)
        }

        /** What each live node received, then one protocol period on each, then delivery. */
        suspend fun round() {
            inbounds.values.forEach { it.drain() }
            swims.values.forEach { it.tick() }
            network.drain()
            rounds++
        }

        /** Runs rounds until [done] holds; fails if it takes more than [cap]. */
        suspend fun roundsUntil(cap: Int, done: () -> Boolean) {
            val start = rounds
            while (!done()) {
                assertTrue(rounds - start < cap, "not done after $cap rounds (${swims.size} nodes)")
                round()
            }
        }

        fun everyoneSees(node: NodeId, state: MemberState, incarnation: Long? = null): Boolean =
            swims.values.all { swim ->
                val m = swim.members.getValue(node)
                m.state == state && (incarnation == null || m.incarnation == incarnation)
            }
    }

    /** I8's constant: a change reaches every live node within C * log2(N) rounds. */
    private val C = 4

    private fun logBound(n: Int): Int = ceil(C * log2(n.toDouble())).toInt()

    /** Kills the last node and runs rounds until every survivor has it dead. */
    private suspend fun killLast(g: Gossip) {
        val victim = g.nodes.last()
        repeat(3) { g.round() }
        assertTrue(g.everyoneSees(victim, MemberState.ALIVE))
        g.kill(victim)
        // The first probe to reach the victim, its direct and indirect timeouts, T, then dissemination.
        val bound = (g.nodes.size - 1) + 3 * g.rtt + g.t + logBound(g.nodes.size)
        g.roundsUntil(bound) { g.everyoneSees(victim, MemberState.DEAD, incarnation = 0) }
    }

    @Test
    fun gossip_detects_failure() = runTest {
        val g = Gossip(n = 5)
        val victim = g.nodes.last()
        val changes = ArrayList<Member>()
        backgroundScope.launch { g.swims.getValue(g.nodes.first()).changes.collect { changes += it } }

        killLast(g)

        assertTrue(g.swims.values.all { it.dead == setOf(victim) && it.suspect.isEmpty() })
        assertEquals(listOf(Member(victim, MemberState.SUSPECT, 0), Member(victim, MemberState.DEAD, 0)), changes)
    }

    @Test
    fun gossip_detects_recovery() = runTest {
        val g = Gossip(n = 5)
        val victim = g.nodes.last()
        killLast(g)

        g.network.restart(victim)
        g.start(victim, incarnation = 1)
        g.roundsUntil(logBound(g.nodes.size)) { g.everyoneSees(victim, MemberState.ALIVE, incarnation = 1) }
        assertTrue(g.swims.values.all { it.alive == g.nodes.toSet() })
    }

    @Test
    fun gossip_suspect_refuted_by_incarnation() = runTest {
        val g = Gossip(n = 3, t = 10)
        val (accuser, accused, witness) = g.nodes
        // A rumour reaches the accuser: the witness's table says the accused is suspect at incarnation 0.
        val rumour = Envelope.newBuilder().setFrom(witness.name).setTo(accuser.name).addMembership(
            MembershipEntry.newBuilder().setNode(accused.name).setState(MembershipEntry.State.SUSPECT).setIncarnation(0),
        ).build()
        g.network.endpoint(witness).send(accuser, rumour)
        g.network.drain()
        g.round()
        assertEquals(Member(accused, MemberState.SUSPECT, 0), g.swims.getValue(accuser).members.getValue(accused))

        // Refuted strictly before T rounds pass, or the accuser would have declared the accused dead.
        g.roundsUntil(g.t - 1) { g.everyoneSees(accused, MemberState.ALIVE, incarnation = 1) }
        assertTrue(g.swims.values.all { it.suspect.isEmpty() && it.dead.isEmpty() })
    }

    @Test
    fun gossip_ping_req_masks_one_lost_link() = runTest {
        val g = Gossip(n = 3, k = 1)
        val (left, right, bridge) = g.nodes
        // Two overlapping sides: the link between left and right is gone, the bridge reaches both.
        g.network.networkPartition(listOf(setOf(left, bridge), setOf(right, bridge)))

        repeat(4 * (3 * g.rtt + g.t)) { g.round() }

        assertTrue(g.swims.values.all { it.alive == g.nodes.toSet() }, g.swims.values.map { it.members.values }.toString())
        assertTrue(g.swims.values.all { it.members.values.all { m -> m.incarnation == 0L } })
    }

    @Test
    fun I8_membership_change_reaches_all_within_log_n_rounds() = runTest {
        for (n in listOf(5, 7)) {
            val g = Gossip(n = n, seed = n.toLong())
            val victim = g.nodes.last()
            repeat(3) { g.round() }
            g.kill(victim)
            // Detection is the detector's business; I8 counts from the first node that knows.
            g.roundsUntil(n + 3 * g.rtt + g.t) { g.swims.values.any { victim in it.dead } }
            g.roundsUntil(logBound(n)) { g.everyoneSees(victim, MemberState.DEAD) }

            // A recovery starts at exactly one node, so this half measures dissemination alone.
            g.network.restart(victim)
            g.start(victim, incarnation = 1)
            g.roundsUntil(logBound(n)) { g.everyoneSees(victim, MemberState.ALIVE, incarnation = 1) }
        }
    }

    @Test
    fun scripted_membership_answers_what_the_test_set() = runTest {
        val (a, b, c) = List(3) { NodeId("node-${it + 1}") }
        val membership = ScriptedMembership(listOf(a, b, c))
        val changes = ArrayList<Member>()
        backgroundScope.launch { membership.changes.collect { changes += it } }
        yield()
        assertEquals(setOf(a, b, c), membership.alive)

        membership.set(b, MemberState.SUSPECT)
        membership.set(c, MemberState.DEAD, incarnation = 2)
        yield()

        assertEquals(setOf(a), membership.alive)
        assertEquals(setOf(b), membership.suspect)
        assertEquals(setOf(c), membership.dead)
        assertEquals(listOf(Member(b, MemberState.SUSPECT, 0), Member(c, MemberState.DEAD, 2)), changes)
    }
}
