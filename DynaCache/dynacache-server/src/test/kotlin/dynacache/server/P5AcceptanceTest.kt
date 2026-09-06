package dynacache.server

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cluster.ReplicationConfig
import dynacache.engine.Reply
import io.microraft.RaftConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import redis.clients.jedis.Protocol
import redis.clients.jedis.params.SetParams
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

/**
 * CP spec 12's exit criterion: both engines in one cluster, reached by an unmodified client.
 * The three nodes of P2 and P4 are also the three CP members, so one process holds an AP
 * partition and a Raft member and a client cannot tell which one it is talking to.
 */
class P5AcceptanceTest {

    private val ids = List(3) { NodeId("node-${it + 1}") }

    /** Read at send time by both transports, so ephemeral ports are filled in after binding. */
    private val addresses = ConcurrentHashMap<NodeId, HostPort>()
    private val cpAddresses = ConcurrentHashMap<NodeId, HostPort>()

    private var nodes: List<ClusterNode> = emptyList()

    @AfterEach
    fun stopWhateverIsUp() = stopCluster()

    @Test
    fun P5_acceptance_two_engines_one_cluster() {
        startCluster()
        val leader = cpLeader()
        aCounterThroughUnmodifiedJedis(leader)

        val held = RespClient(leader.respPort).use { holder ->
            RespClient(leader.respPort).use { rival ->
                val taken = theLockIsTakenByOneConnection(holder)
                theSecondConnectionIsRefused(rival)
                taken
            }
        }

        kill(leader)
        theLockOutlivesTheLeaderThatGrantedIt(held)
        closingTheSessionReleasesTheLock(held)
    }

    /**
     * CP spec 6.2's dual interface at the socket: Jedis speaks Redis and the dispatcher re-targets
     * each of these onto the AtomicLong behind the `cp:counter:` namespace. The `GET` is what says
     * which engine answered -- a CP counter replies `:6` where the AP engine would have replied
     * with the bulk string `6`, so a value that came back as a Long came through the Raft log.
     */
    private fun aCounterThroughUnmodifiedJedis(leader: ClusterNode) {
        Jedis("127.0.0.1", leader.respPort).use { redis ->
            assertEquals("OK", redis.set("cp:counter:x", "5", SetParams.setParams().ex(10)))
            assertEquals(6L, redis.incr("cp:counter:x"))
            assertEquals(6L, redis.sendCommand(Protocol.Command.GET, "cp:counter:x"))
        }
    }

    /**
     * CP spec 6.1 through a raw RESP client, since Jedis has no way to spell a verb it does not
     * know. The connection's own session is created on the way (CP spec 4), so nothing here says
     * who is asking; the reply is `ok` and the fencing token the lock handed out.
     */
    private fun theLockIsTakenByOneConnection(holder: RespClient): Held {
        holder.send("CP.LOCK.TRY", LOCK, "30000")
        val granted = holder.read() as Reply.Array
        assertEquals(Reply.Integer(1), granted.items[0], "the lock was not granted")
        // The session this connection was given on the way in; CP.SESSION.CREATE names it rather
        // than making a second one (T44), and it is the owner CP.LOCK.STATE reports.
        holder.send("CP.SESSION.CREATE")
        return Held((holder.read() as Reply.Integer).value, (granted.items[1] as Reply.Integer).value)
    }

    /** Who holds the lock and under which fencing token: what has to survive a leader's death. */
    private data class Held(val session: Long, val token: Long)

    /** A second connection is a second session, so the lock refuses it: `ok` 0 and no token (I13). */
    private fun theSecondConnectionIsRefused(rival: RespClient) {
        rival.send("CP.LOCK.TRY", LOCK, "30000")
        assertEquals(Reply.Array(listOf(Reply.Integer(0), Reply.Integer(0))), rival.read())
    }

    /**
     * I14 at the socket: the lock is the log's, not the leader's. The node that granted it is gone
     * along with the connection that asked, and what a surviving member reports is the same owner
     * and the same fencing token -- a token that moved would let two holders both believe they are
     * current, which is the whole point of fencing it.
     */
    private fun theLockOutlivesTheLeaderThatGrantedIt(held: Held) {
        val state = RespClient(cpLeader().respPort).use { survivor ->
            survivor.send("CP.LOCK.STATE", LOCK)
            survivor.read() as Reply.Array
        }
        assertEquals(Reply.Integer(held.session), state.items[0], "the lock changed hands over the failover")
        assertEquals(Reply.Integer(held.token), state.items[1], "the fencing token moved over the failover")
    }

    /**
     * C18 and I15: what a session held goes with it. The connection that took the lock died with
     * the node, so the close is sent by session id from a surviving member (CP spec 6.6) -- which
     * is also what a heartbeat timeout would do a session lifetime later, only on demand. The
     * waiting client then gets the lock with a *greater* token, so a fenced resource can tell the
     * new holder from the old one (C17).
     */
    private fun closingTheSessionReleasesTheLock(held: Held) {
        RespClient(cpLeader().respPort).use { rival ->
            rival.send("CP.SESSION.CLOSE", held.session.toString())
            assertEquals(Reply.Simple("OK"), rival.read())

            rival.send("CP.LOCK.TRY", LOCK, "30000")
            val granted = rival.read() as Reply.Array
            assertEquals(Reply.Integer(1), granted.items[0], "the closed session kept the lock")
            val token = (granted.items[1] as Reply.Integer).value
            assertTrue(token > held.token, "the new holder's token $token did not pass ${held.token}")
        }
    }

    // ---- the harness --------------------------------------------------------------------------

    private fun startCluster() {
        nodes = ids.map { id ->
            ClusterNode(
                self = id,
                nodes = ids.toSet(),
                addresses = addresses,
                respPort = 0,
                grpcPort = 0,
                config = ReplicationConfig(n = 3, w = 2, r = 2),
                partitionCount = 4,
                gossipPeriod = 100.milliseconds,
                cpMembers = ids,
                cpAddresses = cpAddresses,
                cpPort = 0,
                cpRaft = FAST_ELECTIONS,
            )
        }
        nodes.forEach {
            addresses[it.self] = HostPort("127.0.0.1", it.grpcPort)
            cpAddresses[it.self] = HostPort("127.0.0.1", it.cpPort)
        }
        nodes.forEach { it.start() }
    }

    /**
     * The node `CP.INFO` names as leader (CP spec 6.7) and that will take a command right now,
     * polled to a deadline. Both halves are needed: an election is not instant, and a member wins
     * one a little before it may replicate, since a fresh leader has to apply its own term's first
     * entry before its stamps can be trusted (C19). Until then it names itself and refuses itself,
     * which is a `-NOTLEADER` a real client rides out by retrying (CP spec 9.1 step 3).
     */
    private fun cpLeader(): ClusterNode = awaitValue("a CP member led and would replicate") {
        nodes.firstNotNullOfOrNull(::leaderAccordingTo)?.takeIf(::replicatesNow)
    }

    /** Whether [node] answers a replicated read of its own rather than pointing at someone else. */
    private fun replicatesNow(node: ClusterNode): Boolean = runCatching {
        RespClient(node.respPort).use { client ->
            client.send("CP.LONG.GET", PROBE)
            client.read() !is Reply.Error
        }
    }.getOrDefault(false)

    /** Who [node] believes leads, or null when it knows of nobody yet or cannot be reached. */
    private fun leaderAccordingTo(node: ClusterNode): ClusterNode? = runCatching {
        RespClient(node.respPort).use { client ->
            client.send("CP.INFO")
            val named = ((client.read() as? Reply.Array)?.items?.firstOrNull() as? Reply.Bulk)?.bytes
                ?: return@use null
            nodes.firstOrNull { it.self.name == named.toString(Charsets.ISO_8859_1) }
        }
    }.getOrNull()

    /** [node] is gone: the process, its RESP port and its Raft member, all at once. */
    private fun kill(node: ClusterNode) {
        nodes = nodes - node
        node.close()
    }

    private fun stopCluster() {
        nodes.forEach { runCatching { it.close() } }
        nodes = emptyList()
    }

    /** Real time, so every wait is a poll to a deadline; nothing here sleeps for a fixed span. */
    private fun <T : Any> awaitValue(what: String, within: Duration = Duration.ofSeconds(20), value: () -> T?): T {
        val giveUpAt = System.nanoTime() + within.toNanos()
        while (true) {
            value()?.let { return it }
            assertTrue(System.nanoTime() < giveUpAt, "$what: not within $within")
        }
    }

    private companion object {

        /**
         * The kit's timings (T45), not production's: a failover the test waits on has to resolve
         * in seconds. Production keeps MicroRaft's defaults.
         */
        val FAST_ELECTIONS: RaftConfig = RaftConfig.newBuilder()
            .setLeaderElectionTimeoutMillis(200)
            .setLeaderHeartbeatPeriodSecs(1)
            .setLeaderHeartbeatTimeoutSecs(2)
            .build()

        const val LOCK = "cp:lock:demo"

        /** A key nothing else touches: reading it is the smallest replicated ask a leader answers. */
        const val PROBE = "cp:counter:probe"
    }
}
