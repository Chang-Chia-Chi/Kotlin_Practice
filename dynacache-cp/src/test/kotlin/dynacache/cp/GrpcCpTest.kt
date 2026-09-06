package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The CP subsystem over real sockets: MicroRaft's own traffic on `RaftService`, a client's work on
 * `CpService`. Every member is a gRPC server on an ephemeral localhost port.
 */
class GrpcCpTest {

    private val kit = GrpcCpKit()
    private val counter = Key("cp:counter:c")

    @AfterEach
    fun tearDown() = kit.close()

    /** The transport adapter's own test: three members on sockets elect a leader and replicate. */
    @Test
    fun raft_group_forms_over_grpc_on_localhost() {
        val leader = kit.leader()

        assertEquals(kit.members.size, leader.node.committedMembers.members.size)
        assertEquals(Reply.Integer(1), kit.applyDirect(leader.config.nodeId, Command.Cp.LongIncr(counter)))
    }

    /**
     * CP spec 9.1 step 3: a CP follower asked directly does not forward, it names the leader and
     * lets the client retry there.
     */
    @Test
    fun cp_notleader_hint_on_follower() {
        val leader = kit.leader().config.nodeId
        val follower = kit.live().first { it != leader }
        kit.awaitLeaderKnown(follower)

        val reply = kit.applyDirect(follower, Command.Cp.LongIncr(counter)) as Reply.Error

        assertEquals("NOTLEADER", reply.kind)
        assertTrue(reply.message.contains(leader.name)) {
            "the hint '${reply.message}' should name the real leader ${leader.name}"
        }
    }

    /** CP spec 9.1 step 2: a node with no Raft node of its own still serves the command. */
    @Test
    fun cp_non_leader_forwards() {
        kit.leader()

        assertEquals(Reply.Integer(1), forwarded(Command.Cp.LongIncr(counter)))
        assertEquals(Reply.Integer(1), kit.applyDirect(kit.leader().config.nodeId, Command.Cp.LongGet(counter)))
    }

    /**
     * The AP-only node's believed leader dies under it. Its next command still succeeds: the
     * `-NOTLEADER` or the silence sends it back to `GetInfo` for the member that took over.
     */
    @Test
    fun cp_forwarding_rediscovers_leader_after_failover() {
        val first = kit.leader().config.nodeId
        assertEquals(Reply.Integer(1), forwarded(Command.Cp.LongIncr(counter)))

        kit.killMember(first)

        assertEquals(Reply.Integer(2), forwarded(Command.Cp.LongIncr(counter)))
        assertNotEquals(first, kit.leader().config.nodeId)
    }

    /** `CP.INFO` (CP spec 6.7): the leader, the members, then the log's three indices. */
    @Test
    fun cp_info_reports_leader_and_members() {
        val leader = kit.leader().config.nodeId
        forwarded(Command.Cp.LongIncr(counter))

        val info = kit.apOnly.info().get(REPLY_TIMEOUT_SECS, SECONDS) as Reply.Array

        assertEquals(Reply.Bulk(leader.name.toByteArray()), info.items[0])
        assertEquals(kit.members.map { Reply.Bulk(it.name.toByteArray()) }, (info.items[1] as Reply.Array).items)
        assertEquals(5, info.items.size)
    }

    private fun forwarded(command: Command.Cp): Reply =
        kit.apOnly.submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    private companion object {
        const val REPLY_TIMEOUT_SECS = 30L
    }
}
