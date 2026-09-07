package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Snapshots and restarts through the CP engine's seam and the store's: a member restarted from
 * what its store kept (I20), and one brought up by a snapshot because it fell behind the log.
 */
class CpSnapshotTest {

    @TempDir
    lateinit var dir: Path

    private lateinit var kit: CpTestKit
    private val counter = Key("cp:counter:c")
    private val lock = Key("cp:lock:l")
    private val semaphore = Key("cp:sem:s")
    private val latch = Key("cp:latch:l")
    private val reference = Key("cp:ref:r")

    @BeforeEach
    fun start() {
        kit = CpTestKit(fileStoreDir = dir)
    }

    @AfterEach
    fun tearDown() = kit.close()

    private fun submit(command: Command): Reply =
        kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)

    /** Every primitive gets a value, so a field the snapshot forgot would show as a difference. */
    private fun populate(): Long {
        val session = (submit(Command.Cp.SessionCreate()) as Reply.Integer).value
        submit(Command.Cp.LongSet(counter, 7, ttl = Duration.ofHours(1)))
        submit(Command.Cp.LockTry(lock, session, Duration.ofHours(1)))
        submit(Command.Cp.SemInit(semaphore, 3))
        submit(Command.Cp.SemAcquire(semaphore, session, 2))
        submit(Command.Cp.LatchSet(latch, 5))
        submit(Command.Cp.RefSet(reference, "bytes".toByteArray()))
        return session
    }

    private fun commitIndex(): Long = kit.leader().node.report().log.commitIndex

    private fun snapshotOn(member: NodeId) {
        kit.awaitApplied(member, commitIndex())
        kit.runtime(member).node.takeSnapshot().get(REPLY_TIMEOUT_SECS, SECONDS)
    }

    private fun stateOf(member: NodeId) = kit.runtime(member).stateMachine.state

    /** CP spec 10.7: the member on disk restarts from its own snapshot, before the leader sends it anything. */
    @Test
    fun cp_snapshot_restore_roundtrip() {
        populate()
        val member = kit.fileMember
        snapshotOn(member)
        val index = commitIndex()
        val leaderState = kit.leader().stateMachine.state

        kit.killMember(member)
        kit.restartMember(member)

        assertEquals(leaderState, stateOf(member), "restored from the snapshot on disk")
        assertEquals(index, kit.runtime(member).node.report().log.lastSnapshotIndex)
    }

    /** I20: a snapshot plus the entries after it lands on the same state as replaying every entry from empty. */
    @Test
    fun I20_restore_equals_continuous_replay() {
        val session = populate()
        val leader = kit.leader().config.nodeId
        val member = kit.live().first { it != leader && it != kit.fileMember }
        snapshotOn(member)
        submit(Command.Cp.LongIncr(counter))
        submit(Command.Cp.LockUnlock(lock, session, token = 1))
        submit(Command.Cp.LockTry(lock, session, Duration.ofHours(1)))
        submit(Command.Cp.SemRelease(semaphore, session, 1))
        submit(Command.Cp.LatchDown(latch))
        submit(Command.Cp.RefCas(reference, "bytes".toByteArray(), "other".toByteArray()))
        submit(Command.Cp.SessionCreate())
        val index = commitIndex()

        kit.killMember(member)
        kit.restartMember(member)
        kit.awaitApplied(member, index)

        assertEquals(stateOf(leader), stateOf(member), "the leader replayed everything from empty")
        assertTrue(kit.runtime(member).node.report().log.lastSnapshotIndex > 0, "it did start from a snapshot")
    }

    /** A member that missed more entries than the log keeps after a snapshot is brought up by the snapshot, not by replay. */
    @Test
    fun lagging_member_is_brought_up_by_snapshot() {
        populate()
        val leader = kit.leader()
        val member = kit.live().first { it != leader.config.nodeId }
        // Far more than MicroRaft keeps after a snapshot (a tenth of the 100-commit interval), so
        // the leader has truncated the entries the member missed and can only send the snapshot.
        kit.killMember(member)
        repeat(40) { submit(Command.Cp.LongIncr(counter)) }
        snapshotOn(leader.config.nodeId)
        val index = commitIndex()

        kit.restartMember(member)
        kit.awaitApplied(member, index)

        assertEquals(stateOf(leader.config.nodeId), stateOf(member))
        assertEquals(
            leader.node.report().log.lastSnapshotIndex,
            kit.runtime(member).node.report().log.lastSnapshotIndex,
            "the leader's snapshot was installed, not its log replayed",
        )
    }

    /**
     * C17 across a snapshot: a member brought up by an installed snapshot holds the token that
     * snapshot carried, the next holder is granted a strictly greater one, and the sessions the
     * snapshot carried are alive on that member, permits and all.
     */
    @Test
    fun cp_snapshot_install_preserves_tokens_and_sessions() {
        val session = populate()
        val leader = kit.leader().config.nodeId
        val member = kit.live().first { it != leader }
        // Far more than the log keeps after a snapshot, so the member can only be brought up by one.
        kit.killMember(member)
        repeat(40) { submit(Command.Cp.LongIncr(counter)) }
        snapshotOn(leader)

        kit.restartMember(member)
        kit.awaitApplied(member, commitIndex())

        assertTrue(kit.runtime(member).node.report().log.lastSnapshotIndex > 0, "it came up on a snapshot")
        assertEquals(1L, tokenOn(member), "the token the snapshot carried")
        assertEquals(
            tableOn(leader, CpPrimitive.SESSIONS),
            tableOn(member, CpPrimitive.SESSIONS),
            "the sessions came with the snapshot, heartbeats and timeouts included",
        )

        submit(Command.Cp.LockUnlock(lock, session, token = 1))
        val granted = submit(Command.Cp.LockTry(lock, session, Duration.ofHours(1))) as Reply.Array
        submit(Command.Cp.SemRelease(semaphore, session, 2))
        kit.awaitApplied(member, commitIndex())

        assertEquals(Reply.Integer(2), granted.items[1], "C17: the next token is strictly greater")
        assertEquals(2L, tokenOn(member), "and the member that came up on the snapshot agrees")
        assertEquals(
            Reply.Integer(3),
            kit.runtime(member).stateMachine.read(Command.Cp.SemAvailable(semaphore)),
            "the permits the snapshot's session held were still there to give back",
        )
    }

    /** The fencing token [member] holds for [lock] at its own applied index. */
    private fun tokenOn(member: NodeId): Long =
        ((kit.runtime(member).stateMachine.read(Command.Cp.LockState(lock)) as Reply.Array).items[1] as Reply.Integer).value

    private fun tableOn(member: NodeId, id: Int) = stateOf(member).tables.first { it.id == id }

    private fun io.microraft.RaftNode.report() = getReport().get(REPLY_TIMEOUT_SECS, SECONDS).result

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
    }
}
