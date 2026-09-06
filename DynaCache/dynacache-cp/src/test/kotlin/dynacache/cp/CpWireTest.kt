package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import io.microraft.model.message.InstallSnapshotRequest
import io.microraft.model.message.InstallSnapshotResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.time.Duration

/** Every entry a leader appends comes back from its wire form equal, stamp and all. */
class CpWireTest {

    private fun roundTrip(operation: Any): Any = CpWire.decodeOperation(CpWire.encodeOperation(operation))

    /** A snapshot with a row per primitive, the shape the store and the wire both carry. */
    private val snapshot = Primitives().run {
        val session = (apply(Command.Cp.SessionCreate(Duration.ofSeconds(15))) as Reply.Integer).value
        apply(Command.Cp.LongSet(Key("cp:counter:c"), 7, ttl = Duration.ofHours(1)))
        apply(Command.Cp.LockTry(Key("cp:lock:l"), session, lease = Duration.ofSeconds(30)))
        apply(Command.Cp.SemInit(Key("cp:sem:s"), 3))
        apply(Command.Cp.SemAcquire(Key("cp:sem:s"), session, 2))
        apply(Command.Cp.LatchSet(Key("cp:latch:l"), 5))
        apply(Command.Cp.RefSet(Key("cp:ref:r"), byteArrayOf(0, 127, -1)))
        stateMachine.state
    }

    /** The chunk's own byte form: a version, log time, and one opaque table per primitive. */
    @Test
    fun snapshot_round_trips_through_its_bytes() {
        assertEquals(6, snapshot.tables.size, "one table per primitive")
        assertEquals(snapshot, CpWire.decodeSnapshot(CpWire.encodeSnapshot(snapshot)))
    }

    /**
     * The layout before T70 opened with the log-time long, whose top byte reads here as version 0.
     * The project is pre-release, so such a snapshot is refused by name rather than migrated.
     */
    @Test
    fun a_snapshot_from_before_the_version_bump_is_refused() {
        val old = ByteArrayOutputStream().also { DataOutputStream(it).writeLong(1_788_656_400_001L) }.toByteArray()

        val refused = assertThrows(CpWire.UnsupportedSnapshotVersion::class.java) { CpWire.decodeSnapshot(old) }

        assertTrue(refused.message.orEmpty().contains("version 0"), "names the version it found: ${refused.message}")
    }

    /** The install of a snapshot at a lagging member: the chunk, the members that hold it, and the group view. */
    @Test
    fun install_snapshot_round_trips_on_the_wire() {
        val members = CpWire.models.createRaftGroupMembersViewBuilder()
            .setLogIndex(0)
            .setMembers(listOf("cp1", "cp2", "cp3").map(CpWire::endpoint))
            .setVotingMembers(listOf("cp1", "cp2", "cp3").map(CpWire::endpoint))
            .build()
        val chunk = CpWire.models.createSnapshotChunkBuilder()
            .setIndex(100).setTerm(2).setOperation(snapshot).setSnapshotChunkIndex(0).setSnapshotChunkCount(1)
            .setGroupMembersView(members)
            .build()
        val request = CpWire.models.createInstallSnapshotRequestBuilder()
            .setGroupId("g").setSender(CpEndpoint(NodeId("cp1"))).setTerm(2)
            .setSenderLeader(true).setSnapshotTerm(2).setSnapshotIndex(100).setTotalSnapshotChunkCount(1)
            .setSnapshotChunk(chunk).setSnapshottedMembers(listOf(CpWire.endpoint("cp1")))
            .setGroupMembersView(members).setQuerySequenceNumber(4).setFlowControlSequenceNumber(5)
            .build()

        val decoded = CpWire.decode(CpWire.encode(request)) as InstallSnapshotRequest

        assertEquals(request.snapshotIndex, decoded.snapshotIndex)
        assertEquals(snapshot, decoded.snapshotChunk!!.operation, "the chunk's state came back equal, bytes and all")
        assertEquals(request.snapshotChunk!!.snapshotChunkCount, decoded.snapshotChunk!!.snapshotChunkCount)
        assertEquals(request.snapshottedMembers, decoded.snapshottedMembers)
        assertEquals(members.members, decoded.groupMembersView.members)
        assertEquals(request.flowControlSequenceNumber, decoded.flowControlSequenceNumber)

        val announcement = CpWire.models.createInstallSnapshotRequestBuilder()
            .setGroupId("g").setSender(CpEndpoint(NodeId("cp1"))).setTerm(2)
            .setSnapshotIndex(100).setTotalSnapshotChunkCount(1).setSnapshottedMembers(emptyList()).setGroupMembersView(members)
            .build()
        assertNull((CpWire.decode(CpWire.encode(announcement)) as InstallSnapshotRequest).snapshotChunk, "the first request carries no chunk")

        val response = CpWire.models.createInstallSnapshotResponseBuilder()
            .setGroupId("g").setSender(CpEndpoint(NodeId("cp2"))).setTerm(2)
            .setSnapshotIndex(100).setRequestedSnapshotChunkIndex(0).setQuerySequenceNumber(4).setFlowControlSequenceNumber(5)
            .build()
        val decodedResponse = CpWire.decode(CpWire.encode(response)) as InstallSnapshotResponse
        assertEquals(response.snapshotIndex, decodedResponse.snapshotIndex)
        assertEquals(response.requestedSnapshotChunkIndex, decodedResponse.requestedSnapshotChunkIndex)
        assertEquals(response.sender, decodedResponse.sender)
    }

    @Test
    fun cp_op_round_trips_with_its_stamp() {
        val op = CpOp(1_788_656_400_001L, Command.Cp.LongSet(Key("cp:counter:c"), 5, ttl = Duration.ofSeconds(1)))
        assertEquals(op, roundTrip(op))
        assertEquals(
            CpOp(7, Command.Cp.LongExpire(Key("cp:counter:c"), Duration.ofMillis(1500))),
            roundTrip(CpOp(7, Command.Cp.LongExpire(Key("cp:counter:c"), Duration.ofMillis(1500)))),
        )
        val getAdd = CpOp(8, Command.Cp.LongGetAdd(Key("cp:counter:c"), -3))
        assertEquals(getAdd, roundTrip(getAdd))
    }

    /** A conditional SET carries its `NX`/`XX` into the log, so a follower applies the same rule. */
    @Test
    fun conditional_set_commands_round_trip() {
        listOf(
            Command.Cp.LongSet(Key("cp:counter:c"), 5, condition = Command.Set.Condition.NX),
            Command.Cp.LongSet(Key("cp:counter:c"), 5, Duration.ofSeconds(30), Command.Set.Condition.XX),
            Command.Cp.RefSet(Key("cp:ref:r"), "v".toByteArray(), condition = Command.Set.Condition.XX),
            Command.Cp.RefSet(
                Key("cp:ref:r"),
                "v".toByteArray(),
                Duration.ofMillis(30_000),
                Command.Set.Condition.NX,
            ),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    @Test
    fun lock_commands_round_trip() {
        val key = Key("cp:lock:l")
        listOf(
            Command.Cp.LockTry(key, session = 7, lease = Duration.ofSeconds(30)),
            Command.Cp.LockUnlock(key, session = 7, token = 3),
            Command.Cp.LockRenew(key, session = 7, token = 3, lease = Duration.ofMillis(1500)),
            Command.Cp.LockForceUnlock(key),
            Command.Cp.LockState(key),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    /**
     * Tag 6 was `DECRBY`, a command variant nothing but this decoder ever produced -- the
     * dispatcher folds Redis's `DECRBY` into an `ADD` with a negative delta (CP spec 6.2, which
     * gives `INCRBY` a verb and `DECRBY` none). T62 deleted it and retired the number rather
     * than reusing it, so a peer still holding an old log entry is told what it sent.
     */
    @Test
    fun the_retired_decrby_tag_is_refused() {
        val encoded = CpWire.encode(Command.Cp.LongIncrBy(Key("cp:counter:c"), 4))
        encoded[0] = RETIRED_DECR_BY_TAG

        val refused = assertThrows(IllegalStateException::class.java) { CpWire.decodeCommand(encoded) }

        assertTrue(refused.message.orEmpty().contains("$RETIRED_DECR_BY_TAG"), "names the tag: ${refused.message}")
        assertTrue(refused.message.orEmpty().contains("DECRBY"), "names the command: ${refused.message}")
    }

    @Test
    fun semaphore_commands_round_trip() {
        val key = Key("cp:sem:s")
        listOf(
            Command.Cp.SemInit(key, permits = 5),
            Command.Cp.SemAcquire(key, session = 7, permits = 2),
            Command.Cp.SemRelease(key, session = 7, permits = 2),
            Command.Cp.SemAvailable(key),
            Command.Cp.SemDrain(key, session = 7),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    @Test
    fun latch_commands_round_trip() {
        val key = Key("cp:latch:l")
        listOf(
            Command.Cp.LatchSet(key, count = 3),
            Command.Cp.LatchDown(key),
            Command.Cp.LatchGet(key),
            Command.Cp.LatchReset(key, count = 5),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    /** A reference is opaque bytes, so its commands round-trip byte for byte, TTL and all. */
    @Test
    fun reference_commands_round_trip() {
        val key = Key("cp:ref:r")
        listOf(
            Command.Cp.RefSet(key, byteArrayOf(0, 127, -1)),
            Command.Cp.RefSet(key, "hello".toByteArray(), ttl = Duration.ofMillis(1500)),
            Command.Cp.RefGet(key),
            Command.Cp.RefCas(key, "hello".toByteArray(), byteArrayOf(0, -128)),
            Command.Cp.RefExpire(key, Duration.ofMillis(1500)),
            Command.Cp.RefTtl(key),
            Command.Cp.RefTtl(key, Command.Ttl.Precision.MILLIS),
            Command.Cp.RefPersist(key),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    @Test
    fun session_commands_round_trip() {
        listOf(
            Command.Cp.SessionCreate(Duration.ofSeconds(15)),
            Command.Cp.SessionHeartbeat(session = 7),
            Command.Cp.SessionClose(session = 7),
        ).forEach { assertEquals(CpOp(9, it), roundTrip(CpOp(9, it))) }
    }

    @Test
    fun session_closed_round_trips() {
        assertEquals(SessionClosed(42, session = 7), roundTrip(SessionClosed(42, session = 7)))
    }

    @Test
    fun ttl_tick_round_trips() {
        assertEquals(TtlTick(42), roundTrip(TtlTick(42)))
    }

    @Test
    fun new_term_round_trips() {
        assertEquals(NewTerm(3), roundTrip(NewTerm(3)))
    }

    private companion object {
        /** The first byte of an encoded command is its tag; 6 is the one T62 retired. */
        const val RETIRED_DECR_BY_TAG: Byte = 6
    }
}
