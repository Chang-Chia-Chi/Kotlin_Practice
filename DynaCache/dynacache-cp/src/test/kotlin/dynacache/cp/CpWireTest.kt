package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import io.microraft.model.message.InstallSnapshotRequest
import io.microraft.model.message.InstallSnapshotResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Duration

/** Every entry a leader appends comes back from its wire form equal, stamp and all. */
class CpWireTest {

    private fun roundTrip(operation: Any): Any = CpWire.decodeOperation(CpWire.encodeOperation(operation))

    /** A snapshot with one row per primitive, the shape the store and the wire both carry. */
    private val snapshot = CpStateMachine.Snapshot(
        lastAppliedTs = 1_788_656_400_001L,
        counters = mapOf(Key("cp:counter:c") to AtomicLongStateMachine.Counter(7, expiresAt = 1_788_656_500_000L)),
        locks = mapOf(Key("cp:lock:l") to FencedLockStateMachine.Lock(owner = 3, token = 9, leaseUntil = 1_788_656_430_000L, holds = 2)),
        semaphores = mapOf(Key("cp:sem:s") to SemaphoreStateMachine.Semaphore(available = 1, holders = mapOf(3L to 2))),
        latches = mapOf(Key("cp:latch:l") to 5),
        references = mapOf(Key("cp:ref:r") to AtomicReferenceStateMachine.Reference(byteArrayOf(0, 127, -1), expiresAt = null)),
        sessions = SessionRegistry.State(lastId = 3, sessions = mapOf(3L to SessionRegistry.Session(1_788_656_400_000L, 15_000))),
    )

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
}
