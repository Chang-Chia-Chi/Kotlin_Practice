package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

/** Every entry a leader appends comes back from its wire form equal, stamp and all. */
class CpWireTest {

    private fun roundTrip(operation: Any): Any = CpWire.decodeOperation(CpWire.encodeOperation(operation))

    @Test
    fun cp_op_round_trips_with_its_stamp() {
        val op = CpOp(1_788_656_400_001L, Command.Cp.LongSet(Key("cp:counter:c"), 5, ttl = Duration.ofSeconds(1)))
        assertEquals(op, roundTrip(op))
        assertEquals(
            CpOp(7, Command.Cp.LongExpire(Key("cp:counter:c"), Duration.ofMillis(1500))),
            roundTrip(CpOp(7, Command.Cp.LongExpire(Key("cp:counter:c"), Duration.ofMillis(1500)))),
        )
    }

    @Test
    fun lock_commands_round_trip() {
        val key = Key("cp:lock:l")
        listOf(
            Command.Cp.LockTry(key, session = 7, ttl = Duration.ofSeconds(30)),
            Command.Cp.LockUnlock(key, session = 7, token = 3),
            Command.Cp.LockRenew(key, session = 7, token = 3, ttl = Duration.ofMillis(1500)),
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
