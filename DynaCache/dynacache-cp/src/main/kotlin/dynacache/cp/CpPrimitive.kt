package dynacache.cp

/**
 * One CP primitive (CP spec 3), seen by the composite [CpStateMachine]: its own table, and the
 * three things the composite does to every primitive alike. What a primitive does with a command
 * stays on the primitive itself, typed: the composite's `when` over the sealed `Command.Cp` is
 * what routes a command, and the compiler checks that routing is total.
 *
 * A snapshot is a list of (id, bytes), so only the primitive ever reads inside its own table
 * (CP spec 10.7): adding a primitive is a new class, a line in the composite's list, a branch in
 * its `when`, and nothing at all in the wire codec.
 */
interface CpPrimitive {

    /** This primitive's byte in a snapshot; one of [CpPrimitive]'s constants. */
    val id: Int

    /** A TTL tick at log time [now]; a primitive without leases or TTLs keeps the default. */
    fun sweep(now: Long) = Unit

    /**
     * A session's death, in the one entry that ends it (C18, I15): the composite offers it to
     * every primitive, so a primitive that holds nothing on behalf of a session keeps the default.
     */
    fun releaseAllOf(session: Long) = Unit

    /** This primitive's table, as bytes only this primitive reads. */
    fun snapshot(): ByteArray

    /** Replaces this primitive's table with what [snapshot] wrote. */
    fun restore(bytes: ByteArray)

    /**
     * The id of each primitive, spelled out and never reused: a snapshot on disk outlives the
     * order the composite happens to list its primitives in.
     */
    companion object {
        const val LONGS = 1
        const val LOCKS = 2
        const val SEMAPHORES = 3
        const val LATCHES = 4
        const val REFERENCES = 5
        const val SESSIONS = 6
    }
}
