package dynacache.engine.persist

import dynacache.engine.Key
import dynacache.engine.PartitionId

/**
 * The versions a node holds its keys under, as bytes this module never looks inside (T67). A
 * version is the cluster's dotted version vector (spec 2.5) and version vectors live in the
 * cluster module, which the engine cannot depend on; the engine's part is only to write those
 * bytes down beside the value they belong to, carry them behind the command that moved them, and
 * hand them back after a crash. That is what lets a restarted node take part in quorum reads,
 * read repair and anti-entropy with the authority it had (C2, I2).
 *
 * The pair is never split, because both reading directions run where the value is read or
 * written: [on] inside the snapshot's cut and [of] right after the command that moved the
 * version, each on the partition's own thread. [restored] runs during recovery, before the node
 * serves anyone.
 *
 * One implementation persists anything -- the cluster's versioned store, which registers itself
 * with the engine it is built over -- and [NONE] is a node with no cluster beside it.
 */
interface KeyVersions {

    /** Every key [partition] holds a version for, live value or not: a tombstone is a version alone. */
    fun on(partition: PartitionId): Map<Key, ByteArray>

    /** The version held for [key], [NO_VERSION] when none is. */
    fun of(key: Key): ByteArray

    /** Recovery: [key] was persisted under [version], and is held under it again. */
    fun restored(key: Key, version: ByteArray)

    companion object {

        /** What a key with no version reads and writes as: no bytes at all. */
        val NO_VERSION = ByteArray(0)

        /** A node that versions nothing: it holds none, offers none, and takes none back. */
        val NONE: KeyVersions = object : KeyVersions {
            override fun on(partition: PartitionId): Map<Key, ByteArray> = emptyMap()
            override fun of(key: Key): ByteArray = NO_VERSION
            override fun restored(key: Key, version: ByteArray) = Unit
        }
    }
}
