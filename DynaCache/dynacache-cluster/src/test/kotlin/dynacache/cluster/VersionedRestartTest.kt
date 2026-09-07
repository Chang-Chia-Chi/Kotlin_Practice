package dynacache.cluster

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import dynacache.engine.persist.DotCeilingStore
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.persist.SnapshotEngine
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * What a node still knows about its own versions after a crash (T67). The versioned store is the
 * engine's [dynacache.engine.persist.KeyVersions], so the snapshot writes each version down beside
 * its value, the log carries each behind the command that moved it, and recovery hands them back:
 * a restarted node holds every key under the version it held before (I2) and its dot counter
 * resumes above every dot it ever gave a write (C2).
 */
class VersionedRestartTest {

    @TempDir
    lateinit var root: Path

    private val clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
    private val self = NodeId("node-1")
    private val alpha = Key("alpha")
    private val bravo = Key("bravo")
    private val charlie = Key("charlie")

    /**
     * One node's engine, store and local persistence over [dir], as [dynacache.server] assembles
     * them: the store registers itself with the engine, and [ceilings] is the dot ceiling that
     * outlives the process. [crash] is a power cut after the last durable append: the log's file
     * is closed and nothing is saved on the way out.
     */
    private inner class Node(dir: Path, ceilings: DotCeilingStore) {
        val engine = ApEngine(4, clock)
        val counter = DotCounter.of(self, emptyList(), ceilings)
        val store = VersionedStore(engine, counter)
        val snapshots = SnapshotEngine(engine, dir, clock, fsync = FsyncPolicy.ALWAYS)

        init {
            snapshots.restore()
        }

        fun write(command: Command.Keyed): Dvv = store.write(command).get().second

        fun crash() {
            engine.wal?.close()
            engine.close()
        }
    }

    /**
     * The invariant itself: write under versions, snapshot, write more (a fresh key, an overwrite
     * and a delete), crash, recover. Every key is held under the version it was held under before
     * the crash, the keys written after the snapshot included, and the delete comes back as the
     * tombstone it was rather than as a key that never existed.
     */
    @Test
    fun I2_versions_survive_restart() {
        val dir = Files.createDirectories(root.resolve("data"))
        val ceilings = DotCeilingStore.inMemory()
        val before = Node(dir, ceilings)
        val expected = HashMap<Key, Dvv>()

        expected[alpha] = before.write(Command.Set(alpha, "1".toByteArray()))
        expected[bravo] = before.write(Command.Set(bravo, "2".toByteArray()))
        before.snapshots.save()
        // Everything past the cut reaches disk only through the log, and each entry's version
        // with it: a key born after the snapshot, an overwrite of one in it, and a tombstone.
        expected[charlie] = before.write(Command.Set(charlie, "3".toByteArray()))
        expected[alpha] = before.write(Command.Set(alpha, "1-again".toByteArray()))
        expected[bravo] = before.write(Command.Del(bravo))
        before.crash()

        val after = Node(dir, ceilings)

        assertEquals(expected, expected.keys.associateWith { after.store.version(it) })
        assertEquals(Reply.Bulk("1-again".toByteArray()), after.engine.submit(Command.Get(alpha)).get())
        assertEquals(Reply.Bulk("3".toByteArray()), after.engine.submit(Command.Get(charlie)).get())
        val tombstone = requireNotNull(after.store.held(bravo).get()) { "the delete's version is no longer held" }
        assertNull(tombstone.value, "a tombstone comes back as a version with no value")
        assertEquals(expected[bravo], tombstone.dvv)
        after.crash()
    }

    /**
     * C2 across a restart, with the counter derived from the rebuilt table rather than from the
     * ceiling: the node comes back with a dot ceiling that has never been reserved, so the only
     * floor it has is the versions it just restored, and the next write is still above every dot
     * it ever handed out.
     */
    @Test
    fun dvv_no_counter_reuse() {
        val dir = Files.createDirectories(root.resolve("data"))
        val before = Node(dir, DotCeilingStore.inMemory())
        val handedOut = listOf(alpha, bravo, charlie).map { before.write(Command.Set(it, "v".toByteArray())).dot }
        before.snapshots.save()
        val afterTheCut = before.write(Command.Set(alpha, "w".toByteArray())).dot
        before.crash()

        // A ceiling that knows nothing: only the table the restore rebuilds can raise the counter.
        val after = Node(dir, DotCeilingStore.inMemory())
        val next = after.write(Command.Set(bravo, "x".toByteArray())).dot

        assertEquals(self, next.node)
        assertTrue(next.counter > afterTheCut.counter, "$next reuses a counter: $handedOut then $afterTheCut")
        after.crash()
    }

    /**
     * Spec 5.2 through the kit's cluster: a replica that went down and came back off its own
     * snapshot answers the quorum read under the version it always had, so the coordinator finds
     * no divergence, reverts nothing and pushes nothing. Before versions were persisted the
     * replica answered with no version at all and was repaired every time it restarted.
     */
    @Test
    fun restarted_replica_answers_quorum_read_with_its_version() = runTest {
        val cluster = InProcessCluster(nodeCount = 3, n = 3, w = 3, r = 3, scope = backgroundScope)
        val key = Key("orders:4711")
        val (coordinator, replica, contact) = cluster.ring.preferenceList(key, 3)
        cluster.writeVia(coordinator, key, "fresh".toByteArray())
        cluster.drainMessages()
        val held = requireNotNull(cluster.store(replica).version(key))

        val dir = Files.createDirectories(root.resolve(replica.name))
        SnapshotEngine(cluster.engine(replica), dir, clock).save()
        cluster.restart(replica)
        SnapshotEngine(cluster.engine(replica), dir, clock).restore()

        assertEquals(held, cluster.store(replica).version(key), "the restarted replica holds the version it had")
        assertEquals(Reply.Bulk("fresh".toByteArray()), cluster.readVia(contact, key))
        cluster.drainMessages()
        assertEquals(0, cluster.replication(coordinator).divergentReads.get(), "the restarted replica read as behind")
        assertEquals(0, cluster.replication(coordinator).repairsSent.get(), "the restarted replica was repaired")
        cluster.close()
    }
}
