package infra.snapshotcache.bootstrap

import infra.snapshotcache.api.GenerationSource
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.RefreshResult
import infra.snapshotcache.api.ShuttingDownException
import infra.snapshotcache.api.SnapshotCacheConfig
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * The composition root of plan 2.2's 2026-08-30 amendment, exercised only through what a
 * downstream module can actually see: [openSnapshotCache] and the two `api` surfaces the
 * returned [ManagedSnapshotCache] exposes. Nothing here names `core`, which is the point -
 * if the seam needs an internal type to be usable, it has not closed the gap it exists for.
 *
 * Real DuckDB, following the P7/P8 precedent: the thing under test is the wiring, and a
 * fake store would wire something that is not the production graph.
 */
internal class OpenSnapshotCacheTest {

    private val orders = GroupId("orders")
    private val customers = GroupId("customers")

    /** Two rows with distinct ids: enough to pass the non-disableable `non_empty` and `key_unique` gate. */
    private fun source(tag: String) = GenerationSource { ctx ->
        ctx.target.createStatement().use { st ->
            st.execute("CREATE TABLE t_a (id BIGINT NOT NULL, name VARCHAR NOT NULL)")
            st.execute("INSERT INTO t_a VALUES (1, '$tag-g${ctx.generation}-1'), (2, '$tag-g${ctx.generation}-2')")
        }
    }

    private fun config(root: Path, clearStale: Boolean = true) = SnapshotCacheConfig(
        storagePath = root.resolve("cache"),
        tempDirectory = root.resolve("spill"),
        servingMemoryLimit = "200MB",
        clearStaleFilesOnStartup = clearStale,
        leaseDrainTimeout = Duration.ofMillis(200),
    )

    @Test
    fun `serves both groups through the public seam, one directory per group`(@TempDir root: Path) {
        val cfg = config(root)
        val managed = openSnapshotCache(
            config = cfg,
            sources = mapOf(orders to source("orders"), customers to source("customers")),
        )
        try {
            assertThat(managed.admin.triggerRefresh(orders).result).isEqualTo(RefreshResult.SUCCESS)
            assertThat(managed.admin.triggerRefresh(customers).result).isEqualTo(RefreshResult.SUCCESS)

            // Spec 3.1's /data/cache/<group>/ layout, derived from GroupId - two groups can
            // no longer be pointed at one directory and collide on gen_0000000001.db.
            assertThat(generationFiles(cfg.storagePath.resolve("orders"))).containsExactly("gen_0000000001.db")
            assertThat(generationFiles(cfg.storagePath.resolve("customers"))).containsExactly("gen_0000000001.db")

            // Acquire and release entirely through api types; the content proves the two
            // groups are served from their own files rather than a shared one.
            managed.cache.withSnapshot(orders) { snap ->
                assertThat(snap.generation).isEqualTo(1L)
                assertThat(nameOfRowOne(snap.connection())).isEqualTo("orders-g1-1")
            }
            managed.cache.withSnapshot(customers) { snap ->
                assertThat(nameOfRowOne(snap.connection())).isEqualTo("customers-g1-1")
            }

            // The lease-accounting model of the P8 end-of-test assertions: files on disk
            // correspond exactly to live generations, and every refcount is back to zero.
            for (group in listOf(orders, customers)) {
                val live = managed.admin.liveGenerations(group)
                assertThat(live.map { it.generation }).containsExactly(1L)
                assertThat(live.single().refCount).isZero()
                assertThat(live.single().leases).isEmpty()
            }
        } finally {
            managed.close()
        }

        // Post-close leak evidence: an undetached generation or an unclosed serving
        // instance leaves a Windows file lock, so a clean recursive delete is proof there
        // is neither. Spec 10.2 step 1 is checked by the refusal that follows.
        deleteRecursively(cfg.storagePath)
        assertThat(Files.exists(cfg.storagePath)).isFalse()
        assertThatThrownBy { managed.cache.acquire(orders, Duration.ZERO) }
            .isInstanceOf(ShuttingDownException::class.java)
        managed.close() // idempotent
    }

    @Test
    fun `startup wipe collects stale files when the config says so, and only then`(@TempDir root: Path) {
        val stale = { cfg: SnapshotCacheConfig ->
            val dir = Files.createDirectories(cfg.storagePath.resolve("orders"))
            Files.write(dir.resolve("gen_0000000009.db.tmp"), byteArrayOf(1, 2, 3))
            dir
        }

        val wiping = config(root.resolve("on"), clearStale = true)
        val dirWiped = stale(wiping)
        openSnapshotCache(wiping, mapOf(orders to source("orders"))).use {
            assertThat(generationFiles(dirWiped))
                .describedAs("spec 10.1 step 1: an unowned leftover is deleted at startup")
                .isEmpty()
        }

        // The other half of the assertion: the wipe is driven by the config field, not
        // unconditional. Without this, a hardcoded wipe would pass the case above.
        val keeping = config(root.resolve("off"), clearStale = false)
        val dirKept = stale(keeping)
        openSnapshotCache(keeping, mapOf(orders to source("orders"))).use {
            assertThat(generationFiles(dirKept)).containsExactly("gen_0000000009.db.tmp")
        }
    }

    /**
     * The review's blocking finding. `DuckDbGenerationStore.close()` closes every connection
     * it issued; a lease still outstanding after the drain means a consumer thread may be
     * mid-query on one, and a DuckDB connection used from two threads crashes the JVM rather
     * than raising - so the sweep would turn a slow consumer into a SIGSEGV that no
     * `runCatching` can catch. A dirty drain must therefore leave the stores alone.
     *
     * Self-managed temp directory rather than `@TempDir`: this test ends with the DuckDB
     * instances deliberately still open, so per-test cleanup would fail on the Windows file
     * locks. Best-effort teardown, on the P8 end-to-end precedent.
     *
     * The clean-drain half of the split is asserted by the first test in this class, whose
     * post-`close()` recursive delete only succeeds if every handle was released.
     */
    @Test
    fun `a timed-out drain leaves reader connections open instead of closing them under a live query`() {
        val root = Files.createTempDirectory("snapshotcache-bootstrap-dirty-drain")
        try {
            val cfg = config(root)
            val managed = openSnapshotCache(cfg, mapOf(orders to source("orders")))
            assertThat(managed.admin.triggerRefresh(orders).result).isEqualTo(RefreshResult.SUCCESS)

            // Never closed: this is the consumer that outlives the drain budget.
            val leaked = managed.cache.acquire(orders, Duration.ZERO)
            val reader = leaked.connection()
            assertThat(reader.isClosed).isFalse()

            managed.close()

            assertThat(reader.isClosed)
                .describedAs("a connection the consumer may still be querying must survive a dirty drain")
                .isFalse()
            // Still usable, which is the actual guarantee - "not closed" would also be true
            // of a connection the store had wrecked some other way.
            assertThat(nameOfRowOne(reader)).isEqualTo("orders-g1-1")

            leaked.close()
        } finally {
            runCatching {
                Files.walk(root).use { paths ->
                    paths.sorted(Comparator.reverseOrder()).forEach { runCatching { Files.delete(it) } }
                }
            }
        }
    }

    /**
     * With the wipe off, a leftover is unowned but not unreachable. Numbering restarted at 1,
     * so `promote`'s ATOMIC_MOVE would have overwritten a lower-numbered leftover, and a
     * higher-numbered one would never be reclaimed because no registry record ever names it.
     */
    @Test
    fun `numbering starts above the highest file on disk when the wipe is off`(@TempDir root: Path) {
        val cfg = config(root, clearStale = false)
        val dir = Files.createDirectories(cfg.storagePath.resolve("orders"))
        Files.write(dir.resolve("gen_0000000015.db"), byteArrayOf(1, 2, 3))

        openSnapshotCache(cfg, mapOf(orders to source("orders"))).use { managed ->
            val outcome = managed.admin.triggerRefresh(orders)
            assertThat(outcome.result).isEqualTo(RefreshResult.SUCCESS)
            assertThat(outcome.generation)
                .describedAs("the first build must land above the leftover, not on top of it")
                .isEqualTo(16L)
            assertThat(generationFiles(dir))
                .describedAs("the leftover must survive intact, not be overwritten by promote")
                .contains("gen_0000000015.db", "gen_0000000016.db")
        }
    }

    /**
     * Spec 10.1 step 1 says every `gen_*` file under the cache directory. Two shapes the
     * per-group pass missed: the flat layout that predates per-group directories, and a group
     * dropped from the config - precisely the directory nothing would ever revisit. The
     * filename pattern is the safety here, not group membership.
     */
    @Test
    fun `the wipe collects the flat layout and directories of groups no longer served`(@TempDir root: Path) {
        val cfg = config(root)
        val flat = Files.createDirectories(cfg.storagePath)
        Files.write(flat.resolve("gen_0000000042.db"), byteArrayOf(1))
        Files.write(flat.resolve("gen_0000000042.db.wal"), byteArrayOf(2))
        val orphan = Files.createDirectories(cfg.storagePath.resolve("group-we-stopped-serving"))
        Files.write(orphan.resolve("gen_0000000007.db.tmp"), byteArrayOf(3))
        Files.write(orphan.resolve("gen_0000000007.db.tmp.wal"), byteArrayOf(4))
        // Not a generation file: the pattern is what makes deleting inside a caller's
        // directory defensible, so anything else must be left exactly where it is.
        Files.write(orphan.resolve("notes.txt"), byteArrayOf(5))

        openSnapshotCache(cfg, mapOf(orders to source("orders"))).use {
            assertThat(generationFiles(flat).filter { name -> name.startsWith("gen_") })
                .describedAs("the flat layout is under the cache directory too")
                .isEmpty()
            assertThat(generationFiles(orphan)).containsExactly("notes.txt")
        }
    }

    private fun generationFiles(dir: Path): List<String> =
        Files.list(dir).use { entries -> entries.map { it.fileName.toString() }.sorted().toList() }

    private fun nameOfRowOne(connection: java.sql.Connection): String =
        connection.createStatement().use { st ->
            st.executeQuery("SELECT name FROM t_a WHERE id = 1").use { rs ->
                check(rs.next()) { "expected a row" }
                rs.getString(1)
            }
        }

    private fun deleteRecursively(root: Path) {
        Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) } }
    }
}
