package infra.snapshotarchive

import infra.snapshotcache.api.CopyOutResult
import infra.snapshotcache.api.CopyOutSpec
import infra.snapshotcache.api.GenerationInfo
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.Snapshot
import infra.snapshotcache.api.SnapshotCache
import io.minio.MinioClient
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.SimpleFormatter

/*
 * Shared fakes for the archive layer's three suites.
 *
 * Tickets 03, 04 and 05 each grew their own near-identical copies, recorded each time as a
 * deviation on the grounds that hoisting would mean editing an earlier phase's test. That
 * reasoning does not hold - a new file edits nothing - and by the third copy the duplication
 * was the larger problem. Consolidated here after the M3 review.
 *
 * Each fake is a superset of the three it replaces, so no suite lost a capability: hooks that
 * only one suite used are simply null in the others.
 */

/**
 * A [SnapshotCache] over one real DuckDB generation file, attached READ_ONLY exactly as the
 * serving store attaches it.
 *
 * The archive layer is a consumer of the public API (D30) and its tests consume the same
 * surface - hence a hand-written fake rather than reaching into `core`, which ArchUnit would
 * have permitted in tests but which would have made these suites depend on framework
 * internals they have no business knowing.
 *
 * [dataAsOf] is deliberately mutable: the diff's whole correctness argument is about which
 * moment the lease was taken at, so a test has to be able to move it.
 */
internal class FileBackedCache(private val file: Path) : SnapshotCache {

    override val defaultWaitBudget: Duration = Duration.ofSeconds(5)

    @Volatile
    var dataAsOf: Instant = DEFAULT_DATA_AS_OF

    @Volatile
    var generation: Long = DEFAULT_GENERATION

    /** Runs on the thread that asks for a connection: the export tasks' observation point. */
    @Volatile
    var onConnection: (() -> Unit)? = null

    val liveLeases = AtomicInteger()

    override fun <T> withSnapshot(group: GroupId, waitBudget: Duration, block: (Snapshot) -> T): T {
        liveLeases.incrementAndGet()
        return FileBackedSnapshot(file, dataAsOf, generation, onConnection) { liveLeases.decrementAndGet() }
            .use(block)
    }

    override fun copyOut(group: GroupId, spec: CopyOutSpec, waitBudget: Duration): CopyOutResult =
        throw NotImplementedError("the archive layer works on the lease's own connection; nothing is copied out")

    override fun acquire(group: GroupId, waitBudget: Duration): Snapshot =
        throw NotImplementedError("withSnapshot scopes the lease so it cannot outlive the run")

    override fun currentInfo(group: GroupId): GenerationInfo? =
        throw NotImplementedError("the archive layer reads generation and dataAsOf off the lease it holds")

    private companion object {
        val DEFAULT_DATA_AS_OF: Instant = Instant.parse("2026-08-29T10:00:00Z")
        const val DEFAULT_GENERATION = 7L
    }
}

internal class FileBackedSnapshot(
    private val file: Path,
    override val dataAsOf: Instant,
    override val generation: Long,
    private val onConnection: (() -> Unit)? = null,
    private val onRelease: () -> Unit = {},
) : Snapshot {

    private val issued = CopyOnWriteArrayList<Connection>()

    override fun connection(): Connection {
        onConnection?.invoke()
        val connection = DriverManager.getConnection("jdbc:duckdb:")
        connection.createStatement().use { statement ->
            statement.execute("ATTACH '${file.toAbsolutePath()}' AS g (READ_ONLY)")
            statement.execute("USE g")
        }
        issued += connection
        return connection
    }

    override fun close() {
        issued.forEach { runCatching { it.close() } }
        onRelease()
    }
}

/**
 * In-memory object store holding real bytes, so a suite can round-trip real Parquet through
 * it while a suite that only cares which keys exist still reads sizes off the same map.
 *
 * The three hooks are each a single suite's observation point: [beforePut] at upload time,
 * [beforeDelete] at the instant an object disappears, [beforeGet] to watch downloads overlap.
 */
internal class RecordingObjectStore : ObjectStore(unusedClient(), "test-bucket") {

    val stored = ConcurrentHashMap<String, ByteArray>()

    @Volatile
    var beforePut: ((String) -> Unit)? = null

    @Volatile
    var beforeDelete: ((String) -> Unit)? = null

    @Volatile
    var beforeGet: ((String) -> Unit)? = null

    override fun put(key: String, file: Path) {
        beforePut?.invoke(key)
        stored[key] = Files.readAllBytes(file)
    }

    override fun sizeOf(key: String): Long? = stored[key]?.size?.toLong()

    override fun delete(key: String) {
        beforeDelete?.invoke(key)
        stored.remove(key)
    }

    override fun get(key: String, file: Path) {
        beforeGet?.invoke(key)
        Files.write(file, requireNotNull(stored[key]) { "no object at '$key'" })
    }

    /** Plants an object of [bytes] length without a source file, for size-only fixtures. */
    fun seed(key: String, bytes: Long) {
        stored[key] = ByteArray(bytes.toInt())
    }

    private companion object {

        /** Never dialled: every method is overridden. Building one opens no socket. */
        fun unusedClient(): MinioClient = MinioClient.builder()
            .endpoint("http://127.0.0.1:1")
            .credentials("unused", "unused")
            .build()
    }
}

/**
 * Captures WARN and SEVERE records at the JUL root. jboss-logging has no other provider on
 * this test classpath, so it falls back to java.util.logging - the same route the D31 skip
 * alert, the watchdog's FAILED verdict and the staleness alert all take.
 */
internal class LogCapture : Handler() {

    private val formatter = SimpleFormatter()
    val messages = CopyOnWriteArrayList<String>()

    /**
     * SEVERE only. [ArchiveMaintenance.sweep] swallows a group's exception so the schedule
     * survives it, so a test that drives `sweep` has to look here or it would pass on a pass
     * that threw.
     */
    val failures = CopyOnWriteArrayList<String>()

    override fun publish(record: LogRecord) {
        if (record.level.intValue() >= Level.WARNING.intValue()) {
            val message = runCatching { formatter.formatMessage(record) }.getOrElse { record.message ?: "" }
            messages += message
            if (record.level.intValue() >= Level.SEVERE.intValue()) failures += message
        }
    }

    override fun flush() = Unit

    override fun close() = Unit

    fun uninstall() {
        java.util.logging.Logger.getLogger("").removeHandler(this)
    }

    companion object {
        fun install(): LogCapture = LogCapture().also { java.util.logging.Logger.getLogger("").addHandler(it) }
    }
}
