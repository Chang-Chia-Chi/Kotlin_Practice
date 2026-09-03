package sftp.connector.source

import sftp.connector.client.SftpClient
import sftp.connector.error.ConfigurationError
import sftp.connector.transport.RemoteFile
import java.time.Clock
import java.time.Instant
import kotlin.time.Duration

/**
 * Whether a listed file is finished on the server and safe to hand over.
 *
 * A listing shows a file the moment its uploader creates it, which on most uploads is long before
 * the last byte lands. Nothing on the protocol says whether a writer still has it open, so a check
 * is evidence rather than proof: the one exception is a marker the uploader writes when it is
 * done, and that needs the uploader's cooperation. A check runs once per listed file per poll and
 * may keep memory between polls; a file it turns away is looked at again on the next one.
 */
fun interface ReadinessCheck {
    suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness
}

sealed interface Readiness {
    data object Ready : Readiness

    /** Not yet. [reason] is what a log line says about a file that never becomes ready. */
    data class NotReady(val reason: String) : Readiness

    /**
     * Not a file for the consumer at all - a marker, say. Neither handed over nor counted as
     * waiting, so a directory full of them does not read as a directory full of stuck uploads.
     */
    data object Skip : Readiness
}

/** What a check may ask of the world: the server, on a session of its own, and the time. */
class ReadinessContext(private val client: SftpClient, val clock: Clock) {

    /** What the server says about [path] right now, or null when there is nothing there. */
    suspend fun stat(path: String): RemoteFile? = client.stat(path)
}

/**
 * Ready once the size has been seen unchanged [checks] times at least [interval] apart.
 *
 * The observations are spread across polls rather than taken inside one, because taking them
 * inside one means waiting [interval] for every file in the directory, in turn, while holding
 * the listing's session - a hundred files would make a poll take a quarter of an hour. The
 * evidence is the same either way: the size held still for at least that long. A size that
 * changes starts the count over.
 *
 * What it remembers is bounded and forgetting is safe in the only direction that matters: a
 * forgotten file is a new file, which costs it one more poll and never hands over a file early.
 */
class SizeStable(
    val checks: Int,
    val interval: Duration,
    // ponytail: files remembered at once, oldest dropped first; a knob if a directory ever holds more.
    remembered: Int = 10_000,
) : ReadinessCheck {

    init {
        if (checks < 1) throw ConfigurationError("sizeStable needs at least one check, not $checks")
        if (interval <= Duration.ZERO) throw ConfigurationError("sizeStable needs a positive interval, not $interval")
    }

    private class Observed(var size: Long, var counted: Int, var lastCountedAt: Instant)

    private val memory = object : LinkedHashMap<String, Observed>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Observed>?): Boolean = size > remembered
    }

    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness = synchronized(memory) {
        val now = ctx.clock.instant()
        val known = memory[file.path]
        val observed = when {
            known == null || known.size != file.size -> Observed(file.size, 1, now).also { memory[file.path] = it }
            now >= known.lastCountedAt.plusMillis(interval.inWholeMilliseconds) -> known.apply { counted++; lastCountedAt = now }
            else -> known
        }
        if (observed.counted >= checks) Readiness.Ready
        else Readiness.NotReady("size ${file.size} seen ${observed.counted} of $checks times $interval apart")
    }
}

/** Ready once the file was last modified at least [duration] ago. A slow appender fails it until it stops. */
class MinAge(val duration: Duration) : ReadinessCheck {

    init {
        if (duration <= Duration.ZERO) throw ConfigurationError("minAge needs a positive duration, not $duration")
    }

    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness {
        val oldEnough = file.modifiedAt.plusMillis(duration.inWholeMilliseconds)
        return if (!ctx.clock.instant().isBefore(oldEnough)) Readiness.Ready
        else Readiness.NotReady("modified at ${file.modifiedAt}, less than $duration ago")
    }
}

/**
 * Ready once `<path><suffix>` exists, which is the uploader saying it has finished. The only check
 * here that cannot be fooled, and the only one that needs the uploader to know about it. The
 * markers themselves are skipped, not handed over.
 */
class MarkerFile(val suffix: String) : ReadinessCheck {

    init {
        if (suffix.isEmpty()) throw ConfigurationError("markerFile needs a suffix, or every file would be its own marker")
    }

    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness = when {
        file.name.endsWith(suffix) -> Readiness.Skip
        ctx.stat(file.path + suffix) != null -> Readiness.Ready
        else -> Readiness.NotReady("no marker at ${file.path}$suffix")
    }
}

/** Every check in turn; the first that is not [Readiness.Ready] is the answer. */
class AllOf(vararg checks: ReadinessCheck) : ReadinessCheck {

    val checks: List<ReadinessCheck> = checks.toList()

    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness {
        for (each in checks) {
            val readiness = each.check(file, ctx)
            if (readiness != Readiness.Ready) return readiness
        }
        return Readiness.Ready
    }
}

operator fun ReadinessCheck.plus(other: ReadinessCheck): ReadinessCheck =
    if (this is AllOf) AllOf(*checks.toTypedArray(), other) else AllOf(this, other)
