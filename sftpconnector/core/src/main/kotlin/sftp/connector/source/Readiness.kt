package sftp.connector.source

import kotlinx.coroutines.delay
import sftp.connector.error.ConfigurationError
import sftp.connector.transport.RemoteFile
import java.time.Clock
import kotlin.time.Duration

/**
 * Whether a listed file is finished on the server and safe to hand over.
 *
 * A listing shows a file the moment its uploader creates it, which on most uploads is long before
 * the last byte lands. Nothing on the protocol says whether a writer still has it open, so a check
 * is evidence rather than proof: the one exception is a marker the uploader writes when it is
 * done, and that needs the uploader's cooperation. A check is asked once per poll, about that
 * poll's candidates; a file it turns away is looked at again on the next one.
 */
fun interface ReadinessCheck {
    suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness

    /**
     * The poll's candidates at once. A check that has to wait answers them together, so the wait
     * is paid once per poll rather than once per file; any other check answers one file at a time
     * and need not override this.
     */
    suspend fun check(files: List<RemoteFile>, ctx: ReadinessContext): Map<RemoteFile, Readiness> =
        files.associateWith { check(it, ctx) }
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

/**
 * What a check may ask of the world: what the server says about a path, and the time.
 *
 * A stat function rather than the client it comes from, because that one call is the whole of
 * what a check has ever needed. Handing over the client made every check's world the pool, the
 * transport and the retry policy behind it, so asking whether a file that grew is turned away
 * meant standing a server up to be asked - and none of that is what the check is about.
 */
class ReadinessContext(
    /** What the server says about a path right now, or null when there is nothing there. */
    val stat: suspend (String) -> RemoteFile?,
    val clock: Clock,
)

/**
 * Ready once the size has been seen unchanged [checks] times, [interval] apart, within the poll
 * that listed the file.
 *
 * The observations are taken over the poll's candidates as a batch: every file is stated, one
 * [interval] passes, every file is stated again. So a poll pays `(checks - 1) x interval` once,
 * however many files it listed, and pays it with the listing's session already back in the pool -
 * which is what makes waiting inside the poll affordable. Remembering sizes across polls instead
 * was built first and taken out: on an hourly pipeline it made every file wait for the second
 * poll, an hour where this reads as ten seconds.
 *
 * A file that is not on the server by the time it is stated is not ready; the next poll will not
 * list it.
 */
class SizeStable(val checks: Int, val interval: Duration) : ReadinessCheck {

    init {
        if (checks < 1) throw ConfigurationError("sizeStable needs at least one check, not $checks")
        if (interval <= Duration.ZERO) throw ConfigurationError("sizeStable needs a positive interval, not $interval")
    }

    /** A file on its own is a batch of one, and costs the same wait. */
    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness = check(listOf(file), ctx).getValue(file)

    // ponytail: one stat at a time; a fan-out bounded by the pool if a directory of a thousand files ever makes this the slow part.
    override suspend fun check(files: List<RemoteFile>, ctx: ReadinessContext): Map<RemoteFile, Readiness> {
        val turnedAway = mutableMapOf<RemoteFile, Readiness>()
        var holdingStill = mutableMapOf<RemoteFile, Long>()
        for (file in files) {
            val size = ctx.stat(file.path)?.size
            if (size == null) turnedAway[file] = GONE else holdingStill[file] = size
        }
        repeat(checks - 1) {
            delay(interval)
            val stillHolding = mutableMapOf<RemoteFile, Long>()
            for ((file, size) in holdingStill) {
                when (val now = ctx.stat(file.path)?.size) {
                    size -> stillHolding[file] = size
                    null -> turnedAway[file] = GONE
                    else -> turnedAway[file] = Readiness.NotReady("size went from $size to $now within $interval")
                }
            }
            holdingStill = stillHolding
        }
        return files.associateWith { turnedAway[it] ?: Readiness.Ready }
    }

    private companion object {
        private val GONE = Readiness.NotReady("gone from the server since it was listed")
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

/**
 * Every check in turn; the first that is not [Readiness.Ready] is the answer. Over a batch, each
 * check is asked only about the files every earlier check let through, so a check that waits
 * waits for nothing already turned away.
 */
class AllOf(vararg checks: ReadinessCheck) : ReadinessCheck {

    val checks: List<ReadinessCheck> = checks.toList()

    override suspend fun check(file: RemoteFile, ctx: ReadinessContext): Readiness {
        for (each in checks) {
            val readiness = each.check(file, ctx)
            if (readiness != Readiness.Ready) return readiness
        }
        return Readiness.Ready
    }

    override suspend fun check(files: List<RemoteFile>, ctx: ReadinessContext): Map<RemoteFile, Readiness> {
        val turnedAway = mutableMapOf<RemoteFile, Readiness>()
        var stillReady = files
        for (each in checks) {
            if (stillReady.isEmpty()) break
            val answers = each.check(stillReady, ctx)
            for (file in stillReady) {
                val answer = answers.getValue(file)
                if (answer != Readiness.Ready) turnedAway[file] = answer
            }
            stillReady = stillReady.filterNot { it in turnedAway }
        }
        return files.associateWith { turnedAway[it] ?: Readiness.Ready }
    }
}

operator fun ReadinessCheck.plus(other: ReadinessCheck): ReadinessCheck =
    if (this is AllOf) AllOf(*checks.toTypedArray(), other) else AllOf(this, other)
