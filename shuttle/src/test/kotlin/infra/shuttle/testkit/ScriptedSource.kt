package infra.shuttle.testkit

import infra.shuttle.core.Digest
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.Fetcher
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.SourceView
import infra.shuttle.core.StagedObject
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import java.io.IOException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Clock
import java.time.Instant
import java.util.Collections
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap

/**
 * The trigger of spec 5.1 as a script (D20): each step appends one `RouteEvent`; `events()` is a cold
 * flow that replays them. Every `Seen` carries an ack and a nack that record into [acks] and [nacks].
 */
class ScriptedSource(private val clock: Clock) {
    data class Nack(val identity: SourceIdentity, val redeliver: Boolean)

    private val steps = mutableListOf<RouteEvent>()
    val acks: MutableList<SourceIdentity> = Collections.synchronizedList(mutableListOf())
    val nacks: MutableList<Nack> = Collections.synchronizedList(mutableListOf())

    fun seen(identity: SourceIdentity, source: SourceView = SourceView(identity.sourceName)) =
        step(RouteEvent.Seen(identity, source, ack = { acks += identity }, nack = { nacks += Nack(identity, it) }))
    fun pollCompleted(listed: Set<SourceIdentity>, truncated: Boolean = false) = step(RouteEvent.PollCompleted(clock.instant(), listed, truncated))
    fun pollFailed(cause: Throwable) = step(RouteEvent.PollFailed(cause))
    fun pollSkipped() = step(RouteEvent.PollSkipped)
    fun routeDown(cause: Throwable) = step(RouteEvent.RouteDown(cause))

    fun events(): Flow<RouteEvent> = steps.toList().asFlow()

    private fun step(event: RouteEvent) = apply { steps += event }

    companion object {
        fun identity(name: String, route: String = "drop", directory: String = "sftp:/in", size: Long = 10, mtime: Instant = Instant.EPOCH) =
            SourceIdentity(RouteName(route), SourceKind.SFTP, directory, name, size, mtime)
    }
}

/** The `Fetcher` of spec 4.1 stage 1 over scripted bytes; `gone` makes a path throw `NoSuchFileException`, `failNext` an `IOException`, one-shot. */
class ScriptedFetcher(private val clock: Clock) : Fetcher {
    data class Call(val path: String, val into: Path, val algorithm: DigestAlgorithm)

    private val files = ConcurrentHashMap<String, ByteArray>()
    val calls: MutableList<Call> = Collections.synchronizedList(mutableListOf())
    @Volatile var failNext = false

    fun file(path: String, bytes: ByteArray) = apply { files[path] = bytes }
    fun gone(path: String) = apply { files.remove(path) }

    override suspend fun invoke(path: String, into: Path, algorithm: DigestAlgorithm): StagedObject {
        calls += Call(path, into, algorithm)
        if (failNext) {
            failNext = false
            throw IOException("injected: fetch failed")
        }
        val bytes = files[path] ?: throw NoSuchFileException(path)
        Files.write(into, bytes)
        return StagedObject(path.substringAfterLast('/'), into, bytes.size.toLong(), clock.instant(), digestOf(bytes, algorithm), null)
    }
}

internal fun digestOf(bytes: ByteArray, algorithm: DigestAlgorithm): Digest {
    val jca = when (algorithm) { DigestAlgorithm.MD5 -> "MD5"; DigestAlgorithm.SHA256 -> "SHA-256"; DigestAlgorithm.SHA1 -> "SHA-1" }
    return Digest(algorithm, HexFormat.of().formatHex(MessageDigest.getInstance(jca).digest(bytes)))
}
