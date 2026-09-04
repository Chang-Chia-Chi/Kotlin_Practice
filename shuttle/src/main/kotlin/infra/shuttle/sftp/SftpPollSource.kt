package infra.shuttle.sftp

import infra.shuttle.core.AckAction
import infra.shuttle.core.Digest
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.Fetcher
import infra.shuttle.core.FileReadiness
import infra.shuttle.core.HostKey
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStore
import infra.shuttle.core.Source
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.SourceView
import infra.shuttle.core.StagedObject
import infra.shuttle.core.of
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import sftp.connector.client.LocalFile
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.PostAction
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.source.AllOf
import sftp.connector.source.MinAge
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent
import sftp.connector.source.SftpSource
import sftp.connector.source.SizeStable
import sftp.connector.source.plus
import sftp.connector.transport.RemoteFile
import java.io.IOException
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import sftp.connector.config.Digest as ConnectorDigest

/**
 * Spec 5.1's `poll` trigger on SFTP: the connector's `watch(directory, every)` read as a route's
 * `RouteEvent` flow, with the connector's per-file ack and nack passed through and its `download`
 * as the route's [Fetcher].
 *
 * The connector's in-flight set is the one truth about what is out (spec 4.6, D1): a path in
 * flight is not handed over again until its file is settled, a watch that ends gives back every
 * file it handed over, and its `PollCompleted` says what is still out and whether the listing was
 * cut short. This module decides nothing of its own. [RouteEvent.PollCompleted]'s `listed` is the
 * connector's `inFlight` plus what this tick emitted, because spec 4.6 acks every STORED row a
 * complete listing did not name and a file between its store and its move is exactly such a row.
 *
 * The one thing kept here is a handle table, not a mirror: the `FileSeen` a path was handed over
 * on, so that the fetcher downloads through the hand-over that launched the pipeline - the
 * connector checks the bytes against the size it listed, which is what keeps a re-drop between
 * listing and fetch from landing under the first file's identity. Nothing is decided from the
 * table; it is written on hand-over and cleared on the answer.
 *
 * The flow never throws (ticket 07 deviation 4): a watch that ends on a failure no tick could
 * survive - a rejected password, a rejected host key - ends with one [RouteEvent.RouteDown] and
 * completes, and the runner's supervisor restarts the route.
 */
class SftpPollSource(
    private val source: SftpSource,
    private val route: RouteName,
    private val poll: Source.Poll,
    private val clock: Clock,
) {

    private val handles = ConcurrentHashMap<String, SftpEvent.FileSeen>()

    /**
     * The route's events, in the order the watch produces them. `PollStarted` is not an event of
     * its own here: it is where the poll's `startedAt` is read, before the listing, as spec 4.6
     * needs it.
     */
    fun events(): Flow<RouteEvent> = flow {
        var startedAt = clock.instant()
        var emitted = mutableSetOf<SourceIdentity>()
        source.watch(poll.directory, poll.every).collect { event ->
            when (event) {
                is SftpEvent.PollStarted -> {
                    startedAt = clock.instant()
                    emitted = mutableSetOf()
                }
                is SftpEvent.FileSeen -> {
                    handles[event.file.path] = event
                    val identity = identityOf(event.file)
                    emitted += identity
                    emit(seenEvent(event, identity))
                }
                is SftpEvent.PollCompleted -> emit(
                    RouteEvent.PollCompleted(
                        startedAt = startedAt,
                        listed = emitted + event.inFlight.map(::identityOf),
                        truncated = event.truncated,
                    ),
                )
                is SftpEvent.PollFailed -> emit(RouteEvent.PollFailed(event.error))
                is SftpEvent.PollSkipped -> emit(RouteEvent.PollSkipped)
            }
        }
    }.catch { emit(RouteEvent.RouteDown(it)) }

    /**
     * Spec 4.1 stage 1: the connector's download of the file [path] names, onto the staging path
     * the pipeline chose. A file that has gone from the server since it was listed answers null,
     * and the connector has already given its place back; the [IOException] here is what makes the
     * pipeline count an attempt and nack, and that nack is then a no-op the connector logs.
     */
    val fetcher: Fetcher = ::fetch

    private suspend fun fetch(path: String, into: Path, algorithm: DigestAlgorithm): StagedObject {
        val seen = checkNotNull(handles[path]) { "$path is not a file this poll handed over" }
        val local = seen.download(into) ?: throw IOException("$path has gone from the server since it was listed")
        return staged(local, seen.file.name, seen.file.modifiedAt, algorithm)
    }

    private fun identityOf(file: RemoteFile) = SourceIdentity(
        route = route,
        sourceKind = SourceKind.SFTP,
        sourceRef = "${poll.store}:${poll.directory}",
        sourceName = file.name,
        sourceSize = file.size,
        sourceMtime = file.modifiedAt,
    )

    /**
     * Spec 5.3: the ack is the connector's configured post action - move, delete or none - and the
     * nack is the connector's, which for a polled file is always none: the file stays and the next
     * poll is its redelivery. Both drop the handle whatever the action does - and only their own,
     * so an answer that lands after the connector has handed the path over again does not drop
     * the newcomer's.
     */
    private fun seenEvent(seen: SftpEvent.FileSeen, identity: SourceIdentity) = RouteEvent.Seen(
        identity = identity,
        source = SourceView(seen.file.path),
        ack = { answering(seen) { seen.ack() } },
        nack = { redeliver -> answering(seen) { seen.nack(NackedByRoute(route.value), redeliver) } },
    )

    private suspend fun answering(seen: SftpEvent.FileSeen, answer: suspend () -> Unit) {
        try {
            answer()
        } finally {
            handles.remove(seen.file.path, seen)
        }
    }

    private class NackedByRoute(route: String) : RuntimeException("route $route did not process this file this time")
}

/**
 * Spec 5.1's fetch by path: the object a message named, on a store nothing polls. There is no
 * `FileSeen` to download through, so the path is stat'd and the entry it answers with is downloaded
 * onto the staging path the pipeline chose. A path that is not there is the [IOException] the
 * pipeline counts as an attempt, the same answer a polled file that has gone gives.
 */
fun sftpFetcher(client: SftpClient): Fetcher = { path, into, algorithm ->
    val remote = client.stat(path) ?: throw IOException("$path is not on the server")
    staged(client.download(remote, into), remote.name, remote.modifiedAt, algorithm)
}

/**
 * What a connector download is to the pipeline. The connector already summed the bytes as they
 * streamed, so re-reading the file is only for an algorithm the connector has no name for.
 */
private fun staged(local: LocalFile, name: String, mtime: Instant, algorithm: DigestAlgorithm) = StagedObject(
    name = name,
    path = local.path,
    size = local.size,
    mtime = mtime,
    digest = if (local.digestAlgorithm == connectorDigest(algorithm)) Digest(algorithm, local.digest) else Digest.of(local.path, algorithm),
    contentType = null,
)

/**
 * Spec 13.1's `sftp` object store and one route's `poll` - or no poll at all - as the connector's
 * own configuration.
 *
 * Everything the connector checks it checks here, so a store that cannot make a connector says so
 * at boot. [resolve] turns a [Secret] into its value; reading the environment is the host's
 * (ticket 14). [algorithm] is the route's digest, which becomes the connector's staging digest so
 * that the sum the download already computed is the one the pipeline wants.
 *
 * [sessions] and [transfers] are rule 9's arithmetic as a pool: how many places the caller has
 * budgeted this connector, and how many of them may be carrying bytes at once. They are stated
 * here rather than applied to the finished configuration, because a connector only exists once the
 * builder has checked the numbers against each other - transfers that outnumber sessions could
 * never all run, and are refused at boot instead of quietly capped at the first busy minute.
 */
fun sftpConnectorConfig(
    store: SftpStore,
    poll: Source.Poll?,
    algorithm: DigestAlgorithm,
    resolve: (Secret) -> String,
    sessions: Int,
    transfers: Int,
): SftpConnectorConfig = sftpConnector(store.name) {
    endpoint {
        host = store.host.orEmpty()
        port = store.port
    }
    auth { password(store.user?.let(resolve).orEmpty(), store.password?.let(resolve).orEmpty()) }
    hostKey = when (val policy = store.hostKey) {
        is HostKey.Strict -> HostKeyPolicy.Strict(policy.knownHosts)
        HostKey.AcceptAll -> HostKeyPolicy.AcceptAll
    }
    pool {
        maxSize = sessions
        minIdle = minOf(minIdle, sessions)
        keepAlive = store.keepAlive
        idleTimeout = store.idleTimeout
        idleCutoff = store.idleCutoff
        drainTimeout = store.drainTimeout
        cancelGrace = store.cancelGrace
    }
    resilience { bulkhead { maxConcurrentTransfers = transfers } }
    // A store used only as a target or as a subscribed route's `fetch.store` states no `poll`, and
    // then no polling block at all: no directory, no `onAck`, nothing for the start-up probe to
    // check - which is what lets one connector serve every route on such a store (progress 18).
    if (poll != null) polling {
        directories(poll.directory)
        onAck = postAction("onAck", poll.onAck)
        onNack = postAction("onNack", poll.onNack)
        readiness = readinessOf(poll.readiness)
        // Spec 5.1: one listing of a directory at a time; a tick that finds the last one still
        // running is a PollSkipped rather than a second lister.
        overlap = OverlapPolicy.SKIP
        store.staging?.let { declared -> staging { dir = declared.dir; digest = connectorDigest(algorithm) } }
    }
}

/**
 * Spec 5.3's poll vocabulary; rule 12 has already refused anything else by the time a route runs.
 *
 * `callback` is an ack action of any trigger, so it reaches a poll too, and what it asks of the file
 * is nothing: the channel is the pipeline's own call before the ACKED write (ticket 19), and the
 * connector is left doing what `none` does - the file stays, and D40 bounds the re-checks.
 */
private fun postAction(knob: String, action: AckAction?): PostAction = when (action) {
    null, AckAction.None, is AckAction.Callback -> PostAction.Noop
    is AckAction.Move -> PostAction.Move(action.folder)
    AckAction.Delete -> PostAction.Delete
    else -> throw IllegalArgumentException("$knob: $action is not something a poll on SFTP can do to a file")
}

/** An empty list is a store whose files are ready the moment they are listed; the connector reads that as `AllOf()`. */
private fun readinessOf(checks: List<FileReadiness>): ReadinessCheck =
    checks.map {
        when (it) {
            is FileReadiness.SizeStable -> SizeStable(it.checks, it.interval)
            is FileReadiness.MinAge -> MinAge(it.age)
        }
    }.reduceOrNull { all, next -> all + next } ?: AllOf()

/** SHA1 has no name in the connector, so its downloads are summed with SHA256 and the pipeline re-reads. */
private fun connectorDigest(algorithm: DigestAlgorithm): ConnectorDigest =
    if (algorithm == DigestAlgorithm.MD5) ConnectorDigest.MD5 else ConnectorDigest.SHA256
