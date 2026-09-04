package infra.shuttle.sftp

import infra.shuttle.core.AckAction
import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryChannel
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.FileReadiness
import infra.shuttle.core.Hook
import infra.shuttle.core.HookPoint
import infra.shuttle.core.Pool
import infra.shuttle.core.ProcessingChain
import infra.shuttle.core.Route
import infra.shuttle.core.RouteEvent
import infra.shuttle.core.RouteName
import infra.shuttle.core.RouteRunner
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStore
import infra.shuttle.core.ShuttleMetrics
import infra.shuttle.core.Source
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.Staging
import infra.shuttle.core.StateStore
import infra.shuttle.core.Target
import infra.shuttle.core.TransferPipeline
import infra.shuttle.core.TransferState
import infra.shuttle.core.gib
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.RecordingChannel
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.error.AuthenticationFailed
import sftp.connector.pool.SftpPool
import sftp.connector.source.SftpSource
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.transport.jsch.JschTransport
import sftp.connector.config.Digest as ConnectorDigest
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.temporal.ChronoUnit
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.milliseconds

/**
 * Spec 5.1 to 5.3 against the connector's embedded SSHD: a real directory feeds a route through the
 * real `RouteRunner`, `TransferPipeline`, in-memory state store and in-memory target. Real time,
 * because a server is not on virtual time; the poll interval is the shortest the connector allows,
 * every wait is a `HookDriver` arrival or a flow's first element under a `withTimeout`, and there
 * is no sleep anywhere.
 */
class SftpPollSourceTest {

    @TempDir lateinit var remoteRoot: Path
    @TempDir lateinit var connectorStage: Path
    @TempDir lateinit var routeStage: Path

    private val clock = ClockFixture()
    private val registry = SimpleMeterRegistry()
    private val store = InMemoryStateStore(clock)
    private val target = InMemoryTarget("landing")
    private var wakes = 0

    @Test
    fun a_file_on_the_server_becomes_one_Seen_whose_identity_is_the_store_directory_name_size_and_mtime() = runBlocking {
        val file = seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            val seen = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.Seen>().first() }

            assertEquals(
                SourceIdentity(
                    RouteName(ROUTE), SourceKind.SFTP, "vendor:/drop", "first.csv",
                    Files.size(file), Files.getLastModifiedTime(file).toInstant().truncatedTo(ChronoUnit.SECONDS),
                ),
                seen.identity,
            )
            assertEquals("/drop/first.csv", seen.source.path, "the fetcher is handed the path the server quoted")
        }
    }

    /** Spec 4.1 stage 4 and D6: the move is visible to the next listing, so it happens after the store and before ACKED. */
    @Test
    fun S1_the_vendor_drop_route_moves_the_file_to_temp_only_after_the_target_holds_it() = runBlocking {
        seed("first.csv", CONTENT)
        val hook = HookDriver().apply { pauseAt(HookPoint.afterStore); pauseAt(HookPoint.afterLedgerAcked) }

        withRoute(AckAction.Move("temp/"), hook) { run ->
            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterStore) }
            assertEquals(setOf("first.csv"), target.keys, "the target holds the object")
            assertEquals(CONTENT, String(target.bytes("first.csv")))
            assertTrue(remoteRoot.resolve("drop/first.csv").exists(), "and the source file has not moved yet")
            assertFalse(remoteRoot.resolve("drop/temp/first.csv").exists())
            hook.resume(HookPoint.afterStore)

            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterLedgerAcked) }
            assertFalse(remoteRoot.resolve("drop/first.csv").exists(), "the ack moved it before the row was written ACKED")
            assertTrue(remoteRoot.resolve("drop/temp/first.csv").exists())
            hook.resume(HookPoint.afterLedgerAcked)

            run.cancelAndJoin()
            assertEquals(TransferState.DONE, store.transfers.single().state, "nobody to notify, so ACKED is DONE")
        }
    }

    /** The other half of spec 5.3's poll vocabulary: `delete` is the same order, with nothing left behind. */
    @Test
    fun S1_the_mirror_route_deletes_the_file_after_the_target_holds_it() = runBlocking {
        seed("first.csv", CONTENT)
        val hook = HookDriver().apply { pauseAt(HookPoint.afterStore); pauseAt(HookPoint.afterLedgerAcked) }

        withRoute(AckAction.Delete, hook) { run ->
            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterStore) }
            assertEquals(setOf("first.csv"), target.keys)
            assertTrue(remoteRoot.resolve("drop/first.csv").exists(), "still there until the target holds it")
            hook.resume(HookPoint.afterStore)

            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterLedgerAcked) }
            assertFalse(remoteRoot.resolve("drop/first.csv").exists(), "and gone once it does")
            hook.resume(HookPoint.afterLedgerAcked)

            run.cancelAndJoin()
            assertEquals(TransferState.DONE, store.transfers.single().state)
        }
    }

    /**
     * Spec 5.2: the listing is a claim, not a fact. The connector answers a download of a file that
     * has gone with null and has already given its place back; the fetcher turns that into a stage
     * error, so the row stays SEEN with one attempt charged, the nack is the no-op the connector
     * logs, and nothing reaches the collector.
     */
    @Test
    fun a_file_removed_between_the_listing_and_the_fetch_leaves_a_SEEN_row_and_no_error() = runBlocking {
        seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            // Collected in a job of its own, because the poll has to stay alive: a `first()` would
            // cancel the watch and give the file back before it could be fetched at all.
            val seens = Channel<RouteEvent.Seen>(Channel.UNLIMITED)
            val collecting = launch { source.events().collect { if (it is RouteEvent.Seen) seens.send(it) } }
            val seen = withTimeout(TIMEOUT) { seens.receive() }
            Files.delete(remoteRoot.resolve("drop/first.csv"))

            pipelineFor(routeOf(AckAction.Move("temp/"))).run(seen, source.fetcher)

            val row = store.transfers.single()
            assertEquals(TransferState.SEEN, row.state, "nothing beyond SEEN")
            assertEquals(1, row.attempts)
            assertTrue(target.keys.isEmpty(), "and nothing stored")
            assertTrue(Files.list(routeStage).use { it.findAny().isEmpty }, "the run's staging directory is gone")
            collecting.cancelAndJoin()
        }
    }

    /**
     * Ticket 41: the handle the fetcher downloads through is the connector's answer to "what is in
     * flight at this path" ([SftpSource.inFlightAt]), and it is the very `FileSeen` the watch
     * emitted - so the connector still checks the bytes against the size *the listing* saw. A file
     * re-dropped at the same path with a different size between the listing and the fetch is
     * refused: the row stays SEEN with one attempt charged, and nothing lands in the target under
     * the first file's identity. This is what the lookup had to preserve when the source's own
     * path-to-handle table went (progress 31 deviation 1).
     */
    @Test
    fun a_file_re_dropped_with_a_different_size_between_the_listing_and_the_fetch_is_refused() = runBlocking {
        val file = seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            // As above: the watch has to stay alive, or the file is given back before it is fetched.
            val seens = Channel<RouteEvent.Seen>(Channel.UNLIMITED)
            val collecting = launch { source.events().collect { if (it is RouteEvent.Seen) seens.send(it) } }
            val seen = withTimeout(TIMEOUT) { seens.receive() }
            file.writeText(CONTENT + "2,7\n")
            assertEquals(CONTENT.length.toLong(), seen.identity.sourceSize, "the identity is the file as it was listed")
            assertTrue(Files.size(file) > CONTENT.length, "and the same path now holds a bigger one")

            pipelineFor(routeOf(AckAction.Move("temp/"))).run(seen, source.fetcher)

            val row = store.transfers.single()
            assertEquals(TransferState.SEEN, row.state, "what arrived is not the file the listing described")
            assertEquals(1, row.attempts)
            assertTrue(target.keys.isEmpty(), "so nothing was stored under the first file's identity")
            assertTrue(Files.list(routeStage).use { it.findAny().isEmpty }, "the run's staging directory is gone")
            collecting.cancelAndJoin()
        }
    }

    /**
     * Ticket 41: a path nothing is in flight at - never listed, already answered, or given back when
     * a watch ended - has no handle to download through. That is the same stage error a file gone
     * from the server gives, so the pipeline charges an attempt and nacks, rather than the
     * `IllegalStateException` a missing entry in a table of the source's own used to be.
     */
    @Test
    fun a_fetch_for_a_path_nothing_is_in_flight_at_is_a_stage_error_naming_the_path() = runBlocking {
        withConnector(AckAction.Move("temp/")) { source ->
            val failed = runCatching {
                source.fetcher("/drop/never-listed.csv", routeStage.resolve("copy"), DigestAlgorithm.MD5)
            }.exceptionOrNull()

            val refused = assertInstanceOf(IOException::class.java, failed, "nothing in flight at the path is a stage error")
            assertTrue("/drop/never-listed.csv" in refused.message.orEmpty(), "which names it: ${refused.message}")
        }
    }

    /**
     * Spec 11 through ticket 07 deviation 4: a rejected password is not a flaky tick, so the watch
     * ends with it. The flow does not throw - it says `RouteDown` and completes, and the supervisor
     * restarts the route on the backoff.
     */
    @Test
    fun a_wrong_password_ends_the_flow_with_RouteDown_as_its_last_event() = runBlocking {
        remoteRoot.resolve("drop").createDirectories()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val poll = pollOf(AckAction.Move("temp/"))
            // Built without SftpConnector.start: its start-up probe would refuse the wrong password
            // before a watch existed, and what is under test is a watch that ends on one.
            val config = connectorConfig(server, poll) { "not the password" }
            val pool = SftpPool(JschTransport(config, registry), config, registry)
            val source = SftpPollSource(SftpSource(SftpClient(pool, config, registry), config, registry), RouteName(ROUTE), poll, clock)

            val events = mutableListOf<RouteEvent>()
            withTimeout(TIMEOUT) { source.events().collect { events += it } }

            val down = assertInstanceOf(RouteEvent.RouteDown::class.java, events.last())
            assertInstanceOf(AuthenticationFailed::class.java, down.cause)
            assertTrue(events.none { it is RouteEvent.PollFailed }, "a rejection is not something the next tick could survive")
        }
    }

    /**
     * Spec 4.6: reconciliation acks every STORED row a complete listing did not name, so a file
     * between its store and its move - in flight, and for that reason not handed over again - has
     * to be in `listed` all the same, or its ledger would say acked while the move had not happened.
     * `listed` is the connector's own `inFlight` plus what the tick emitted; nothing here keeps a
     * copy of that set (ticket 31).
     */
    @Test
    fun a_poll_lists_every_identity_still_in_flight_from_an_earlier_poll() = runBlocking {
        val file = seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            val completions = Channel<RouteEvent.PollCompleted>(Channel.UNLIMITED)
            var handedOver = 0
            val collecting = launch {
                source.events().collect { event ->
                    when (event) {
                        // Fetched and then neither acked nor nacked: the pipeline's window between
                        // the store and the ack, which is the window reconciliation exists for.
                        is RouteEvent.Seen -> {
                            handedOver++
                            source.fetcher(event.source.path, routeStage.resolve("copy-$handedOver"), DigestAlgorithm.MD5)
                        }
                        is RouteEvent.PollCompleted -> completions.send(event)
                        else -> Unit
                    }
                }
            }
            withTimeout(TIMEOUT) { completions.receive() }
            val next = withTimeout(TIMEOUT) { completions.receive() }

            assertEquals(1, handedOver, "a file in flight is not handed over twice")
            assertEquals(setOf(identityOf(file)), next.listed, "and a poll that emitted nothing still names it")
            assertFalse(next.truncated, "one file is nowhere near maxFilesPerPoll")
            collecting.cancelAndJoin()
        }
    }

    /**
     * Spec 4.6: a truncated listing skips the repair. `truncated` is the connector's own flag: its
     * listing stops at the cap without looking past it, so a directory holding exactly the cap
     * reads as truncated, and nothing here reads `maxFilesPerPoll` to guess (ticket 31).
     */
    @Test
    fun a_listing_that_reaches_maxFilesPerPoll_completes_truncated() = runBlocking {
        seed("a.csv", CONTENT)
        seed("b.csv", CONTENT)
        seed("c.csv", CONTENT)

        withConnector(AckAction.Move("temp/"), maxFilesPerPoll = 2) { source ->
            val completed = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.PollCompleted>().first() }

            assertTrue(completed.truncated)
            assertEquals(2, completed.listed.size, "and it names only what it got to")
        }
    }

    /**
     * D40: a finished file that stays under `none` comes back every poll and, inside
     * `recheckFinished`, is skipped without a fetch or a write. The skip gives the file back with a
     * nack (ticket 21), so the connector's place for it comes free and the next poll hands it over
     * again; nothing on the server moves.
     */
    @Test
    fun a_finished_file_skipped_inside_recheckFinished_is_given_back_and_handed_over_again() = runBlocking {
        seed("first.csv", CONTENT)

        withConnector(AckAction.None) { source ->
            val pipeline = pipelineFor(routeOf(AckAction.None))
            val seens = Channel<RouteEvent.Seen>(Channel.UNLIMITED)
            val collecting = launch {
                source.events().collect { if (it is RouteEvent.Seen) { pipeline.run(it, source.fetcher); seens.send(it) } }
            }

            val first = withTimeout(TIMEOUT) { seens.receive() }
            val again = withTimeout(TIMEOUT) { seens.receive() }
            val once_more = withTimeout(TIMEOUT) { seens.receive() }

            assertEquals(first.identity, again.identity, "the same file, because D40's window costs no place for ever")
            assertEquals(again.identity, once_more.identity)
            assertEquals(TransferState.DONE, store.transfers.single().state, "one row, finished on the first hand-over")
            assertEquals(0.0, registry.counter(ShuttleMetrics.TRANSFERS, "route", ROUTE, "outcome", "reacked").count(), "and never fetched again inside the window")
            assertTrue(remoteRoot.resolve("drop/first.csv").exists(), "and the nack that freed it left the file alone")
            collecting.cancelAndJoin()
        }
    }

    /**
     * A route that ends - cancelled, or restarted after `RouteDown` - is a watch that ends, and the
     * connector gives back every file a watch handed over and never had an answer for (connector
     * ticket 19). The source nacks nothing of its own when its flow ends (ticket 31): the file
     * below was fetched from an earlier tick and never answered, and it is the connector's
     * give-back, counted as `cancelled`, that lets the next watch list it again.
     */
    @Test
    fun a_watch_that_ends_gives_back_every_file_it_handed_over_so_the_next_watch_lists_them() = runBlocking {
        val file = seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            val completions = Channel<RouteEvent.PollCompleted>(Channel.UNLIMITED)
            val collecting = launch {
                source.events().collect { event ->
                    // Fetched and never answered, so the D40 sweep leaves it alone: the only thing
                    // that can give this one back is the end of the watch.
                    if (event is RouteEvent.Seen) source.fetcher(event.source.path, routeStage.resolve("copy"), DigestAlgorithm.MD5)
                    if (event is RouteEvent.PollCompleted) completions.send(event)
                }
            }
            withTimeout(TIMEOUT) { completions.receive() }
            val second = withTimeout(TIMEOUT) { completions.receive() }
            assertEquals(setOf(identityOf(file)), second.listed, "still out with the route on the second poll")
            collecting.cancelAndJoin()

            assertEquals(0.0, registry.get("sftp_inflight").gauge().value(), "the connector gave it back the moment the watch ended")
            assertEquals(1.0, registry.get("sftp_ack_total").tag("outcome", "cancelled").counter().count(), "as a watch-ended give-back, not a nack")
            val again = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.Seen>().first() }
            assertEquals(identityOf(file), again.identity)
        }
    }

    /**
     * Review finding B1. Under a backlog - the pipeline slower than `every`, `parallelism` 1 - the
     * D40 give-back used to nack a file whose pipeline had only just started; the next tick handed
     * it over again, the old pipeline fetched on the new hand-over and acked the old one ("already
     * settled", nothing moved), and the new hand-over was never settled: DONE in the ledger, still
     * in the drop directory, and one of the connector's places gone until restart.
     */
    @Test
    fun B1_a_file_whose_pipeline_outlasts_the_poll_interval_is_still_moved_once_and_holds_no_place_afterwards() = runBlocking {
        val files = listOf("a.csv", "b.csv", "c.csv").map { seed(it, CONTENT) }
        val slow = object : StateStore by store {
            override suspend fun find(identity: SourceIdentity) = delay(EVERY * 3).let { store.find(identity) }
        }
        val hook = HookDriver().apply { pauseAt(HookPoint.afterLedgerAcked) }

        withConnector(AckAction.Move("temp/")) { source ->
            val route = routeOf(AckAction.Move("temp/"))
            val pipeline = TransferPipeline(
                route, DigestAlgorithm.MD5, slow, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5),
                emptyMap(), { true }, { wakes++ }, hook, clock, registry, Staging(routeStage), usableSpace = { 10.gib },
            )
            val run = launch { RouteRunner(route, pipeline, source.fetcher, slow, { wakes++ }, clock, registry).run(source.events()) }
            repeat(files.size) {
                withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterLedgerAcked) }
                hook.resume(HookPoint.afterLedgerAcked)
                hook.pauseAt(HookPoint.afterLedgerAcked)
            }
            run.cancelAndJoin()

            assertEquals(files.map { TransferState.DONE }, store.transfers.map { it.state })
            assertEquals(emptyList<Path>(), remoteRoot.resolve("drop").listDirectoryEntries("*.csv"), "every DONE file left the drop directory")
            assertEquals(files.map { it.fileName.toString() }.toSet(), remoteRoot.resolve("drop/temp").listDirectoryEntries().map { it.fileName.toString() }.toSet())
            assertEquals(0.0, registry.get("sftp_inflight").gauge().value(), "and the connector holds no place for any of them")
            val fresh = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.PollCompleted>().first() }
            assertEquals(emptySet<SourceIdentity>(), fresh.listed, "a fresh watch has nothing left to hand over")
        }
    }

    /**
     * D2 as amended by ticket 31: the same name uploaded again, with a new size and mtime, while
     * the first upload is still being worked, is a different identity but the same path, and the
     * connector's in-flight set is path-exclusive (connector D48): the newer copy is not handed
     * over until the first is settled, so a pipeline that is fetching or acking the first never
     * meets the second. The source refuses nothing by hand; what is asserted is the connector's
     * own gauge and the events it did not send.
     */
    @Test
    fun a_path_uploaded_again_while_its_first_file_is_still_in_flight_is_not_handed_over_until_the_first_is_settled() = runBlocking {
        val file = seed("first.csv", CONTENT)
        val hook = HookDriver().apply { pauseAt(HookPoint.afterFetch) }

        withConnector(AckAction.Move("temp/")) { source ->
            val pipeline = pipelineFor(routeOf(AckAction.Move("temp/")), hook)
            val seens = Channel<RouteEvent.Seen>(Channel.UNLIMITED)
            val completions = Channel<RouteEvent.PollCompleted>(Channel.UNLIMITED)
            val collecting = launch {
                source.events().collect { event ->
                    when (event) {
                        is RouteEvent.Seen -> { seens.send(event); launch { pipeline.run(event, source.fetcher) } }
                        is RouteEvent.PollCompleted -> completions.send(event)
                        else -> Unit
                    }
                }
            }
            val first = withTimeout(TIMEOUT) { seens.receive() }
            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterFetch) }
            file.writeText(CONTENT + "2,7\n")
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.from(Files.getLastModifiedTime(file).toInstant().plusSeconds(5)))

            repeat(3) { withTimeout(TIMEOUT) { completions.receive() } }
            assertTrue(seens.isEmpty, "the second upload was not handed over while the first is in flight")
            assertEquals(1.0, registry.get("sftp_inflight").gauge().value(), "and the connector holds a place for the first only")

            hook.resume(HookPoint.afterFetch)
            withTimeout(TIMEOUT) { completions.receive() }
            collecting.cancelAndJoin()
            assertEquals(first.identity, store.transfers.single().identity)
            assertEquals(TransferState.DONE, store.transfers.single().state)
            assertTrue(seens.isEmpty, "the first's move took the path with it, so the newer copy was never handed over at all")
            assertEquals(0.0, registry.get("sftp_inflight").gauge().value(), "everything given back once the run is over")
        }
    }

    /**
     * Review finding Spec 1. Spec 5.3's `callback` is an ack action of any trigger, a poll included:
     * the pipeline calls the channel itself before the ACKED write (ticket 19), and the connector's
     * post action is what `none` is, so the file stays in the drop directory and the next listing is
     * D40's re-check. Before the fix the mapping refused `Callback` and the route never started.
     */
    @Test
    fun SPEC1_a_polled_route_with_a_callback_ack_calls_the_channel_before_ACKED_and_leaves_the_file() = runBlocking {
        seed("first.csv", CONTENT)
        val upstream = RecordingChannel("upstream")
        val hook = HookDriver().apply { pauseAt(HookPoint.afterAck); pauseAt(HookPoint.afterLedgerAcked) }

        withRoute(AckAction.Callback("upstream"), hook, mapOf(upstream.name to upstream)) { run ->
            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterAck) }
            assertEquals(setOf("first.csv"), target.keys, "the target holds the object")
            assertEquals(1, upstream.events.size, "and the callback has been called")
            assertEquals(DeliveryMoment.ACKED, upstream.events.single().moment)
            assertEquals(TransferState.STORED, store.transfers.single().state, "before any ledger write")
            hook.resume(HookPoint.afterAck)

            withTimeout(TIMEOUT) { hook.awaitArrival(HookPoint.afterLedgerAcked) }
            assertTrue(remoteRoot.resolve("drop/first.csv").exists(), "a callback ack does to the file what none does")
            assertFalse(remoteRoot.resolve("drop/temp").exists())
            hook.resume(HookPoint.afterLedgerAcked)

            assertTrue(run.isActive, "and the route never went down")
        }

        assertEquals(TransferState.DONE, store.transfers.single().state, "nobody to notify, so ACKED is DONE")
        assertEquals(1, upstream.events.size, "D40 skips the file that stayed, so the callback is not called again")
    }

    private fun identityOf(file: Path) = SourceIdentity(
        RouteName(ROUTE), SourceKind.SFTP, "vendor:/drop", file.fileName.toString(),
        Files.size(file), Files.getLastModifiedTime(file).toInstant().truncatedTo(ChronoUnit.SECONDS),
    )

    private fun seed(name: String, content: String): Path {
        val file = remoteRoot.resolve("drop").createDirectories().resolve(name)
        file.writeText(content)
        return file
    }

    /** One started connector and the poll source over it, closed however [block] ends. */
    private suspend fun withConnector(
        onAck: AckAction,
        maxFilesPerPoll: Int? = null,
        block: suspend CoroutineScope.(SftpPollSource) -> Unit,
    ) {
        remoteRoot.resolve("drop").createDirectories()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val poll = pollOf(onAck)
            val config =
                if (maxFilesPerPoll == null) connectorConfig(server, poll) { (it as Secret.Literal).value }
                else cappedConnectorConfig(server, poll, maxFilesPerPoll)
            val connector = SftpConnector.start(config, meterRegistry = registry)
            try {
                coroutineScope { block(SftpPollSource(connector.source, RouteName(ROUTE), poll, clock)) }
            } finally {
                connector.close()
            }
        }
    }

    /** The same, with the real runner and pipeline already collecting the source's events. */
    private suspend fun withRoute(
        onAck: AckAction,
        hook: Hook = Hook.None,
        channels: Map<ChannelName, DeliveryChannel> = emptyMap(),
        block: suspend CoroutineScope.(Job) -> Unit,
    ) =
        withConnector(onAck) { source ->
            val route = routeOf(onAck)
            val runner = RouteRunner(route, pipelineFor(route, hook, channels), source.fetcher, store, { wakes++ }, clock, registry)
            coroutineScope {
                val run = launch { runner.run(source.events()) }
                try {
                    block(run)
                } finally {
                    run.cancelAndJoin()
                }
            }
        }

    private fun routeOf(onAck: AckAction) =
        Route(name = ROUTE, source = pollOf(onAck), target = Target("minio", bucket = "landing"))

    private fun pipelineFor(route: Route, hook: Hook = Hook.None, channels: Map<ChannelName, DeliveryChannel> = emptyMap()) = TransferPipeline(
        route, DigestAlgorithm.MD5, store, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5),
        emptyMap(), { true }, { wakes++ }, hook, clock, registry, Staging(routeStage),
        usableSpace = { 10.gib }, channels = channels,
    )

    /** The host's mapping, with this one route holding the whole of the store's budget. */
    private fun connectorConfig(server: EmbeddedSftpServer, poll: Source.Poll, resolve: (Secret) -> String) =
        sftpConnectorConfig(sftpStore(server), poll, DigestAlgorithm.MD5, resolve, sessions = 4, transfers = 4)

    /**
     * The same connector with a listing cap a test can reach with three files. No `SftpStore` field
     * carries the cap, and a built configuration can no longer be edited into one (connector T21),
     * so this case is described to the connector's own DSL instead of adjusted afterwards.
     */
    private fun cappedConnectorConfig(server: EmbeddedSftpServer, poll: Source.Poll, cap: Int) =
        sftpConnector("vendor") {
            endpoint { host = server.host; port = server.port }
            auth { password(USER, PASSWORD) }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 4 }
            polling {
                directories(poll.directory)
                onAck = move("temp/")
                readiness = sizeStable(checks = 1, interval = 1.milliseconds)
                overlap = OverlapPolicy.SKIP
                maxFilesPerPoll = cap
                staging { dir = connectorStage; digest = ConnectorDigest.MD5 }
            }
        }

    private fun pollOf(onAck: AckAction) = Source.Poll(
        store = "vendor", directory = "/drop", every = EVERY,
        readiness = listOf(FileReadiness.SizeStable(checks = 1, interval = 1.milliseconds)),
        onAck = onAck,
    )

    private fun sftpStore(server: EmbeddedSftpServer) = SftpStore(
        name = "vendor", host = server.host, port = server.port,
        user = Secret.Literal(USER), password = Secret.Literal(PASSWORD),
        pool = Pool(maxSize = 4), staging = Staging(connectorStage),
    )

    private companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"
        const val ROUTE = "vendor-drop"
        const val CONTENT = "id,amount\n1,42\n"
        val EVERY = 200.milliseconds
        const val TIMEOUT = 30_000L
    }
}
