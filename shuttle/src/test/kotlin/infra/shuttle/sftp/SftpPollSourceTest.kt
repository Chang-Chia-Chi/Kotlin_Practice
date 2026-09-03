package infra.shuttle.sftp

import infra.shuttle.core.AckAction
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
import infra.shuttle.core.Source
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.Staging
import infra.shuttle.core.Target
import infra.shuttle.core.TransferPipeline
import infra.shuttle.core.TransferState
import infra.shuttle.core.gib
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.HookDriver
import infra.shuttle.testkit.InMemoryStateStore
import infra.shuttle.testkit.InMemoryTarget
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
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
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.error.AuthenticationFailed
import sftp.connector.pool.SftpPool
import sftp.connector.source.SftpSource
import sftp.connector.testkit.EmbeddedSftpServer
import sftp.connector.transport.jsch.JschTransport
import java.nio.file.Files
import java.nio.file.Path
import java.time.temporal.ChronoUnit
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
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
            val config = sftpConnectorConfig(sftpStore(server), poll, DigestAlgorithm.MD5) { "not the password" }
            val pool = SftpPool(JschTransport(config, registry), config, registry)
            val source = SftpPollSource(SftpSource(SftpClient(pool, config, registry), config, registry), config, RouteName(ROUTE), poll, clock)

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
     * Spec 4.6: a truncated listing skips the repair. The connector reports counts rather than a
     * flag, so the reading is `seen >= maxFilesPerPoll`: at the cap, the listing stopped where the
     * cap is and nothing proves the directory held no more.
     */
    @Test
    fun a_listing_that_reaches_maxFilesPerPoll_completes_truncated() = runBlocking {
        seed("a.csv", CONTENT)
        seed("b.csv", CONTENT)
        seed("c.csv", CONTENT)

        withConnector(AckAction.Move("temp/"), { it.copy(polling = it.polling.copy(maxFilesPerPoll = 2)) }) { source ->
            val completed = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.PollCompleted>().first() }

            assertTrue(completed.truncated)
            assertEquals(2, completed.listed.size, "and it names only what it got to")
        }
    }

    /**
     * D40 (ticket 06 deviation 4): a finished row that came back inside `recheckFinished` leaves the
     * pipeline without an ack or a nack. Nothing else would ever give the connector that file's
     * place back, so a later poll does, and the file is handed over again as it would be after a
     * restart.
     */
    @Test
    fun a_Seen_the_route_neither_answered_nor_fetched_is_given_back_and_handed_over_again() = runBlocking {
        seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            val seens = Channel<RouteEvent.Seen>(Channel.UNLIMITED)
            val collecting = launch { source.events().collect { if (it is RouteEvent.Seen) seens.send(it) } }

            val first = withTimeout(TIMEOUT) { seens.receive() }
            val again = withTimeout(TIMEOUT) { seens.receive() }

            assertEquals(first.identity, again.identity, "the same file, because D40's window costs no place for ever")
            assertTrue(remoteRoot.resolve("drop/first.csv").exists(), "and the nack that freed it left the file alone")
            collecting.cancelAndJoin()
        }
    }

    /**
     * A route that ends - cancelled, or restarted after `RouteDown` - gives every file it was
     * holding back. Only the tick that handed a file over withdraws it, so without this the file
     * below would stay in the connector's in-flight set for the life of the process and no later
     * poll, on this route or its restart, would ever list it again.
     */
    @Test
    fun a_run_that_ends_gives_back_every_file_it_was_holding_so_the_next_run_lists_them() = runBlocking {
        val file = seed("first.csv", CONTENT)

        withConnector(AckAction.Move("temp/")) { source ->
            val completions = Channel<RouteEvent.PollCompleted>(Channel.UNLIMITED)
            val collecting = launch {
                source.events().collect { event ->
                    // Fetched and never answered, so the D40 sweep leaves it alone: the only thing
                    // that can give this one back is the end of the run.
                    if (event is RouteEvent.Seen) source.fetcher(event.source.path, routeStage.resolve("copy"), DigestAlgorithm.MD5)
                    if (event is RouteEvent.PollCompleted) completions.send(event)
                }
            }
            withTimeout(TIMEOUT) { completions.receive() }
            withTimeout(TIMEOUT) { completions.receive() }
            collecting.cancelAndJoin()

            val again = withTimeout(TIMEOUT) { source.events().filterIsInstance<RouteEvent.Seen>().first() }
            assertEquals(identityOf(file), again.identity)
        }
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
        tune: (SftpConnectorConfig) -> SftpConnectorConfig = { it },
        block: suspend CoroutineScope.(SftpPollSource) -> Unit,
    ) {
        remoteRoot.resolve("drop").createDirectories()
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val poll = pollOf(onAck)
            val config = tune(sftpConnectorConfig(sftpStore(server), poll, DigestAlgorithm.MD5) { (it as Secret.Literal).value })
            val connector = SftpConnector.start(config, meterRegistry = registry)
            try {
                coroutineScope { block(SftpPollSource(connector.source, config, RouteName(ROUTE), poll, clock)) }
            } finally {
                connector.close()
            }
        }
    }

    /** The same, with the real runner and pipeline already collecting the source's events. */
    private suspend fun withRoute(onAck: AckAction, hook: Hook = Hook.None, block: suspend CoroutineScope.(Job) -> Unit) =
        withConnector(onAck) { source ->
            val route = routeOf(onAck)
            val runner = RouteRunner(route, pipelineFor(route, hook), source.fetcher, store, { wakes++ }, clock, registry)
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

    private fun pipelineFor(route: Route, hook: Hook = Hook.None) = TransferPipeline(
        route, DigestAlgorithm.MD5, store, target, ProcessingChain(emptyList(), DigestAlgorithm.MD5),
        emptyMap(), { true }, { wakes++ }, hook, clock, registry, Staging(routeStage),
    ) { 10.gib }

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
