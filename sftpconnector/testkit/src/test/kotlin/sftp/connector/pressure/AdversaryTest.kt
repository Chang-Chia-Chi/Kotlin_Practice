package sftp.connector.pressure

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.client.LocalFile
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SftpException
import sftp.connector.pool.virtualClock
import sftp.connector.source.Readiness
import sftp.connector.source.ReadinessCheck
import sftp.connector.source.SftpEvent
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * The seeded adversary: a real [SftpConnector] over the fake transport, the network played by
 * [Adversary] from the sequence's own random, thousands of sequences of random consumer and
 * world operations on virtual time, and every invariant checked after every one of them.
 *
 * The model is pure: the server as two maps of names, the files the consumer holds and what
 * became of each, and nothing else. The world is the connector as shipped. A failure is
 * rethrown with the seed, a replay line and the full operation sequence, after the same seed has
 * been rerun on ever shorter prefixes to find the shortest one that still fails.
 *
 * Two facts of the fake shape this world. It answers on the caller's own coroutine, so a listing
 * longer than the client's channel buffer would park the one test thread; the directory is kept
 * under forty files. And a hang-up is never faulted, because a real disconnect closes the socket
 * whatever the peer does. The registry's lifetime jitter is unseeded, so it is set to zero here.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class AdversaryTest {

    @TempDir
    lateinit var local: Path

    @Test
    fun `model_thousands of random sequences with every invariant checked after every op`() {
        val started = TimeSource.Monotonic.markNow()
        repeat(MAIN_SEQUENCES) { i -> runShrinking(SEED + i, i, MAIN_OPS) }
        println("adversary: $MAIN_SEQUENCES sequences x $MAIN_OPS ops in ${started.elapsedNow()}")
    }

    /**
     * I15 under its own id. The invariant is checked after every operation of every sequence the
     * test above runs - the ledger in [World.reconcile], the landed-move clause in [World.ack] -
     * which is stronger coverage than any one scenario and is exactly why it had no `I15_` test.
     * It needs one anyway: spec 17.1 names invariant tests `I<n>_<description>`, so `-Dtest='I15*'`
     * has to find something, and a broken ledger should fail a test named for what it broke rather
     * than one named `model_`. One fixed seed of its own, end to end, and the per-operation
     * assertions stay where they are.
     */
    @Test
    fun `I15_every acked file is at the ack target and no landed move is reported as failed`() {
        runShrinking(SEED + I15_SEED_OFFSET, index = 0, ops = MAIN_OPS)
    }

    /** One long run: what the world holds at op 1,000 is what it holds at the end, within a band. */
    @Test
    fun `leak_fifty thousand ops on one seed keep sessions, threads and post-GC heap flat`() {
        var atThousand: Triple<Int, Int, Long>? = null
        var atEnd: Triple<Int, Int, Long>? = null
        runSequence(SEED + LEAK_SEED_OFFSET, LEAK_OPS, closeAt = -1) { step, world ->
            if (step == 1_000) atThousand = world.footprint()
            if (step == LEAK_OPS - 1) atEnd = world.footprint()
        }
        val (sessionsBefore, threadsBefore, heapBefore) = checkNotNull(atThousand)
        val (sessionsAfter, threadsAfter, heapAfter) = checkNotNull(atEnd)
        println("adversary leak run: sessions $sessionsBefore -> $sessionsAfter, threads $threadsBefore -> $threadsAfter, post-GC heap ${heapBefore shr 20} MB -> ${heapAfter shr 20} MB")
        assertThat(sessionsAfter).describedAs("open sessions").isBetween(sessionsBefore - MAX_SIZE, sessionsBefore + MAX_SIZE)
        assertThat(threadsAfter - threadsBefore).describedAs("threads gained").isLessThanOrEqualTo(2)
        assertThat(heapAfter - heapBefore).describedAs("post-GC heap gained").isLessThanOrEqualTo(HEAP_BAND)
    }

    private fun runShrinking(seed: Long, index: Int, ops: Int) {
        val closeAt = closeAtFor(seed, ops)
        val failure = runCatching { runSequence(seed, ops, closeAt) }.exceptionOrNull() as? SequenceFailure ?: return
        var shortest = failure
        for (prefix in minOf(ops, failure.step + 1) downTo 0) {
            shortest = runCatching { runSequence(seed, prefix, closeAt) }.exceptionOrNull() as? SequenceFailure ?: break
        }
        throw AssertionError(
            buildString {
                appendLine("ADVERSARY FAILURE - fixed base seed=$SEED, sequence #$index, per-sequence seed=$seed")
                appendLine("replay: runSequence(seed=$seed, ops=${shortest.ops}, closeAt=$closeAt)")
                appendLine("shortest failing prefix: ${shortest.ops} ops (the full sequence first failed at step ${failure.step} of $ops)")
                appendLine("operation sequence:")
                shortest.log.forEach { appendLine("  $it") }
                appendLine("cause: ${shortest.cause}")
            },
            shortest.cause,
        )
    }

    /** One sequence in five closes the connector somewhere in the middle, with whatever is then in flight. */
    private fun closeAtFor(seed: Long, ops: Int): Int = Random(seed xor CLOSE_SALT).let { if (it.nextInt(5) == 0) it.nextInt(ops) else -1 }

    private class SequenceFailure(val step: Int, val ops: Int, val log: List<String>, cause: Throwable) : Exception(cause)

    private fun runSequence(seed: Long, ops: Int, closeAt: Int, watch: (Int, World) -> Unit = { _, _ -> }) =
        runTest(timeout = 10.minutes) {
            val rnd = Random(seed)
            val world = World(this, rnd, local.resolve("seq-$seed-$ops").createDirectories())
            val log = mutableListOf<String>()
            var step = 0
            try {
                world.start()
                while (step < ops) {
                    if (step == closeAt) {
                        log += "[$step] close (this sequence closes mid-way)"
                        break
                    }
                    val op = pickOp(rnd, world)
                    log += "[$step] ${op.name.lowercase()}"
                    world.perform(op, log)
                    val tail = TAILS[rnd.nextInt(TAILS.size)]
                    advanceTimeBy(tail)
                    runCurrent()
                    world.checkStep(quiescent = tail == SETTLE, log)
                    watch(step, world)
                    step++
                }
                log += "[end] close"
                world.endOfSequence(log)
            } catch (failure: Throwable) {
                runCatching { world.abandon() }
                throw SequenceFailure(step, ops, log, failure)
            }
        }

    // ------------------------------------------------------------------ op generation

    private enum class Op(val weight: Int) {
        POLL(6), ACK(5), NACK(2), DOWNLOAD(4), CANCEL_COLLECTOR(1), KILL_SESSION(2), ADVANCE_CLOCK(2),
        ADD_FILE(5), BLOCK_TARGET(1), REMOVE_FILE(2), GROW_FILE(2),
    }

    private fun pickOp(rnd: Random, world: World): Op {
        val candidates = Op.entries.filter { world.possible(it) }
        var roll = rnd.nextInt(candidates.sumOf { it.weight })
        for (op in candidates) {
            roll -= op.weight
            if (roll < 0) return op
        }
        return candidates.last()
    }

    // ------------------------------------------------------------------ the model

    private enum class Fate { IN_FLIGHT, ACKING, ACKED, ACK_FAILED, NACKED, GONE, WITHDRAWN }

    /** One file handed to the consumer, and what became of it. */
    private class Held(val id: Int, val event: FileSeen) {
        val name: String = event.file.name
        val listedSize: Long = event.file.size
        var fate = Fate.IN_FLIGHT
        var download: Job? = null

        /** Grew on the server after it was listed, so the size the rename retry looks for is stale by design. */
        var grown = false
        val settled: Boolean get() = fate != Fate.IN_FLIGHT
        val inFlight: Boolean get() = fate == Fate.IN_FLIGHT || fate == Fate.ACKING
    }

    /** One collection of `poll`, and what the model expected it to hand over. */
    private class PollRun(val log: OpLog) {
        lateinit var job: Job
        var listing: Model.Listing? = null
        val handed = mutableListOf<Held>()
        var completed: SftpEvent.PollCompleted? = null
        var outcome: Result<Unit>? = null
        var cancelled = false
    }

    /** The server and the consumer as the model knows them: pure, and updated only by the ops and their outcomes. */
    private class Model {
        val source = LinkedHashMap<String, Int>()
        val temp = LinkedHashMap<String, Int>()
        val excluded = HashSet<Pair<String, Int>>()
        val held = mutableListOf<Held>()
        var counter = 0
        var ids = 0
        var wireFaultSinceClosed = false

        val inFlight: Int get() = held.count { it.inFlight }

        fun holds(name: String, size: Int): Boolean = held.any { it.inFlight && it.name == name && it.listedSize == size.toLong() }

        /**
         * What a listing answered now reports, and which of those files are out with the consumer at
         * this instant. The client buffers the listing and the tick checks the in-flight set only as
         * it drains that buffer, so a file busy now may be free by then: it may go either way, while
         * a file free now must be handed over and a file not listed never is.
         */
        class Listing(val listed: List<Pair<String, Int>>, val busy: Set<Pair<String, Int>>)

        fun listingView(): Listing {
            val listed = source.entries.map { it.key to it.value }.filter { it !in excluded }
            return Listing(listed, listed.filter { (name, size) -> holds(name, size) }.toSet())
        }

        fun moved(name: String) {
            source.remove(name)?.let { temp[name] = it }
        }
    }

    // ------------------------------------------------------------------ the world

    private class World(private val scope: TestScope, private val rnd: Random, private val stage: Path) {
        val transport: FakeSftpTransport = FakeSftpTransport { network.answer(it) }.directory(DROP)
        val network: Adversary = Adversary(rnd, transport) { "$TEMP/${it.substringAfterLast('/')}" }
        val meters = SimpleMeterRegistry()
        val model = Model()

        /** Operations still running, each with the assertions to make once it has finished. */
        private val pending = mutableListOf<Pair<Job, () -> Unit>>()
        private var poll: PollRun? = null
        private var closing = false
        lateinit var connector: SftpConnector

        private val config = sftpConnector("adversary") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            pool {
                maxSize = MAX_SIZE; minIdle = 1; acquireTimeout = ACQUIRE_TIMEOUT; cancelGrace = GRACE; drainTimeout = DRAIN
                housekeepingInterval = HOUSEKEEPING; maxLifetimeJitter = 0.0
            }
            polling {
                staging { dir = stage }; directories(DROP); onAck = move("temp/"); maxInFlight = MAX_IN_FLIGHT
                readiness = ReadinessCheck { _, _ -> Readiness.Ready }
            }
            resilience {
                retry { maxAttempts = 3; backoff = exponential(1.seconds, max = 4.seconds, jitter = false) }
                // A window of one, so every counted failure opens the breaker on the call that raised it and a
                // closed breaker means no counted failure since it last closed - which is what the sweep checks.
                circuitBreaker { slidingWindow = 1; failureRateThreshold = 50; waitInOpen = 30.seconds }
                operationTimeout = 10.seconds; transferTimeout = 10.seconds
            }
        }

        suspend fun start() {
            connector = SftpConnector.start(config, transport, meters, scope.virtualClock(), StandardTestDispatcher(scope.testScheduler))
            network.onLanded = { from -> model.moved(from.substringAfterLast('/')) }
            network.armed = true
        }

        fun possible(op: Op): Boolean = when (op) {
            Op.POLL -> poll == null
            Op.ACK, Op.NACK -> model.held.isNotEmpty()
            Op.DOWNLOAD -> model.held.any { it.fate == Fate.IN_FLIGHT && it.download == null }
            Op.CANCEL_COLLECTOR -> poll != null
            Op.KILL_SESSION -> network.alive.isNotEmpty()
            Op.ADD_FILE -> model.source.size < MAX_FILES
            Op.BLOCK_TARGET -> model.source.keys.any { it !in model.temp }
            Op.REMOVE_FILE -> model.source.isNotEmpty() || model.temp.isNotEmpty()
            Op.GROW_FILE -> model.source.isNotEmpty()
            Op.ADVANCE_CLOCK -> true
        }

        suspend fun perform(op: Op, log: MutableList<String>) {
            transport.calls.clear()
            when (op) {
                Op.POLL -> startPoll(log)
                Op.ACK -> ack(log)
                Op.NACK -> nack(log)
                Op.DOWNLOAD -> download(log)
                Op.CANCEL_COLLECTOR -> cancelCollector(log)
                Op.KILL_SESSION -> {
                    val session = network.alive.elementAt(rnd.nextInt(network.alive.size))
                    network.kill(session)
                    log += "      session #$session is dead"
                }
                Op.ADVANCE_CLOCK -> {
                    val by = if (rnd.nextBoolean()) HOUSEKEEPING + 1.milliseconds else SETTLE
                    log += "      by $by"
                    scope.advanceTimeBy(by)
                    scope.runCurrent()
                }
                Op.ADD_FILE -> {
                    val name = "f${++model.counter}.csv"
                    val size = 1 + rnd.nextInt(64)
                    transport.file("$DROP/$name", ByteArray(size))
                    model.source[name] = size
                    log += "      $name ($size bytes)"
                }
                Op.BLOCK_TARGET -> {
                    val name = model.source.keys.filter { it !in model.temp }.random(rnd)
                    val size = model.source.getValue(name) + 1
                    transport.file("$TEMP/$name", ByteArray(size))
                    model.temp[name] = size
                    log += "      a stranger's $name at the target"
                }
                Op.REMOVE_FILE -> {
                    val fromTemp = model.temp.isNotEmpty() && (model.source.isEmpty() || rnd.nextInt(3) == 0 || model.temp.size > model.source.size)
                    val (map, dir) = if (fromTemp) model.temp to TEMP else model.source to DROP
                    val name = map.keys.random(rnd)
                    transport.remove("$dir/$name")
                    map.remove(name)
                    log += "      $dir/$name"
                }
                Op.GROW_FILE -> {
                    val name = model.source.keys.random(rnd)
                    val size = model.source.getValue(name) + 1 + rnd.nextInt(16)
                    transport.file("$DROP/$name", ByteArray(size))
                    model.source[name] = size
                    model.held.filter { it.inFlight && it.name == name }.forEach {
                        it.grown = true
                        // A download already under way will come up short, and that shortfall is the breaker's to count.
                        if (it.download != null) model.wireFaultSinceClosed = true
                    }
                    log += "      $name to $size bytes"
                }
            }
        }

        private fun startPoll(log: MutableList<String>) {
            val run = PollRun(OpLog())
            poll = run
            network.onListing = { run.listing = model.listingView() }
            run.job = scope.launch(run.log) {
                run.outcome = runCatching {
                    connector.source.poll(DROP).collect { event ->
                        when (event) {
                            is FileSeen -> {
                                assertThat(model.held.filter { it.inFlight && it.name == event.file.name && it.listedSize == event.file.size })
                                    .describedAs("I7: ${event.file.path} was handed over while the consumer still had it").isEmpty()
                                Held(++model.ids, event).also { model.held += it; run.handed += it }
                            }
                            is SftpEvent.PollCompleted -> run.completed = event
                            else -> Unit
                        }
                    }
                }
                if (poll === run) poll = null
            }
            pending += run.job to {
                val handed = run.handed.map { it.name to it.listedSize.toInt() }
                val failure = checkNotNull(run.outcome).exceptionOrNull()
                log += "      poll -> ${failure?.let { it::class.simpleName } ?: "completed"}, handed over ${handed.map { it.first }}${faultsOf(run.log)}"
                if (run.cancelled || closing) return@to
                if (failure is AssertionError) throw failure
                if (failure == null) {
                    val listing = checkNotNull(run.listing)
                    assertThat(handed).describedAs("I7: a poll hands over only what it listed, and nothing twice").isSubsetOf(listing.listed).doesNotHaveDuplicates()
                    assertThat(listing.listed - listing.busy).describedAs("everything listed and free at listing time is handed over").isSubsetOf(handed)
                    assertThat(run.completed?.emitted).describedAs("PollCompleted.emitted").isEqualTo(handed.size)
                } else {
                    assertThat(failure).describedAs("a poll fails only with a failure the connector has a name for").isInstanceOf(SftpException::class.java)
                    assertThat(handed).describedAs("a failed poll handed nothing over").isEmpty()
                }
            }
        }

        private suspend fun ack(log: MutableList<String>) {
            val held = model.held.random(rnd)
            log += "      ${held.name}#${held.id} (${held.fate})"
            if (held.settled) {
                held.event.ack()
                assertThat(transport.calls.count { it.operation == Operation.Rename }).describedAs("I12: a second answer moves nothing").isZero()
                return
            }
            held.fate = Fate.ACKING
            val opLog = OpLog()
            var outcome: Result<Unit>? = null
            val job = scope.launch(opLog) {
                outcome = runCatching { held.event.ack() }.also { result ->
                    if (result.isSuccess) {
                        held.fate = Fate.ACKED
                        model.moved(held.name)
                    } else {
                        // A rename that landed before its reply was lost has already moved the model's file.
                        held.fate = Fate.ACK_FAILED
                    }
                }
            }
            pending += job to {
                val failure = checkNotNull(outcome).exceptionOrNull()
                log += "      ack ${held.name}#${held.id} -> ${failure?.let { it::class.simpleName } ?: "ok"}${faultsOf(opLog)}"
                if (failure == null || closing) return@to
                assertThat(failure).describedAs("an ack fails only with a failure the connector has a name for").isInstanceOf(SftpException::class.java)
                if (opLog.landedAt != null && !held.grown) {
                    assertThat(opLog.statSawLanded).describedAs("I11: the rename landed and a retry saw it at the target, yet the ack was reported as failed").isFalse()
                }
            }
        }

        private suspend fun nack(log: MutableList<String>) {
            val held = model.held.random(rnd)
            val redeliver = rnd.nextBoolean()
            log += "      ${held.name}#${held.id} (${held.fate}) redeliver=$redeliver"
            held.event.nack(IllegalStateException("adversary: could not process it"), redeliver)
            assertThat(transport.calls).describedAs("a nack under a noop action sends nothing").isEmpty()
            if (held.settled) return
            held.fate = Fate.NACKED
            if (!redeliver) model.excluded += held.name to held.listedSize.toInt()
        }

        private fun download(log: MutableList<String>) {
            val held = model.held.filter { it.fate == Fate.IN_FLIGHT && it.download == null }.random(rnd)
            val target = stage.resolve("${held.id}-${held.name}")
            log += "      ${held.name}#${held.id}"
            // A file that grew since it was listed cannot arrive whole, and that shortfall is the breaker's to count.
            if (model.source[held.name]?.toLong() != held.listedSize) model.wireFaultSinceClosed = true
            val opLog = OpLog()
            var outcome: Result<LocalFile?>? = null
            val job = scope.launch(opLog) {
                outcome = runCatching { held.event.download(target) }.also {
                    held.download = null
                    if (it.getOrNull() == null && it.isSuccess && held.fate == Fate.IN_FLIGHT) held.fate = Fate.GONE
                }
            }
            held.download = job
            pending += job to {
                val result = checkNotNull(outcome)
                log += "      download ${held.name}#${held.id} -> ${result.fold({ it?.let { "${it.size} bytes" } ?: "gone" }, { it::class.simpleName })}${faultsOf(opLog)}"
                result.onSuccess { landed ->
                    if (landed == null) {
                        assertThat(opLog.readSawMissing).describedAs("I15: a file that was on the server was reported gone").isTrue()
                    } else {
                        assertThat(landed.size).isEqualTo(held.listedSize)
                        assertThat(Files.size(target)).isEqualTo(held.listedSize)
                        Files.delete(target)
                    }
                }.onFailure { if (!closing) assertThat(it).describedAs("a download fails only with a failure the connector has a name for").isInstanceOf(SftpException::class.java) }
            }
        }

        private fun cancelCollector(log: MutableList<String>) {
            val run = checkNotNull(poll)
            run.cancelled = true
            run.job.cancel()
            scope.runCurrent()
            run.handed.filter { it.fate == Fate.IN_FLIGHT }.forEach { it.fate = Fate.WITHDRAWN }
            log += "      withdrew ${run.handed.filter { it.fate == Fate.WITHDRAWN }.map { it.name }}"
            poll = null
        }

        // -------------------------------------------------------------- invariants

        suspend fun checkStep(quiescent: Boolean, log: MutableList<String>) {
            settleCompletedJobs()
            if (model.held.size > 30) model.held.removeAll { !it.inFlight && it.download == null }

            assertThat(connector.pool.stats().total).describedAs("I1: sessions never exceed maxSize").isLessThanOrEqualTo(MAX_SIZE)
            assertThat(gauge("sftp_inflight").toInt()).describedAs("I7/I8: the in-flight gauge against the model").isEqualTo(model.inFlight)
            assertThat(counter("sftp_pool_leak_total")).describedAs("no lease is held past the leak threshold").isZero()

            if (network.wireFaultSeen) model.wireFaultSinceClosed = true
            network.wireFaultSeen = false
            val breaker = gauge("sftp_breaker_state").toInt()
            if (!model.wireFaultSinceClosed) {
                assertThat(breaker).describedAs("the breaker never counts a Fatal, an OverwriteRefused or an answered refusal").isZero()
            }
            // Seen closed only when nothing faulted is still in flight to open it later.
            if (breaker == 0 && quiescent) model.wireFaultSinceClosed = false

            if (quiescent) checkQuiescent(log)
        }

        private fun settleCompletedJobs() {
            val done = pending.filter { it.first.isCompleted }
            pending -= done.toSet()
            done.forEach { it.second() }
        }

        private suspend fun checkQuiescent(log: MutableList<String>) {
            assertThat(pending.map { it.first }).describedAs("every operation but a poll waiting for room has finished at quiescence").allMatch { it === poll?.job }
            if (poll != null) assertThat(model.inFlight).describedAs("a poll still open at quiescence is waiting for room").isEqualTo(MAX_IN_FLIGHT)
            val stats = connector.pool.stats()
            assertThat(transport.openSessions).describedAs("no orphan session: the transport's open sessions are the pool's ($stats)").isEqualTo(stats.total)
            assertThat(partials()).describedAs("I13: no partial file at quiescence").isEmpty()
            reconcile()
            proveTheBound(log)
        }

        /** I15's ledger: the server holds exactly what the model says. */
        private fun reconcile() {
            val files = transport.snapshot().filterValues { it != null }.mapValues { it.value!! }
            val expected = model.source.mapKeys { "$DROP/${it.key}" } + model.temp.mapKeys { "$TEMP/${it.key}" }
            assertThat(files).describedAs("I15: every file is where the model says, and no file is silently gone").isEqualTo(expected)
        }

        /** I1 and I4 in their observable form: a quiet pool fills to its size and refuses the next caller. */
        private fun proveTheBound(log: MutableList<String>) {
            network.armed = false
            val leases = List(MAX_SIZE) { scope.async { connector.pool.acquire() } }
            val oneTooMany = scope.async { runCatching { connector.pool.acquire() } }
            scope.advanceTimeBy(ACQUIRE_TIMEOUT + 1.milliseconds)
            scope.runCurrent()
            assertThat(leases).describedAs("I4: a quiet pool fills to maxSize, so no permit has leaked").allMatch { it.isCompleted && it.getCompletionExceptionOrNull() == null }
            assertThat(oneTooMany.isCompleted && oneTooMany.getCompleted().exceptionOrNull() is PoolExhausted).describedAs("I1: the caller past maxSize is refused").isTrue()
            scope.launch { leases.forEach { it.getCompleted().release() } }
            scope.runCurrent()
            network.armed = true
            log += "      [quiescent: ledger and bound proved]"
        }

        /** I9 and the end state: closed within the bound, nothing open, nothing partial, every session accounted for. */
        suspend fun endOfSequence(log: MutableList<String>) {
            val began = scope.currentTime
            connector.close()
            val took = (scope.currentTime - began).milliseconds
            assertThat(took).describedAs("I9: close returns within drainTimeout + cancelGrace").isLessThanOrEqualTo(DRAIN + GRACE + 100.milliseconds)
            closing = true
            poll?.let { it.cancelled = true; it.job.cancel() }
            scope.advanceTimeBy(1.minutes)
            scope.runCurrent()
            settleCompletedJobs()
            assertThat(pending).describedAs("operations still running a minute after close").isEmpty()
            assertThat(transport.openSessions).describedAs("sessions open after close").isZero()
            assertThat(connector.pool.stats().total).describedAs("entries the pool still holds after close").isZero()
            assertThat(counter("sftp_pool_evicted_total")).describedAs("every entry Closed: every session opened has been evicted for a reason").isEqualTo(counter("sftp_pool_created_total"))
            assertThat(partials()).describedAs("I13: partial files after close").isEmpty()
            log += "      closed in $took"
        }

        suspend fun abandon() {
            closing = true
            poll?.let { it.cancelled = true; it.job.cancel() }
            pending.forEach { it.first.cancel() }
            connector.close()
        }

        fun footprint(): Triple<Int, Int, Long> {
            System.gc()
            val runtime = Runtime.getRuntime()
            return Triple(transport.openSessions, Thread.activeCount(), runtime.totalMemory() - runtime.freeMemory())
        }

        private fun partials() = stage.listDirectoryEntries().filter { it.name.endsWith(".part") }
        private fun gauge(name: String) = meters.get(name).gauge().value()
        private fun counter(name: String) = meters.find(name).counters().sumOf { it.count() }
        private fun faultsOf(opLog: OpLog) = if (opLog.faults.isEmpty()) "" else "; network: ${opLog.faults}"
    }

    private companion object {
        const val SEED = 20260903L
        const val CLOSE_SALT = 0x5EEDL
        const val LEAK_SEED_OFFSET = 1_000_000L

        /** Past the main run's five thousand and past the leak run, so I15 gets a sequence nothing else drives. */
        const val I15_SEED_OFFSET = 2_000_000L
        const val MAIN_SEQUENCES = 5000
        const val MAIN_OPS = 40
        const val LEAK_OPS = 50_000
        const val HEAP_BAND = 32L shl 20

        const val DROP = "/drop"
        const val TEMP = "/drop/temp"
        const val MAX_SIZE = 2
        const val MAX_IN_FLIGHT = 3
        const val MAX_FILES = 40
        val ACQUIRE_TIMEOUT = 5.seconds
        val DRAIN = 2.seconds
        val GRACE = 1.seconds
        val HOUSEKEEPING = 1.minutes

        /** Past every stall, timeout, backoff and the housekeeping round that starts at the five-minute mark. */
        val SETTLE: Duration = 5.minutes + 31.seconds
        val TAILS = listOf(Duration.ZERO, 100.milliseconds, SETTLE, SETTLE)
    }
}
