package sftp.connector.client

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.CircuitOpen
import sftp.connector.error.ConnectFailed
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OverwriteRefused
import sftp.connector.error.PermissionDenied
import sftp.connector.error.PoolExhausted
import sftp.connector.error.SessionLost
import sftp.connector.pool.SftpPool
import sftp.connector.pool.virtualClock
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Call
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path
import java.time.Instant
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * What a retry knows, operation by operation, against a scripted server on virtual time.
 *
 * A reply that went missing carries no information: the request may have landed or not. Every
 * test here stages that moment - the fake carries the request out and then loses the session -
 * and asks what the caller is told afterwards. The answer has to be the truth about the server,
 * never the truth about the last session.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class RetrySemanticsTest {

    @TempDir
    lateinit var stage: Path

    private val meters = SimpleMeterRegistry()

    private lateinit var pool: SftpPool

    /**
     * The rename landed and its reply was lost. The retry finds no source, looks at the target,
     * finds a file of the size the source had, and reports what happened: success.
     */
    @Test
    fun `I11_a rename retried after a lost reply reports success when the target holds the expected size`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) landAndLoseTheReply(call) }
            .file(FROM, CONTENT)
        val client = clientOver(server)

        client.rename(FROM, TO, Overwrite.REFUSE, server.listed(FROM))

        assertThat(server.calls.filter { it.operation == Operation.Rename })
            .describedAs("the retry found the landed file before it sent anything")
            .containsExactly(Call(Operation.Rename, session = 1, path = FROM))
        assertThat(server.calls).contains(Call(Operation.Stat, session = 2, path = TO))
        assertThat(retries("rename")).isEqualTo(1.0)
        assertThat(server.calls).describedAs("the session the reply was lost on").contains(Call(Operation.Close, 1))
        assertNothingIsStillOut()
    }

    /**
     * The same lost reply, but the file at the target is not the one that was moved: it has
     * another size. The source is at neither place, and that is what the caller is told.
     */
    @Test
    fun `I11_a stranger's file at the target is not taken for the landed one`() = runTest {
        val server = fakeServer { call ->
            if (call.isFirstOf(Operation.Rename)) {
                remove(FROM)
                file(TO, "not it")
                throw SessionLost(Attempt(ENDPOINT, "rename", FROM), "the tunnel went quiet")
            }
        }.file(FROM, CONTENT)
        val client = clientOver(server)

        assertThat(failureOf { client.rename(FROM, TO, listed = server.listed(FROM)) })
            .isInstanceOf(NoSuchFile::class.java)
            .hasMessageContaining("path=$FROM")
            .hasMessageContaining("attempt=2")
        assertNothingIsStillOut()
    }

    /**
     * Under `REPLACE` the target is expected to be occupied - yesterday's file of the same name -
     * and a file of the same size there is the common case, not a stranger. A retry after a reply
     * lost before anything landed must not take it for its own landed file: the file it was told
     * to move is still where it was, and it is moved (T17 lens 5 H2; D46).
     */
    @Test
    fun `I15_under REPLACE a file already at the target with the source's size is not taken for the landed one`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) throw SessionLost(Attempt(ENDPOINT, "rename", FROM), "the tunnel went quiet") }
            .file(FROM, CONTENT)
            .file(TO, OTHER_OF_THE_SAME_LENGTH, modifiedAt = Instant.parse("2023-12-31T00:00:00Z"))
        val client = clientOver(server)

        client.rename(FROM, TO, Overwrite.REPLACE, server.listed(FROM))

        assertFalse(FROM in server.snapshot(), "the source was moved")
        assertEquals(CONTENT, server.bytesAt(TO)!!.decodeToString(), "the file at the target is the one that was moved")
        assertNothingIsStillOut()
    }

    /**
     * The same, when even the modification time cannot tell the two apart - a file at the target
     * written in the same second. Then the source decides: the file as it was listed, still at
     * the source, is a rename that did not land, whatever the target looks like.
     */
    @Test
    fun `I15_under REPLACE a look-alike at the target does not outweigh the listed file still at the source`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) throw SessionLost(Attempt(ENDPOINT, "rename", FROM), "the tunnel went quiet") }
            .file(FROM, CONTENT)
            .file(TO, OTHER_OF_THE_SAME_LENGTH)
        val client = clientOver(server)

        client.rename(FROM, TO, Overwrite.REPLACE, server.listed(FROM))

        assertFalse(FROM in server.snapshot(), "the source was moved")
        assertEquals(CONTENT, server.bytesAt(TO)!!.decodeToString(), "the file at the target is the one that was moved")
        assertTrue(Call(Operation.Stat, session = 2, path = FROM) in server.calls, "the retry looked at the source")
        assertNothingIsStillOut()
    }

    /**
     * The reply is lost on the last permitted try, so no retry will look for the landed file.
     * What the retry would have known is not thrown away with it: one look after the last try
     * decides landed or not, so a file that moved is reported as moved rather than as "still
     * where it was" (T17 lens 5 M4; I15's bound).
     */
    @Test
    fun `a rename whose reply is lost on the last permitted try is looked into before it is reported`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) landAndLoseTheReply(call) }
            .file(FROM, CONTENT)
        val client = clientOver(server) { resilience { retry { maxAttempts = 1 } } }

        client.rename(FROM, TO, Overwrite.REFUSE, server.listed(FROM))

        assertTrue(Call(Operation.Stat, session = 2, path = TO) in server.calls, "the look at the target, on a fresh session: ${server.calls}")
        assertEquals(0.0, retries("rename"), "no retry was permitted")
        assertNothingIsStillOut()
    }

    /** A caller that did not list the file pays one round trip to learn the size, once, before the first request. */
    @Test
    fun `I11_a rename with no size given measures the source once before sending anything`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) landAndLoseTheReply(call) }
            .file(FROM, CONTENT)
        val client = clientOver(server)

        client.rename(FROM, TO, Overwrite.REPLACE)

        val beforeTheFirstRename = server.calls.takeWhile { it.operation != Operation.Rename }
        assertThat(beforeTheFirstRename.filter { it.operation == Operation.Stat })
            .describedAs("the source measured, on the first session, before the rename went out")
            .containsExactly(Call(Operation.Stat, session = 1, path = FROM))
        assertNothingIsStillOut()
    }

    /**
     * Under a refusing policy the look at the target runs once. A retry that looked again would
     * find its own landed file and refuse itself - a phantom failure with the one disposition
     * that says never try again.
     */
    @Test
    fun `a refusing rename asks about the target once, so its retry is not refused by its own landed file`() = runTest {
        val server = fakeServer { call -> if (call.isFirstOf(Operation.Rename)) landAndLoseTheReply(call) }
            .file(FROM, CONTENT)
        val client = clientOver(server)

        client.rename(FROM, TO, Overwrite.REFUSE, server.listed(FROM))

        assertThat(server.calls.filter { it.operation == Operation.Stat && it.path == TO }.map { it.session })
            .describedAs("the policy's look on the first session, and only the landed-file look on the second")
            .containsExactly(1, 2)
    }

    @Test
    fun `an upload under a refusing policy asks once, so its retry writes over what it left`() = runTest {
        val local = stage.resolve("ledger.csv").also { it.writeText(CONTENT) }
        val server = fakeServer { call ->
            if (call.isFirstOf(Operation.Write)) {
                file(TO, CONTENT)
                throw SessionLost(Attempt(ENDPOINT, "write", TO), "the tunnel went quiet")
            }
        }
        val client = clientOver(server)

        client.upload(local, TO, Overwrite.REFUSE)

        assertThat(server.calls.filter { it.operation == Operation.Stat }).containsExactly(Call(Operation.Stat, 1, TO))
        assertThat(server.calls.filter { it.operation == Operation.Write }.map { it.session }).containsExactly(1, 2)
        assertThat(retries("upload")).isEqualTo(1.0)
        assertNothingIsStillOut()
    }

    @Test
    fun `a delete retried after a lost reply reads a missing path as the delete having worked`() = runTest {
        val server = fakeServer { call ->
            if (call.isFirstOf(Operation.Delete)) {
                remove(FROM)
                throw SessionLost(Attempt(ENDPOINT, "delete", FROM), "the tunnel went quiet")
            }
        }.file(FROM, CONTENT)
        val client = clientOver(server)

        client.delete(FROM)

        assertThat(server.calls.filter { it.operation == Operation.Delete }.map { it.session }).containsExactly(1, 2)
        assertThat(retries("delete")).isEqualTo(1.0)
        assertNothingIsStillOut()
    }

    /** A path that was never there is the answer, on the first try, and asking again would ask the same unchanged question. */
    @Test
    fun `a delete of a path that is not there is reported on the first try and not tried again`() = runTest {
        val server = FakeSftpTransport()
        val client = clientOver(server)

        assertThat(failureOf { client.delete(FROM) }).isInstanceOf(NoSuchFile::class.java).hasMessageContaining("attempt=1")

        assertThat(server.calls.filter { it.operation == Operation.Delete }).hasSize(1)
        assertThat(retries("delete")).isZero()
    }

    @Test
    fun `a mkdir retried after a lost reply finds its directory there and is content`() = runTest {
        val server = fakeServer { call ->
            if (call.isFirstOf(Operation.Mkdir)) {
                directory(TO)
                throw SessionLost(Attempt(ENDPOINT, "mkdir", TO), "the tunnel went quiet")
            }
        }
        val client = clientOver(server)

        client.mkdir(TO)

        assertThat(server.calls.filter { it.operation == Operation.Mkdir }.map { it.session }).containsExactly(1, 2)
        assertNothingIsStillOut()
    }

    @Test
    fun `a download that loses its session starts over into a fresh partial file on a new one`() = runTest {
        val server = fakeServer { call ->
            if (call.isFirstOf(Operation.Read)) throw SessionLost(Attempt(ENDPOINT, "read", FROM), "the tunnel went quiet")
        }.file(FROM, CONTENT)
        val client = clientOver(server)

        val landed = client.download(RemoteFileOf(FROM, CONTENT))

        assertThat(landed.path.readText()).isEqualTo(CONTENT)
        assertThat(stage.listDirectoryEntries().map { it.fileName.toString() }).containsExactly("ledger.csv")
        assertThat(server.calls.filter { it.operation == Operation.Read }.map { it.session }).containsExactly(1, 2)
        assertThat(retries("download")).isEqualTo(1.0)
        assertNothingIsStillOut()
    }

    /**
     * On a directory another system moves files out of, a listed file that is gone by the time
     * it is fetched is ordinary. Fetching it three times and charging the server for it would
     * open the breaker on a healthy server doing exactly what it is for.
     */
    @Test
    fun `a download of a file that is gone is not tried again and not held against the server`() = runTest {
        val server = FakeSftpTransport()
        val client = clientOver(server) { resilience { circuitBreaker { slidingWindow = 1 } } }

        assertThat(failureOf { client.download(RemoteFileOf(FROM, CONTENT)) }).isInstanceOf(NoSuchFile::class.java)

        assertThat(server.calls.filter { it.operation == Operation.Read }).hasSize(1)
        assertThat(breakerState()).describedAs("a breaker of one call, which one counted failure would have opened").isZero()
        assertNothingIsStillOut()
    }

    /** Fatal, refused, and wait-a-tick failures each say so themselves; the retry reads it and stops. */
    @Test
    fun `a failure whose disposition says never is not tried again and not counted`() = runTest {
        val wrongPassword = fakeServer { call ->
            if (call.operation == Operation.Connect) throw AuthenticationFailed(Attempt(ENDPOINT, "connect"), "wrong password")
        }
        assertThat(failureOf { clientOver(wrongPassword) { oneCallBreaker() }.exists(FROM) })
            .isInstanceOf(AuthenticationFailed::class.java)
        assertThat(wrongPassword.calls).hasSize(1)
        assertThat(breakerState()).isZero()

        val occupied = FakeSftpTransport().file(TO, "already here")
        val local = stage.resolve("ledger.csv").also { it.writeText(CONTENT) }
        assertThat(failureOf { clientOver(occupied) { oneCallBreaker() }.upload(local, TO, Overwrite.REFUSE) })
            .isInstanceOf(OverwriteRefused::class.java)
        assertThat(occupied.calls.filter { it.operation == Operation.Write }).isEmpty()
        assertThat(breakerState()).isZero()

        val forbidden = fakeServer { call ->
            if (call.operation == Operation.Delete) throw PermissionDenied(Attempt(ENDPOINT, "delete", FROM), "refused on permissions")
        }.file(FROM, CONTENT)
        assertThat(failureOf { clientOver(forbidden).delete(FROM) }).isInstanceOf(PermissionDenied::class.java)
        assertThat(forbidden.calls.filter { it.operation == Operation.Delete }).hasSize(1)
        assertThat(retries("delete")).isZero()
    }

    /** S3 on virtual time: the breaker's wait in open is measured on the clock the connector was given. */
    @Test
    fun `S3_an open breaker fails a call fast without a session, and a probe after the wait closes it`() = runTest {
        var refusals = 0
        val server = fakeServer { call ->
            if (call.operation == Operation.Connect && refusals++ < 2) throw ConnectFailed(Attempt(ENDPOINT, "connect"), "the proxy refused")
        }.file(FROM, CONTENT)
        val client = clientOver(server) {
            resilience {
                retry { maxAttempts = 1 }
                circuitBreaker { slidingWindow = 2; failureRateThreshold = 50; waitInOpen = 1.minutes }
            }
        }
        repeat(2) { assertThat(failureOf { client.exists(FROM) }).isInstanceOf(ConnectFailed::class.java) }

        assertThat(failureOf { client.exists(FROM) })
            .isInstanceOf(CircuitOpen::class.java)
            .hasMessageContaining("op=exists")
        assertThat(server.calls).describedAs("dials: two that failed, none for the call the breaker stopped").hasSize(2)
        assertThat(breakerState()).isEqualTo(2)

        testScheduler.advanceTimeBy(1.minutes + 1.seconds)
        assertThat(client.exists(FROM)).describedAs("the half-open probe").isTrue()
        assertThat(breakerState()).isZero()
        assertNothingIsStillOut()
    }

    /**
     * Spec 9: the breaker counts recoverable errors and nothing else. The library it is built on
     * also counts a *slow success* - anything over a minute, by default - and opens on a window
     * of them, so a pipeline moving large files over a slow link was being throttled by its own
     * safety mechanism with no failure having occurred (T17 lens 5 H1). Measured on the clock the
     * connector was given, so the minute passes without being waited for.
     */
    @Test
    fun `a success slower than the library's own minute is not held against the server`() = runTest {
        val server = fakeServer { call -> if (call.operation == Operation.Stat) delay(61.seconds) }.file(FROM, CONTENT)
        val client = clientOver(server) { resilience { operationTimeout = 2.minutes; circuitBreaker { slidingWindow = 2 } } }

        repeat(2) { assertTrue(client.exists(FROM)) }

        assertEquals(0, breakerState(), "a breaker of two calls, both slow successes, is still closed")
        assertTrue(client.exists(FROM), "the third call is not refused")
        assertEquals(0.0, retries("exists"))
        assertNothingIsStillOut()
    }

    /**
     * A try that runs out of time is cancelled - which is what frees the thread - and then
     * reported as what it was: a request that may still land, worth another go and worth
     * counting against the server. Whether the session survives is the pool's ladder's call,
     * made on what actually stopped the call: the fake stops on the spot, so here it does.
     */
    @Test
    fun `a try that runs out of time is reported as a timeout and tried again`() = runTest {
        var hung = false
        val server = fakeServer { call ->
            if (call.operation == Operation.Stat && !hung) {
                hung = true
                awaitCancellation()
            }
        }.file(FROM, CONTENT)
        val client = clientOver(server) { pool { acquireTimeout = 1.seconds }; resilience { operationTimeout = 2.seconds } }

        assertThat(client.exists(FROM)).isTrue()

        assertThat(server.calls.filter { it.operation == Operation.Stat }).hasSize(2)
        assertThat(retries("exists")).isEqualTo(1.0)
        assertNothingIsStillOut()
    }

    /** A caller that gave up looks, from inside, exactly like a try that ran out of time. It is not one. */
    @Test
    fun `a caller's own cancellation is not mistaken for a timeout and not tried again`() = runTest {
        val server = fakeServer { call -> if (call.operation == Operation.Stat) awaitCancellation() }.file(FROM, CONTENT)
        val client = clientOver(server)

        val walkedAway = launch { client.exists(FROM) }
        testScheduler.advanceTimeBy(100.milliseconds)
        walkedAway.cancel()
        walkedAway.join()

        assertThat(walkedAway.isCancelled).isTrue()
        assertThat(server.calls.filter { it.operation == Operation.Stat }).hasSize(1)
        assertThat(retries("exists")).isZero()
        assertNothingIsStillOut()
    }

    /** The failure that ends a call names the try it was, and the wait for a session names the call that queued. */
    @Test
    fun `a failure names which try it was, and a full pool names the operation that was queued for it`() = runTest {
        val alwaysDown = fakeServer { call ->
            // Numbered the way the real transport numbers its failures: by the try it is inside.
            if (call.operation == Operation.Stat) throw SessionLost(Attempt.inside(ENDPOINT, "stat", FROM), "the tunnel went quiet")
        }.file(FROM, CONTENT)
        val exhausted = failureOf { clientOver(alwaysDown).exists(FROM) }
        assertThat(exhausted)
            .isInstanceOf(SessionLost::class.java)
            .hasMessageContaining("attempt=3")
        // The budget, not only the number: "attempt=3" and "attempt=1" read alike to anyone who
        // does not know maxAttempts, and "this was the last one" is what decides whether to wait.
        assertTrue(exhausted.message!!.contains("attempt=3 of 3"), "the try and the budget: ${exhausted.message}")
        assertThat(retries("exists")).isEqualTo(2.0)

        val client = clientOver(FakeSftpTransport().file(FROM, CONTENT)) { pool { maxSize = 1; acquireTimeout = 1.seconds } }
        val held = pool.acquire()
        assertThat(failureOf { client.exists(FROM) })
            .isInstanceOf(PoolExhausted::class.java)
            .hasMessageContaining("op=exists")
            .hasMessageContaining("path=$FROM")
        held.release()
    }

    /**
     * A listing hands entries on as they arrive, so starting one over would hand them on twice.
     * It is tried again only while nothing has been handed on yet.
     */
    @Test
    fun `a listing is tried again only while nothing has been handed on`() = runTest {
        val diesOnFirstList = fakeServer { call ->
            if (call.isFirstOf(Operation.List)) throw SessionLost(Attempt(ENDPOINT, "list", "/drop"), "the tunnel went quiet")
        }.file(FROM, CONTENT)
        assertThat(clientOver(diesOnFirstList).list("/drop").toList().map { it.path }).containsExactly(FROM)
        assertThat(retries("list")).isEqualTo(1.0)

        val server = FakeSftpTransport().file("/drop/one.csv", "1").file("/drop/two.csv", "2")
        val client = clientOver(server)
        var handedOn = 0
        // The session dies underneath the listing after its first entry: staged from the entry
        // callback, which is the only place the fake lets a failure land mid-listing.
        val listing = client.list("/drop") { if (handedOn++ == 1) throw SessionLost(Attempt(ENDPOINT, "list", "/drop"), "the tunnel went quiet"); true }
        assertThat(failureOf { listing.toList() }).isInstanceOf(SessionLost::class.java)
        assertThat(server.calls.filter { it.operation == Operation.List }).hasSize(1)
    }

    /**
     * Transfers beyond the limit wait their turn rather than taking every session, and one that
     * stops waiting takes nothing with it: the next transfer still gets a turn.
     */
    @Test
    fun `transfers past the limit wait for one to finish, and a waiter that gives up frees its place`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val server = fakeServer { call -> if (call.operation == Operation.Read) gate.await() }
        repeat(4) { server.file("/drop/file-$it.csv", CONTENT) }
        val client = clientOver(server) { pool { maxSize = 4 }; resilience { bulkhead { maxConcurrentTransfers = 2 } } }

        val downloads = (0..2).map { launch { client.download(RemoteFileOf("/drop/file-$it.csv", CONTENT)) } }
        val walkedAway = launch { client.download(RemoteFileOf("/drop/file-3.csv", CONTENT)) }
        testScheduler.runCurrent()
        assertThat(server.calls.filter { it.operation == Operation.Read }).describedAs("transfers on the wire at once").hasSize(2)

        walkedAway.cancel()
        gate.complete(Unit)
        downloads.forEach { it.join() }

        assertThat(server.calls.filter { it.operation == Operation.Read }.map { it.path })
            .containsExactlyInAnyOrder("/drop/file-0.csv", "/drop/file-1.csv", "/drop/file-2.csv")
        assertNothingIsStillOut()
    }

    @Test
    fun `the meters the ticket names are published under the endpoint`() = runTest {
        clientOver(FakeSftpTransport().file(FROM, CONTENT)).exists(FROM)

        assertThat(meters.find("sftp_breaker_state").tag("endpoint", ENDPOINT).gauge()?.value()).isEqualTo(0.0)
        assertThat(meters.find("sftp_retry_total").counters()).describedAs("nothing was retried, so nothing is counted yet").isEmpty()
    }

    private suspend fun failureOf(block: suspend () -> Unit): Throwable =
        runCatching { block() }.exceptionOrNull() ?: throw AssertionError("the call was expected to fail and did not")

    private fun retries(op: String): Double =
        meters.find("sftp_retry_total").tag("endpoint", ENDPOINT).tag("op", op).counter()?.count() ?: 0.0

    private fun breakerState(): Int = meters.get("sftp_breaker_state").gauge().value().toInt()

    private suspend fun assertNothingIsStillOut() {
        assertThat(pool.stats().inUse).describedAs("sessions still out on lease").isZero()
        assertThat(pool.stats().connecting).describedAs("sessions half open").isZero()
    }

    /** The request went out and did what it does, and then the session was lost before the reply came back. */
    private fun FakeSftpTransport.landAndLoseTheReply(call: Call) {
        val to = TO
        file(to, CONTENT)
        remove(call.path!!)
        throw SessionLost(Attempt(ENDPOINT, "rename", call.path), "the tunnel went quiet")
    }

    private fun Call.isFirstOf(operation: Operation) = this.operation == operation && session == 1

    /** A fake whose script can reach the fake itself, to move files about before it fails. */
    private fun fakeServer(answer: suspend FakeSftpTransport.(Call) -> Unit): FakeSftpTransport {
        lateinit var server: FakeSftpTransport
        server = FakeSftpTransport { call -> server.answer(call) }
        return server
    }

    /** The breaker reads the scheduler's clock, so advancing virtual time moves its wait in open. */
    private fun TestScope.clientOver(transport: FakeSftpTransport, extra: SftpConnectorBuilder.() -> Unit = {}): SftpClient {
        val config = configFor(stage, extra)
        pool = SftpPool(transport, config, meters)
        return SftpClient(pool, config, meters, virtualClock())
    }

    private fun SftpConnectorBuilder.oneCallBreaker() {
        resilience { circuitBreaker { slidingWindow = 1 } }
    }

    @Suppress("TestFunctionName")
    private fun RemoteFileOf(path: String, content: String) =
        sftp.connector.transport.RemoteFile(path, content.length.toLong(), java.time.Instant.EPOCH, isDirectory = false)

    private companion object {
        private const val ENDPOINT = "fake.example:22"
        private const val FROM = "/drop/ledger.csv"
        private const val TO = "/drop/temp/ledger.csv"
        private const val CONTENT = "id,amount\n1,42\n"
        private const val OTHER_OF_THE_SAME_LENGTH = "id,amount\n1,41\n"

        private fun configFor(stage: Path, extra: SftpConnectorBuilder.() -> Unit): SftpConnectorConfig =
            sftpConnector("retry-semantics") {
                endpoint { host = "fake.example"; port = 22 }
                auth { password("etl", "s3cret") }
                hostKey = HostKeyPolicy.AcceptAll
                pool { maxSize = 2 }
                polling { staging { dir = stage } }
                resilience { retry { backoff = exponential(1.seconds, max = 4.seconds, jitter = false) } }
                extra()
            }
    }
}
