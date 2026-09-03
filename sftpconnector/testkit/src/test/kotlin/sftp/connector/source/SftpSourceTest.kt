package sftp.connector.source

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.client.Overwrite
import sftp.connector.client.SftpClient
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.PollingBuilder
import sftp.connector.config.sftpConnector
import sftp.connector.pool.SftpPool
import sftp.connector.pool.virtualClock
import sftp.connector.source.SftpEvent.FileGone
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.source.SftpEvent.PollStarted
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.nio.file.Path

/**
 * The source against a scripted server: what a poll hands over, what an answer does to the file,
 * and the three promises the in-flight set makes - once, never twice, and every place back.
 *
 * Everything here runs on virtual time. Nothing waits, and a listing that has to stop for the
 * consumer is proved to have stopped by looking, not by waiting to see whether it moves.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SftpSourceTest {

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    private lateinit var client: SftpClient

    @Test
    fun `a poll is cold, and reports the listing as events`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "22")
        val poll = sourceOver(transport) {}.poll("/drop")

        assertThat(transport.calls.filter { it.operation == Operation.List }).describedAs("listings before anyone collected").isEmpty()

        val events = poll.toList()

        assertThat(events.first()).isEqualTo(PollStarted(1, "/drop"))
        assertThat(events.filterIsInstance<FileSeen>().map { it.file.path }).containsExactly("/drop/a.csv", "/drop/b.csv")
        assertThat(events.last()).isEqualTo(PollCompleted(1, seen = 2, emitted = 2, notReady = 0))
    }

    /**
     * The move an ack performs, against a server without the POSIX rename extension and with the
     * target already occupied, so the whole replace sequence has to run. That the file ends up in
     * the folder and nowhere else is the ack; that the set is empty afterwards is the place coming
     * back.
     */
    @Test
    fun `an ack moves the file into its folder and gives the place back`() = runTest {
        val transport = FakeSftpTransport()
            .directory("/drop").file("/drop/a.csv", "new").directory("/drop/temp").file("/drop/temp/a.csv", "old")
        val source = sourceOver(transport) { onAck = move("temp/", Overwrite.REPLACE) }

        source.poll("/drop").toList().filterIsInstance<FileSeen>().single().ack()

        assertThat(client.exists("/drop/a.csv")).isFalse()
        assertThat(client.stat("/drop/temp/a.csv")?.size).isEqualTo(3L)
        assertThat(inFlight()).isZero()
    }

    @Test
    fun `a delete action removes the file, and a noop leaves it where it was`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")
        val source = sourceOver(transport) { onAck = delete(); onNack = noop() }

        val (a, b) = source.poll("/drop").toList().filterIsInstance<FileSeen>()
        a.ack()
        b.nack(IllegalStateException("could not parse it"))

        assertThat(client.exists("/drop/a.csv")).isFalse()
        assertThat(client.exists("/drop/b.csv")).isTrue()
        assertThat(inFlight()).isZero()
    }

    @Test
    fun `a nacked file is handed over again on a later poll unless told otherwise`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")
        val source = sourceOver(transport) {}

        val (a, b) = source.poll("/drop").toList().filterIsInstance<FileSeen>()
        a.nack(IllegalStateException("try again"), redeliver = true)
        b.nack(IllegalStateException("never again"), redeliver = false)

        val again = source.poll("/drop").toList()
        assertThat(again.filterIsInstance<FileSeen>().map { it.file.path }).containsExactly("/drop/a.csv")
        assertThat(again.last()).isEqualTo(PollCompleted(2, seen = 2, emitted = 1, notReady = 0))
    }

    /**
     * I12. The second answer to a file does nothing, whichever of the two it is. The ack action is
     * a move, so a second ack that ran would fail on a source that is no longer there; the nack
     * action is a delete, so a nack that ran after the ack would remove the file from the folder
     * it was moved to. Neither happens, and the record of requests says so.
     */
    @Test
    fun `I12_ack and nack are each accepted once per file`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").directory("/drop/temp")
        val source = sourceOver(transport) { onAck = move("temp/"); onNack = delete() }
        val seen = source.poll("/drop").toList().filterIsInstance<FileSeen>().single()

        seen.ack()
        seen.ack()
        seen.nack(IllegalStateException("too late"))

        assertThat(transport.calls.count { it.operation == Operation.Rename }).describedAs("moves").isEqualTo(1)
        assertThat(transport.calls.count { it.operation == Operation.Delete }).describedAs("deletes").isZero()
        assertThat(client.exists("/drop/temp/a.csv")).isTrue()
        assertThat(inFlight()).isZero()
    }

    /**
     * I7. A file the consumer still has is not handed over by the next poll, however many times
     * the directory is listed. It is counted as seen, because it was, and once the consumer gives
     * it back it is handed over again.
     */
    @Test
    fun `I7_a file in flight is not handed over by any poll`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1")) {}
        val held = source.poll("/drop").toList().filterIsInstance<FileSeen>().single()

        repeat(3) {
            val events = source.poll("/drop").toList()
            assertThat(events.filterIsInstance<FileSeen>()).describedAs("handed over while still in flight").isEmpty()
            assertThat(events.last()).isEqualTo(PollCompleted(it + 2L, seen = 1, emitted = 0, notReady = 0))
        }

        held.ack()
        assertThat(source.poll("/drop").toList().filterIsInstance<FileSeen>()).hasSize(1)
    }

    /**
     * I7 with two polls racing for the same file through a full set. Both have found the file
     * free and both are waiting for room; room comes free twice, one at a time. The first to be
     * let in takes the file, and the second must find it taken *after* being let in - which is
     * the check made under the lock, and the one a check made before the wait cannot replace.
     */
    @Test
    fun `I7_a file two waiting polls both want is handed over once`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a1.csv", "1").file("/drop/a2.csv", "2")
        val source = sourceOver(transport) { maxInFlight = 2 }
        val held = source.poll("/drop").toList().filterIsInstance<FileSeen>()
        transport.file("/drop/b.csv", "3")

        val first = async { source.poll("/drop").toList() }
        val second = async { source.poll("/drop").toList() }
        runCurrent()
        assertThat(first.isCompleted || second.isCompleted).describedAs("a poll got through a full set").isFalse()

        held[0].ack()
        runCurrent()
        held[1].ack()
        runCurrent()

        val handedOver = (first.await() + second.await()).filterIsInstance<FileSeen>().map { it.file.path }
        assertThat(handedOver).containsExactly("/drop/b.csv")
        assertThat(inFlight()).isEqualTo(1)
    }

    /**
     * The other way a collection ends without an answer: the consumer's own block throws. Nobody
     * is going to ack a file whose processing failed the whole collection, and a place nobody gives
     * back is a place lost until restart.
     */
    @Test
    fun `a consumer whose block throws gives every place back as well`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")) {}

        val failure = runCatching {
            source.poll("/drop").collect { if (it is FileSeen && it.file.name == "b.csv") throw IllegalStateException("bad row") }
        }.exceptionOrNull()

        assertThat(failure).isInstanceOf(IllegalStateException::class.java)
        assertThat(inFlight()).describedAs("places still taken after the failure").isZero()
        assertThat(source.poll("/drop").toList().filterIsInstance<FileSeen>()).hasSize(2)
    }

    /**
     * I8. A collector cancelled with files in its hands gives all of them back: the set is empty,
     * every one is handed over again by the next poll, and the meters say they came back as
     * cancelled rather than as anything the consumer decided.
     */
    @Test
    fun `I8_cancelling a collector with unacked files gives every place back`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")) {}
        val received = mutableListOf<FileSeen>()
        val collector = launch {
            source.poll("/drop").collect { event ->
                if (event is FileSeen) received += event
                if (received.size == 2) awaitCancellation()
            }
        }
        runCurrent()
        assertThat(received).hasSize(2)
        assertThat(inFlight()).isEqualTo(2)

        collector.cancelAndJoin()

        assertThat(inFlight()).describedAs("places still taken after the cancel").isZero()
        assertThat(registry.get("sftp_ack_total").tag("outcome", "cancelled").counter().count()).isEqualTo(2.0)
        assertThat(source.poll("/drop").toList().filterIsInstance<FileSeen>().map { it.file.path })
            .containsExactly("/drop/a.csv", "/drop/b.csv")
    }

    /**
     * The backpressure. With room for one file, the second is not handed over until the first
     * comes back, and the poll is neither finished nor failed in the meantime - it is waiting.
     */
    @Test
    fun `the listing waits when maxInFlight files are out, and moves on when one comes back`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")) {
            maxInFlight = 1
        }
        val received = mutableListOf<FileSeen>()
        val collector = launch { source.poll("/drop").collect { if (it is FileSeen) received += it } }

        runCurrent()
        assertThat(received.map { it.file.path }).containsExactly("/drop/a.csv")
        assertThat(collector.isCompleted).describedAs("poll finished without handing over the second file").isFalse()

        received.single().ack()
        runCurrent()

        assertThat(received.map { it.file.path }).containsExactly("/drop/a.csv", "/drop/b.csv")
        assertThat(collector.isCompleted).isTrue()
    }

    @Test
    fun `a file that is not ready is counted and looked at again next poll`() = runTest {
        var finished = false
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1")) {
            readiness = ReadinessCheck { _, _ -> if (finished) Readiness.Ready else Readiness.NotReady("still writing") }
        }

        val first = source.poll("/drop").toList()
        assertThat(first.filterIsInstance<FileSeen>()).isEmpty()
        assertThat(first.last()).isEqualTo(PollCompleted(1, seen = 1, emitted = 0, notReady = 1))

        finished = true
        assertThat(source.poll("/drop").toList().last()).isEqualTo(PollCompleted(2, seen = 1, emitted = 1, notReady = 0))
    }

    /**
     * The folders the actions move files into sit inside the watched directory, which is the
     * usual layout, and a walk that descended into them would hand every dealt-with file back to
     * the consumer. Any other subdirectory is walked.
     */
    @Test
    fun `the folders actions move files into are left out of a recursive walk`() = runTest {
        val transport = FakeSftpTransport()
            .directory("/drop").file("/drop/a.csv", "1")
            .directory("/drop/sub").file("/drop/sub/b.csv", "2")
            .directory("/drop/temp").file("/drop/temp/done.csv", "3")
            .directory("/drop/failed").file("/drop/failed/bad.csv", "4")
        val source = sourceOver(transport) { recursive = true; onAck = move("temp/"); onNack = move("failed/") }

        val seen = source.poll("/drop").toList().filterIsInstance<FileSeen>().map { it.file.path }

        assertThat(seen).containsExactly("/drop/a.csv", "/drop/sub/b.csv")
    }

    @Test
    fun `a file gone at download time is reported, and needs no answer`() = runTest {
        val transport = FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1")
        val source = sourceOver(transport) {}
        val events = mutableListOf<SftpEvent>()

        source.poll("/drop").collect { event ->
            events += event
            if (event is FileSeen) {
                transport.remove(event.file.path)
                assertThat(event.download()).isNull()
            }
        }

        assertThat(events.filterIsInstance<FileGone>().map { it.file.path }).containsExactly("/drop/a.csv")
        assertThat(events.indexOfFirst { it is FileGone }).isEqualTo(events.indexOfFirst { it is FileSeen } + 1)
        assertThat(inFlight()).isZero()
        assertThat(registry.get("sftp_poll_files").tag("state", "gone").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `a poll of a directory the connector was not configured for is refused at the call`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop")) {}

        assertThatThrownBy { source.poll("/elsewhere") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("/elsewhere")
    }

    @Test
    fun `the source publishes what a dashboard needs to watch a directory`() = runTest {
        val source = sourceOver(FakeSftpTransport().directory("/drop").file("/drop/a.csv", "1").file("/drop/b.csv", "2")) {}
        val (a, b) = source.poll("/drop").toList().filterIsInstance<FileSeen>()
        a.ack()
        b.nack(IllegalStateException("no"))

        assertThat(registry.get("sftp_poll_seconds").tag("result", "ok").timer().count()).isEqualTo(1L)
        assertThat(registry.get("sftp_poll_files").tag("state", "seen").counter().count()).isEqualTo(2.0)
        assertThat(registry.get("sftp_poll_files").tag("state", "emitted").counter().count()).isEqualTo(2.0)
        assertThat(registry.get("sftp_poll_files").tag("state", "notReady").counter().count()).isEqualTo(0.0)
        assertThat(registry.get("sftp_ack_total").tag("outcome", "ack").counter().count()).isEqualTo(1.0)
        assertThat(registry.get("sftp_ack_total").tag("outcome", "nack").counter().count()).isEqualTo(1.0)
        assertThat(inFlight()).isZero()
        assertThat(registry.meters.filter { it.id.name.startsWith("sftp_poll") || it.id.name in setOf("sftp_inflight", "sftp_ack_total") })
            .allSatisfy { assertThat(it.id.getTag("endpoint")).isEqualTo("fake.example:22") }
    }

    private fun inFlight(): Int = registry.get("sftp_inflight").gauge().value().toInt()

    private fun TestScope.sourceOver(transport: FakeSftpTransport, polling: PollingBuilder.() -> Unit): SftpSource {
        val config = sftpConnector("source-demo") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            polling {
                staging { dir = stage }
                directories("/drop")
                readiness = ALWAYS_READY
                polling()
            }
        }
        val pool = SftpPool(transport, config, registry, virtualClock())
        client = SftpClient(pool, config, registry)
        return SftpSource(client, config, registry, virtualClock())
    }

    private companion object {
        /** The readiness checks have tests of their own; here every file is finished. */
        private val ALWAYS_READY = ReadinessCheck { _, _ -> Readiness.Ready }
    }
}
