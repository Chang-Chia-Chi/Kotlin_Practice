package sftp.connector.source

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.source.SftpEvent.FileSeen
import sftp.connector.source.SftpEvent.PollCompleted
import sftp.connector.testkit.EmbeddedSftpServer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.io.path.writeText

/**
 * The hourly use case end to end against a real server, through a started connector: the folder
 * the start-up made is the folder the ack moves into, and the file the listing reported is the
 * file the download fetched. The three scenarios here are the ones the fake cannot vouch for,
 * because each is about what the server does with a file that is really on a disk.
 */
class SourceAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var stage: Path

    private val registry = SimpleMeterRegistry()

    /** S5. The file was listed, and by the time the consumer went for it something else had taken it. */
    @Test
    fun `S5_a file removed between the listing and the download answers null, not an error`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withConnector { connector ->
            val events = mutableListOf<SftpEvent>()
            connector.source.poll("/drop").collect { event ->
                events += event
                if (event is FileSeen) {
                    Files.delete(remoteRoot.resolve("drop/ledger.csv"))
                    assertThat(event.download()).isNull()
                    assertThat(inFlight()).describedAs("places taken the instant the null arrived").isZero()
                }
            }

            assertThat(events.filterIsInstance<FileSeen>().map { it.file.name }).containsExactly("ledger.csv")
            assertThat(events.last()).isInstanceOf(PollCompleted::class.java)
            assertThat(inFlight()).isZero()
            assertThat(stage.listDirectoryEntries()).isEmpty()
        }
    }

    /**
     * S7. The consumer knows the file from an earlier run and only the server-side move is
     * missing, so it acks without downloading. The move runs, and no bytes cross the wire: the
     * staging directory is as empty afterwards as before.
     */
    @Test
    fun `S7_an ack without a download runs the move and transfers nothing`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withConnector { connector ->
            connector.source.poll("/drop").toList().filterIsInstance<FileSeen>().single().ack()
        }

        assertThat(remoteRoot.resolve("drop/ledger.csv").exists()).isFalse()
        assertThat(remoteRoot.resolve("drop/temp/ledger.csv").readText()).isEqualTo(CONTENT)
        assertThat(stage.listDirectoryEntries()).isEmpty()
    }

    /**
     * S12. Two polls of the same directory at once - which is what a `PROCEED` overlap will be -
     * and the file is with the consumer of the first while the second lists. It is handed over
     * once: the second poll sees it, counts it, and does not emit it.
     */
    @Test
    fun `S12_a file listed again while in flight is handed over once`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("drop/ledger.csv").writeText(CONTENT)

        withConnector { connector ->
            val held = CompletableDeferred<FileSeen>()
            val first = launch {
                connector.source.poll("/drop").collect { if (it is FileSeen) { held.complete(it); awaitCancellation() } }
            }
            held.await()

            val second = connector.source.poll("/drop").toList()

            assertThat(second.filterIsInstance<FileSeen>()).describedAs("handed over a second time").isEmpty()
            assertThat(second.last()).isEqualTo(PollCompleted(2, seen = 1, emitted = 0, notReady = 0, inFlight = listOf(held.await().file)))
            first.cancelAndJoin()
            assertThat(inFlight()).isZero()
        }
    }

    private fun inFlight(): Int = registry.get("sftp_inflight").gauge().value().toInt()

    private suspend fun withConnector(block: suspend (SftpConnector) -> Unit) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD).use { server ->
            val config = sftpConnector("source-demo") {
                endpoint { host = server.host; port = server.port }
                auth { password(USER, PASSWORD) }
                hostKey = HostKeyPolicy.AcceptAll
                polling {
                    staging { dir = stage }
                    directories("/drop")
                    onAck = move("temp/")
                    readiness = ReadinessCheck { _, _ -> Readiness.Ready }
                }
            }
            val connector = SftpConnector.start(config, meterRegistry = registry)
            try {
                block(connector)
            } finally {
                connector.backgroundWork.cancelAndJoin()
            }
        }
    }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
        private const val CONTENT = "id,amount\n1,42\n"
    }
}
