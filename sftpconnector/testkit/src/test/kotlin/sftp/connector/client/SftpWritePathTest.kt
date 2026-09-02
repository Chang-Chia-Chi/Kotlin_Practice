package sftp.connector.client

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.NoSuchFile
import sftp.connector.error.OverwriteRefused
import sftp.connector.error.ServerFailure
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.writeText

/**
 * The write path against a scripted server that has no POSIX rename extension, which is the server
 * the sequence is written for. An extension turns a replacement into one request and there is
 * nothing left to arrange; a server without one refuses, and everything interesting about an
 * overwrite is what the connector does with that refusal.
 *
 * The refusal is a generic one - the same status the server uses for everything it will not do -
 * so the tests below are mostly about what the connector is careful *not* to conclude from it.
 */
class SftpWritePathTest {

    @TempDir
    lateinit var local: Path

    private lateinit var pool: SftpPool

    @Test
    fun `an upload puts the local file where it was told to`() = runBlocking<Unit> {
        val server = FakeSftpTransport().directory("/drop")
        val client = clientOver(server)

        client.upload(fileHolding(CONTENT), "/drop/ledger.csv")

        assertThat(client.stat("/drop/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `an upload told to replace writes over what was there`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", "an older, longer file"))

        client.upload(fileHolding(CONTENT), "/drop/ledger.csv", Overwrite.REPLACE)

        assertThat(client.stat("/drop/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `an upload that was told not to replace sends nothing at an occupied path`() = runBlocking<Unit> {
        val server = FakeSftpTransport().file("/drop/ledger.csv", "the file that is already there")
        val client = clientOver(server)

        assertThatThrownBy { runBlocking { client.upload(fileHolding(CONTENT), "/drop/ledger.csv") } }
            .isInstanceOf(OverwriteRefused::class.java)
            .hasMessageContaining("already something at /drop/ledger.csv")

        assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Write)
        assertThat(client.stat("/drop/ledger.csv")?.size).isEqualTo("the file that is already there".length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `a rename moves the file and leaves nothing at the old name`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", CONTENT))

        client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv")

        assertThat(client.exists("/drop/ledger.csv")).isFalse()
        assertThat(client.stat("/drop/temp/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `a rename that was told not to replace leaves both files where they were`() = runBlocking<Unit> {
        val server = FakeSftpTransport()
            .file("/drop/ledger.csv", CONTENT)
            .file("/drop/temp/ledger.csv", "yesterday's run")
        val client = clientOver(server)

        assertThatThrownBy { runBlocking { client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv") } }
            .isInstanceOf(OverwriteRefused::class.java)

        // Nothing was sent at all. The connector decides this one, because a server with the POSIX
        // rename extension would have replaced the target and called it a success.
        assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Rename, Operation.Delete)
        assertThat(client.exists("/drop/ledger.csv")).isTrue()
        assertThat(client.stat("/drop/temp/ledger.csv")?.size).isEqualTo("yesterday's run".length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `a rename told to replace clears the target and sends the rename again`() = runBlocking<Unit> {
        val server = FakeSftpTransport()
            .file("/drop/ledger.csv", CONTENT)
            .file("/drop/temp/ledger.csv", "yesterday's run")
        val client = clientOver(server)

        client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv", Overwrite.REPLACE)

        assertThat(client.exists("/drop/ledger.csv")).isFalse()
        assertThat(client.stat("/drop/temp/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        // Refused, the way cleared, sent again: the sequence a server without the extension forces,
        // and the reason the target path holds nothing in between.
        assertThat(server.calls.map { it.operation }.filter { it in setOf(Operation.Rename, Operation.Delete) })
            .containsExactly(Operation.Rename, Operation.Delete, Operation.Rename)
        assertNothingIsStillOut()
    }

    /**
     * A server refuses a rename it cannot perform at all - across filesystems, say - with the same
     * status it refuses an occupied target with, so a refusal on its own says nothing about why.
     * Looking before clearing is what keeps a replacement from sending a delete and a second rename
     * for a target that was never in the way, and what gets the caller the refusal it actually got
     * rather than the one the pointless second attempt would have produced.
     */
    @Test
    fun `a rename refused while nothing is at the target is passed on rather than met with a delete`() =
        runBlocking<Unit> {
            val server = FakeSftpTransport(answer = { call ->
                if (call.operation == Operation.Rename) throw crossFilesystem()
            }).file("/drop/ledger.csv", CONTENT)
            val client = clientOver(server)

            assertThatThrownBy {
                runBlocking { client.rename("/drop/ledger.csv", "/other/ledger.csv", Overwrite.REPLACE) }
            }
                .isInstanceOf(ServerFailure::class.java)
                .hasMessageContaining("across filesystems")

            assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Delete)
            assertThat(server.calls.count { it.operation == Operation.Rename }).isEqualTo(1)
            assertNothingIsStillOut()
        }

    /**
     * What a rename after a lost reply looks like from the layer that will retry it: the source is
     * gone because the first attempt landed. It has to arrive as a missing *source*, so the retry
     * knows to go and look at the target - which is why the target being already gone is never
     * passed on as a missing path.
     */
    @Test
    fun `a rename whose source is gone reports the source and touches nothing`() = runBlocking<Unit> {
        val server = FakeSftpTransport().file("/drop/temp/ledger.csv", CONTENT)
        val client = clientOver(server)

        assertThatThrownBy { runBlocking { client.rename("/drop/ledger.csv", "/drop/temp/ledger.csv", Overwrite.REPLACE) } }
            .isInstanceOf(NoSuchFile::class.java)
            .hasMessageContaining("/drop/ledger.csv")

        assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Delete)
        assertThat(client.stat("/drop/temp/ledger.csv")?.size).isEqualTo(CONTENT.length.toLong())
        assertNothingIsStillOut()
    }

    @Test
    fun `a delete removes the file, and says so when there was nothing to remove`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", CONTENT))

        client.delete("/drop/ledger.csv")

        assertThat(client.exists("/drop/ledger.csv")).isFalse()
        assertThatThrownBy { runBlocking { client.delete("/drop/ledger.csv") } }
            .isInstanceOf(NoSuchFile::class.java)
        assertNothingIsStillOut()
    }

    @Test
    fun `mkdir with parents creates the whole path, and a second run is content with what it finds`() =
        runBlocking<Unit> {
            val server = FakeSftpTransport()
            val client = clientOver(server)

            client.mkdir("/drop/temp/2026", parents = true)
            client.mkdir("/drop/temp/2026", parents = true)

            assertThat(client.stat("/drop")?.isDirectory).isTrue()
            assertThat(client.stat("/drop/temp")?.isDirectory).isTrue()
            assertThat(client.stat("/drop/temp/2026")?.isDirectory).isTrue()
            // Three levels asked for twice. The second run asks all three again and is refused all
            // three times, and looks at each one to find a directory already there - which is the
            // outcome, so nothing is reported and nothing is created twice.
            assertThat(server.calls.count { it.operation == Operation.Mkdir }).isEqualTo(6)
            assertNothingIsStillOut()
        }

    @Test
    fun `mkdir creates one directory unless it is asked to fill in the path above it`() = runBlocking<Unit> {
        val server = FakeSftpTransport().directory("/drop")
        val client = clientOver(server)

        client.mkdir("/drop/temp")

        assertThat(client.stat("/drop/temp")?.isDirectory).isTrue()
        assertThat(server.calls.count { it.operation == Operation.Mkdir }).isEqualTo(1)
        assertNothingIsStillOut()
    }

    /**
     * A directory wanted where a file already sits is not the outcome anybody asked for, so the
     * server's refusal is passed on rather than read as "it was already there".
     */
    @Test
    fun `mkdir over a file is refused rather than treated as already done`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop", CONTENT))

        assertThatThrownBy { runBlocking { client.mkdir("/drop") } }.isInstanceOf(ServerFailure::class.java)
        assertNothingIsStillOut()
    }

    @Test
    fun `withSession keeps one session for the whole block`() = runBlocking<Unit> {
        val server = FakeSftpTransport().file("/drop/ledger.csv", CONTENT)
        val client = clientOver(server)

        val held = client.withSession {
            mkdir("/drop/temp")
            rename("/drop/ledger.csv", "/drop/temp/ledger.csv")
            // Asked from inside the block: one session is out, and it is out for all of it.
            pool.stats().inUse
        }

        assertThat(held).isEqualTo(1)
        assertThat(server.calls.count { it.operation == Operation.Connect }).isEqualTo(1)
        assertThat(server.calls.filter { it.session != 0 }.map { it.session }.distinct()).containsExactly(1)
        assertNothingIsStillOut()
    }

    @Test
    fun `withSession gives the session back whether the block returns, throws or is cancelled`() =
        runBlocking<Unit> {
            val client = clientOver(FakeSftpTransport().directory("/drop"))

            client.withSession { realpath("/drop") }
            assertNothingIsStillOut()

            runCatching { client.withSession { throw IOException("the caller's own work failed") } }
            assertNothingIsStillOut()

            val insideTheBlock = CompletableDeferred<Unit>()
            val running = launch {
                client.withSession {
                    insideTheBlock.complete(Unit)
                    CompletableDeferred<Unit>().await()
                }
            }
            insideTheBlock.await()
            running.cancel()
            running.join()
            assertNothingIsStillOut()
        }

    /**
     * The pool lends the same session to the next caller the moment this one is finished with it,
     * so a reference kept past the block is a second caller on somebody else's session. It is made
     * to stop working rather than asked to.
     */
    @Test
    fun `a session kept past the block it was given to refuses to work`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().directory("/drop"))

        val kept = client.withSession { this }

        assertThatThrownBy { runBlocking { kept.realpath("/drop") } }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("cannot be kept past the block")
        assertNothingIsStillOut()
    }

    @Test
    fun `the client publishes how long each write operation took`() = runBlocking<Unit> {
        val registry = SimpleMeterRegistry()
        val config = configFor()
        pool = SftpPool(FakeSftpTransport().file("/drop/ledger.csv", CONTENT), config)
        val client = SftpClient(pool, config, registry)

        client.upload(fileHolding(CONTENT), "/drop/other.csv")
        client.rename("/drop/ledger.csv", "/drop/moved.csv")
        client.delete("/drop/moved.csv")
        client.mkdir("/drop/temp")
        client.withSession { realpath("/drop") }

        assertThat(registry.find(OP_SECONDS).timers().map { it.id.getTag("op") to it.id.getTag("result") })
            .contains("upload" to "ok", "rename" to "ok", "delete" to "ok", "mkdir" to "ok", "session" to "ok")
    }

    private suspend fun assertNothingIsStillOut() {
        assertThat(pool.stats().inUse).describedAs("sessions still out on lease").isZero()
    }

    private fun fileHolding(content: String): Path = local.resolve("ledger.csv").apply { writeText(content) }

    private fun clientOver(transport: FakeSftpTransport): SftpClient {
        val config = configFor()
        pool = SftpPool(transport, config)
        return SftpClient(pool, config)
    }

    private companion object {
        private const val OP_SECONDS = "sftp_op_seconds"

        private const val CONTENT = "id,amount\n1,42\n"

        private fun crossFilesystem() = ServerFailure(
            Attempt("fake.example:22", "rename", "/drop/ledger.csv"),
            statusCode = 4,
            detail = "the server will not rename across filesystems",
        )

        private fun configFor(): SftpConnectorConfig = sftpConnector("write-path") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
        }
    }
}
