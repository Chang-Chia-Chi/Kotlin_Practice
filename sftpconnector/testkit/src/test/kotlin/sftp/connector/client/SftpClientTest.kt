package sftp.connector.client

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.Digest
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.Disposition
import sftp.connector.error.NoSuchFile
import sftp.connector.error.UnsafeFileName
import sftp.connector.pool.SftpPool
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Operation
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText

/**
 * The read path against a scripted server, which is where everything that is not about a real
 * socket belongs: what a listing hands on and what it leaves out, what a missing path means to
 * each operation, and whether the session comes back afterwards.
 *
 * That last one is asserted after nearly every test here. A client that borrows a session and does
 * not give it back leaves the pool one session smaller for the life of the process, and the
 * failure surfaces in some later, unrelated piece of work rather than in the code that caused it.
 */
class SftpClientTest {

    @TempDir
    lateinit var stage: Path

    private lateinit var pool: SftpPool

    @Test
    fun `a listing hands on the files of a directory and leaves the directories out`() = runBlocking<Unit> {
        val client = clientOver(
            FakeSftpTransport()
                .file("/drop/one.csv", "1")
                .directory("/drop/archive")
                .file("/drop/two.csv", "22")
                .file("/drop/archive/old.csv", "buried"),
        )

        val seen = client.list("/drop").toList()

        assertThat(seen.map { it.path }).containsExactly("/drop/one.csv", "/drop/two.csv")
        assertThat(seen.map { it.size }).containsExactly(1L, 2L)
        assertThat(seen.map { it.name }).containsExactly("one.csv", "two.csv")
        assertNothingIsStillOut()
    }

    @Test
    fun `a listing stops after the entries the caller asked for`() = runBlocking<Unit> {
        val server = FakeSftpTransport()
        repeat(500) { server.file("/drop/file-$it.csv", "x") }
        val reported = mutableListOf<String>()
        val client = clientOver(server)

        val seen = client.list("/drop", maxEntries = 3, filter = { reported += it.path; true }).toList()

        assertThat(seen).hasSize(3)
        // What the server was asked to report, not merely what the consumer received: a listing
        // that stopped only at the flow's edge would still have read the whole directory.
        assertThat(reported).hasSize(3)
        assertNothingIsStillOut()
    }

    @Test
    fun `a filter keeps entries away from the consumer without ending the listing`() = runBlocking<Unit> {
        val client = clientOver(
            FakeSftpTransport()
                .file("/drop/one.csv", "1")
                .file("/drop/notes.txt", "ignore me")
                .file("/drop/two.csv", "22"),
        )

        val seen = client.list("/drop", filter = { it.path.endsWith(".csv") }).toList()

        assertThat(seen.map { it.path }).containsExactly("/drop/one.csv", "/drop/two.csv")
    }

    @Test
    fun `a consumer that stops collecting stops the listing and gives the session back`() = runBlocking<Unit> {
        val server = FakeSftpTransport()
        repeat(500) { server.file("/drop/file-$it.csv", "x") }
        val reported = mutableListOf<String>()
        val client = clientOver(server)

        val seen = client.list("/drop", filter = { reported += it.path; true }).take(2).toList()

        assertThat(seen).hasSize(2)
        // The channel between the listing and the consumer buffers, so the server is asked for
        // more than two - but for a bounded few rather than for all five hundred.
        assertThat(reported.size).isLessThan(100)
        assertNothingIsStillOut()
    }

    @Test
    fun `nothing is listed until somebody collects`() = runBlocking<Unit> {
        val server = FakeSftpTransport().file("/drop/one.csv", "1")
        val client = clientOver(server)

        val listing = client.list("/drop")

        assertThat(server.calls).isEmpty()
        listing.toList()
        assertThat(server.calls.map { it.operation }).contains(Operation.List)
    }

    @Test
    fun `stat answers with the file, and with nothing at all for a path that is not there`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/one.csv", "hello"))

        assertThat(client.stat("/drop/one.csv")?.size).isEqualTo(5L)
        assertThat(client.stat("/drop/gone.csv")).isNull()
        assertThat(client.exists("/drop/one.csv")).isTrue()
        assertThat(client.exists("/drop/gone.csv")).isFalse()
        assertNothingIsStillOut()
    }

    @Test
    fun `a download lands under its final name with the digest of the bytes that arrived`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", CONTENT))
        val listed = client.list("/drop").toList().single()

        val landed = client.download(listed)

        assertThat(landed.path).isEqualTo(stage.resolve("ledger.csv"))
        assertThat(landed.path.readText()).isEqualTo(CONTENT)
        assertThat(landed.size).isEqualTo(CONTENT.length.toLong())
        assertThat(landed.digest).isEqualTo(SHA256_OF_CONTENT)
        assertThat(landed.digestAlgorithm).isEqualTo(Digest.SHA256)
        assertThat(stage.listDirectoryEntries()).containsExactly(landed.path)
        assertNothingIsStillOut()
    }

    @Test
    fun `a download can be told where to put the file`() = runBlocking<Unit> {
        val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", CONTENT))
        val listed = client.list("/drop").toList().single()
        val elsewhere = stage.resolve("vendor-a-ledger.csv")

        val landed = client.download(listed, elsewhere)

        assertThat(landed.path).isEqualTo(elsewhere)
        assertThat(elsewhere.readText()).isEqualTo(CONTENT)
    }

    /**
     * The case the layer above has to be able to tell apart from a download that failed. The
     * connector names the missing file and reports it with the class that means "not this file",
     * so a poll can turn it into an event about the file instead of a retry of the operation.
     */
    @Test
    fun `a file that vanished between the listing and the download says so, and leaves nothing behind`() =
        runBlocking<Unit> {
            val server = FakeSftpTransport().file("/drop/ledger.csv", CONTENT)
            val client = clientOver(server)
            val listed = client.list("/drop").toList().single()
            server.remove("/drop/ledger.csv")

            assertThatThrownBy { runBlocking { client.download(listed) } }
                .isInstanceOf(NoSuchFile::class.java)
                .hasMessageContaining("/drop/ledger.csv")

            assertThat(stage).isEmptyDirectory()
            assertNothingIsStillOut()
        }

    @Test
    fun `I13_no partial file survives a download that broke half way through`() = runBlocking<Unit> {
        val server = FakeSftpTransport(answer = { call ->
            if (call.operation == Operation.Read) throw IOException("the tunnel went away")
        }).file("/drop/ledger.csv", CONTENT)
        val client = clientOver(server)
        val listed = client.list("/drop").toList().single()

        assertThatThrownBy { runBlocking { client.download(listed) } }.isInstanceOf(IOException::class.java)

        assertThat(stage).isEmptyDirectory()
        assertNothingIsStillOut()
    }

    @Test
    fun `a download of a file that grew since it was listed is refused and leaves nothing behind`() =
        runBlocking<Unit> {
            val server = FakeSftpTransport().file("/drop/ledger.csv", CONTENT)
            val client = clientOver(server)
            val listed = client.list("/drop").toList().single()
            // The uploader was still writing when the directory was listed, so what arrives now is
            // not the file the listing described.
            server.file("/drop/ledger.csv", CONTENT + "2,17\n")

            assertThatThrownBy { runBlocking { client.download(listed) } }
                .hasMessageContaining("is not the whole file")

            assertThat(stage).isEmptyDirectory()
            assertNothingIsStillOut()
        }

    /**
     * A listed name is the server's word, and the join of that word to the staging directory is
     * the one place it touches the local filesystem. `..` names the directory above the staging
     * directory on every operating system.
     */
    @Test
    fun `a listed name of dot-dot is refused before anything is read or written`() = runBlocking<Unit> {
        assertRefusedWithoutATrace(listedAs("/drop/.."))
    }

    /**
     * On Windows `..\..\evil.csv` is a path two directories up; elsewhere it is one odd file
     * name. It is refused on both, because a name that means two different things depending on
     * where the connector happens to run is never one it should write.
     */
    @Test
    fun `a listed name with a backslash segment is refused on every operating system`() = runBlocking<Unit> {
        assertRefusedWithoutATrace(listedAs("/drop/..\\..\\evil.csv"))
    }

    /**
     * The one absolute-looking shape a name can take after the last slash is a drive-relative
     * Windows path, which `Path.resolve` rewrites on Windows - to a different name on the same
     * drive, or to another drive entirely. Either way it stops being the listed name and is
     * refused. Where it is a plain file name it lands under exactly that name and nowhere else;
     * on either operating system nothing lands outside the staging directory.
     */
    @Test
    fun `a listed name that looks like a drive path lands under exactly that name or not at all`() = runBlocking<Unit> {
        val server = listedAs("/drop/C:evil")
        val client = clientOver(server)
        val listed = client.list("/drop").toList().single()

        val outcome = runCatching { client.download(listed) }

        assertThat(stage.listDirectoryEntries().map { it.fileName.toString() }).isSubsetOf(listOf("C:evil"))
        if (outcome.isFailure) assertRefusal(outcome.exceptionOrNull()!!, server, "/drop/C:evil")
        assertNothingIsStillOut()
    }

    @Test
    fun `a plain listed name still lands in the staging directory`() = runBlocking<Unit> {
        val client = clientOver(listedAs("/drop/plain.csv"))
        val listed = client.list("/drop").toList().single()

        val landed = client.download(listed)

        assertThat(landed.path).isEqualTo(stage.resolve("plain.csv"))
        assertThat(landed.path.readText()).isEqualTo(CONTENT)
        assertNothingIsStillOut()
    }

    private fun listedAs(remotePath: String): FakeSftpTransport = FakeSftpTransport().file(remotePath, CONTENT)

    private suspend fun assertRefusedWithoutATrace(server: FakeSftpTransport) {
        val client = clientOver(server)
        val listed = client.list("/drop").toList().single()

        val failure = catchThrowable { runBlocking { client.download(listed) } }

        assertRefusal(failure, server, listed.path)
        assertNothingIsStillOut()
    }

    /** Refused with the class that means "no retry, nothing against the server", and with no trace on disk or on the wire. */
    private fun assertRefusal(failure: Throwable?, server: FakeSftpTransport, remotePath: String) {
        assertThat(failure).isInstanceOf(UnsafeFileName::class.java)
            .hasMessageContaining(remotePath)
            .hasMessageContaining(stage.toString())
        assertThat((failure as UnsafeFileName).disposition).isEqualTo(Disposition.ACCEPT_THE_REFUSAL)
        assertThat(stage).isEmptyDirectory()
        assertThat(server.calls.map { it.operation }).doesNotContain(Operation.Read, Operation.Write)
    }

    @Test
    fun `the client publishes how long each operation took and how it went`() = runBlocking<Unit> {
        val registry = SimpleMeterRegistry()
        val server = FakeSftpTransport(answer = { call ->
            if (call.operation == Operation.Read) {
                throw NoSuchFile(Attempt("fake.example:22", "read", "/drop/ledger.csv"), "the server has no such path")
            }
        }).file("/drop/ledger.csv", CONTENT)
        val config = configFor(stage)
        val client = SftpClient(SftpPool(server, config), config, registry)

        val listed = client.list("/drop").toList().single()
        client.exists("/drop/ledger.csv")
        runCatching { client.download(listed) }

        assertThat(registry.find(OP_SECONDS).timers().map { it.id.getTag("op") to it.id.getTag("result") })
            .containsExactlyInAnyOrder(
                "list" to "ok",
                "exists" to "ok",
                // A file that is not there is worth another look on a later tick, which is the
                // whole difference between this label and the one that stops the connector.
                "download" to "recoverable",
            )
        assertThat(registry.find(OP_SECONDS).timers()).isNotEmpty.allSatisfy {
            assertThat(it.id.getTag("endpoint")).isEqualTo("fake.example:22")
        }
    }

    private suspend fun assertNothingIsStillOut() {
        assertThat(pool.stats().inUse).describedAs("sessions still out on lease").isZero()
        assertThat(pool.stats().connecting).describedAs("sessions half open").isZero()
    }

    private fun clientOver(transport: FakeSftpTransport): SftpClient {
        val config = configFor(stage)
        pool = SftpPool(transport, config)
        return SftpClient(pool, config)
    }

    private companion object {
        private const val OP_SECONDS = "sftp_op_seconds"

        private const val CONTENT = "id,amount\n1,42\n"

        /** Taken from `sha256sum` over exactly those bytes, not from this code. */
        private const val SHA256_OF_CONTENT =
            "0f7573cb5487f607c74e1f891a1ded6a94a24d81b4c46f6ab92e1c65dd6f36d8"

        private fun configFor(stage: Path): SftpConnectorConfig = sftpConnector("read-path") {
            endpoint { host = "fake.example"; port = 22 }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            polling { staging { dir = stage } }
        }
    }
}
