package infra.shuttle.sftp

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.TargetRef
import infra.shuttle.testkit.ObjectStoreTargetContract
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.SftpConnector
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.testkit.EmbeddedSftpServer
import java.nio.file.Files
import java.nio.file.Path

/**
 * Spec 7.3 against the connector's embedded SSHD: the shared target contract plus what only a
 * server can show - the repair of a crash between the upload and the rename, a file taken away
 * behind the target's back, and a directory that is not there.
 */
class SftpTargetTest : ObjectStoreTargetContract() {

    @TempDir lateinit var remoteRoot: Path

    private lateinit var server: EmbeddedSftpServer
    private lateinit var connector: SftpConnector
    private lateinit var sftpTarget: SftpTarget

    @BeforeEach fun start(): Unit = runBlocking {
        Files.createDirectories(remoteRoot.resolve(DIRECTORY))
        server = EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD)
        connector = SftpConnector.start(
            sftpConnector("partner") {
                endpoint { host = server.host; port = server.port }
                auth { password(USER, PASSWORD) }
                hostKey = HostKeyPolicy.AcceptAll
                pool { maxSize = 2 }
            },
            meterRegistry = SimpleMeterRegistry(),
        )
        sftpTarget = SftpTarget(connector.client, "/$DIRECTORY", Dispatchers.IO)
    }

    @AfterEach fun stop(): Unit = runBlocking {
        connector.close()
        server.close()
    }

    /**
     * Spec 7.3 and I6: a process that died between the upload and the rename left the bytes under
     * the partial name and never moved them. The next store of that key has to be able to take the
     * partial name back - it is this adapter's own name and nothing else may be using it - and to
     * land its own content over whatever is at the key.
     */
    @Test
    fun I6_a_store_that_died_between_the_upload_and_the_rename_is_repaired_by_the_next_store() = runBlocking {
        withTimeout(TIMEOUT) {
            sftpTarget.store(KEY, file("v1", "one"), emptyMap())
            connector.client.upload(file("v2", "two"), "/$DIRECTORY/$KEY$PARTIAL")

            val ref = sftpTarget.store(KEY, file("v3", "three"), emptyMap())

            assertEquals("three", String(currentBytes(KEY)), "the newest content is the one at the key")
            assertTrue(sftpTarget.verify(ref))
            assertEquals(listOf("a.csv"), namesUnder("out"), "exactly one copy, and no partial file left behind")
        }
    }

    /**
     * Spec 7.3: `verify` is a stat. The partner owns the folder, so a copy can be taken out of it
     * between the store and the check, and the ref's own size and mtime are what say the file the
     * server is answering about is the file this ref named.
     */
    @Test
    fun verify_is_false_for_a_copy_that_has_been_taken_away_or_written_over() = runBlocking {
        withTimeout(TIMEOUT) {
            val ref = sftpTarget.store(KEY, file("v1", "one"), emptyMap())
            assertTrue(sftpTarget.verify(ref))

            Files.writeString(onServer(KEY), "somebody else's file")
            assertFalse(sftpTarget.verify(ref), "a different file under the same name is not this ref")

            Files.delete(onServer(KEY))
            assertFalse(sftpTarget.verify(ref))
        }
    }

    /** The ordinary shape: a `key` pattern that is just a name lands in the target directory itself. */
    @Test
    fun a_key_with_no_folder_in_it_lands_in_the_target_directory() = runBlocking {
        withTimeout(TIMEOUT) {
            val ref = sftpTarget.store("flat.csv", file("flat", "one"), emptyMap())

            assertEquals(TargetRef("sftp", "/$DIRECTORY", "flat.csv", ref.ref, 3), ref)
            assertEquals("one", String(currentBytes("flat.csv")))
            assertEquals(listOf("flat.csv"), namesUnder("."))
        }
    }

    /**
     * Spec 12.1: the folder is the partner's and is never created here, so a start-up against a
     * typo or a folder nobody made refuses with the path in the message - which is the difference
     * between a deployment that fails now and one that fails at the first file, hours later.
     */
    @Test
    fun probe_passes_on_the_target_directory_and_fails_naming_a_path_that_is_not_a_directory() = runBlocking {
        withTimeout(TIMEOUT) {
            sftpTarget.probe()

            val missing = runCatching { SftpTarget(connector.client, "/no-such-folder", Dispatchers.IO).probe() }.exceptionOrNull()
            assertTrue(missing is IllegalStateException && missing.message!!.contains("/no-such-folder"), "$missing")

            Files.writeString(remoteRoot.resolve("a-file"), "not a directory")
            val notADirectory = runCatching { SftpTarget(connector.client, "/a-file", Dispatchers.IO).probe() }.exceptionOrNull()
            assertTrue(notADirectory is IllegalStateException && notADirectory.message!!.contains("/a-file"), "$notADirectory")
        }
    }

    private fun namesUnder(folder: String): List<String> =
        Files.list(onServer(folder)).use { entries -> entries.map { it.fileName.toString() }.sorted().toList() }

    override fun target(): ObjectStoreTarget = sftpTarget
    override fun location() = "/$DIRECTORY"
    override suspend fun currentBytes(key: String): ByteArray = Files.readAllBytes(onServer(key))

    private fun onServer(path: String): Path = remoteRoot.resolve(DIRECTORY).resolve(path)

    private companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"
        const val DIRECTORY = "landing"
        const val KEY = "out/a.csv"
        const val PARTIAL = ".part"
        const val TIMEOUT = 30_000L
    }
}
