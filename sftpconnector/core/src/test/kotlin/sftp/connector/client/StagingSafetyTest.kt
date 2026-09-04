package sftp.connector.client

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.Digest
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.UnsafeFileName
import java.io.IOException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.attribute.PosixFileAttributeView
import java.nio.file.attribute.PosixFilePermissions

/**
 * Who else can reach the file a download is being written into, and what happens when they have.
 *
 * Staging writes to `<dir>/<name>.part` under a name the *server* chose, so the name is known to
 * whoever names the file and the path it lands on is predictable. That is only safe while the
 * directory is the connector's own: on a shared temp directory - which is what the connector used
 * to ship as its default, and `/tmp` is mode 1777 - any other local account can put a symbolic
 * link at that name first and have the download written through it, into a file the connector's
 * user may write but never meant to.
 *
 * Both halves are checked here, because either alone leaves the other's hole open: the partial
 * file is opened in a way that cannot follow a link, and the directory it is opened in is one only
 * this process can put anything into.
 */
class StagingSafetyTest {

    @TempDir
    lateinit var stage: Path

    @TempDir
    lateinit var elsewhere: Path

    @Test
    fun `a download refuses to write through a symbolic link left at the partial file's name`() = runBlocking<Unit> {
        val victim = Files.write(elsewhere.resolve("victim.txt"), ORIGINAL)
        val partial = stage.resolve("ledger.csv.part")
        assumeTrue(canLinkTo(partial, victim), "this filesystem will not make a symbolic link")

        val refused = assertThrows(UnsafeFileName::class.java) {
            runBlocking {
                StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size.toLong(), ATTEMPT) {
                    it.write(CONTENT)
                }
            }
        }

        assertTrue("link" in refused.message.orEmpty(), refused.message)
        assertArrayEquals(ORIGINAL, Files.readAllBytes(victim))
        // The link is left where it was. The connector did not put it there, an operator has to
        // know it is there, and taking it away quietly would remove the only evidence of that.
        assertTrue(Files.isSymbolicLink(partial))
    }

    /**
     * The same guard, said in a way every platform can stage: a symbolic link needs a privilege
     * Windows does not hand out by default, so the test above can only skip there, and a guard
     * that is only ever exercised on one operating system is a guard nobody is watching. What both
     * cases have in common is that something the connector did not write is sitting at the partial
     * file's name, and the answer is the same refusal rather than an `IOException` from the open.
     */
    @Test
    fun `anything at the partial file's name that this connector did not write is refused`() = runBlocking<Unit> {
        Files.createDirectory(stage.resolve("ledger.csv.part"))

        val refused = assertThrows(UnsafeFileName::class.java) {
            runBlocking {
                StagingArea(Digest.SHA256).receive(stage.resolve("ledger.csv"), CONTENT.size.toLong(), ATTEMPT) {
                    it.write(CONTENT)
                }
            }
        }

        assertTrue("ledger.csv.part" in refused.message.orEmpty(), refused.message)
    }

    /**
     * The stale partial file spec 6.3 describes - resume is deferred by 14.1, so a `.part` from a
     * run that died is a fragment nobody will ever finish - still goes, because it is a plain file
     * this connector wrote. Only what it did not write is refused.
     */
    @Test
    fun `a plain partial file an earlier run left behind is replaced`() = runBlocking<Unit> {
        val target = stage.resolve("ledger.csv")
        Files.write(stage.resolve("ledger.csv.part"), "half a file".toByteArray())

        val landed = StagingArea(Digest.SHA256).receive(target, CONTENT.size.toLong(), ATTEMPT) { it.write(CONTENT) }

        assertArrayEquals(CONTENT, Files.readAllBytes(landed.path))
    }

    /**
     * The default has to be somewhere a connector nobody finished configuring can still write, and
     * the JVM's temp directory is that place - but the temp directory *itself* is shared with every
     * other account on the host. So the default is a directory made inside it, once per process,
     * by `Files.createTempDirectory`: the platform makes it owner-only where it has permissions to
     * express that, and the name is random, which is the half that matters even where it has not,
     * because a link can only be planted at a path that can be predicted.
     */
    @Test
    fun `the shipped default staging directory is this process's own, not the shared temp directory`() {
        val default = minimalConnector().polling.staging.dir

        assertNotEquals(TEMP_DIRECTORY, default)
        assertTrue(default.startsWith(TEMP_DIRECTORY), "$default is not under $TEMP_DIRECTORY")
        assertTrue(Files.isDirectory(default))
        // One per process, not one per configuration built: a directory per builder would leave a
        // trail of them in the temp directory and split one connector's staging across two runs.
        assertSame(default, minimalConnector().polling.staging.dir)

        Files.getFileAttributeView(default, PosixFileAttributeView::class.java)?.let {
            assertEquals(PosixFilePermissions.fromString("rwx------"), it.readAttributes().permissions())
        }
    }

    private fun canLinkTo(link: Path, target: Path): Boolean =
        try {
            Files.createSymbolicLink(link, target)
            Files.isSymbolicLink(link)
        } catch (unsupported: IOException) {
            false
        } catch (forbidden: UnsupportedOperationException) {
            false
        }

    private fun minimalConnector(): SftpConnectorConfig = sftpConnector("vendor-drop") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "s3cret") }
        hostKey = HostKeyPolicy.Strict(Path.of("/etc/etl/known_hosts"))
    }

    private companion object {
        private val CONTENT = "id,amount\n1,42\n".toByteArray()
        private val ORIGINAL = "the bytes that were already there\n".toByteArray()

        private val ATTEMPT = Attempt("sftp.example:22", "download", "/drop/ledger.csv")

        private val TEMP_DIRECTORY: Path =
            Path.of(System.getProperty("java.io.tmpdir")).toRealPath(LinkOption.NOFOLLOW_LINKS)
    }
}
