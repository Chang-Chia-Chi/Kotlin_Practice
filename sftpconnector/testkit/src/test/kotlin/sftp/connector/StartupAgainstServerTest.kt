package sftp.connector

import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.PollingBuilder
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.ConfigurationError
import sftp.connector.testkit.EmbeddedSftpServer
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText

/**
 * Start-up against a real server, which is the only place these checks mean anything.
 *
 * Every one of them is about a thing the configuration cannot know and a listing cannot show: a
 * directory the account cannot see, a folder it may not create, a move the server will not make.
 * The last of those is the reason the whole check exists - it is invisible from every angle except
 * trying it, because the server refuses a move it cannot make with the same featureless status it
 * refuses everything else with.
 */
class StartupAgainstServerTest {

    @TempDir
    lateinit var remoteRoot: Path

    @TempDir
    lateinit var local: Path

    /**
     * S6. The action target sits on a second filesystem, which from the client is an ordinary
     * folder with an ordinary listing and an ordinary stat - right up to the moment an ack tries
     * to move a file into it, which on this pipeline is an hour after the deployment. Here it is
     * the deployment that fails, and the message says which move it was and what to change.
     */
    @Test
    fun `S6_a move target on another filesystem stops the connector from starting`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("elsewhere").createDirectories()

        withServer(separateFilesystemAt = "elsewhere") { server ->
            assertThatThrownBy { runBlocking { start(server) { directories("/drop"); onAck = move("/elsewhere/temp") } } }
                .isInstanceOf(ConfigurationError::class.java)
                .hasMessageContaining("/drop")
                .hasMessageContaining("/elsewhere/temp")
                .hasMessageContaining("same filesystem")
        }

        // Nothing of the check survives the failure. A start-up that refuses and leaves a file
        // behind on somebody else's server is a start-up nobody will let run twice.
        assertThat(remoteRoot.resolve("drop").listDirectoryEntries()).isEmpty()
        assertThat(remoteRoot.resolve("elsewhere/temp").listDirectoryEntries()).isEmpty()
    }

    /**
     * The same server and the same impossible move, with the marker rename turned off. It starts,
     * which is the whole meaning of that knob: the deployment has chosen to find out at the first
     * ack instead. That it starts *here* is what proves the rename is what the knob controls.
     */
    @Test
    fun `startupProbe off skips the marker rename and starts anyway`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()
        remoteRoot.resolve("elsewhere").createDirectories()

        withServer(separateFilesystemAt = "elsewhere") { server ->
            val connector = start(server) {
                directories("/drop")
                onAck = move("/elsewhere/temp")
                startupProbe = false
            }
            connector.backgroundWork.cancelAndJoin()
        }

        // The folder was still made, because that is the other knob. Nothing was written into the
        // watched directory at all, which is what "skips the marker rename" means from outside.
        assertThat(remoteRoot.resolve("elsewhere/temp").isDirectory()).isTrue()
        assertThat(remoteRoot.resolve("drop").listDirectoryEntries()).isEmpty()
    }

    @Test
    fun `a watched directory that is not there stops the connector from starting`() = runBlocking<Unit> {
        withServer { server ->
            assertThatThrownBy { runBlocking { start(server) { directories("/drop") } } }
                .isInstanceOf(ConfigurationError::class.java)
                .hasMessageContaining("/drop")
                .hasMessageContaining("There is nothing at")
        }
    }

    /**
     * A path that resolves is not a path that exists. The server canonicalises a name that leads
     * nowhere without complaint - measured against this one - so resolving a watched directory
     * cannot be the whole check, and a typo in a configured path is the most ordinary fault there
     * is. This is the test that would go green if the second half were dropped.
     */
    @Test
    fun `a watched directory that is a file stops the connector from starting`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").writeText("not a directory")

        withServer { server ->
            assertThatThrownBy { runBlocking { start(server) { directories("/drop") } } }
                .isInstanceOf(ConfigurationError::class.java)
                .hasMessageContaining("/drop is a file, and a watched directory has to be a directory")
        }
    }

    /**
     * The self-move fault in the spelling the configuration cannot catch. "drop" and "/drop" are
     * not the same string and the builder has no way to learn that they are the same folder; the
     * server does, and the check that has its answer is the one that has to ask. Left through, this
     * connector would hand the same file to every poll it ever ran and succeed at every step.
     */
    @Test
    fun `an action target the server resolves onto the watched directory stops the connector`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()

        withServer { server ->
            assertThatThrownBy { runBlocking { start(server) { directories("drop"); onAck = move("/drop") } } }
                .isInstanceOf(ConfigurationError::class.java)
                .hasMessageContaining("move it onto itself")
        }
    }

    @Test
    fun `the connector makes the folder its actions move files into, and leaves nothing else behind`() =
        runBlocking<Unit> {
            remoteRoot.resolve("drop").createDirectories()

            withServer { server ->
                val connector = start(server) { directories("/drop"); onAck = move("temp/") }
                connector.backgroundWork.cancelAndJoin()
            }

            assertThat(remoteRoot.resolve("drop/temp").isDirectory()).isTrue()
            assertThat(remoteRoot.resolve("drop/temp").listDirectoryEntries()).isEmpty()
            assertThat(remoteRoot.resolve("drop").listDirectoryEntries().map { it.fileName.toString() })
                .containsExactly("temp")
        }

    /**
     * The other half of that knob: an account that may not create directories has them made
     * upstream instead, and the connector's job is then to say so when somebody forgot.
     */
    @Test
    fun `a folder the connector was told not to create stops it when nobody has created it`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()

        withServer { server ->
            assertThatThrownBy {
                runBlocking {
                    start(server) {
                        directories("/drop")
                        onAck = move("temp/")
                        createActionTargets = false
                    }
                }
            }
                .isInstanceOf(ConfigurationError::class.java)
                .hasMessageContaining("/drop/temp")
                .hasMessageContaining("createActionTargets is off")
        }

        assertThat(remoteRoot.resolve("drop").listDirectoryEntries()).isEmpty()
    }

    /**
     * A start-up that ran every check without being asked to move anything, twice over. The second
     * run finds the folder already there, which is the ordinary case in production - every restart
     * after the first - and it has to be as quiet as the first.
     */
    @Test
    fun `a connector that has started once starts again over what it left`() = runBlocking<Unit> {
        remoteRoot.resolve("drop").createDirectories()

        withServer { server ->
            repeat(2) {
                start(server) { directories("/drop"); onAck = move("temp/") }.backgroundWork.cancelAndJoin()
            }
        }

        assertThat(remoteRoot.resolve("drop/temp").isDirectory()).isTrue()
    }

    private suspend fun withServer(
        separateFilesystemAt: String? = null,
        block: suspend (EmbeddedSftpServer) -> Unit,
    ) {
        EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD, separateFilesystemAt = separateFilesystemAt)
            .use { block(it) }
    }

    private suspend fun start(server: EmbeddedSftpServer, polling: PollingBuilder.() -> Unit): SftpConnector =
        SftpConnector.start(configFor(server, polling))

    private fun configFor(server: EmbeddedSftpServer, describePolling: PollingBuilder.() -> Unit): SftpConnectorConfig =
        sftpConnector("startup-demo") {
            endpoint { host = server.host; port = server.port }
            auth { password(USER, PASSWORD) }
            // The embedded server generates a fresh key per instance, so there is no key a test
            // could have recorded in advance.
            hostKey = HostKeyPolicy.AcceptAll
            polling { staging { dir = local }; describePolling() }
        }

    private companion object {
        private const val USER = "etl"
        private const val PASSWORD = "s3cret"
    }
}
