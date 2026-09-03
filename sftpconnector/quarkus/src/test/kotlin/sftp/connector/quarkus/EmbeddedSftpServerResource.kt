package sftp.connector.quarkus

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import sftp.connector.testkit.EmbeddedSftpServer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectories

/**
 * A real SFTP server, started before the host reads its configuration.
 *
 * It has to be this rather than a fixture in the test class, because the server's port is chosen
 * by the operating system and the connector is configured, built and started while the
 * application boots - long before a test method runs. This is the only hook that comes early
 * enough to tell the host where the server ended up.
 */
class EmbeddedSftpServerResource : QuarkusTestResourceLifecycleManager {

    override fun start(): Map<String, String> {
        remoteRoot = Files.createTempDirectory("sftp-quarkus-remote")
        stagingDir = Files.createTempDirectory("sftp-quarkus-staging")
        watchedDirectory().createDirectories()
        val started = EmbeddedSftpServer.start(remoteRoot, USER, PASSWORD)
        running = started
        return mapOf(
            "sftp.connector.endpoint.host" to started.host,
            "sftp.connector.endpoint.port" to started.port.toString(),
            "sftp.connector.auth.user" to USER,
            "sftp.connector.auth.password" to PASSWORD,
            "sftp.connector.polling.staging.dir" to stagingDir.toString(),
        )
    }

    /** Also runs after a [start] that threw, which is why it asks rather than assumes. */
    override fun stop() {
        running?.close()
        running = null
    }

    companion object {
        const val USER = "etl"
        const val PASSWORD = "s3cret"

        private var running: EmbeddedSftpServer? = null

        val server: EmbeddedSftpServer get() = checkNotNull(running) { "the embedded server is not running" }

        lateinit var remoteRoot: Path
            private set

        lateinit var stagingDir: Path
            private set

        /** The one directory the host is configured to watch, as a local path. */
        fun watchedDirectory(): Path = remoteRoot.resolve("drop")
    }
}
