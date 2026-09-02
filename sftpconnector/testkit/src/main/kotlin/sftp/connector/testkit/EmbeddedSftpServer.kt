package sftp.connector.testkit

import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.sftp.server.SftpSubsystemFactory
import java.nio.file.Path

/**
 * A real SSH and SFTP server on loopback, serving one directory.
 *
 * Tests get a server rather than a stand-in, which matters because most of what the JSch
 * adapter does - the key exchange, the signature algorithms, the channel lifecycle, the shape
 * of an error - is negotiated with a peer, and a stand-in would simply agree with whatever the
 * adapter did.
 *
 * The host key is generated per instance and never written to disk, so nothing a test trusts
 * outlives the test. The port is chosen by the operating system, so instances never collide.
 */
class EmbeddedSftpServer private constructor(
    private val sshd: SshServer,

    /** The directory the server exposes as its root. Write files here to make them appear. */
    val root: Path,
) : AutoCloseable {

    val host: String get() = LOOPBACK
    val port: Int get() = sshd.port

    override fun close() {
        sshd.stop(true)
    }

    companion object {
        private const val LOOPBACK = "127.0.0.1"

        /**
         * [offersSftp] is a fault hook. An SSH server that authenticates and then refuses the
         * `sftp` subsystem is a real deployment - a locked-down account, a server built for shell
         * access - and it fails at a different point from every other failure a test can stage.
         */
        fun start(
            root: Path,
            user: String,
            password: String,
            offersSftp: Boolean = true,
        ): EmbeddedSftpServer {
            val sshd = SshServer.setUpDefaultServer().apply {
                host = LOOPBACK
                port = 0
                // RSA because the JDK generates it without help from a crypto provider, and
                // because it is what forces the rsa-sha2 signatures the connector's JSch fork
                // exists to support.
                keyPairProvider = SimpleGeneratorHostKeyProvider().apply { algorithm = "RSA" }
                passwordAuthenticator = PasswordAuthenticator { offeredUser, offeredPassword, _ ->
                    offeredUser == user && offeredPassword == password
                }
                subsystemFactories = if (offersSftp) listOf(SftpSubsystemFactory()) else emptyList()
                fileSystemFactory = VirtualFileSystemFactory(root)
            }
            sshd.start()
            return EmbeddedSftpServer(sshd, root)
        }
    }
}
