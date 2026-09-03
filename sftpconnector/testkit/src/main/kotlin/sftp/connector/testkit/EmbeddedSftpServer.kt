package sftp.connector.testkit

import org.apache.sshd.common.channel.RequestHandler
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory
import org.apache.sshd.common.session.ConnectionService
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.sftp.server.SftpFileSystemAccessor
import org.apache.sshd.sftp.server.SftpSubsystemFactory
import org.apache.sshd.sftp.server.SftpSubsystemProxy
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.CopyOption
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

    /**
     * Sessions the server is holding right now. A client that hung up is gone from here within
     * a moment; one that never hung up stays for as long as its keepalive keeps it alive, which
     * is what makes a leaked session observable from the one place it cannot hide.
     */
    val liveSessions: Int get() = sshd.activeSessions.size

    /**
     * Cuts every session the server is holding, the way a restart, an idle reaper or a firewall
     * dropping the flow does: no notice to the client, which goes on believing it has a session
     * until it tries to use one. It returns once the server side is really closed, so a test never
     * races the kill it just asked for.
     */
    fun killLiveSessions() {
        sshd.activeSessions.toList().forEach { it.close(true).await() }
    }

    override fun close() {
        sshd.stop(true)
    }

    companion object {
        private const val LOOPBACK = "127.0.0.1"

        /**
         * [offersSftp] is a fault hook. An SSH server that authenticates and then refuses the
         * `sftp` subsystem is a real deployment - a locked-down account, a server built for shell
         * access - and it fails at a different point from every other failure a test can stage.
         *
         * [onGlobalRequest] is told the name of every global request a client sends, which is how
         * a keepalive becomes observable: it is a request with no reply worth reading, so the only
         * proof that a client is speaking on an idle session is the server hearing it. The
         * observer answers nothing, so the server behaves exactly as it would without one.
         *
         * [separateFilesystemAt] names a folder directly under [root] that the server treats as a
         * second filesystem: a rename into or out of it is refused, the way a kernel refuses one
         * across a mount point. It is a fault hook because a test cannot mount a second disk, and
         * it is worth having because the refusal is invisible - the server answers with the one
         * featureless status it uses for everything it will not do, and a target on another
         * filesystem is therefore indistinguishable from a working one until something tries a
         * move. Both roots are ordinary directories in [root]; only the rename knows the
         * difference, which is exactly what a real filesystem boundary is like from a client.
         */
        fun start(
            root: Path,
            user: String,
            password: String,
            offersSftp: Boolean = true,
            onGlobalRequest: (String) -> Unit = {},
            separateFilesystemAt: String? = null,
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
                subsystemFactories = if (offersSftp) listOf(sftpSubsystem(root, separateFilesystemAt)) else emptyList()
                fileSystemFactory = VirtualFileSystemFactory(root)
                globalRequestHandlers = listOf(
                    RequestHandler<ConnectionService> { _, request, _, _ ->
                        onGlobalRequest(request)
                        // Unsupported and not a reply, so the handlers the server came with still
                        // get their turn and nothing here changes what the client is told.
                        RequestHandler.Result.Unsupported
                    },
                ) + globalRequestHandlers.orEmpty()
            }
            sshd.start()
            return EmbeddedSftpServer(sshd, root)
        }

        private fun sftpSubsystem(root: Path, separateFilesystemAt: String?) = SftpSubsystemFactory().apply {
            val elsewhere = separateFilesystemAt?.let { root.resolve(it).toAbsolutePath().normalize() } ?: return@apply
            fileSystemAccessor = object : SftpFileSystemAccessor {
                override fun renameFile(
                    subsystem: SftpSubsystemProxy,
                    source: Path,
                    target: Path,
                    options: Collection<CopyOption>,
                ) {
                    if (source.isUnder(elsewhere) != target.isUnder(elsewhere)) {
                        // What the kernel answers a rename(2) between two mounts with, and the
                        // exception the JDK gives that answer its own name. It carries no status
                        // of its own onto the wire: the server has one refusal for everything it
                        // cannot do, which is the whole reason this has to be tried rather than
                        // asked about.
                        throw AtomicMoveNotSupportedException(
                            source.toString(),
                            target.toString(),
                            "the two paths are on different filesystems",
                        )
                    }
                    SftpFileSystemAccessor.DEFAULT.renameFile(subsystem, source, target, options)
                }
            }
        }

        private fun Path.isUnder(directory: Path): Boolean = toAbsolutePath().normalize().startsWith(directory)
    }
}
