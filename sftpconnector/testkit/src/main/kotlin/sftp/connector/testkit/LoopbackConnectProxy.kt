package sftp.connector.testkit

import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The smallest HTTP proxy an SSH client will tunnel through: it reads the CONNECT request,
 * dials the address it names, answers `200`, and from then on copies bytes both ways without
 * looking at them.
 *
 * It exists because the network this connector was written for puts a CONNECT proxy between the
 * service and the SFTP server, and a test on plain loopback would never exercise that. Wiring a
 * proxy is one line of adapter code and exactly the kind of one line that is discovered to be
 * wrong on the first day in production.
 */
class LoopbackConnectProxy private constructor(private val server: ServerSocket) : AutoCloseable {

    val host: String get() = LOOPBACK
    val port: Int get() = server.localPort

    private val sockets = CopyOnWriteArrayList<Socket>()

    @Volatile
    private var relaying = true

    /**
     * Stops moving bytes in either direction while leaving both sockets open, which is the
     * failure a read timeout exists for: the peer has neither answered nor hung up, so nothing
     * short of the clock will ever unblock the reader.
     */
    fun stall() {
        relaying = false
    }

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }

    private fun accept() {
        while (!server.isClosed) {
            val client = try {
                server.accept()
            } catch (closed: IOException) {
                return
            }
            sockets += client
            daemon("connect-proxy-tunnel") { tunnel(client) }
        }
    }

    private fun tunnel(client: Socket) {
        val target = try {
            val (host, port) = readConnectRequest(client.getInputStream())
            Socket(host, port)
        } catch (refused: IOException) {
            runCatching { client.close() }
            return
        }
        sockets += target
        client.getOutputStream().write(ESTABLISHED)
        client.getOutputStream().flush()
        daemon("connect-proxy-upstream") { copy(client, target) }
        copy(target, client)
    }

    /**
     * Reads one byte at a time and stops at the blank line. Anything more would swallow the
     * first bytes of the SSH handshake, which arrive immediately behind the header.
     */
    private fun readConnectRequest(input: InputStream): Pair<String, Int> {
        val header = StringBuilder()
        while (!header.endsWith(END_OF_HEADER)) {
            val byte = input.read()
            if (byte < 0) throw IOException("the client closed before finishing its CONNECT request")
            header.append(byte.toChar())
        }
        val requestLine = header.lineSequence().first()
        val authority = requestLine.split(' ').getOrNull(1)
            ?: throw IOException("not a CONNECT request line: $requestLine")
        val separator = authority.lastIndexOf(':')
        val port = authority.substring(separator + 1).toIntOrNull()
            ?: throw IOException("no port in the CONNECT target: $authority")
        return authority.substring(0, separator) to port
    }

    private fun copy(from: Socket, to: Socket) {
        try {
            val buffer = ByteArray(BUFFER_BYTES)
            while (true) {
                val read = from.getInputStream().read(buffer)
                if (read < 0) break
                // A stalled tunnel keeps reading, so the sender's own buffers never fill and it
                // never learns that nothing is arriving at the other end.
                if (!relaying) continue
                to.getOutputStream().write(buffer, 0, read)
                to.getOutputStream().flush()
            }
        } catch (endOfTunnel: IOException) {
            // Either end closing is how a tunnel ends; there is nothing to report.
        } finally {
            runCatching { to.close() }
            runCatching { from.close() }
        }
    }

    companion object {
        private const val LOOPBACK = "127.0.0.1"
        private const val END_OF_HEADER = "\r\n\r\n"
        private const val BUFFER_BYTES = 8 * 1024
        private val ESTABLISHED = "HTTP/1.0 200 Connection established\r\n\r\n".toByteArray()

        fun start(): LoopbackConnectProxy {
            val server = ServerSocket(0, 0, InetAddress.getLoopbackAddress())
            val proxy = LoopbackConnectProxy(server)
            daemon("connect-proxy-accept") { proxy.accept() }
            return proxy
        }

        private fun daemon(name: String, body: () -> Unit) {
            Thread(body, name).apply { isDaemon = true }.start()
        }
    }
}
