package sftp.connector.testkit

import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

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

    private val tunnels = CopyOnWriteArrayList<Tunnel>()

    /** One client's tunnel through the proxy, with its own switch, so a stall reaches only the tunnels that exist. */
    private class Tunnel {
        @Volatile
        var relaying = true
    }

    @Volatile
    private var refusing = false

    private val deliveredToClient = AtomicLong()

    @Volatile
    private var holdAfter = Long.MAX_VALUE

    @Volatile
    private var held: CountDownLatch? = null

    @Volatile
    private var whenHeld: () -> Unit = {}

    /**
     * Stops moving bytes in either direction on every tunnel open right now, while leaving their
     * sockets open, which is the failure a read timeout exists for: the peer has neither answered
     * nor hung up, so nothing short of the clock will ever unblock the reader. A tunnel opened
     * afterwards relays normally, which is what lets a retry on a fresh session get through.
     */
    fun stall() {
        tunnels.forEach { it.relaying = false }
    }

    @Volatile
    private var blackHoleAfter = Long.MAX_VALUE

    @Volatile
    private var blackHoled: CountDownLatch? = null

    @Volatile
    private var whenBlackHoled: () -> Unit = {}

    private val readFromClient = AtomicLong()

    /**
     * Reads [afterBytes] more from the client and then stops reading from it for good, so the
     * client's own send buffer fills and its next write blocks with nothing at the far end to
     * drain it - the black hole a firewall whose state expired, or a NAT that forgot the flow,
     * makes of a tunnel that still looks open.
     *
     * This is the opposite of [stall], and the difference is the direction: a stall keeps reading
     * and throws the bytes away, so the sender's buffers never fill and only the clock ever
     * unblocks its *read*. A black hole stops reading, so it is the sender's own *write* that
     * blocks - the one fault neither the cooperative tier (no chunk ever completes to ask the
     * monitor) nor the keepalive tier (its probe is a write behind the same lock) can end. Only a
     * socket close can. [whenBlocked] runs the moment it stops reading; close() releases it.
     */
    fun blackHoleClientAfter(afterBytes: Long, whenBlocked: () -> Unit) {
        this.whenBlackHoled = whenBlocked
        blackHoled = CountDownLatch(1)
        readFromClient.set(0)
        blackHoleAfter = afterBytes
    }

    /**
     * Answers every CONNECT from now on with a refusal and hangs up, the way a proxy does while
     * the network behind it is down. Nothing already tunnelled is touched. [acceptConnections]
     * ends it.
     */
    fun refuseConnections() {
        refusing = true
    }

    fun acceptConnections() {
        refusing = false
    }

    /**
     * Delivers [bytes] more bytes to the client and then holds the tunnel still until [resume],
     * calling [whenHeld] the moment it stops.
     *
     * Not the same fault as [stall], and the difference is the bytes behind it: a stall throws
     * them away, so the conversation can never carry on, while a hold leaves them queued in the
     * tunnel where they were. That is what lets a test stop a transfer at a point of its own
     * choosing, act, and then let the same transfer continue - rather than guessing at a moment
     * with a timer and hoping.
     */
    fun holdAfter(bytes: Long, whenHeld: () -> Unit) {
        this.whenHeld = whenHeld
        held = CountDownLatch(1)
        deliveredToClient.set(0)
        holdAfter = bytes
    }

    /** Lets a held tunnel carry on from exactly where it stopped. */
    fun resume() {
        holdAfter = Long.MAX_VALUE
        held?.countDown()
    }

    /**
     * How many bytes have reached the client since the last [holdAfter] armed. It is what the
     * server was actually made to send, which is the only place a transfer that stopped early and
     * one that ran to the end of the file look different from outside.
     */
    val bytesDelivered: Long get() = deliveredToClient.get()

    private val tunnelsAsked = AtomicLong()

    /** How many tunnels clients have asked for, refused ones included. A client that did not dial is visible here. */
    val connectsAsked: Long get() = tunnelsAsked.get()

    /**
     * Runs [action] once, the next time the client sends anything at all - after what it sent has
     * been passed on, so an action that stalls the tunnel loses the reply and not the request.
     *
     * On a stalled tunnel that moment is the only one a test can act on with any confidence: the
     * request is on the wire, so the thread that sent it is committed to waiting for an answer
     * that is never coming. Anything a test does before then may land on a call that has not
     * started, which is a different thing entirely and not the thing under test.
     */
    fun onNextClientRequest(action: () -> Unit) {
        whenClientSpeaks = action
    }

    @Volatile
    private var whenClientSpeaks: () -> Unit = {}

    override fun close() {
        // Released first, so a relay thread parked on a hold or a black hole is not left waiting
        // on a latch nothing will ever count down.
        resume()
        blackHoled?.countDown()
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
            tunnelsAsked.incrementAndGet()
            if (refusing) throw IOException("refusing every CONNECT for now")
            Socket(host, port)
        } catch (refused: IOException) {
            runCatching {
                client.getOutputStream().write(REFUSED)
                client.getOutputStream().flush()
                client.close()
            }
            return
        }
        sockets += target
        val tunnel = Tunnel().also { tunnels += it }
        client.getOutputStream().write(ESTABLISHED)
        client.getOutputStream().flush()
        daemon("connect-proxy-upstream") { copy(client, target, toClient = false, tunnel) }
        copy(target, client, toClient = true, tunnel)
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

    private fun copy(from: Socket, to: Socket, toClient: Boolean, tunnel: Tunnel) {
        try {
            val buffer = ByteArray(BUFFER_BYTES)
            while (true) {
                if (!toClient && readFromClient.get() >= blackHoleAfter) {
                    // Once: stop draining the client, so its send buffer fills and its write
                    // blocks. Park until close() - nothing here reads from it again.
                    blackHoleAfter = Long.MAX_VALUE
                    whenBlackHoled()
                    blackHoled?.await()
                }
                val read = from.getInputStream().read(buffer)
                if (read < 0) break
                if (!toClient) readFromClient.addAndGet(read.toLong())
                // A stalled tunnel keeps reading, so the sender's own buffers never fill and it
                // never learns that nothing is arriving at the other end.
                if (tunnel.relaying) {
                    to.getOutputStream().write(buffer, 0, read)
                    to.getOutputStream().flush()
                }
                if (!toClient) whenClientSpeaks.also { whenClientSpeaks = {} }()
                if (!tunnel.relaying) continue
                if (toClient && deliveredToClient.addAndGet(read.toLong()) >= holdAfter) {
                    // Once only: the count is not reset, so the next chunk does not stop again.
                    holdAfter = Long.MAX_VALUE
                    whenHeld()
                    // Everything the server sends from here waits in its own socket buffer, which
                    // is what makes resuming pick up the same conversation rather than a broken one.
                    held?.await()
                }
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
        private val REFUSED = "HTTP/1.0 503 Service Unavailable\r\n\r\n".toByteArray()

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
