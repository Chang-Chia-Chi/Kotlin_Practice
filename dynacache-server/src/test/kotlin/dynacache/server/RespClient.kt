package dynacache.server

import dynacache.engine.Reply
import java.net.Socket

/**
 * The test kit's Redis client: a plain socket, RESP2 out and one [Reply] in. Deliberately
 * unbuffered and unclever, so a test that fails fails about the server and not about the client.
 * Every read carries a timeout, which is what stands in for a sleep at this boundary.
 */
class RespClient(port: Int, timeoutMillis: Int = 10_000) : AutoCloseable {

    private val socket = Socket("127.0.0.1", port).apply {
        soTimeout = timeoutMillis
        tcpNoDelay = true
    }
    private val decoder = RespDecoder()
    private val chunk = ByteArray(8 * 1024)

    /** Writes one command as the RESP2 array of bulk strings a real client sends. */
    fun send(vararg words: String) {
        val frame = StringBuilder("*${words.size}\r\n")
        words.forEach { frame.append('$').append(it.length).append("\r\n").append(it).append("\r\n") }
        sendRaw(frame.toString())
    }

    /** Writes [text] as it stands, for the frames a well-behaved client would never send. */
    fun sendRaw(text: String) {
        socket.getOutputStream().apply {
            write(text.toByteArray(Charsets.ISO_8859_1))
            flush()
        }
    }

    /** The next reply, reading from the socket until one whole frame has arrived. */
    fun read(): Reply {
        while (true) {
            decoder.nextReply()?.let { return it }
            val read = socket.getInputStream().read(chunk)
            check(read >= 0) { "the server closed the connection before a whole reply arrived" }
            decoder.feed(chunk.copyOf(read))
        }
    }

    /** True when the server has closed its end: how a protocol error is observed from outside. */
    fun serverClosed(): Boolean = socket.getInputStream().read() < 0

    override fun close() = socket.close()
}
