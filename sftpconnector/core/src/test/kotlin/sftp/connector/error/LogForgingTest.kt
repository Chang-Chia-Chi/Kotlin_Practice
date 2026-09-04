package sftp.connector.error

import com.jcraft.jsch.JSchException
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import sftp.connector.transport.jsch.JschErrorMapper
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * Whether somebody who can name a file, or a server that can word its own errors, can write lines
 * into this connector's log.
 *
 * A newline is a legal character in a file name on every POSIX filesystem, so an ordinary vendor
 * with write access to a drop directory can produce one without any server compromise. Every
 * failure the connector raises puts its path in its message and most of them are logged; against a
 * plain-text appender - and the connector does not choose the host's appender - one record then
 * becomes two, and the second can be spelled to read like one of the connector's own.
 *
 * The rendering is guarded rather than the input, because the input is not always a name: the
 * server's own error text goes the same way, and the connector has no say in how that is worded.
 */
class LogForgingTest {

    /** Built here rather than inside the capture, so its host-key warning is not what is read. */
    private val config = sftpConnector("vendor-drop") {
        endpoint { host = "sftp.example" }
        auth { password("etl", "s3cret") }
        hostKey = HostKeyPolicy.AcceptAll
    }

    @Test
    fun `a path holding a newline is rendered on one line`() {
        val attempt = Attempt("sftp.example:22", "download", "/drop/two\nINFO all is well.csv")

        val message = UnsafeFileName(attempt, "the listed name cannot be a file name").message.orEmpty()

        assertEquals(1, message.lines().size, message)
        assertTrue("""two\nINFO""" in message, message)
    }

    @Test
    fun `a detail holding a newline is rendered on one line`() {
        val attempt = Attempt("sftp.example:22", "list", "/drop")

        val message = ServerFailure(attempt, 4, "the server said: no\rWARN nothing to see").message.orEmpty()

        assertEquals(1, message.lines().size, message)
        assertTrue("""no\rWARN""" in message, message)
    }

    /**
     * The one place a wording nobody has read reaches a log line, and the one the connector has
     * least control over: it prints the server's text verbatim so the mapping table can be updated
     * by copying rather than by reconstruction. Verbatim has to stop at the line ending.
     */
    @Test
    fun `an unmapped server message is logged on one line`() {
        val logged = capturingStandardError {
            JschErrorMapper(SimpleMeterRegistry(), config).translating(Attempt("sftp.example:22", "list", "/drop")) {
                throw JSchException("something new\nWARN the connector is fine")
            }
        }

        val warnings = logged.lines().filter { "No mapping for this failure" in it }
        assertEquals(1, warnings.size, logged)
        assertTrue("""new\nWARN""" in warnings.single(), logged)
        // Everything from the stack trace on is the logging backend rendering the exception's own
        // message, which no logger escapes and which is out of the connector's hands; what the
        // connector answers for is the line it writes itself, and that is what is checked here.
        val writtenByTheConnector = logged.lines().takeWhile { "com.jcraft.jsch.JSchException" !in it }
        assertTrue(writtenByTheConnector.none { it.trimStart().startsWith("WARN the connector is fine") }, logged)
    }

    private fun capturingStandardError(body: () -> Unit): String {
        val captured = ByteArrayOutputStream()
        val original = System.err
        System.setErr(PrintStream(captured, true))
        try {
            runCatching(body)
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }
}
