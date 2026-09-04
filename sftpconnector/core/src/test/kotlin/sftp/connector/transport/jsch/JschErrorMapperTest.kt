package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.JSchProxyException
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.SftpConnectorConfig
import sftp.connector.config.sftpConnector
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.ConnectFailed
import sftp.connector.error.NoSuchFile
import sftp.connector.error.PermissionDenied
import sftp.connector.error.ServerFailure
import sftp.connector.error.SessionLost
import sftp.connector.error.Unknown
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.PrintStream
import kotlin.coroutines.cancellation.CancellationException
import com.jcraft.jsch.SftpException as JschStatusException

/**
 * The half of the error table that needs no server: the SFTP status codes, and what happens to a
 * wording the table has never seen.
 *
 * The rows that need a real server to produce them honestly are proven in the testkit against the
 * embedded one, because a message this test made up would only ever prove that the table agrees
 * with itself.
 */
class JschErrorMapperTest {

    private val meters = SimpleMeterRegistry()
    private val mapper = JschErrorMapper(meters, CONFIG)

    @Test
    fun `a missing path is recoverable and leaves the session in the pool`() {
        val failure = mapping(JschStatusException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "No such file"))

        assertThat(failure).isInstanceOf(NoSuchFile::class.java)
        assertThat((failure as NoSuchFile).poisons).isFalse()
        assertThat(failure).hasMessageContaining("No such file")
    }

    @Test
    fun `a permission refusal is recoverable and leaves the session in the pool`() {
        val failure = mapping(JschStatusException(ChannelSftp.SSH_FX_PERMISSION_DENIED, "Permission denied"))

        assertThat(failure).isInstanceOf(PermissionDenied::class.java)
        assertThat((failure as PermissionDenied).poisons).isFalse()
    }

    /**
     * A status reply is proof the channel parsed the request and answered, so the refusal is of
     * the request and not evidence against the session carrying it.
     */
    @Test
    fun `any other status code is the server refusing, the code is kept, and the session survives`() {
        val failure = mapping(JschStatusException(ChannelSftp.SSH_FX_FAILURE, "Failure"))

        assertThat(failure).isInstanceOf(ServerFailure::class.java)
        assertThat((failure as ServerFailure).statusCode).isEqualTo(ChannelSftp.SSH_FX_FAILURE)
        assertThat(failure.poisons).isFalse()
    }

    /**
     * JSch reports a dead connection through the same type and the same generic status code it
     * uses for the server's own refusals. Read by code alone this would be a server failure, and
     * a broken session would be handed to the next caller.
     */
    @Test
    fun `a status failure carrying an IO error is a lost session, whatever its code says`() {
        val broken = JschStatusException(ChannelSftp.SSH_FX_FAILURE, "wrapped", IOException("Pipe closed"))

        val failure = mapping(broken)

        assertThat(failure).isInstanceOf(SessionLost::class.java)
        assertThat((failure as SessionLost).poisons).isTrue()
        assertThat(failure).hasMessageContaining("Pipe closed")
    }

    /**
     * `endpoint=` names the target, which is not the host that refused. Without the proxy in the
     * message the operator pings a server that is perfectly healthy.
     */
    @Test
    fun `a proxy that will not open a tunnel is a failure to connect, and names the proxy`() {
        val failure = mapping(JSchProxyException("ProxyHTTP: java.net.ConnectException: Connection refused"))

        assertThat(failure).isInstanceOf(ConnectFailed::class.java)
        assertTrue(failure.message!!.contains("proxy.internal:3128"), "the address that refused: ${failure.message}")
    }

    /**
     * The methods JSch quotes are the ones the *server* offers; the connector only ever sends a
     * password, and whose password it was is the first thing anyone asks.
     */
    @Test
    fun `a rejected credential names the account the connector offered`() {
        val failure = mapping(JSchException("Auth fail for methods 'password,keyboard-interactive,publickey'"))

        assertThat(failure).isInstanceOf(AuthenticationFailed::class.java)
        assertTrue(failure.message!!.contains("\"etl\""), "the account: ${failure.message}")
        assertTrue(failure.message!!.contains("offers a password"), "what was actually tried: ${failure.message}")
    }

    /**
     * The wording JSch uses for a channel asked of a session that has already gone. It is the one
     * row no staged failure produces yet, because the transport opens its channel during connect
     * and nothing above it has a live session to ask a second one of.
     */
    @Test
    fun `a channel asked of a dead session is a lost session`() {
        val failure = mapping(JSchException("session is down"))

        assertThat(failure).isInstanceOf(SessionLost::class.java)
        assertThat((failure as SessionLost).poisons).isTrue()
    }

    /**
     * Later layers wrap calls that already went through here. Classifying a decided failure again
     * would bury it inside an unrecognised one - a rejected password would come back out as
     * something to retry - and would count a meter whose only reading that matters is zero.
     */
    @Test
    fun `a failure that has already been classified is passed straight back`() {
        val alreadyMapped = AuthenticationFailed(ATTEMPT, "the server rejected the credential")

        val failure = mapper.translating(ATTEMPT) {
            runCatching { mapper.translating(ATTEMPT) { throw alreadyMapped } }.exceptionOrNull()!!
        }

        assertThat(failure).isSameAs(alreadyMapped)
        assertThat(unmappedCount()).isEqualTo(0.0)
    }

    @Test
    fun `a wording the table has never seen keeps its raw text, warns, and is counted`() {
        val stderr = capturingStandardError {
            val failure = mapping(JSchException("some wording no release has used yet"))

            assertThat(failure).isInstanceOf(Unknown::class.java)
            assertThat((failure as Unknown).rawMessage).isEqualTo("some wording no release has used yet")
            assertThat(failure.poisons).isTrue()
            assertThat(failure.cause).isInstanceOf(JSchException::class.java)
        }

        assertThat(stderr).contains("WARN", "some wording no release has used yet")
        assertThat(unmappedCount()).isEqualTo(1.0)
    }

    @Test
    fun `a wording the table knows is not counted as unmapped`() {
        mapping(JSchException("Auth fail for methods 'password'"))
        mapping(JschStatusException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "No such file"))

        assertThat(meters.find("sftp_error_unmapped_total").counter()).isNull()
    }

    /**
     * Cancellation is the caller changing its mind. Wrapped, it would reach a retry ladder as an
     * ordinary failure and the connector would go on doing work nobody is waiting for.
     */
    @Test
    fun `a cancellation passes through untouched`() {
        val cancelled = CancellationException("the collector went away")

        assertThatThrownBy { mapper.translating(ATTEMPT) { throw cancelled } }.isSameAs(cancelled)
        assertThat(unmappedCount()).isEqualTo(0.0)
    }

    @Test
    fun `a call that succeeds is left alone`() {
        assertThat(mapper.translating(ATTEMPT) { "/inbox" }).isEqualTo("/inbox")
    }

    /**
     * What JSch says when the far side accepts the TCP connection and closes it before sending
     * its version line - a proxy whose upstream is down behind a port publisher that still
     * accepts, or a toxic removed from under a handshake. No session was ever established, so it
     * is a failure to connect, and a known one: found by T15 once and by the P4 partition row on
     * every run.
     */
    @Test
    fun `a connection the far side closes during the version exchange is a failure to connect`() {
        val failure = mapping(JSchException("connection is closed by foreign host"))

        assertThat(failure).isInstanceOf(ConnectFailed::class.java)
        assertThat(unmappedCount()).isZero()
    }

    private fun mapping(thrown: Exception): Throwable =
        runCatching { mapper.translating(ATTEMPT) { throw thrown } }.exceptionOrNull()!!

    private fun unmappedCount(): Double =
        meters.find("sftp_error_unmapped_total").counter()?.count() ?: 0.0

    /** The test binding writes to standard error and looks it up on every call. */
    private fun capturingStandardError(body: () -> Unit): String {
        val captured = ByteArrayOutputStream()
        val original = System.err
        System.setErr(PrintStream(captured, true))
        try {
            body()
        } finally {
            System.setErr(original)
        }
        return captured.toString()
    }

    private companion object {
        private val ATTEMPT = Attempt("sftp.example:22", "list", "/inbox")

        private val CONFIG: SftpConnectorConfig = sftpConnector("vendor-drop") {
            endpoint {
                host = "sftp.example"
                proxy { httpConnect("proxy.internal", 3128) }
            }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
        }
    }
}
