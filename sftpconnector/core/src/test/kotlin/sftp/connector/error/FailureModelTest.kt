package sftp.connector.error

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The five rows of the failure model, asserted as the four decisions each row makes.
 *
 * The point of these is that a caller never has to combine `recoverable`, `poisons` and `fatal`
 * itself. If that combination is ever wrong here, it is wrong in one place instead of in every
 * caller that guessed at it.
 */
class FailureModelTest {

    @Test
    fun `a poisoning recoverable failure retries on a fresh session and evicts the one it had`() {
        val disposition = SessionLost(ATTEMPT, "the tunnel went quiet").disposition

        assertThat(disposition).isEqualTo(Disposition.RETRY_ON_A_FRESH_SESSION)
        assertThat(disposition.retry).isEqualTo(Retry.IMMEDIATELY)
        assertThat(disposition.countsAgainstTheBreaker).isTrue()
        assertThat(disposition.lease).isEqualTo(LeaseFate.EVICTED)
        assertThat(disposition.watch).isEqualTo(WatchReaction.REPORT_THE_FAILURE)
    }

    @Test
    fun `a recoverable failure that does not poison keeps the session it was using`() {
        val disposition = NoSuchFile(ATTEMPT, "the server has no such path").disposition

        assertThat(disposition).isEqualTo(Disposition.RETRY_ON_THIS_SESSION)
        assertThat(disposition.retry).isEqualTo(Retry.IMMEDIATELY)
        assertThat(disposition.countsAgainstTheBreaker).isTrue()
        assertThat(disposition.lease).isEqualTo(LeaseFate.RETURNED)
    }

    /** The one exception to "recoverable means try again now". */
    @Test
    fun `a permission refusal waits a full tick rather than asking again immediately`() {
        val denied = PermissionDenied(ATTEMPT, "the server refused on permissions")

        assertThat(denied.poisons).isFalse()
        assertThat(denied.disposition).isEqualTo(Disposition.RETRY_ON_THE_NEXT_TICK)
        assertThat(denied.disposition.retry).isEqualTo(Retry.AFTER_A_FULL_TICK)
        assertThat(denied.disposition.lease).isEqualTo(LeaseFate.RETURNED)
    }

    @Test
    fun `a fatal failure is never retried and is never held against the server`() {
        val disposition = AuthenticationFailed(ATTEMPT, "the server rejected the credential").disposition

        assertThat(disposition.retry).isEqualTo(Retry.NEVER)
        assertThat(disposition.countsAgainstTheBreaker).isFalse()
        assertThat(disposition.lease).isEqualTo(LeaseFate.EVICTED)
    }

    @Test
    fun `an exhausted pool fails the attempt and an open breaker skips the tick`() {
        assertThat(PoolExhausted(ATTEMPT).disposition).isEqualTo(Disposition.FAIL_THE_ATTEMPT)
        assertThat(CircuitOpen(ATTEMPT).disposition).isEqualTo(Disposition.SKIP_THE_TICK)

        assertThat(PoolExhausted(ATTEMPT).disposition.lease).isEqualTo(LeaseFate.NONE_HELD)
        assertThat(CircuitOpen(ATTEMPT).disposition.watch).isEqualTo(WatchReaction.REPORT_A_SKIP)
        assertThat(CircuitOpen(ATTEMPT).disposition.countsAgainstTheBreaker).isFalse()
    }

    @Test
    fun `I10_a fatal failure stops the watch and no other failure does`() {
        assertThat(EVERY_FAILURE).isNotEmpty()
        EVERY_FAILURE.forEach { failure ->
            val stops = failure.disposition.watch == WatchReaction.STOP
            assertThat(stops)
                .describedAs("%s stops the watch", failure::class.simpleName)
                .isEqualTo(failure is Fatal)
        }
    }

    /**
     * A failure with no context in its message costs an operator the search through the
     * surrounding log for which connector, which file and which try this was.
     */
    @Test
    fun `every failure raised while running names the endpoint, the operation and the try`() {
        EVERY_FAILURE.filterNot { it is ConfigurationError }.forEach { failure ->
            assertThat(failure.message)
                .describedAs(failure::class.simpleName)
                .contains("endpoint=sftp.example:22", "op=download", "path=/inbox/x.csv", "attempt=3")
        }
    }

    /**
     * The compiler checks this `when` covers the hierarchy, so a failure class added without a
     * decision about how it behaves fails the build rather than reaching production undecided.
     * Naming the row it belongs to is what proves the decision was actually made.
     */
    private fun rowOf(failure: SftpException): Disposition = when (failure) {
        is ConnectFailed, is SessionLost, is OperationTimeout, is ServerFailure, is Unknown ->
            Disposition.RETRY_ON_A_FRESH_SESSION

        is NoSuchFile -> Disposition.RETRY_ON_THIS_SESSION
        is PermissionDenied -> Disposition.RETRY_ON_THE_NEXT_TICK
        is AuthenticationFailed, is HostKeyRejected, is ConfigurationError -> Disposition.STOP_THE_CONNECTOR
        is PoolExhausted -> Disposition.FAIL_THE_ATTEMPT
        is CircuitOpen -> Disposition.SKIP_THE_TICK
    }

    @Test
    fun `every failure class lands on the row the failure model puts it on`() {
        EVERY_FAILURE.forEach { failure ->
            assertThat(failure.disposition).describedAs(failure::class.simpleName).isEqualTo(rowOf(failure))
        }
    }

    private companion object {
        private val ATTEMPT = Attempt("sftp.example:22", "download", "/inbox/x.csv", number = 3)

        private val EVERY_FAILURE: List<SftpException> = listOf(
            ConnectFailed(ATTEMPT, "no session"),
            SessionLost(ATTEMPT, "the connection broke"),
            OperationTimeout(ATTEMPT, "took too long"),
            ServerFailure(ATTEMPT, statusCode = 4, detail = "the server refused"),
            Unknown(ATTEMPT, "a wording nobody has read"),
            PermissionDenied(ATTEMPT, "refused on permissions"),
            NoSuchFile(ATTEMPT, "no such path"),
            AuthenticationFailed(ATTEMPT, "wrong credential"),
            HostKeyRejected(ATTEMPT, "wrong key"),
            ConfigurationError("nothing was configured"),
            PoolExhausted(ATTEMPT),
            CircuitOpen(ATTEMPT),
        )
    }
}
