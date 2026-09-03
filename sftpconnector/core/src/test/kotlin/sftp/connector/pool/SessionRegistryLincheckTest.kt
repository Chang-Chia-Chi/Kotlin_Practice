package sftp.connector.pool

import org.jetbrains.lincheck.datastructures.ModelCheckingOptions
import org.jetbrains.lincheck.datastructures.Operation
import org.junit.jupiter.api.Test
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.sftpConnector
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * The registry's one lock across every interleaving the model checker can reach: two callers
 * checking out at once never get the same entry (I2), and the counts published from under the
 * lock describe a moment that existed. Linearizability against this class run one thread at a
 * time is the specification.
 *
 * Every operation here is one of the registry's calls and nothing more. The sequences the pool
 * builds from them - check out, dial, fill; hand back, hang up, close - are outside the lock by
 * design, so their intermediate states are observable on purpose and a linearizability check
 * of them would report the design. Those sequences are the adversary's and the pool review's;
 * this is the lock.
 */
class SessionRegistryLincheckTest {

    private val registry = SessionRegistry(CONFIG.pool, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)) { 0 }

    @Operation(cancellableOnSuspension = false)
    suspend fun checkOut(): Long? = registry.checkOut(Throwable("lincheck"))?.entry?.id

    @Operation(cancellableOnSuspension = false)
    suspend fun stats(): String = registry.stats().toString()

    @Operation(cancellableOnSuspension = false)
    suspend fun beginClosing() = registry.beginClosing()

    @Operation(cancellableOnSuspension = false)
    suspend fun closeEverything(): List<Long> = registry.closeEverything().map { it.entry.id }

    @Test
    fun `I2_an entry is handed to at most one caller at a time, across interleavings`() =
        ModelCheckingOptions()
            .iterations(10)
            .invocationsPerIteration(500)
            .threads(2)
            .actorsPerThread(3)
            .check(this::class.java)

    private companion object {
        val CONFIG = sftpConnector("lincheck") {
            endpoint { host = "sftp.example" }
            auth { password("etl", "s3cret") }
            hostKey = HostKeyPolicy.AcceptAll
            pool { maxSize = 3 }
        }
    }
}
