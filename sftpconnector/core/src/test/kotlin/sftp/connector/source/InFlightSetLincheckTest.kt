package sftp.connector.source

import org.jetbrains.lincheck.datastructures.IntGen
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions
import org.jetbrains.lincheck.datastructures.Operation
import org.jetbrains.lincheck.datastructures.Param
import org.junit.jupiter.api.Test
import sftp.connector.transport.RemoteFile
import java.time.Instant

/**
 * The in-flight set's lock across every interleaving the model checker can reach: a file enters
 * at most once at a time (I7), leaving gives exactly that file's place back (I8), a file
 * excluded for good never enters again, and a file at a name another file holds never enters
 * while that one is out - and leaving takes out only the file that entered, never its namesake.
 * Linearizability against this class run one thread at a time is the whole specification.
 *
 * What is checked is the lock body - `enter` and `exit`, the non-suspending core - and not the
 * suspending `admit` around it, for two reasons found on the way. The set's own design is not
 * linearizable at `admit`: a duplicate that passes the first look while another poll admits the
 * same file waits for room it will then not use, which no sequential run can produce and
 * `SftpSourceTest` proves directly. And the checker's verifier failed inside itself on the
 * suspending wrapper, so the room the semaphore keeps is left to the library that keeps it.
 */
@Param(name = "file", gen = IntGen::class, conf = "0:3")
class InFlightSetLincheckTest {

    private val set = InFlightSet(FILES.size)

    @Operation
    fun enter(@Param(name = "file") file: Int): Boolean = set.enter(FILES[file.asFile()])

    @Operation
    fun exit(@Param(name = "file") file: Int): Boolean = set.exit(FILES[file.asFile()], forGood = false)

    @Operation
    fun exclude(@Param(name = "file") file: Int): Boolean = set.exit(FILES[file.asFile()], forGood = true)

    @Operation
    fun holds(@Param(name = "file") file: Int): Boolean = set.holds(FILES[file.asFile()])

    @Operation
    fun size(): Int = set.size

    @Test
    fun `I7_I8_the in-flight set's lock is linearizable across interleavings`() =
        ModelCheckingOptions()
            .iterations(10)
            .invocationsPerIteration(500)
            .threads(2)
            .actorsPerThread(3)
            .check(this::class.java)

    /** The generator has been seen to stray outside its configured range; every integer names a file. */
    private fun Int.asFile(): Int = mod(FILES.size)

    private companion object {
        /**
         * Three files at three names, and a fourth at the first's name with another size: the same
         * name uploaded again while the first is out. Every operation over it is the path rule from
         * one side or the other, which one operation added for the purpose would not be.
         */
        val FILES = List(3) { RemoteFile("/drop/f$it.csv", it.toLong(), Instant.EPOCH, isDirectory = false) } +
            RemoteFile("/drop/f0.csv", 99, Instant.EPOCH, isDirectory = false)
    }
}
