package sftp.connector.transport.jsch

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.Session
import com.jcraft.jsch.SftpATTRS
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import sftp.connector.error.ServerFailure
import sftp.connector.transport.Listing
import sftp.connector.transport.RemoteFile
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * What the adapter does with a directory entry whose name is not a name.
 *
 * The connector guards the *local* join - a listed name is checked before it is joined to the
 * staging directory - and did not guard the *remote* one: the listing built every entry's path as
 * the directory joined to whatever the server called it, and the source quotes that path straight
 * back as the source of a move and the argument of a delete. A server answering `READDIR` for
 * `/drop` with `../../../home/etl/.ssh/authorized_keys` therefore got the account's own key file
 * moved into a folder the sender can read, or unlinked, by a connector doing exactly what it was
 * told.
 *
 * A conforming server cannot send such an entry, which is why this cannot be written against a
 * real server and why the testkit's fake - which drops anything holding a separator before it
 * reports it - is structurally incapable of staging it. So the SSH library's own channel stands in
 * here, and is asked to say the things only a hostile or broken server would say. The same
 * stand-in pins the other half of the same question - how a path this connector chose is spelled
 * to the library - which had no test under it either.
 */
class JschListingNamesTest {

    @Test
    fun `a listed name that is not one path segment never becomes a remote path`() = runBlocking<Unit> {
        val channel = listing(NOT_NAMES + "ledger.csv")
        val connection = connectionOver(channel)

        val handedOver = mutableListOf<RemoteFile>()
        connection.list("/drop") { handedOver += it; Listing.CONTINUE }

        assertEquals(listOf("/drop/ledger.csv"), handedOver.map { it.path })

        // The ack, as the source performs it: whatever the listing handed over is quoted back to
        // the server as the source of a move and as the argument of a delete. One entry was handed
        // over, so exactly one of each goes out, and it is the one that never left the directory.
        handedOver.forEach { connection.rename(it.path, "/drop/done/${it.name}") }
        handedOver.forEach { connection.delete(it.path) }

        verify(channel).rename("/drop/ledger.csv", "/drop/done/ledger.csv")
        verify(channel).rm("/drop/ledger.csv")
        verify(channel, times(1)).rename(anyString(), anyString())
        verify(channel, times(1)).rm(anyString())
    }

    /**
     * The refusal is not silent: a directory whose entries are being dropped is a fact about the
     * server that nobody would otherwise learn. One line per entry, naming the endpoint and the
     * name as the server spelled it - spelled back with its control characters escaped, so a name
     * holding a newline cannot forge a second log record out of the line reporting it.
     */
    @Test
    fun `a refused entry is reported once, on one line, naming the endpoint and the raw name`() {
        val logged = capturingStandardError {
            runBlocking {
                connectionOver(listing(listOf("../../../home/etl/.ssh/authorized_keys", "two\nlines")))
                    .list("/drop") { Listing.CONTINUE }
            }
        }

        val warnings = logged.lines().filter { REFUSAL in it }
        assertEquals(2, warnings.size, logged)
        assertTrue(warnings.all { "sftp.example:22" in it }, logged)
        assertTrue(warnings.any { "../../../home/etl/.ssh/authorized_keys" in it }, logged)
        assertTrue(warnings.any { """two\nlines""" in it }, logged)
    }

    /**
     * The other server-supplied string that ends up in front of a path join: the probe resolves
     * the watched directory once and every listing, move target and log line for the rest of the
     * run is built on the answer, so an answer holding a line break would be built on too.
     */
    @Test
    fun `a resolved path the server answered with something no path can hold is refused`() = runBlocking<Unit> {
        val channel = mock(ChannelSftp::class.java)
        Mockito.`when`(channel.realpath("inbound")).thenReturn("/home/etl/inbound\nWARN all is well")
        val connection = connectionOver(channel)

        val refused = assertThrows(ServerFailure::class.java) { runBlocking { connection.realpath("inbound") } }

        assertTrue("no path can hold" in refused.message.orEmpty(), refused.message)
        assertTrue("""inbound\nWARN""" in refused.message.orEmpty(), refused.message)
    }

    /**
     * D37's two deliberate exceptions, pinned. JSch reads `*` and `?` in the last component of a
     * path as a glob for `ls`, `stat`, `get`, `put`, `rename` and `rm` - a delete of `*.csv`
     * removed every file that matched - so the adapter escapes them before the library sees them.
     * It does *not* escape them for `mkdir` and `realpath`, because JSch expands neither and an
     * escape there would be sent to the server as part of the name; the startup probe makes both
     * of those calls with an operator-supplied path, so the exception is load-bearing and was
     * resting on a reading of the library's sources with no test under it.
     *
     * This pins what the adapter *sends*. What the library then does with it stays where it was,
     * proved against the embedded server by the escaping tests, because `*` is not a legal
     * character in a file name on Windows and a test that made one could only ever run on POSIX.
     */
    @Test
    fun `mkdir and realpath are sent the path as written, and everything else is sent it escaped`() =
        runBlocking<Unit> {
            val channel = mock(ChannelSftp::class.java)
            Mockito.`when`(channel.realpath("/drop/star*dir")).thenReturn("/drop/star*dir")
            val connection = connectionOver(channel)

            connection.mkdir("/drop/star*dir")
            connection.realpath("/drop/star*dir")
            connection.delete("/drop/star*.csv")
            connection.rename("/drop/star*.csv", "/drop/done/one.csv")

            verify(channel).mkdir("/drop/star*dir")
            verify(channel).realpath("/drop/star*dir")
            verify(channel).rm("""/drop/star\*.csv""")
            verify(channel).rename("""/drop/star\*.csv""", "/drop/done/one.csv")
        }

    private fun listing(names: List<String>): ChannelSftp {
        val channel = mock(ChannelSftp::class.java)
        Mockito.doAnswer { invocation ->
            val selector = invocation.getArgument<ChannelSftp.LsEntrySelector>(1)
            for (name in names) {
                if (selector.select(entryNamed(name)) == ChannelSftp.LsEntrySelector.BREAK) break
            }
            null
        }.`when`(channel).ls(anyString(), Mockito.any())
        return channel
    }

    private fun entryNamed(name: String): ChannelSftp.LsEntry =
        mock(ChannelSftp.LsEntry::class.java).also {
            Mockito.`when`(it.filename).thenReturn(name)
            Mockito.`when`(it.attrs).thenReturn(mock(SftpATTRS::class.java))
        }

    private fun connectionOver(channel: ChannelSftp) = JschConnection(
        session = mock(Session::class.java),
        channel = channel,
        io = Dispatchers.Unconfined,
        errors = JschErrorMapper(SimpleMeterRegistry()),
        endpoint = ENDPOINT,
    )

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
        private const val ENDPOINT = "sftp.example:22"

        /** Enough of the WARN to find it, and short enough to survive a rewording of the rest. */
        private const val REFUSAL = "is not a name"

        /**
         * Names no conforming server sends, every one of which used to become a path the connector
         * would quote back: a climb out of the listed directory, a descent into it, a rooted path,
         * the empty name, and the two characters a name cannot hold at all. The separator's absence
         * is what makes every other name safe, and these are the ways of not having that.
         */
        private val NOT_NAMES = listOf(
            "../../../home/etl/.ssh/authorized_keys",
            "sub/nested.csv",
            "/etc/shadow",
            "",
            "two\nlines.csv",
            "nul\u0000byte.csv",
        )
    }
}
