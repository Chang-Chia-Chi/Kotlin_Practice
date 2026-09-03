package sftp.connector.pressure

import kotlinx.coroutines.delay
import sftp.connector.error.Attempt
import sftp.connector.error.AuthenticationFailed
import sftp.connector.error.ConnectFailed
import sftp.connector.error.NoSuchFile
import sftp.connector.error.ServerFailure
import sftp.connector.error.SessionLost
import sftp.connector.error.Unknown
import sftp.connector.testkit.FakeSftpTransport
import sftp.connector.testkit.FakeSftpTransport.Call
import sftp.connector.testkit.FakeSftpTransport.Operation
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * What the network did to one operation, carried in that operation's coroutine context so the
 * hook can tell whose call it is answering. The model that judges the operation's outcome cannot
 * see the wire: a rename the server carried out before its reply was lost, a stat that then saw
 * the landed file, a read that found the path already gone.
 */
internal class OpLog : AbstractCoroutineContextElement(OpLog) {
    companion object Key : CoroutineContext.Key<OpLog>

    var landedAt: String? = null
    var statSawLanded = false
    var readSawMissing = false
    val faults = mutableListOf<String>()
}

/**
 * The network, as a seeded random plays it through the fake transport's one hook.
 *
 * Every call the connector makes is answered, delayed, refused or lost by a roll of the
 * sequence's own random, so a seed replays the same network exactly. A session the test has
 * killed answers nothing again. A hang-up is never faulted: a real disconnect closes the socket
 * whatever the peer does, and faulting it here would leak in the fake's own accounting rather
 * than the pool's.
 */
internal class Adversary(
    private val rnd: Random,
    private val transport: FakeSftpTransport,
    /** Where this world's only rename puts a file, so a lost reply can be a landed one. */
    private val moveTarget: (String) -> String,
) {
    /** Off during start-up and measurement, when the world must answer truthfully. */
    var armed = false

    /** Told the moment a listing is answered, which is the instant a model snapshot is exact. */
    var onListing: () -> Unit = {}

    /** Told the moment the server carries out a rename whose reply is then lost, with the source path. */
    var onLanded: (String) -> Unit = {}

    /** Set by every fault the breaker is entitled to count; read and cleared by the invariant sweep. */
    var wireFaultSeen = false

    private val dead = HashSet<Int>()
    private var opened = 0
    private val background = OpLog()

    /** Sessions opened and not yet hung up on, as the hook saw them. */
    val alive = LinkedHashSet<Int>()

    fun kill(session: Int) {
        dead += session
    }

    suspend fun answer(call: Call) {
        val log = coroutineContext[OpLog] ?: background
        when (call.operation) {
            Operation.Close, Operation.Abort -> alive -= call.session
            Operation.Connect -> {
                if (armed) roll(call, log)
                alive += ++opened
            }
            else -> {
                if (call.session in dead) lost(call, log, "killed")
                if (armed) roll(call, log)
                when (call.operation) {
                    Operation.List -> onListing()
                    Operation.Read -> if (transport.snapshot()[call.path] == null) log.readSawMissing = true
                    Operation.Stat -> if (call.path == log.landedAt) log.statSawLanded = true
                    else -> Unit
                }
            }
        }
    }

    private suspend fun roll(call: Call, log: OpLog) {
        val op = call.operation
        val attempt = Attempt.inside(ENDPOINT, op.name.lowercase(), call.path)
        val outcome = rnd.nextInt(100)
        when {
            op == Operation.Connect -> when {
                outcome < 70 -> return
                outcome < 82 -> stall(call, log)
                outcome < 97 -> wire(call, log, "refused", ConnectFailed(attempt, "adversary: the proxy refused the tunnel"))
                else -> refuse(call, log, AuthenticationFailed(attempt, "adversary: the server rejected the password"))
            }
            outcome < 72 -> return
            outcome < 80 -> stall(call, log)
            outcome < 88 -> lost(call, log, "lost")
            outcome < 92 -> if (op == Operation.Rename) landedThenLost(call, log, attempt) else lost(call, log, "lost")
            outcome < 95 -> refuse(call, log, ServerFailure(attempt, 4, "adversary: the server refused this request"))
            outcome < 98 -> if (op in MAY_BE_MISSING) refuse(call, log, NoSuchFile(attempt, "adversary: no such path ${call.path}")) else return
            else -> wire(call, log, "unknown wording", Unknown(attempt, "adversary: a wording the table has never seen"))
        }
    }

    private suspend fun stall(call: Call, log: OpLog) {
        val took = DELAYS[rnd.nextInt(DELAYS.size)]
        if (took >= LONG_ENOUGH_TO_TIME_OUT) wireFaultSeen = true
        log.faults += "${describe(call)} stalled $took"
        delay(took)
    }

    private suspend fun lost(call: Call, log: OpLog, how: String): Nothing =
        wire(call, log, how, SessionLost(Attempt.inside(ENDPOINT, call.operation.name.lowercase(), call.path), "adversary: the session died under this call"))

    private fun wire(call: Call, log: OpLog, how: String, failure: Throwable): Nothing {
        wireFaultSeen = true
        log.faults += "${describe(call)} $how"
        throw failure
    }

    private fun refuse(call: Call, log: OpLog, failure: Throwable): Nothing {
        log.faults += "${describe(call)} ${failure::class.simpleName}"
        throw failure
    }

    /** The server did the rename, and the reply never came back: the I11 case. Only when it could have. */
    private suspend fun landedThenLost(call: Call, log: OpLog, attempt: Attempt): Nothing {
        val from = call.path!!
        val to = moveTarget(from)
        val bytes = transport.bytesAt(from)
        if (bytes == null || to in transport.snapshot()) lost(call, log, "lost")
        transport.remove(from)
        transport.file(to, bytes)
        log.landedAt = to
        onLanded(from)
        wire(call, log, "landed then lost", SessionLost(attempt, "adversary: the rename landed and its reply was lost"))
    }

    private fun describe(call: Call) = "${call.operation.name.lowercase()}#${call.session}${call.path?.let { " $it" } ?: ""}"

    private companion object {
        const val ENDPOINT = "fake.example:22"
        val DELAYS = listOf(10.milliseconds, 1.seconds, 30.seconds)

        /** The world's operation timeout; a stall at or past it becomes an `OperationTimeout`, which the breaker counts. */
        val LONG_ENOUGH_TO_TIME_OUT: Duration = 10.seconds
        val MAY_BE_MISSING = setOf(Operation.Stat, Operation.Rename, Operation.Delete)
    }
}
