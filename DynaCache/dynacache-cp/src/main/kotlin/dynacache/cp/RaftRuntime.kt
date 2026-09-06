package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Reply
import io.microraft.Ordered
import io.microraft.RaftNode
import io.microraft.RaftRole
import io.microraft.transport.Transport
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * One CP member's MicroRaft node: the replicated log, the state machine that applies it, the
 * transport it reaches the other members through and the store it remembers itself in. The
 * transport and the store are the seams (plan 2.3): tests pass an in-memory transport and store,
 * production the gRPC transport and the file store, and this class knows nothing about either.
 */
class RaftRuntime(val config: CpConfig, transport: Transport, store: CpStore = InMemoryRaftStore()) : AutoCloseable {

    init {
        require(config.isCpMember) { "${config.nodeId} is not a CP member of ${config.cpMembers}" }
    }

    val endpoint = CpEndpoint(config.nodeId)

    /** Completes when this member leads and may stamp; replaced once it stops leading. */
    @Volatile
    private var leadership = CompletableFuture<RaftRuntime>()

    /** The last term whose first entry this member applied. */
    @Volatile
    private var appliedTerm = 0

    val stateMachine = CpStateMachine(currentTerm = { node.term.term }, onTermApplied = ::termApplied)

    val node: RaftNode = RaftNode.newBuilder()
        .setGroupId(config.groupId)
        .setConfig(config.raft)
        .setTransport(transport)
        .setStateMachine(stateMachine)
        .setStore(store)
        .setRaftNodeReportListener { report ->
            if (report.role != RaftRole.LEADER && leadership.isDone) leadership = CompletableFuture()
        }
        // A member that ran before resumes from what its store kept: the term and vote Raft must
        // not forget, its log and its last snapshot (I20). A fresh one starts from the config.
        .let { builder ->
            store.restored()?.let(builder::setRestoredState)
                ?: builder.setLocalEndpoint(endpoint).setInitialGroupMembers(config.endpoints)
        }
        .build()

    /**
     * True when this member is the one that may replicate; every other member answers NOTLEADER.
     * A fresh leader counts only once its term's first entry is applied: until then its applied
     * time may trail what the old leader committed, and a stamp taken from it could turn time
     * back (C19).
     */
    val isLeader: Boolean get() = endpoint == node.term.leaderEndpoint && appliedTerm == node.term.term

    private fun termApplied(term: Int) {
        appliedTerm = term
        if (isLeader) leadership.complete(this)
    }

    /** The stamp of the last entry this leader appended; guarded by [appendLock]. */
    private var lastStampedTs = 0L
    private val appendLock = Any()

    /**
     * Appends [command] to the log stamped with log time. The stamp and the append happen under
     * one lock, so entries carry strictly increasing stamps in log order (C19) however many
     * client threads submit at once.
     */
    fun replicate(command: Command.Cp): CompletableFuture<Ordered<Reply>> =
        synchronized(appendLock) { node.replicate(CpOp(stamp(), command)) }

    /**
     * The TTL tick (CP spec 5): when this member leads and nothing has been appended for a tick
     * interval of its clock, appends a [TtlTick] so log time moves on every member. A caller runs
     * it every tick interval in production and step by step in a test; a non-leader does nothing.
     * Once the tick is applied, every session whose timeout ran out at that log time gets a
     * [SessionClosed] entry (CP spec 9.3), from the leader alone since only it saw its tick commit.
     * Completes with the index of the last entry it appended once committed, or 0 when none was.
     */
    fun tick(): CompletableFuture<Long> {
        val tick = synchronized(appendLock) {
            val idle = config.clock.millis() >= lastStampedTs + config.tickInterval.toMillis()
            if (isLeader && idle) node.replicate<Any?>(TtlTick(stamp())) else return CompletableFuture.completedFuture(0L)
        }
        return tick.thenCompose { applied ->
            val closed = stateMachine.lapsedSessions().map { session ->
                synchronized(appendLock) { node.replicate<Any?>(SessionClosed(stamp(), session)) }
            }
            closed.lastOrNull()?.thenApply { it.commitIndex } ?: CompletableFuture.completedFuture(applied.commitIndex)
        }
    }

    /** CP spec 5: `max(clock_now, last_committed_ts + 1)`, and past whatever this leader stamped already. */
    private fun stamp(): Long {
        lastStampedTs = maxOf(config.clock.millis(), stateMachine.lastAppliedTs + 1, lastStampedTs + 1)
        return lastStampedTs
    }

    fun start(): RaftRuntime = apply { node.start().join() }

    /** Blocks until this member has won an election, or the configured timeout passes. */
    fun awaitLeadership(): RaftRuntime =
        leadership.get(config.leaderElectionTimeout.toMillis(), TimeUnit.MILLISECONDS)

    /** The future that [awaitLeadership] waits on, for a caller racing several members. */
    val elected: CompletableFuture<RaftRuntime> get() = leadership

    override fun close() {
        runCatching { node.terminate().join() }
    }
}
