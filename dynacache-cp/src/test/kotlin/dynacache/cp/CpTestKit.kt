package dynacache.cp

import dynacache.cluster.NodeId
import io.microraft.RaftConfig
import io.microraft.RaftEndpoint
import io.microraft.model.message.RaftMessage
import io.microraft.transport.Transport
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * A CP group of [size] members in one process, wired over an in-memory transport: MicroRaft's
 * messages are handed straight to the target member's node, and a killed member neither sends nor
 * receives. Nothing here waits on wall-clock time; a caller awaits an election through the
 * members' own leadership futures and a committed apply through the engine's future.
 */
class CpTestKit(size: Int = 3) : AutoCloseable {

    /** Elections in a test should not take a second; a real deployment keeps MicroRaft's defaults. */
    private val raft: RaftConfig = RaftConfig.newBuilder()
        .setLeaderElectionTimeoutMillis(200)
        .setLeaderHeartbeatPeriodSecs(1)
        .setLeaderHeartbeatTimeoutSecs(5)
        .build()

    val members: List<NodeId> = (1..size).map { NodeId("cp$it") }

    /** Each member has a clock of its own, so a test can put a leader's clock ahead of a follower's. */
    private val clocks: Map<NodeId, MutableClock> = members.associateWith { MutableClock(EPOCH) }

    private val runtimes = ConcurrentHashMap<NodeId, RaftRuntime>()
    private val engines = ConcurrentHashMap<NodeId, CpEngine>()
    private val killed = ConcurrentHashMap.newKeySet<NodeId>()

    init {
        members.forEach(::spawn)
        runtimes.values.forEach { it.start() }
    }

    fun engine(member: NodeId): CpEngine = engines.getValue(member)

    fun runtime(member: NodeId): RaftRuntime = runtimes.getValue(member)

    fun live(): List<NodeId> = members.filterNot(killed::contains)

    fun clock(member: NodeId): MutableClock = clocks.getValue(member)

    /** The member that may replicate, once an election has produced one. */
    fun leader(): RaftRuntime {
        val candidates = live().map { runtimes.getValue(it) }
        CompletableFuture.anyOf(*candidates.map { it.elected }.toTypedArray())
            .get(ELECTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        return candidates.firstOrNull { it.isLeader }
            ?: error("a member reported leadership but none holds it now")
    }

    fun leaderEngine(): CpEngine = engines.getValue(leader().config.nodeId)

    /**
     * Returns once [member] has applied the entry at [index]. A follower learns the commit on the
     * leader's next append or heartbeat, so this asks the node's report until it says so.
     */
    fun awaitApplied(member: NodeId, index: Long) {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ELECTION_TIMEOUT_MILLIS)
        val node = runtime(member).node
        while (node.report.get(ELECTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).result.log.commitIndex < index) {
            check(System.nanoTime() < deadline) { "$member has not applied index $index" }
            Thread.onSpinWait()
        }
    }

    /** Stops [member] and cuts it off the transport, as a crashed node would be. */
    fun killMember(member: NodeId) {
        killed.add(member)
        runtimes.remove(member)?.close()
        engines.remove(member)
    }

    /**
     * Brings [member] back as a fresh node with the same endpoint. It has no state of its own
     * (see the store note in [RaftRuntime]) and catches up from the leader's log.
     */
    fun restartMember(member: NodeId) {
        killed.remove(member)
        spawn(member).start()
    }

    override fun close() {
        runtimes.values.forEach { it.close() }
        runtimes.clear()
        engines.clear()
    }

    private fun spawn(member: NodeId): RaftRuntime {
        val runtime = RaftRuntime(CpConfig(member, members, raft = raft, clock = clocks.getValue(member)), transportFor(member))
        runtimes[member] = runtime
        engines[member] = CpEngine(runtime)
        return runtime
    }

    private fun transportFor(sender: NodeId) = object : Transport {
        override fun send(target: RaftEndpoint, message: RaftMessage) {
            val receiver = (target as CpEndpoint).nodeId
            if (sender in killed || receiver in killed) return
            runtimes[receiver]?.node?.handle(message)
        }

        override fun isReachable(endpoint: RaftEndpoint): Boolean =
            (endpoint as CpEndpoint).nodeId !in killed
    }

    /** Time moves only when a test says so; the Raft thread reads it, hence volatile. */
    class MutableClock(@Volatile var now: Instant) : Clock() {
        fun advance(by: Duration) { now += by }
        override fun instant(): Instant = now
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
    }

    private companion object {
        const val ELECTION_TIMEOUT_MILLIS = 10_000L
        val EPOCH: Instant = Instant.parse("2026-09-06T00:00:00Z")
    }
}
