package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.testkit.MutableClock
import io.microraft.RaftConfig
import io.microraft.RaftEndpoint
import io.microraft.model.message.RaftMessage
import io.microraft.transport.Transport
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * A CP group of [size] members in one process, wired over an in-memory transport: MicroRaft's
 * messages are handed straight to the target member's node, a killed member neither sends nor
 * receives, and a network partition ([partition]) drops everything crossing it. Every member
 * keeps its state in an in-memory store across a kill and restart; [fileMember] keeps it on disk
 * under [fileStoreDir] when one is given. Nothing here waits on wall-clock time; a caller awaits
 * an election through the members' own leadership futures and a committed apply through the
 * engine's future.
 */
class CpTestKit(size: Int = 3, private val fileStoreDir: Path? = null) : AutoCloseable {

    /**
     * Elections in a test should not take seconds, and a snapshot should come round within one;
     * a real deployment keeps MicroRaft's defaults.
     */
    private val raft: RaftConfig = RaftConfig.newBuilder()
        .setLeaderElectionTimeoutMillis(200)
        .setLeaderHeartbeatPeriodSecs(1)
        .setLeaderHeartbeatTimeoutSecs(1)
        .setCommitCountToTakeSnapshot(SNAPSHOT_EVERY)
        .build()

    val members: List<NodeId> = (1..size).map { NodeId("cp$it") }

    /** The member restarted from disk when the kit has a [fileStoreDir]; from memory otherwise. */
    val fileMember: NodeId = members.last()

    private val stores: Map<NodeId, CpStore> = members.associateWith { InMemoryRaftStore() }
    private val isolated = ConcurrentHashMap.newKeySet<NodeId>()

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
        val candidates = live().mapNotNull { runtimes[it] }
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

    /** Brings [member] back from its store: its own term, vote, log and snapshot, then the leader's log. */
    fun restartMember(member: NodeId) {
        killed.remove(member)
        spawn(member).start()
    }

    /** A network partition: [cut] reach only each other and the rest only each other, until [heal]. */
    fun partition(cut: List<NodeId>) {
        isolated.clear()
        isolated.addAll(cut)
    }

    fun heal() = isolated.clear()

    override fun close() {
        runtimes.values.forEach { it.close() }
        runtimes.clear()
        engines.clear()
    }

    private fun spawn(member: NodeId): RaftRuntime {
        // The file store is opened afresh on every start, so a restart really reads the disk.
        val store = if (member == fileMember && fileStoreDir != null) FileRaftStore(fileStoreDir.resolve(member.name)) else stores.getValue(member)
        val runtime = RaftRuntime(CpConfig(member, members, raft = raft, clock = clocks.getValue(member)), transportFor(member), store)
        runtimes[member] = runtime
        engines[member] = CpEngine(runtime)
        return runtime
    }

    private fun transportFor(sender: NodeId) = object : Transport {
        override fun send(target: RaftEndpoint, message: RaftMessage) {
            val receiver = (target as CpEndpoint).nodeId
            if (sender in killed || receiver in killed) return
            if ((sender in isolated) != (receiver in isolated)) return
            runtimes[receiver]?.node?.handle(message)
        }

        override fun isReachable(endpoint: RaftEndpoint): Boolean =
            (endpoint as CpEndpoint).nodeId !in killed
    }

    private companion object {
        const val ELECTION_TIMEOUT_MILLIS = 10_000L
        const val SNAPSHOT_EVERY = 100
        val EPOCH: Instant = Instant.parse("2026-09-06T00:00:00Z")
    }
}
