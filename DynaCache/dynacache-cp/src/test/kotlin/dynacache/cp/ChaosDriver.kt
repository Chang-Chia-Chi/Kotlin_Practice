package dynacache.cp

import dynacache.cluster.NodeId
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import java.nio.file.Path
import java.time.Duration
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * A seeded chaos driver over [CpTestKit]: a plan of random client operations - lock try, unlock and
 * renew on a few keys, counter increments and CAS, session heartbeats - interleaved with faults -
 * leader kills, follower kills, restarts, and network partitions - every kill followed by a restart
 * within the loop. The plan is fixed by the seed; a run over a real Raft group is what varies.
 *
 * The lock chaos ([runLockChaos]) is single-threaded, so its interleaving of clients is the seed's
 * and its checks run inline: it asserts mutual exclusion (I13) as it goes and returns the fencing
 * tokens per key for a monotonicity check (I14). The counter history ([counterHistory]) runs its
 * clients concurrently, so overlapping operations give the linearizability checker something to
 * reorder (C20); the faults between rounds stay the seed's.
 */
class ChaosDriver(seed: Long, fileStoreDir: Path? = null) : AutoCloseable {

    val kit = CpTestKit(fileStoreDir = fileStoreDir)
    private val rnd = Random(seed)

    /** The member currently out of the group (killed or on the minority side of a partition), if any. */
    private var out: NodeId? = null
    private var partitioned = false
    private var fileMemberRestarted = false

    /** Leader failovers cost an election (the heartbeat timeout); a run allows only a few, so it stays quick. */
    private var leaderKills = 0

    override fun close() = kit.close()

    /**
     * Runs [steps] of lock, counter and session operations from [sessions] clients on a handful of
     * keys, interleaved with faults, checking mutual exclusion inline. Returns the fencing tokens
     * granted per lock key in the order they were issued, for the caller to assert strictly climb.
     */
    fun runLockChaos(steps: Int, sessions: Int = 4): Map<Key, List<Long>> {
        val ids = (1..sessions).map { (submit(Command.Cp.SessionCreate(Duration.ofHours(1))) as Reply.Integer).value }
        val locks = (1..3).map { Key("cp:lock:$it") }
        val counters = (1..2).map { Key("cp:counter:$it") }
        val owner = HashMap<Key, Long?>()
        val token = HashMap<Key, Long>()
        val issued = locks.associateWith { mutableListOf<Long>() }

        repeat(steps) { step ->
            if (rnd.nextInt(3) == 0) {
                fault()
            } else when (rnd.nextInt(4)) {
                0 -> tryLock(locks.random(rnd), ids.random(rnd), owner, token, issued)
                1 -> unlock(locks.random(rnd), owner, token)
                2 -> submit(if (rnd.nextBoolean()) Command.Cp.LongIncr(counters.random(rnd)) else Command.Cp.LongCas(counters.random(rnd), 0, 1))
                else -> submit(Command.Cp.SessionHeartbeat(ids.random(rnd)))
            }
        }
        restore() // leave the group whole
        return issued
    }

    private fun tryLock(
        key: Key,
        session: Long,
        owner: HashMap<Key, Long?>,
        token: MutableMap<Key, Long>,
        issued: Map<Key, MutableList<Long>>,
    ) {
        val reply = submit(Command.Cp.LockTry(key, session, Duration.ofHours(1))) as? Reply.Array ?: return
        val ok = (reply.items[0] as Reply.Integer).value == 1L
        val tok = (reply.items[1] as Reply.Integer).value
        val held = owner[key]
        if (ok) {
            // I13: a lock is granted only when it was free or already this session's; never stolen.
            check(held == null || held == session) { "$key granted to $session while $held held it" }
            if (tok > (token[key] ?: 0L)) issued.getValue(key).add(tok) // a fresh grant, not a reentrant one
            owner[key] = session
            token[key] = tok
        } else {
            // A denial means someone else holds it (leases here never expire: the clock does not move).
            check(held != null && held != session) { "$key denied to $session but belief says $held" }
        }
    }

    private fun unlock(key: Key, owner: HashMap<Key, Long?>, token: MutableMap<Key, Long>) {
        val session = owner[key] ?: return
        val reply = submit(Command.Cp.LockUnlock(key, session, token.getValue(key)))
        if (reply is Reply.Integer && reply.value == 1L) owner[key] = null
    }

    /**
     * A concurrent workload on one counter for the linearizability checker (C20): [rounds] rounds of
     * [clients] simultaneous increments and reads, a seeded leader kill and restart between rounds.
     * An operation that times out (its leader died under it) is recorded unknown; one that comes back
     * `-NOTLEADER` never took effect and is dropped.
     */
    fun counterHistory(rounds: Int = 8, clients: Int = 3): List<Op<CounterOp, Long>> {
        val counter = Key("cp:counter:lin")
        val clock = AtomicLong()
        val history = Collections.synchronizedList(mutableListOf<Op<CounterOp, Long>>())
        val pool = Executors.newFixedThreadPool(clients)
        try {
            repeat(rounds) { round ->
                val wave = (0 until clients).map { client ->
                    CompletableFuture.runAsync({
                        val op: CounterOp = if (rnd.nextInt(3) == 0) CounterOp.Get else CounterOp.IncrBy(1)
                        val command = if (op is CounterOp.Get) Command.Cp.LongGet(counter) else Command.Cp.LongIncr(counter)
                        val call = clock.getAndIncrement()
                        val reply = try {
                            kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)
                        } catch (timeout: Exception) {
                            null
                        }
                        val ret = clock.getAndIncrement()
                        when {
                            reply is Reply.Integer -> history.add(Op(client, call, ret, op, reply.value))
                            reply is Reply.Bulk && reply.bytes == null -> history.add(Op(client, call, ret, op, 0L))
                            reply == null -> history.add(Op(client, call, Long.MAX_VALUE, op, null))
                            // -NOTLEADER: the command never replicated, so it did not happen; drop it.
                        }
                    }, pool)
                }
                CompletableFuture.allOf(*wave.toTypedArray()).get(WAVE_TIMEOUT_SECS, SECONDS)
                // One failover in the middle: a leader dies under the workload and a successor takes over.
                if (round == rounds / 2) killAndRestartLeader()
            }
        } finally {
            pool.shutdownNow()
            restore()
        }
        return history
    }

    /**
     * Submits [command] to the current leader, riding out a re-election: a `-NOTLEADER` or a timeout
     * is retried against a freshly found leader until [SUBMIT_DEADLINE_SECS] passes. Used by the lock
     * chaos, where every operation is expected to land because a majority is always alive.
     */
    fun submit(command: Command): Reply {
        val deadline = System.nanoTime() + SECONDS.toNanos(SUBMIT_DEADLINE_SECS)
        var last: Reply? = null
        while (System.nanoTime() < deadline) {
            last = try {
                kit.leaderEngine().submit(command).get(REPLY_TIMEOUT_SECS, SECONDS)
            } catch (retry: Exception) {
                null
            }
            if (last != null && !(last is Reply.Error && last.kind == "NOTLEADER")) return last
        }
        return last ?: Reply.Error("TIMEOUT", "no leader answered $command in time")
    }

    // --- Faults ------------------------------------------------------------

    /** One fault a step: if a member is out, bring it back; otherwise take one out. Keeps a majority. */
    private fun fault() {
        if (out != null) restore() else takeOneOut()
    }

    private fun takeOneOut() {
        val leader = kit.leader().config.nodeId
        // Mostly churn a follower (no election); occasionally fail the leader (an election, capped),
        // and kill the file-store member at least once so a restart from disk is exercised (I20).
        val victim = when {
            !fileMemberRestarted && kit.fileMember in kit.live() && kit.fileMember != leader -> kit.fileMember
            leaderKills < MAX_LEADER_KILLS && rnd.nextInt(5) == 0 -> leader.also { leaderKills++ }
            else -> kit.live().first { it != leader }
        }
        if (rnd.nextBoolean()) {
            kit.killMember(victim)
            out = victim
        } else {
            kit.partition(listOf(victim)) // a minority of one: it hears nothing, the majority serves
            out = victim
            partitioned = true
        }
    }

    private fun restore() {
        val member = out ?: return
        if (partitioned) {
            kit.heal()
            partitioned = false
        } else {
            kit.restartMember(member)
            if (member == kit.fileMember) fileMemberRestarted = true
        }
        out = null
    }

    private fun killAndRestartLeader() {
        val leader = kit.leader().config.nodeId
        kit.killMember(leader)
        kit.leader() // wait for the successor before restarting, so the group is never below majority
        kit.restartMember(leader)
    }

    private companion object {
        const val REPLY_TIMEOUT_SECS = 10L
        const val WAVE_TIMEOUT_SECS = 30L
        const val SUBMIT_DEADLINE_SECS = 15L
        const val MAX_LEADER_KILLS = 2
    }
}
