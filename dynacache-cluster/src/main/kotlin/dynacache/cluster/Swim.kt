package dynacache.cluster

import dynacache.cluster.proto.Ack
import dynacache.cluster.proto.Envelope
import dynacache.cluster.proto.Envelope.BodyCase
import dynacache.cluster.proto.MembershipEntry
import dynacache.cluster.proto.Ping
import dynacache.cluster.proto.PingReq
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * One node's SWIM failure detector and membership table (spec 2.4). Time is ticks: [tick] is
 * one protocol period. Each period the node answers what arrived, escalates its open probes
 * (direct ping, then ping-req through [k] intermediaries once [rttTicks] passed with no ack,
 * then suspect once twice that passed again, since the indirect path is two hops each way),
 * declares suspects dead after [suspectTicks], and pings one random non-dead peer. The whole
 * table rides on every envelope; a received row wins when its incarnation is higher, or equal
 * with a stronger state (dead > suspect > alive). Suspicion of self is refuted by bumping the
 * incarnation. Production runs [run] as the node's one gossip coroutine; tests call [tick].
 */
class Swim(
    val self: NodeId,
    peers: Set<NodeId>,
    private val transport: Transport,
    private val random: Random,
    incarnation: Long = 0,
    private val period: Duration = 1.seconds,
    private val k: Int = 3,
    private val rttTicks: Int = 2,
    private val suspectTicks: Int = 3,
) : Membership {

    /** A suspicion names the incarnation it probed, so a node that re-incarnated since is not re-suspected. */
    private class Probe(val target: NodeId, val incarnation: Long, val sentAt: Long, var indirectAt: Long? = null)

    /** A ping sent on someone else's behalf: whose ping-req, under which of their seqs, and when. */
    private class Relay(val requester: NodeId, val seq: Long, val at: Long)

    private val table = LinkedHashMap<NodeId, Member>()
    // ponytail: a bounded buffer that drops the oldest change; the view stays authoritative.
    private val flow = MutableSharedFlow<Member>(extraBufferCapacity = 1024, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    private val probes = LinkedHashMap<Long, Probe>()
    private val relays = HashMap<Long, Relay>()
    private val suspectedAt = HashMap<NodeId, Long>()
    private var now = 0L
    private var seq = 0L

    init {
        (peers + self).sorted().forEach { table[it] = Member(it, MemberState.ALIVE, if (it == self) incarnation else 0) }
    }

    override val members: Map<NodeId, Member> get() = table
    override val changes: Flow<Member> get() = flow

    /** The production driver: one protocol period per [period], forever, on one coroutine. */
    suspend fun run() {
        while (true) {
            tick()
            delay(period)
        }
    }

    /** One protocol period. */
    suspend fun tick() {
        now++
        while (true) handle(transport.inbound.tryReceive().getOrNull() ?: break)
        for ((id, probe) in probes.entries.toList()) {
            val indirectAt = probe.indirectAt
            if (indirectAt == null && now - probe.sentAt >= rttTicks) {
                probe.indirectAt = now
                candidates(setOf(self, probe.target)).shuffled(random).take(k).forEach { send(it, pingReq(id, probe.target)) }
            } else if (indirectAt != null && now - indirectAt >= 2 * rttTicks) {
                probes.remove(id)
                merge(Member(probe.target, MemberState.SUSPECT, probe.incarnation))
            }
        }
        for ((node, since) in suspectedAt.toList()) {
            if (now - since >= suspectTicks) merge(table.getValue(node).copy(state = MemberState.DEAD))
        }
        relays.values.removeIf { now - it.at > 2 * rttTicks }
        val target = candidates(setOf(self)).randomOrNull(random) ?: return
        probes[++seq] = Probe(target, table.getValue(target).incarnation, now)
        send(target, ping(seq))
    }

    private suspend fun handle(envelope: Envelope) {
        envelope.membershipList.forEach {
            merge(Member(NodeId(it.node), MemberState.valueOf(it.state.name), it.incarnation))
        }
        val from = NodeId(envelope.from)
        when (envelope.bodyCase) {
            BodyCase.PING -> send(from, ack(envelope.ping.seq))
            BodyCase.PING_REQ -> {
                relays[++seq] = Relay(from, envelope.pingReq.seq, now)
                send(NodeId(envelope.pingReq.target), ping(seq))
            }
            BodyCase.ACK -> {
                val relay = relays.remove(envelope.ack.seq)
                if (relay != null) send(relay.requester, ack(relay.seq)) else probes.remove(envelope.ack.seq)
            }
            else -> Unit
        }
    }

    private fun merge(incoming: Member) {
        if (incoming.node == self) {
            val mine = table.getValue(self)
            if (incoming.state != MemberState.ALIVE && incoming.incarnation >= mine.incarnation) {
                table[self] = mine.copy(incarnation = incoming.incarnation + 1).also { flow.tryEmit(it) }
            }
            return
        }
        val current = table[incoming.node]
        if (current != null && RANK.compare(incoming, current) <= 0) return
        table[incoming.node] = incoming
        flow.tryEmit(incoming)
        if (incoming.state == MemberState.SUSPECT) suspectedAt.putIfAbsent(incoming.node, now) else suspectedAt.remove(incoming.node)
    }

    private fun candidates(exclude: Set<NodeId>): List<NodeId> =
        table.values.filter { it.state != MemberState.DEAD && it.node !in exclude }.map { it.node }

    private suspend fun send(to: NodeId, body: Envelope.Builder) {
        val piggyback = table.values.map {
            MembershipEntry.newBuilder().setNode(it.node.name)
                .setState(MembershipEntry.State.valueOf(it.state.name)).setIncarnation(it.incarnation).build()
        }
        transport.send(to, body.setFrom(self.name).setTo(to.name).addAllMembership(piggyback).build())
    }

    private fun ping(seq: Long) = Envelope.newBuilder().setPing(Ping.newBuilder().setSeq(seq))
    private fun ack(seq: Long) = Envelope.newBuilder().setAck(Ack.newBuilder().setSeq(seq))
    private fun pingReq(seq: Long, target: NodeId) =
        Envelope.newBuilder().setPingReq(PingReq.newBuilder().setSeq(seq).setTarget(target.name))

    private companion object {
        /** Higher incarnation wins; at equal incarnation dead > suspect > alive. */
        val RANK = compareBy<Member>({ it.incarnation }, { it.state })
    }
}
