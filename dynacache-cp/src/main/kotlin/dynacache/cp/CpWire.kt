package dynacache.cp

import com.google.protobuf.ByteString
import dynacache.cluster.NodeId
import dynacache.cp.proto.AppendEntriesFailure
import dynacache.cp.proto.AppendEntriesSuccess
import dynacache.cp.proto.Candidacy
import dynacache.cp.proto.CpInfo
import dynacache.cp.proto.Granted
import dynacache.cp.proto.RaftEnvelope
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import io.microraft.model.groupop.RaftGroupOp
import io.microraft.model.impl.DefaultRaftModelFactory
import io.microraft.model.log.LogEntry
import io.microraft.model.message.AppendEntriesFailureResponse
import io.microraft.model.message.AppendEntriesRequest
import io.microraft.model.message.AppendEntriesSuccessResponse
import io.microraft.model.message.PreVoteRequest
import io.microraft.model.message.PreVoteResponse
import io.microraft.model.message.RaftMessage
import io.microraft.model.message.TriggerLeaderElectionRequest
import io.microraft.model.message.VoteRequest
import io.microraft.model.message.VoteResponse
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.time.Duration
import dynacache.cp.proto.AppendEntriesRequest as WireAppendEntries
import dynacache.cp.proto.LogEntry as WireLogEntry

/**
 * The wire form of everything that leaves a CP member: MicroRaft's messages as the protobuf of
 * `cp.proto`, and the two engine values inside them - a [Command.Cp] and a [Reply] - as a compact
 * hand encoding in `bytes`.
 *
 * Two encodings rather than one because they change on different clocks. Raft's message set is
 * fixed by the algorithm, so a field-by-field protobuf mapping is written once; the command and
 * reply hierarchies grow a verb per ticket, so a tagged encoding in one `when` is cheaper to
 * extend than a protobuf message per variant.
 */
object CpWire {

    private val models = DefaultRaftModelFactory()

    // --- MicroRaft messages ------------------------------------------------

    fun encode(message: RaftMessage): RaftEnvelope {
        val envelope = RaftEnvelope.newBuilder()
            .setGroupId(message.groupId.toString())
            .setSender((message.sender as CpEndpoint).nodeId.name)
            .setTerm(message.term)
        when (message) {
            is PreVoteRequest -> envelope.preVoteRequest = candidacy(message.lastLogTerm, message.lastLogIndex)
            is PreVoteResponse -> envelope.preVoteResponse = granted(message.isGranted)
            is VoteRequest ->
                envelope.voteRequest = candidacy(message.lastLogTerm, message.lastLogIndex, message.isSticky)
            is VoteResponse -> envelope.voteResponse = granted(message.isGranted)
            is TriggerLeaderElectionRequest ->
                envelope.triggerLeaderElection = candidacy(message.lastLogTerm, message.lastLogIndex)
            is AppendEntriesRequest -> envelope.appendEntriesRequest = WireAppendEntries.newBuilder()
                .setPreviousLogTerm(message.previousLogTerm)
                .setPreviousLogIndex(message.previousLogIndex)
                .setCommitIndex(message.commitIndex)
                .setQuerySequenceNumber(message.querySequenceNumber)
                .setFlowControlSequenceNumber(message.flowControlSequenceNumber)
                .addAllEntries(message.logEntries.map(::encodeEntry))
                .build()
            is AppendEntriesSuccessResponse -> envelope.appendEntriesSuccess = AppendEntriesSuccess.newBuilder()
                .setLastLogIndex(message.lastLogIndex)
                .setQuerySequenceNumber(message.querySequenceNumber)
                .setFlowControlSequenceNumber(message.flowControlSequenceNumber)
                .build()
            is AppendEntriesFailureResponse -> envelope.appendEntriesFailure = AppendEntriesFailure.newBuilder()
                .setExpectedNextIndex(message.expectedNextIndex)
                .setQuerySequenceNumber(message.querySequenceNumber)
                .setFlowControlSequenceNumber(message.flowControlSequenceNumber)
                .build()
            // InstallSnapshot only flows once a member has fallen behind a snapshot, and
            // snapshots are T45; a group without them never reaches this branch.
            else -> throw NotImplementedError("${message.javaClass.simpleName} has no wire form yet (T45)")
        }
        return envelope.build()
    }

    fun decode(envelope: RaftEnvelope): RaftMessage {
        val sender = CpEndpoint(NodeId(envelope.sender))
        val group = envelope.groupId
        val term = envelope.term
        return when (envelope.bodyCase!!) {
            RaftEnvelope.BodyCase.PRE_VOTE_REQUEST -> models.createPreVoteRequestBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setLastLogTerm(envelope.preVoteRequest.lastLogTerm)
                .setLastLogIndex(envelope.preVoteRequest.lastLogIndex)
                .build()
            RaftEnvelope.BodyCase.PRE_VOTE_RESPONSE -> models.createPreVoteResponseBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setGranted(envelope.preVoteResponse.granted)
                .build()
            RaftEnvelope.BodyCase.VOTE_REQUEST -> models.createVoteRequestBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setLastLogTerm(envelope.voteRequest.lastLogTerm)
                .setLastLogIndex(envelope.voteRequest.lastLogIndex)
                .setSticky(envelope.voteRequest.sticky)
                .build()
            RaftEnvelope.BodyCase.VOTE_RESPONSE -> models.createVoteResponseBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setGranted(envelope.voteResponse.granted)
                .build()
            RaftEnvelope.BodyCase.TRIGGER_LEADER_ELECTION -> models.createTriggerLeaderElectionRequestBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setLastLogTerm(envelope.triggerLeaderElection.lastLogTerm)
                .setLastLogIndex(envelope.triggerLeaderElection.lastLogIndex)
                .build()
            RaftEnvelope.BodyCase.APPEND_ENTRIES_REQUEST -> models.createAppendEntriesRequestBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setPreviousLogTerm(envelope.appendEntriesRequest.previousLogTerm)
                .setPreviousLogIndex(envelope.appendEntriesRequest.previousLogIndex)
                .setCommitIndex(envelope.appendEntriesRequest.commitIndex)
                .setQuerySequenceNumber(envelope.appendEntriesRequest.querySequenceNumber)
                .setFlowControlSequenceNumber(envelope.appendEntriesRequest.flowControlSequenceNumber)
                .setLogEntries(envelope.appendEntriesRequest.entriesList.map(::decodeEntry))
                .build()
            RaftEnvelope.BodyCase.APPEND_ENTRIES_SUCCESS -> models.createAppendEntriesSuccessResponseBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setLastLogIndex(envelope.appendEntriesSuccess.lastLogIndex)
                .setQuerySequenceNumber(envelope.appendEntriesSuccess.querySequenceNumber)
                .setFlowControlSequenceNumber(envelope.appendEntriesSuccess.flowControlSequenceNumber)
                .build()
            RaftEnvelope.BodyCase.APPEND_ENTRIES_FAILURE -> models.createAppendEntriesFailureResponseBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setExpectedNextIndex(envelope.appendEntriesFailure.expectedNextIndex)
                .setQuerySequenceNumber(envelope.appendEntriesFailure.querySequenceNumber)
                .setFlowControlSequenceNumber(envelope.appendEntriesFailure.flowControlSequenceNumber)
                .build()
            RaftEnvelope.BodyCase.BODY_NOT_SET -> error("a Raft envelope from $sender carries no message")
        }
    }

    private fun candidacy(lastLogTerm: Int, lastLogIndex: Long, sticky: Boolean = false): Candidacy =
        Candidacy.newBuilder().setLastLogTerm(lastLogTerm).setLastLogIndex(lastLogIndex).setSticky(sticky).build()

    private fun granted(granted: Boolean): Granted = Granted.newBuilder().setGranted(granted).build()

    private fun encodeEntry(entry: LogEntry): WireLogEntry = WireLogEntry.newBuilder()
        .setIndex(entry.index)
        .setTerm(entry.term)
        .setOperation(ByteString.copyFrom(encodeOperation(entry.operation)))
        .build()

    private fun decodeEntry(entry: WireLogEntry): LogEntry = models.createLogEntryBuilder()
        .setIndex(entry.index)
        .setTerm(entry.term)
        .setOperation(decodeOperation(entry.operation.toByteArray()))
        .build()

    // --- Log entry operations ----------------------------------------------

    /** Whatever MicroRaft puts in the log that is not the engine's own work, such as a new term. */
    internal data object Internal

    // The three entries a leader appends (T39): a stamped command, a TTL tick and the first entry
    // of a term. This is the one pair of functions that knows them; everything else here is below
    // the log entry.
    internal fun encodeOperation(operation: Any?): ByteArray = bytes {
        when (operation) {
            is CpOp -> { writeByte(OP_CP_OP); writeLong(operation.ts); writeCommand(operation.command) }
            is TtlTick -> { writeByte(OP_TICK); writeLong(operation.ts) }
            is NewTerm -> { writeByte(OP_NEW_TERM); writeInt(operation.term) }
            // The OP_INTERNAL tag carries no payload, so a membership change encoded through it
            // would arrive at the follower as a no-op and be applied by nobody. CP membership is
            // fixed at startup (CP spec 2.2), so refuse it loudly rather than lose it quietly.
            is RaftGroupOp -> throw NotImplementedError("CP membership changes are deferred (CP spec 2.2)")
            else -> writeByte(OP_INTERNAL)
        }
    }

    internal fun decodeOperation(encoded: ByteArray): Any = read(encoded) {
        when (val tag = readByte().toInt()) {
            OP_CP_OP -> CpOp(readLong(), readCommand())
            OP_TICK -> TtlTick(readLong())
            OP_NEW_TERM -> NewTerm(readInt())
            OP_INTERNAL -> Internal
            else -> error("unknown log operation tag $tag")
        }
    }

    // --- Commands ----------------------------------------------------------

    fun encode(command: Command.Cp): ByteArray = bytes { writeCommand(command) }

    fun decodeCommand(encoded: ByteArray): Command.Cp = read(encoded) { readCommand() }

    private fun DataOutputStream.writeCommand(command: Command.Cp) {
        when (command) {
            is Command.Cp.LongSet -> tagged(CMD_SET, command.key) {
                writeLong(command.value)
                writeLong(command.ttl?.toMillis() ?: NO_TTL)
            }
            is Command.Cp.LongGet -> tagged(CMD_GET, command.key) {}
            is Command.Cp.LongIncr -> tagged(CMD_INCR, command.key) {}
            is Command.Cp.LongDecr -> tagged(CMD_DECR, command.key) {}
            is Command.Cp.LongIncrBy -> tagged(CMD_INCR_BY, command.key) { writeLong(command.delta) }
            is Command.Cp.LongDecrBy -> tagged(CMD_DECR_BY, command.key) { writeLong(command.delta) }
            is Command.Cp.LongCas -> tagged(CMD_CAS, command.key) {
                writeLong(command.expected)
                writeLong(command.new)
            }
            is Command.Cp.LongExpire -> tagged(CMD_EXPIRE, command.key) { writeLong(command.ttl.toMillis()) }
            is Command.Cp.LongTtl -> tagged(CMD_TTL, command.key) {}
            is Command.Cp.LongPersist -> tagged(CMD_PERSIST, command.key) {}
        }
    }

    private fun DataInputStream.readCommand(): Command.Cp = readCommandBody(readByte().toInt())

    private fun DataInputStream.readCommandBody(tag: Int): Command.Cp {
        val key = Key(readBlob())
        return when (tag) {
            CMD_SET -> Command.Cp.LongSet(key, readLong(), readLong().takeIf { it != NO_TTL }?.let(Duration::ofMillis))
            CMD_GET -> Command.Cp.LongGet(key)
            CMD_INCR -> Command.Cp.LongIncr(key)
            CMD_DECR -> Command.Cp.LongDecr(key)
            CMD_INCR_BY -> Command.Cp.LongIncrBy(key, readLong())
            CMD_DECR_BY -> Command.Cp.LongDecrBy(key, readLong())
            CMD_CAS -> Command.Cp.LongCas(key, readLong(), readLong())
            CMD_EXPIRE -> Command.Cp.LongExpire(key, Duration.ofMillis(readLong()))
            CMD_TTL -> Command.Cp.LongTtl(key)
            CMD_PERSIST -> Command.Cp.LongPersist(key)
            else -> error("unknown CP command tag $tag")
        }
    }

    private inline fun DataOutputStream.tagged(tag: Int, key: Key, rest: DataOutputStream.() -> Unit) {
        writeByte(tag)
        writeBlob(key.bytes)
        rest()
    }

    // --- Replies -----------------------------------------------------------

    fun encode(reply: Reply): ByteArray = bytes { writeReply(reply) }

    fun decodeReply(encoded: ByteArray): Reply = read(encoded) { readReply() }

    private fun DataOutputStream.writeReply(reply: Reply) {
        when (reply) {
            is Reply.Simple -> { writeByte(REPLY_SIMPLE); writeUTF(reply.text) }
            is Reply.Error -> { writeByte(REPLY_ERROR); writeUTF(reply.kind); writeUTF(reply.message) }
            is Reply.Integer -> { writeByte(REPLY_INTEGER); writeLong(reply.value) }
            is Reply.Bulk -> {
                writeByte(REPLY_BULK)
                writeBoolean(reply.bytes != null)
                reply.bytes?.let { writeBlob(it) }
            }
            is Reply.Array -> {
                writeByte(REPLY_ARRAY)
                writeInt(reply.items.size)
                reply.items.forEach { writeReply(it) }
            }
        }
    }

    private fun DataInputStream.readReply(): Reply = when (val tag = readByte().toInt()) {
        REPLY_SIMPLE -> Reply.Simple(readUTF())
        REPLY_ERROR -> Reply.Error(readUTF(), readUTF())
        REPLY_INTEGER -> Reply.Integer(readLong())
        REPLY_BULK -> Reply.Bulk(if (readBoolean()) readBlob() else null)
        REPLY_ARRAY -> Reply.Array(List(readInt()) { readReply() })
        else -> error("unknown reply tag $tag")
    }

    /** `CP.INFO` (CP spec 6.7): leader, members, log size, applied index, snapshot index. */
    fun infoReply(info: CpInfo): Reply = Reply.Array(
        listOf(
            Reply.Bulk(info.leader.takeIf(String::isNotEmpty)?.toByteArray()),
            Reply.Array(info.membersList.map { Reply.Bulk(it.toByteArray()) }),
            Reply.Integer(info.logSize),
            Reply.Integer(info.appliedIndex),
            Reply.Integer(info.snapshotIndex),
        ),
    )

    // --- Streams -----------------------------------------------------------

    private inline fun bytes(write: DataOutputStream.() -> Unit): ByteArray {
        val sink = ByteArrayOutputStream()
        DataOutputStream(sink).use(write)
        return sink.toByteArray()
    }

    private inline fun <T> read(encoded: ByteArray, parse: DataInputStream.() -> T): T =
        DataInputStream(ByteArrayInputStream(encoded)).use(parse)

    private fun DataOutputStream.writeBlob(value: ByteArray) {
        writeInt(value.size)
        write(value)
    }

    private fun DataInputStream.readBlob(): ByteArray {
        val value = ByteArray(readInt())
        readFully(value)
        return value
    }

    private const val OP_INTERNAL = 0
    private const val OP_CP_OP = 1
    private const val OP_TICK = 2
    private const val OP_NEW_TERM = 3

    private const val CMD_SET = 1
    private const val CMD_GET = 2
    private const val CMD_INCR = 3
    private const val CMD_DECR = 4
    private const val CMD_INCR_BY = 5
    private const val CMD_DECR_BY = 6
    private const val CMD_CAS = 7
    private const val CMD_EXPIRE = 8
    private const val CMD_TTL = 9
    private const val CMD_PERSIST = 10
    private const val NO_TTL = -1L

    private const val REPLY_SIMPLE = 1
    private const val REPLY_ERROR = 2
    private const val REPLY_INTEGER = 3
    private const val REPLY_BULK = 4
    private const val REPLY_ARRAY = 5
}
