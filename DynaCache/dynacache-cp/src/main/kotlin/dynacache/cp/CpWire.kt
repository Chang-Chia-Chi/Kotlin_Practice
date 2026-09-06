package dynacache.cp

import com.google.protobuf.ByteString
import dynacache.cluster.NodeId
import dynacache.cp.proto.AppendEntriesFailure
import dynacache.cp.proto.AppendEntriesSuccess
import dynacache.cp.proto.Candidacy
import dynacache.cp.proto.CpInfo
import dynacache.cp.proto.Granted
import dynacache.cp.proto.GroupMembersView
import dynacache.cp.proto.RaftEnvelope
import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply
import io.microraft.RaftEndpoint
import io.microraft.model.groupop.RaftGroupOp
import io.microraft.model.impl.DefaultRaftModelFactory
import io.microraft.model.log.LogEntry
import io.microraft.model.log.RaftGroupMembersView
import io.microraft.model.log.SnapshotChunk
import io.microraft.model.message.AppendEntriesFailureResponse
import io.microraft.model.message.AppendEntriesRequest
import io.microraft.model.message.AppendEntriesSuccessResponse
import io.microraft.model.message.InstallSnapshotRequest
import io.microraft.model.message.InstallSnapshotResponse
import io.microraft.model.message.PreVoteRequest
import io.microraft.model.message.PreVoteResponse
import io.microraft.model.message.RaftMessage
import io.microraft.model.message.TriggerLeaderElectionRequest
import io.microraft.model.message.VoteRequest
import io.microraft.model.message.VoteResponse
import io.microraft.report.RaftNodeReport
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.time.Duration
import dynacache.cp.proto.AppendEntriesRequest as WireAppendEntries
import dynacache.cp.proto.InstallSnapshotRequest as WireInstallSnapshotRequest
import dynacache.cp.proto.InstallSnapshotResponse as WireInstallSnapshotResponse
import dynacache.cp.proto.LogEntry as WireLogEntry
import dynacache.cp.proto.SnapshotChunk as WireSnapshotChunk

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

    internal val models = DefaultRaftModelFactory()

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
            is InstallSnapshotRequest -> envelope.installSnapshotRequest = WireInstallSnapshotRequest.newBuilder()
                .setSenderLeader(message.isSenderLeader)
                .setSnapshotTerm(message.snapshotTerm)
                .setSnapshotIndex(message.snapshotIndex)
                .setTotalChunkCount(message.totalSnapshotChunkCount)
                .also { request -> message.snapshotChunk?.let { request.chunk = encode(it) } }
                .addAllSnapshottedMembers(message.snapshottedMembers.map { it.id.toString() })
                .setMembers(encode(message.groupMembersView))
                .setQuerySequenceNumber(message.querySequenceNumber)
                .setFlowControlSequenceNumber(message.flowControlSequenceNumber)
                .build()
            is InstallSnapshotResponse -> envelope.installSnapshotResponse = WireInstallSnapshotResponse.newBuilder()
                .setSnapshotIndex(message.snapshotIndex)
                .setRequestedChunkIndex(message.requestedSnapshotChunkIndex)
                .setQuerySequenceNumber(message.querySequenceNumber)
                .setFlowControlSequenceNumber(message.flowControlSequenceNumber)
                .build()
            else -> error("${message.javaClass.simpleName} is not a message MicroRaft 0.7 sends")
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
            RaftEnvelope.BodyCase.INSTALL_SNAPSHOT_REQUEST -> envelope.installSnapshotRequest.let { request ->
                models.createInstallSnapshotRequestBuilder()
                    .setGroupId(group).setSender(sender).setTerm(term)
                    .setSenderLeader(request.senderLeader)
                    .setSnapshotTerm(request.snapshotTerm)
                    .setSnapshotIndex(request.snapshotIndex)
                    .setTotalSnapshotChunkCount(request.totalChunkCount)
                    .setSnapshotChunk(if (request.hasChunk()) decode(request.chunk) else null)
                    .setSnapshottedMembers(request.snapshottedMembersList.map(::endpoint))
                    .setGroupMembersView(decode(request.members))
                    .setQuerySequenceNumber(request.querySequenceNumber)
                    .setFlowControlSequenceNumber(request.flowControlSequenceNumber)
                    .build()
            }
            RaftEnvelope.BodyCase.INSTALL_SNAPSHOT_RESPONSE -> models.createInstallSnapshotResponseBuilder()
                .setGroupId(group).setSender(sender).setTerm(term)
                .setSnapshotIndex(envelope.installSnapshotResponse.snapshotIndex)
                .setRequestedSnapshotChunkIndex(envelope.installSnapshotResponse.requestedChunkIndex)
                .setQuerySequenceNumber(envelope.installSnapshotResponse.querySequenceNumber)
                .setFlowControlSequenceNumber(envelope.installSnapshotResponse.flowControlSequenceNumber)
                .build()
            RaftEnvelope.BodyCase.BODY_NOT_SET -> error("a Raft envelope from $sender carries no message")
        }
    }

    private fun candidacy(lastLogTerm: Int, lastLogIndex: Long, sticky: Boolean = false): Candidacy =
        Candidacy.newBuilder().setLastLogTerm(lastLogTerm).setLastLogIndex(lastLogIndex).setSticky(sticky).build()

    private fun granted(granted: Boolean): Granted = Granted.newBuilder().setGranted(granted).build()

    internal fun endpoint(id: String): RaftEndpoint = CpEndpoint(NodeId(id))

    internal fun encodeEntry(entry: LogEntry): WireLogEntry = WireLogEntry.newBuilder()
        .setIndex(entry.index)
        .setTerm(entry.term)
        .setOperation(ByteString.copyFrom(encodeOperation(entry.operation)))
        .build()

    internal fun decodeEntry(entry: WireLogEntry): LogEntry = models.createLogEntryBuilder()
        .setIndex(entry.index)
        .setTerm(entry.term)
        .setOperation(decodeOperation(entry.operation.toByteArray()))
        .build()

    // --- Snapshots ---------------------------------------------------------

    fun encode(view: RaftGroupMembersView): GroupMembersView = GroupMembersView.newBuilder()
        .setLogIndex(view.logIndex)
        .addAllMembers(view.members.map { it.id.toString() })
        .addAllVotingMembers(view.votingMembers.map { it.id.toString() })
        .build()

    fun decode(view: GroupMembersView): RaftGroupMembersView = models.createRaftGroupMembersViewBuilder()
        .setLogIndex(view.logIndex)
        .setMembers(view.membersList.map(::endpoint))
        .setVotingMembers(view.votingMembersList.map(::endpoint))
        .build()

    /** A chunk's operation is the state machine's [CpStateMachine.Snapshot]; the store and the wire share this form. */
    fun encode(chunk: SnapshotChunk): WireSnapshotChunk = WireSnapshotChunk.newBuilder()
        .setIndex(chunk.index)
        .setTerm(chunk.term)
        .setOperation(ByteString.copyFrom(encodeSnapshot(chunk.operation as CpStateMachine.Snapshot)))
        .setChunkIndex(chunk.snapshotChunkIndex)
        .setChunkCount(chunk.snapshotChunkCount)
        .setMembers(encode(chunk.groupMembersView))
        .build()

    fun decode(chunk: WireSnapshotChunk): SnapshotChunk = models.createSnapshotChunkBuilder()
        .setIndex(chunk.index)
        .setTerm(chunk.term)
        .setOperation(decodeSnapshot(chunk.operation.toByteArray()))
        .setSnapshotChunkIndex(chunk.chunkIndex)
        .setSnapshotChunkCount(chunk.chunkCount)
        .setGroupMembersView(decode(chunk.members))
        .build()

    /** Log time, then every primitive's table in the order the state machine holds them. */
    internal fun encodeSnapshot(snapshot: CpStateMachine.Snapshot): ByteArray = bytes {
        writeLong(snapshot.lastAppliedTs)
        writeTable(snapshot.counters) { writeLong(it.value); writeLong(it.expiresAt ?: NO_TTL) }
        writeTable(snapshot.locks) {
            writeLong(it.owner ?: NO_OWNER)
            writeLong(it.token)
            writeLong(it.expiresAt)
            writeInt(it.holds)
        }
        writeTable(snapshot.semaphores) { semaphore ->
            writeInt(semaphore.available)
            writeInt(semaphore.holders.size)
            semaphore.holders.forEach { (session, permits) -> writeLong(session); writeInt(permits) }
        }
        writeTable(snapshot.latches) { writeInt(it) }
        writeTable(snapshot.references) { writeBlob(it.value); writeLong(it.expiresAt ?: NO_TTL) }
        writeLong(snapshot.sessions.lastId)
        writeInt(snapshot.sessions.sessions.size)
        snapshot.sessions.sessions.forEach { (id, session) ->
            writeLong(id)
            writeLong(session.lastHeartbeat)
            writeLong(session.timeoutMs)
        }
    }

    internal fun decodeSnapshot(encoded: ByteArray): CpStateMachine.Snapshot = read(encoded) {
        CpStateMachine.Snapshot(
            lastAppliedTs = readLong(),
            counters = readTable { AtomicLongStateMachine.Counter(readLong(), readLong().takeIf { it != NO_TTL }) },
            locks = readTable {
                FencedLockStateMachine.Lock(readLong().takeIf { it != NO_OWNER }, readLong(), readLong(), readInt())
            },
            semaphores = readTable {
                SemaphoreStateMachine.Semaphore(readInt(), List(readInt()) { readLong() to readInt() }.toMap())
            },
            latches = readTable { readInt() },
            references = readTable {
                AtomicReferenceStateMachine.Reference(readBlob(), readLong().takeIf { it != NO_TTL })
            },
            sessions = SessionRegistry.State(
                readLong(),
                List(readInt()) { readLong() to SessionRegistry.Session(readLong(), readLong()) }.toMap(),
            ),
        )
    }

    private inline fun <V> DataOutputStream.writeTable(table: Map<Key, V>, value: DataOutputStream.(V) -> Unit) {
        writeInt(table.size)
        table.forEach { (key, v) -> writeBlob(key.bytes); value(v) }
    }

    private inline fun <V> DataInputStream.readTable(value: DataInputStream.() -> V): Map<Key, V> =
        List(readInt()) { Key(readBlob()) to value() }.toMap()

    // --- Log entry operations ----------------------------------------------

    /** Whatever MicroRaft puts in the log that is not the engine's own work, such as a new term. */
    internal data object Internal

    // The entries a leader appends: a stamped command, a TTL tick, the first entry of a term and
    // a session's closing. This is the one pair of functions that knows them; everything else
    // here is below the log entry.
    internal fun encodeOperation(operation: Any?): ByteArray = bytes {
        when (operation) {
            is CpOp -> { writeByte(OP_CP_OP); writeLong(operation.ts); writeCommand(operation.command) }
            is TtlTick -> { writeByte(OP_TICK); writeLong(operation.ts) }
            is NewTerm -> { writeByte(OP_NEW_TERM); writeInt(operation.term) }
            is SessionClosed -> { writeByte(OP_SESSION_CLOSED); writeLong(operation.ts); writeLong(operation.session) }
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
            OP_SESSION_CLOSED -> SessionClosed(readLong(), readLong())
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
            is Command.Cp.LongGetAdd -> tagged(CMD_GET_ADD, command.key) { writeLong(command.delta) }
            is Command.Cp.LongCas -> tagged(CMD_CAS, command.key) {
                writeLong(command.expected)
                writeLong(command.new)
            }
            is Command.Cp.LongExpire -> tagged(CMD_EXPIRE, command.key) { writeLong(command.ttl.toMillis()) }
            is Command.Cp.LongTtl -> tagged(CMD_TTL, command.key) {
                writeBoolean(command.precision == Command.Ttl.Precision.MILLIS)
            }
            is Command.Cp.LongPersist -> tagged(CMD_PERSIST, command.key) {}
            is Command.Cp.LockTry -> tagged(CMD_LOCK_TRY, command.key) {
                writeLong(command.session)
                writeLong(command.ttl.toMillis())
            }
            is Command.Cp.LockUnlock -> tagged(CMD_LOCK_UNLOCK, command.key) {
                writeLong(command.session)
                writeLong(command.token)
            }
            is Command.Cp.LockRenew -> tagged(CMD_LOCK_RENEW, command.key) {
                writeLong(command.session)
                writeLong(command.token)
                writeLong(command.ttl.toMillis())
            }
            is Command.Cp.LockForceUnlock -> tagged(CMD_LOCK_FORCE_UNLOCK, command.key) {}
            is Command.Cp.LockState -> tagged(CMD_LOCK_STATE, command.key) {}
            is Command.Cp.SemInit -> tagged(CMD_SEM_INIT, command.key) { writeInt(command.permits) }
            is Command.Cp.SemAcquire -> tagged(CMD_SEM_ACQUIRE, command.key) {
                writeLong(command.session)
                writeInt(command.permits)
            }
            is Command.Cp.SemRelease -> tagged(CMD_SEM_RELEASE, command.key) {
                writeLong(command.session)
                writeInt(command.permits)
            }
            is Command.Cp.SemAvailable -> tagged(CMD_SEM_AVAILABLE, command.key) {}
            is Command.Cp.SemDrain -> tagged(CMD_SEM_DRAIN, command.key) { writeLong(command.session) }
            is Command.Cp.LatchSet -> tagged(CMD_LATCH_SET, command.key) { writeInt(command.count) }
            is Command.Cp.LatchDown -> tagged(CMD_LATCH_DOWN, command.key) {}
            is Command.Cp.LatchGet -> tagged(CMD_LATCH_GET, command.key) {}
            is Command.Cp.LatchReset -> tagged(CMD_LATCH_RESET, command.key) { writeInt(command.count) }
            is Command.Cp.RefSet -> tagged(CMD_REF_SET, command.key) {
                writeBlob(command.value)
                writeLong(command.ttl?.toMillis() ?: NO_TTL)
            }
            is Command.Cp.RefGet -> tagged(CMD_REF_GET, command.key) {}
            is Command.Cp.RefCas -> tagged(CMD_REF_CAS, command.key) {
                writeBlob(command.expected)
                writeBlob(command.new)
            }
            is Command.Cp.RefExpire -> tagged(CMD_REF_EXPIRE, command.key) { writeLong(command.ttl.toMillis()) }
            is Command.Cp.RefTtl -> tagged(CMD_REF_TTL, command.key) {
                writeBoolean(command.precision == Command.Ttl.Precision.MILLIS)
            }
            is Command.Cp.RefPersist -> tagged(CMD_REF_PERSIST, command.key) {}
            is Command.Cp.SessionCreate -> tagged(CMD_SESSION_CREATE, command.key) { writeLong(command.timeout.toMillis()) }
            is Command.Cp.SessionHeartbeat -> tagged(CMD_SESSION_HEARTBEAT, command.key) { writeLong(command.session) }
            is Command.Cp.SessionClose -> tagged(CMD_SESSION_CLOSE, command.key) { writeLong(command.session) }
            // CP spec 6.7 is answered from the member's own report (see CpEngine), so an
            // introspection verb never reaches the log and has no encoding to reach it by.
            is Command.Cp.Introspection -> error("$command is answered locally, never replicated")
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
            CMD_GET_ADD -> Command.Cp.LongGetAdd(key, readLong())
            CMD_CAS -> Command.Cp.LongCas(key, readLong(), readLong())
            CMD_EXPIRE -> Command.Cp.LongExpire(key, Duration.ofMillis(readLong()))
            CMD_TTL -> Command.Cp.LongTtl(
                key,
                if (readBoolean()) Command.Ttl.Precision.MILLIS else Command.Ttl.Precision.SECONDS,
            )
            CMD_PERSIST -> Command.Cp.LongPersist(key)
            CMD_LOCK_TRY -> Command.Cp.LockTry(key, readLong(), Duration.ofMillis(readLong()))
            CMD_LOCK_UNLOCK -> Command.Cp.LockUnlock(key, readLong(), readLong())
            CMD_LOCK_RENEW -> Command.Cp.LockRenew(key, readLong(), readLong(), Duration.ofMillis(readLong()))
            CMD_LOCK_FORCE_UNLOCK -> Command.Cp.LockForceUnlock(key)
            CMD_LOCK_STATE -> Command.Cp.LockState(key)
            CMD_SEM_INIT -> Command.Cp.SemInit(key, readInt())
            CMD_SEM_ACQUIRE -> Command.Cp.SemAcquire(key, readLong(), readInt())
            CMD_SEM_RELEASE -> Command.Cp.SemRelease(key, readLong(), readInt())
            CMD_SEM_AVAILABLE -> Command.Cp.SemAvailable(key)
            CMD_SEM_DRAIN -> Command.Cp.SemDrain(key, readLong())
            CMD_LATCH_SET -> Command.Cp.LatchSet(key, readInt())
            CMD_LATCH_DOWN -> Command.Cp.LatchDown(key)
            CMD_LATCH_GET -> Command.Cp.LatchGet(key)
            CMD_LATCH_RESET -> Command.Cp.LatchReset(key, readInt())
            CMD_REF_SET ->
                Command.Cp.RefSet(key, readBlob(), readLong().takeIf { it != NO_TTL }?.let(Duration::ofMillis))
            CMD_REF_GET -> Command.Cp.RefGet(key)
            CMD_REF_CAS -> Command.Cp.RefCas(key, readBlob(), readBlob())
            CMD_REF_EXPIRE -> Command.Cp.RefExpire(key, Duration.ofMillis(readLong()))
            CMD_REF_TTL -> Command.Cp.RefTtl(
                key,
                if (readBoolean()) Command.Ttl.Precision.MILLIS else Command.Ttl.Precision.SECONDS,
            )
            CMD_REF_PERSIST -> Command.Cp.RefPersist(key)
            CMD_SESSION_CREATE -> Command.Cp.SessionCreate(Duration.ofMillis(readLong()))
            CMD_SESSION_HEARTBEAT -> Command.Cp.SessionHeartbeat(readLong())
            CMD_SESSION_CLOSE -> Command.Cp.SessionClose(readLong())
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

    /** What a member can see of the group (CP spec 6.7), read from its own MicroRaft report. */
    fun info(report: RaftNodeReport): CpInfo = CpInfo.newBuilder()
        .setLeader(report.term.leaderEndpoint?.id?.toString().orEmpty())
        // CP membership is fixed at startup (CP spec 2.2), so the initial members are the group.
        .addAllMembers(report.initialMembers.members.map { it.id.toString() })
        .setLogSize(report.log.lastLogOrSnapshotIndex - report.log.lastSnapshotIndex)
        .setAppliedIndex(report.log.commitIndex)
        .setSnapshotIndex(report.log.lastSnapshotIndex)
        .build()

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
    private const val OP_SESSION_CLOSED = 4

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
    private const val CMD_LOCK_TRY = 11
    private const val CMD_LOCK_UNLOCK = 12
    private const val CMD_LOCK_STATE = 13
    private const val CMD_LOCK_RENEW = 14
    private const val CMD_LOCK_FORCE_UNLOCK = 15
    private const val CMD_SESSION_CREATE = 16
    private const val CMD_SESSION_HEARTBEAT = 17
    private const val CMD_SESSION_CLOSE = 18
    private const val CMD_SEM_INIT = 19
    private const val CMD_SEM_ACQUIRE = 20
    private const val CMD_SEM_RELEASE = 21
    private const val CMD_SEM_AVAILABLE = 22
    private const val CMD_SEM_DRAIN = 23
    private const val CMD_LATCH_SET = 24
    private const val CMD_LATCH_DOWN = 25
    private const val CMD_LATCH_GET = 26
    private const val CMD_LATCH_RESET = 27
    private const val CMD_REF_SET = 28
    private const val CMD_REF_GET = 29
    private const val CMD_REF_CAS = 30
    private const val CMD_REF_EXPIRE = 31
    private const val CMD_REF_TTL = 32
    private const val CMD_REF_PERSIST = 33
    private const val CMD_GET_ADD = 34
    private const val NO_TTL = -1L
    private const val NO_OWNER = -1L

    private const val REPLY_SIMPLE = 1
    private const val REPLY_ERROR = 2
    private const val REPLY_INTEGER = 3
    private const val REPLY_BULK = 4
    private const val REPLY_ARRAY = 5
}
