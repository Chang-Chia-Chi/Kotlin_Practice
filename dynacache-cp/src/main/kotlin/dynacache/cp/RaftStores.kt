package dynacache.cp

import dynacache.cp.proto.MemberState
import io.microraft.model.log.LogEntry
import io.microraft.model.log.RaftGroupMembersView
import io.microraft.model.log.SnapshotChunk
import io.microraft.model.persistence.RaftEndpointPersistentState
import io.microraft.model.persistence.RaftTermPersistentState
import io.microraft.persistence.RaftStore
import io.microraft.persistence.RestoredRaftState
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import dynacache.cp.proto.LogEntry as WireLogEntry
import dynacache.cp.proto.SnapshotChunk as WireSnapshotChunk

/**
 * MicroRaft's [RaftStore] plus the one thing it lacks: reading back what was written, so a member
 * restarted from its store resumes with its own term, vote, log and snapshot rather than an empty
 * log (I20, C21). [RaftRuntime] asks once, when it builds the node.
 */
interface CpStore : RaftStore {
    /** Everything this member persisted, or null when it never ran. */
    fun restored(): RestoredRaftState?
}

/** The store of a member that may forget everything when its process ends: tests, and the default. */
class InMemoryRaftStore : CpStore {
    private var endpoint: RaftEndpointPersistentState? = null
    private var members: RaftGroupMembersView? = null
    private var term: RaftTermPersistentState? = null
    private val entries = ArrayList<LogEntry>()
    private val chunks = ArrayList<SnapshotChunk>()

    override fun persistAndFlushLocalEndpoint(state: RaftEndpointPersistentState) { endpoint = state }
    override fun persistAndFlushInitialGroupMembers(view: RaftGroupMembersView) { members = view }
    override fun persistAndFlushTerm(state: RaftTermPersistentState) { term = state }
    override fun persistLogEntries(new: List<LogEntry>) { entries += new }
    override fun truncateLogEntriesFrom(index: Long) { entries.removeIf { it.index >= index } }
    override fun truncateLogEntriesUntil(index: Long) { entries.removeIf { it.index <= index } }
    override fun persistSnapshotChunk(chunk: SnapshotChunk) { chunks += chunk }
    override fun deleteSnapshotChunks(index: Long, count: Int) { chunks.removeIf { it.index == index } }
    override fun flush() = Unit
    override fun restored() = restoredState(endpoint, members, term, chunks, entries)
}

/**
 * The store on disk under [dir]. `member.pb` holds who this member is, its group, term and vote,
 * rewritten whole since it is a few dozen bytes; `log.pb` is the log as delimited `LogEntry`
 * messages, appended to and rewritten only when Raft truncates it; each snapshot chunk is its own
 * `snapshot-<index>-<chunk>.pb`. Every write reaches the file before the call returns.
 */
class FileRaftStore(private val dir: Path) : CpStore {
    private val member = dir.resolve("member.pb")
    private val log = dir.resolve("log.pb")
    private val state: MemberState.Builder =
        if (Files.exists(member)) MemberState.parseFrom(Files.readAllBytes(member)).toBuilder() else MemberState.newBuilder()

    init {
        Files.createDirectories(dir)
    }

    override fun persistAndFlushLocalEndpoint(state: RaftEndpointPersistentState) =
        remember { setEndpoint(state.localEndpoint.id.toString()).setVoting(state.isVoting) }

    override fun persistAndFlushInitialGroupMembers(view: RaftGroupMembersView) =
        remember { setInitialMembers(CpWire.encode(view)) }

    override fun persistAndFlushTerm(state: RaftTermPersistentState) =
        remember { setTerm(state.term).setVotedFor(state.votedFor?.id?.toString().orEmpty()) }

    override fun persistLogEntries(entries: List<LogEntry>) = writeLog(entries, CREATE, APPEND, WRITE)

    override fun truncateLogEntriesFrom(index: Long) = writeLog(entries().filter { it.index < index }, CREATE, TRUNCATE_EXISTING, WRITE)

    override fun truncateLogEntriesUntil(index: Long) = writeLog(entries().filter { it.index > index }, CREATE, TRUNCATE_EXISTING, WRITE)

    override fun persistSnapshotChunk(chunk: SnapshotChunk) {
        Files.write(chunkFile(chunk.index, chunk.snapshotChunkIndex), CpWire.encode(chunk).toByteArray())
    }

    override fun deleteSnapshotChunks(index: Long, count: Int) = repeat(count) { Files.deleteIfExists(chunkFile(index, it)) }

    // ponytail: Files.write hands the bytes to the OS, not the platter; FileChannel.force is the
    // upgrade if a crash between the write and the OS flush must never lose a vote.
    override fun flush() = Unit

    override fun restored(): RestoredRaftState? = restoredState(
        endpoint = state.endpoint.takeIf { it.isNotEmpty() }?.let {
            CpWire.models.createRaftEndpointPersistentStateBuilder().setLocalEndpoint(CpWire.endpoint(it)).setVoting(state.voting).build()
        },
        members = if (state.hasInitialMembers()) CpWire.decode(state.initialMembers) else null,
        term = CpWire.models.createRaftTermPersistentStateBuilder()
            .setTerm(state.term)
            .setVotedFor(state.votedFor.takeIf { it.isNotEmpty() }?.let(CpWire::endpoint))
            .build(),
        chunks = chunks(),
        entries = entries(),
    )

    private fun remember(change: MemberState.Builder.() -> Unit) {
        state.change()
        Files.write(member, state.build().toByteArray())
    }

    private fun writeLog(entries: List<LogEntry>, vararg options: OpenOption) =
        Files.newOutputStream(log, *options).use { out -> entries.forEach { CpWire.encodeEntry(it).writeDelimitedTo(out) } }

    private fun entries(): List<LogEntry> =
        if (!Files.exists(log)) emptyList()
        else Files.newInputStream(log).use { input ->
            generateSequence { WireLogEntry.parseDelimitedFrom(input) }.map(CpWire::decodeEntry).toList()
        }

    private fun chunks(): List<SnapshotChunk> = Files.newDirectoryStream(dir, "snapshot-*.pb").use { files ->
        files.map { CpWire.decode(WireSnapshotChunk.parseFrom(Files.readAllBytes(it))) }
    }

    private fun chunkFile(index: Long, chunk: Int) = dir.resolve("snapshot-$index-$chunk.pb")
}

/**
 * A member's restored state from the parts a store kept: nothing before the first start, else
 * the latest snapshot every chunk of which arrived, and the log entries after it.
 */
private fun restoredState(
    endpoint: RaftEndpointPersistentState?,
    members: RaftGroupMembersView?,
    term: RaftTermPersistentState?,
    chunks: List<SnapshotChunk>,
    entries: List<LogEntry>,
): RestoredRaftState? {
    if (endpoint == null || members == null) return null
    val snapshot = chunks.groupBy { it.index }.values
        .filter { parts -> parts.size == parts.first().snapshotChunkCount }
        .maxByOrNull { parts -> parts.first().index }
        ?.sortedBy { it.snapshotChunkIndex }
        ?.let { parts ->
            CpWire.models.createSnapshotEntryBuilder()
                .setIndex(parts.first().index)
                .setTerm(parts.first().term)
                .setSnapshotChunks(parts)
                .setGroupMembersView(parts.first().groupMembersView)
                .build()
        }
    val since = snapshot?.index ?: 0
    return RestoredRaftState(
        endpoint,
        members,
        term ?: CpWire.models.createRaftTermPersistentStateBuilder().setTerm(0).build(),
        snapshot,
        entries.filter { it.index > since }.sortedBy { it.index },
    )
}
