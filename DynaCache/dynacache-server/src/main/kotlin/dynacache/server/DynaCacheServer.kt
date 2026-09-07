package dynacache.server

import dynacache.cluster.HostPort
import dynacache.cluster.NodeId
import dynacache.cp.CpConfig
import dynacache.cp.CpEngine
import dynacache.cp.CpGrpcServer
import dynacache.cp.FileRaftStore
import dynacache.cp.ForwardingCpEngine
import dynacache.cp.GrpcRaftTransport
import dynacache.cp.InMemoryRaftStore
import dynacache.cp.RaftRuntime
import dynacache.engine.ApEngine
import dynacache.engine.BatchEngine
import dynacache.engine.Command
import dynacache.engine.CommandEngine
import dynacache.engine.CpNamespace
import dynacache.engine.CrossPartitionBatch
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import dynacache.engine.persist.FsyncPolicy
import dynacache.engine.persist.SnapshotEngine
import io.microraft.RaftConfig
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.util.concurrent.EventExecutor
import java.net.InetSocketAddress
import java.nio.file.Path
import java.time.Clock
import java.util.ArrayDeque
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The RESP2 socket in front of the engine. It parses, submits and encodes; it never serializes
 * anything itself and never splits a command, because the engine's partition executors already
 * do both (plan 2.3).
 *
 * [tick] is what the server's scheduler runs once per the engine's `tickMillis`, so the timer
 * wheel is advanced by the one thread the plan gives that job. It defaults to the engine's own
 * tick and is a parameter so a test can watch the schedule without watching the clock.
 *
 * [cp] is this node's CP engine, or null on a node with no CP subsystem; the [CommandDispatcher]
 * in front of both is what a connection actually submits to. [clock] is what "now" means to the
 * parser and to the dispatcher's one conversion, `EXPIRE`'s deadline into the CP verb's span.
 *
 * [ap] is the dispatcher's AP side. It defaults to [engine], which is the single node; a
 * [ClusterNode] passes its router, so the same pipeline serves a cluster without knowing it
 * (T24). [engine] stays the local one either way: it is what the scheduler ticks.
 *
 * [batch] is what `MULTI`/`EXEC` and `EVAL` run on: a capability of the AP engine rather than
 * part of the engine seam (T73), so nothing between the socket and a partition has to implement
 * a batch it cannot run. A [ClusterNode] passes itself, which refuses a batch whose keys it does
 * not coordinate before running it on its own engine.
 *
 * [fsync] is the log's policy, which the server needs for one reason: a policy whose durability
 * deadline is shorter than the engine's tick gets a second schedule of its own, since a log
 * forced once per tick is a log forced once a second (T78).
 */
class DynaCacheServer(
    private val port: Int,
    private val engine: ApEngine,
    cp: CommandEngine? = null,
    ap: CommandEngine = engine,
    private val batch: BatchEngine = engine,
    private val clock: Clock = Clock.systemUTC(),
    private val fsync: FsyncPolicy = FsyncPolicy.NEVER,
    private val tick: () -> Unit = { engine.tick() },
) : AutoCloseable {

    /** Where every connection submits: the AP engine, the CP engine, and CP spec 9.5 between. */
    private val dispatcher = CommandDispatcher(ap, cp)

    private val acceptors = NioEventLoopGroup(1)
    private val workers = NioEventLoopGroup()
    private val scheduler = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "dynacache-tick").apply { isDaemon = true }
    }
    private var channel: Channel? = null

    /** The port the server actually listens on: the one asked for, or the one 0 was given. */
    val boundPort: Int
        get() = (checkNotNull(channel) { "the server is not started" }.localAddress() as InetSocketAddress).port

    fun start() {
        check(channel == null) { "the server is already started" }
        channel = ServerBootstrap()
            .group(acceptors, workers)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(RespFrameDecoder(), CommandHandler(dispatcher, batch, clock))
                }
            })
            .bind(port).sync().channel()
        scheduler.scheduleWithFixedDelay(
            { runCatching(tick) }, // a throwing tick must not kill the schedule
            engine.tickMillis,
            engine.tickMillis,
            TimeUnit.MILLISECONDS,
        )
        // A durability deadline shorter than the engine's tick needs its own cadence, on this same
        // one thread: a deadline force still cannot interleave with a checkpoint's rotate, which
        // the tick above runs (T78).
        fsync.deadline?.toMillis()?.takeIf { it in 1 until engine.tickMillis }?.let { deadline ->
            scheduler.scheduleWithFixedDelay(
                { runCatching { engine.wal?.tick() } },
                deadline,
                deadline,
                TimeUnit.MILLISECONDS,
            )
        }
    }

    /** Stops accepting, stops ticking and releases the event loops. The engine is not ours to close. */
    override fun close() {
        scheduler.shutdownNow()
        channel?.close()?.sync()
        channel = null
        acceptors.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
        workers.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
    }
}

/**
 * Bytes to token lists. [RespDecoder] does the framing and its own buffering, so this handler
 * only hands it what arrived and drains whatever whole commands that produced.
 */
private class RespFrameDecoder : ByteToMessageDecoder() {

    private val decoder = RespDecoder()

    override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
        if (!input.isReadable) return
        val bytes = ByteArray(input.readableBytes())
        input.readBytes(bytes)
        decoder.feed(bytes)
        while (true) out.add(decoder.nextCommand() ?: break)
    }
}

/**
 * One connection's commands. Every reply is written in request order even though the futures
 * behind them finish in whatever order their partitions get to them: a command's future joins a
 * queue when the command arrives, and the queue is drained from the front only while its head is
 * done. The queue is touched from the channel's event loop and nowhere else, so it needs no lock.
 *
 * MULTI, EXEC and DISCARD live here rather than in [CommandParser] because they are not commands
 * at all: they are this connection's state, and they say what the parser's answers are for. The
 * handler is already per-connection and already single-threaded on the event loop, so the buffer
 * needs no lock either.
 */
private class CommandHandler(
    private val engine: CommandEngine,
    private val batch: BatchEngine,
    clock: Clock,
) : ChannelInboundHandlerAdapter() {

    // ponytail: the queue is unbounded, so a client that pipelines without reading grows it
    // until the heap says no; Redis caps its own output buffer. A limit that closes the
    // connection past N pending replies is the repair when a real client misbehaves.
    private val parser = CommandParser(clock)
    private val pending = ArrayDeque<CompletableFuture<Reply>>()

    /**
     * This connection's CP session (CP spec 4), as the reply that created it: the first
     * session-bearing CP verb makes one, every later verb on this connection uses it, and it is
     * forgotten once the group no longer has it. Only the event loop reads or writes it, so it
     * needs no lock; a reply that arrives on an engine's thread hands the forgetting back to
     * [loop] first.
     */
    private var session: CompletableFuture<Reply>? = null

    /** This connection's event loop: the one thread that may touch [session]. */
    private var loop: EventExecutor? = null

    /** The commands buffered since MULTI, or null when this connection is not in one. */
    private var buffered: MutableList<Command>? = null

    /** Whether a frame that failed to parse arrived while buffering; EXEC refuses the lot. */
    private var spoiled = false

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        loop = ctx.executor()
    }

    override fun channelRead(ctx: ChannelHandlerContext, message: Any) {
        @Suppress("UNCHECKED_CAST")
        val tokens = message as List<ByteArray>
        val answer = answer(tokens)
        pending.addLast(answer)
        answer.whenComplete { _, _ -> ctx.channel().eventLoop().execute { drain(ctx) } }
    }

    private fun answer(tokens: List<ByteArray>): CompletableFuture<Reply> {
        val name = tokens[0].toString(Charsets.ISO_8859_1).lowercase()
        if (name in BATCH) {
            if (tokens.size != 1) return done(Reply.Error("ERR", "wrong number of arguments for '$name' command"))
            return when (name) {
                "multi" -> done(multi())
                "discard" -> done(discard())
                else -> exec()
            }
        }
        // EVAL is connection-level for the same reason MULTI is: it is not one command the
        // engine runs but a batch of them, so it never becomes a Command and never queues.
        if (name == "eval") {
            if (buffered != null) {
                spoiled = true
                return done(Reply.Error("ERR", "EVAL inside MULTI is not supported"))
            }
            return evalScript(batch, parser, tokens.drop(1))
        }
        return when (val parsed = parser.parse(tokens)) {
            is Parsed.Ok -> buffered?.let { it += parsed.command; done(QUEUED) } ?: submit(parsed.command)
            // Redis answers the error the moment the bad frame arrives and refuses the whole
            // batch later, so the client learns which command was wrong.
            is Parsed.Failed -> {
                if (buffered != null) spoiled = true
                done(parsed.error)
            }
        }
    }

    /**
     * The command on its way to the dispatcher, with this connection's session put in where a CP
     * verb takes one. `CP.SESSION.CREATE` names the connection's session rather than making a
     * second, so a client that asks explicitly and one that never asks hold the same one.
     */
    private fun submit(command: Command): CompletableFuture<Reply> = when {
        command is Command.Cp.SessionCreate -> session()
        command is Command.Cp.SessionClose -> closeSession(command)
        // CP.SESSION.HEARTBEAT names its session on the wire (CP spec 6.6); every other
        // session-bearing verb takes the connection's.
        command is Command.Cp.Sessioned && command !is Command.Cp.Session -> onSession(command as Command.Cp)
        else -> engine.submit(command)
    }

    /**
     * This connection's session, created on the first verb that needs one. A creation that failed
     * (no CP engine here, or no leader yet) is not remembered, so the next verb tries again.
     */
    private fun session(): CompletableFuture<Reply> {
        val existing = session
        val usable = existing != null && !existing.isCompletedExceptionally &&
            (!existing.isDone || existing.getNow(null) is Reply.Integer)
        if (usable) return checkNotNull(existing)
        return engine.submit(Command.Cp.SessionCreate()).also { session = it }
    }

    /**
     * `CP.SESSION.CLOSE sid` (CP spec 6.6). The verb names its session on the wire, so it may end
     * this connection's session or another connection's; when it ends this one, the cache goes
     * with it and the next verb creates a fresh session rather than naming the closed one.
     */
    private fun closeSession(command: Command.Cp.SessionClose): CompletableFuture<Reply> {
        val mine = session?.takeIf {
            !it.isCompletedExceptionally && it.getNow(null) == Reply.Integer(command.session)
        }
        return engine.submit(command).whenComplete { reply, _ ->
            // +OK is the close; -NOSESSION is a session that had already lapsed. Any other answer
            // (a leader that moved, say) leaves the session where it was, so the cache stands.
            if (mine != null && (reply is Reply.Simple || reply.isNoSession())) forgetSession(mine)
        }
    }

    /**
     * [command] with this connection's session put in. A `-NOSESSION` means the session lapsed
     * between its creation and this verb (CP spec 4), so the cache is dropped and the next verb
     * starts a new session; the error still reaches the client, once.
     */
    private fun onSession(command: Command.Cp): CompletableFuture<Reply> {
        val mine = session()
        return mine.thenCompose { created ->
            if (created is Reply.Integer) engine.submit(command.withSession(created.value)) else done(created)
        }.whenComplete { reply, _ -> if (reply.isNoSession()) forgetSession(mine) }
    }

    /**
     * Forgets this connection's session once [gone] has ended, on the event loop that owns it.
     * [gone] is the cached future itself, so a session created in the meantime is left alone.
     */
    private fun forgetSession(gone: CompletableFuture<Reply>) {
        loop?.execute { if (session === gone) session = null }
    }

    private fun multi(): Reply {
        if (buffered != null) return Reply.Error("ERR", "MULTI calls can not be nested")
        buffered = mutableListOf()
        return OK
    }

    private fun discard(): Reply {
        if (buffered == null) return Reply.Error("ERR", "DISCARD without MULTI")
        forget()
        return OK
    }

    /**
     * The buffer, run in order as one batch. The keys every buffered command names are declared
     * together, so the engine can refuse a span before anything runs (C12); that refusal reaches
     * here as the batch future's failure, since the batch's own answer is the reply array.
     * Errors inside the array stand where they happened and undo nothing (I11).
     */
    private fun exec(): CompletableFuture<Reply> {
        val commands = buffered ?: return done(Reply.Error("ERR", "EXEC without MULTI"))
        val refused = spoiled
        forget()
        if (refused) return done(Reply.Error("EXECABORT", "Transaction discarded because of previous errors."))
        return batch.runBatch(commands.flatMap(CpNamespace::keysOf).distinct()) { ctx ->
            Reply.Array(commands.map(ctx::execute))
        }
    }

    private fun forget() {
        buffered = null
        spoiled = false
    }

    override fun channelReadComplete(ctx: ChannelHandlerContext) = drain(ctx)

    private fun drain(ctx: ChannelHandlerContext) {
        while (pending.isNotEmpty() && pending.first().isDone) {
            ctx.write(Unpooled.wrappedBuffer(encodeReply(pending.removeFirst().replyNow())))
        }
        ctx.flush()
    }

    /**
     * Redis's own answer to a malformed frame: say so once and hang up. The decoder has already
     * discarded the stream, so nothing after the bad bytes could be trusted anyway.
     */
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        // Netty wraps whatever a decoder throws in a DecoderException, so look one layer down.
        val protocol = cause as? RespProtocolException ?: cause.cause as? RespProtocolException
        if (protocol == null) {
            ctx.close()
        } else {
            val error = Reply.Error("ERR", "Protocol error: ${protocol.message}")
            ctx.writeAndFlush(Unpooled.wrappedBuffer(encodeReply(error))).addListener(ChannelFutureListener.CLOSE)
        }
    }
}

/**
 * [command] with the connection's session in it. The lock and semaphore verbs are the ones that
 * hold a resource on a session's behalf (CP spec 4), and none of them names it on the wire.
 */
private fun Command.Cp.withSession(id: Long): Command.Cp = when (this) {
    is Command.Cp.LockTry -> copy(session = id)
    is Command.Cp.LockUnlock -> copy(session = id)
    is Command.Cp.LockRenew -> copy(session = id)
    is Command.Cp.SemAcquire -> copy(session = id)
    is Command.Cp.SemRelease -> copy(session = id)
    is Command.Cp.SemDrain -> copy(session = id)
    else -> error("$this does not take the connection's session")
}

/** The CP state machine's answer for a session that lapsed or was never created (CP spec 6.8). */
private fun Reply?.isNoSession(): Boolean = this is Reply.Error && kind == "NOSESSION"

private val OK = Reply.Simple("OK")
private val QUEUED = Reply.Simple("QUEUED")
private val BATCH = setOf("multi", "exec", "discard")

internal fun done(reply: Reply): CompletableFuture<Reply> = CompletableFuture.completedFuture(reply)

/**
 * One batch, as the connection handler asks for it: both callers -- `EXEC` and `EVAL` -- go
 * through here. A `cp:` key among the declared ones is refused before anything runs, the way a
 * cross-partition span is (C12, C16): the CP engine has no partition the batch could share, so
 * the two namespaces can never be one uninterrupted run.
 */
internal fun BatchEngine.runBatch(
    keys: List<Key>,
    block: (PartitionContext) -> Reply,
): CompletableFuture<Reply> =
    if (keys.any(CpNamespace::owns)) done(Reply.Error("ERR", "a batch cannot name a cp: key"))
    else atomically(keys, block).orBatchError()

/**
 * A batch's answer, with C12's refusal turned back into the reply it carries: `atomically`
 * answers whatever its block returned, so a span that was never allowed to run can only arrive
 * as the future's failure.
 */
private fun CompletableFuture<Reply>.orBatchError(): CompletableFuture<Reply> = exceptionally { failure ->
    val span = failure as? CrossPartitionBatch ?: failure.cause as? CrossPartitionBatch
    span?.error ?: Reply.Error("ERR", failure.cause?.message ?: failure.message ?: "internal error")
}

/** The reply of a future already known to be done; a failed one answers rather than throwing. */
private fun CompletableFuture<Reply>.replyNow(): Reply =
    try {
        getNow(null)
    } catch (failed: Exception) {
        Reply.Error("ERR", failed.cause?.message ?: failed.message ?: "internal error")
    }

/**
 * `dynacache [port] [partitions] [dir] [ALWAYS|EVERY_SECOND|NEVER|GROUP_COMMIT] [cp-self]
 * [cp-members]`, defaulting to Redis's own port, sixteen partitions and `EVERY_SECOND`. With a
 * [dir], the last snapshot there and the log after it are restored before the port opens, a
 * snapshot is saved on the engine's default interval from the tick thread (which is also the
 * log's checkpoint), the log is forced by the same thread once a second (or on the policy's own
 * deadline under `GROUP_COMMIT`), and one more snapshot is saved at shutdown (spec 2.8). With a CP group named, this node either holds the replicated log or forwards to
 * whoever leads it; without one it has no CP engine and every `cp:` key answers `-NOTCP`. The
 * engine outlives nothing here: the shutdown hook closes the socket, then the log, then the
 * engine.
 *
 * The AP cluster is named by flags rather than by more positional arguments, since it has four
 * knobs of its own: `--peers=id=host:port,...` with `--node=<id>` starts one node of a cluster
 * (T24, see [clusterMain]), `--grpc=<port>` and `--quorum=n/w/r` are the other two. Without
 * `--peers` this is the single node it always was, and every positional argument above means
 * the same in both modes.
 */
fun main(args: Array<String>) {
    val flags = args.filter { it.startsWith("--") }
        .associate { it.removePrefix("--").substringBefore('=') to it.substringAfter('=', "") }
    val positional = args.filterNot { it.startsWith("--") }
    val port = positional.getOrNull(0)?.toInt() ?: 6379
    val partitionCount = positional.getOrNull(1)?.toInt() ?: 16
    val dir = positional.getOrNull(2)?.let(Path::of)
    val fsync = positional.getOrNull(3)?.let(FsyncPolicy::valueOf) ?: FsyncPolicy.EVERY_SECOND
    if ("peers" in flags) return clusterMain(flags, port, partitionCount, dir, fsync, positional.getOrNull(5))
    val clock = Clock.systemUTC()
    val engine = ApEngine(partitionCount, clock)
    val snapshots = dir?.let { SnapshotEngine(engine, it, clock, fsync = fsync) }
    snapshots?.restore()
    val cp = cpNodeFromArgs(positional.getOrNull(4), positional.getOrNull(5), dir, clock)
    val server = DynaCacheServer(port, engine, cp?.engine, clock = clock, fsync = fsync) {
        engine.tick()
        engine.wal?.tick()
        cp?.runtime?.tick()
        snapshots?.maybeSave(clock.instant())
    }
    Runtime.getRuntime().addShutdownHook(
        Thread {
            server.close()
            cp?.close()
            snapshots?.close()
            engine.close()
        },
    )
    cp?.runtime?.start()
    server.start()
    println("DynaCache listening on ${server.boundPort} with $partitionCount partitions")
}

/**
 * This node's part in the CP subsystem: the [engine] a connection submits CP work to, and the
 * [runtime] when this node holds the replicated log itself rather than forwarding to it.
 */
internal class CpNode(
    val engine: CommandEngine,
    val runtime: RaftRuntime?,
    private val grpc: CpGrpcServer?,
) : AutoCloseable {

    /** The port this member's `CpService` and `RaftService` listen on; 0 on an AP-only node. */
    val cpPort: Int get() = grpc?.boundPort ?: 0

    /** The gRPC presence goes first, then the engine, which is what closes the Raft node. */
    override fun close() {
        grpc?.close()
        engine.close()
    }
}

/**
 * This node's CP subsystem, built in the one order a member has to be built in: the store (so it
 * reads back what it remembered), then the runtime on top of it, then the engine, then the gRPC
 * presence the other members reach it through.
 *
 * [self] outside [members] is an AP-only node: it holds no Raft node and forwards every CP command
 * to whoever leads (CP spec 2.2, 2.4). [addresses] is read at send time, so members on ephemeral
 * ports can be built first and told each other's ports afterwards. [storeDir] is null for a member
 * that keeps its log in memory, which is a member that cannot come back from a restart.
 */
internal fun cpNode(
    self: NodeId,
    members: List<NodeId>,
    addresses: Map<NodeId, HostPort>,
    port: Int,
    storeDir: Path?,
    clock: Clock,
    raft: RaftConfig = RaftConfig.DEFAULT_RAFT_CONFIG,
): CpNode {
    if (self !in members) return CpNode(ForwardingCpEngine(members, addresses), null, null)
    val store = storeDir?.let { FileRaftStore(it) } ?: InMemoryRaftStore()
    val config = CpConfig(self, members, raft = raft, clock = clock)
    val runtime = RaftRuntime(config, GrpcRaftTransport(self, addresses), store)
    val engine = CpEngine(runtime)
    return CpNode(engine, runtime, CpGrpcServer(runtime, engine, port))
}

/**
 * The CP subsystem the command line named, or null when it named none. [members] is `id@host:port`
 * entries in the same order on every node, since CP membership is fixed at startup (CP spec 2.2);
 * [self] says which entry this node is, and its own entry names the port it binds.
 */
private fun cpNodeFromArgs(self: String?, members: String?, dir: Path?, clock: Clock): CpNode? {
    if (self == null || members.isNullOrBlank()) return null
    val addresses = cpAddressBook(members)
    val node = NodeId(self)
    return cpNode(node, addresses.keys.toList(), addresses, addresses[node]?.port ?: 0, dir?.resolve(CP_DIR), clock)
}

/** `id@host:port,...` as the address book every CP member is given, empty when nothing was named. */
internal fun cpAddressBook(members: String?): Map<NodeId, HostPort> =
    members?.split(",")?.filter(String::isNotBlank)?.associate { entry ->
        val (id, address) = entry.split('@', limit = 2)
        NodeId(id) to HostPort(address.substringBeforeLast(':'), address.substringAfterLast(':').toInt())
    }.orEmpty()

/** Where a CP member's own Raft store lives, under whatever data directory the node was given. */
internal const val CP_DIR = "cp"
