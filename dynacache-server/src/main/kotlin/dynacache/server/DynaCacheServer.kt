package dynacache.server

import dynacache.engine.ApEngine
import dynacache.engine.Command
import dynacache.engine.CrossPartitionBatch
import dynacache.engine.Key
import dynacache.engine.Reply
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
import java.net.InetSocketAddress
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
 */
class DynaCacheServer(
    private val port: Int,
    private val engine: ApEngine,
    private val tick: () -> Unit = { engine.tick() },
) : AutoCloseable {

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
                    ch.pipeline().addLast(RespFrameDecoder(), CommandHandler(engine))
                }
            })
            .bind(port).sync().channel()
        scheduler.scheduleWithFixedDelay(
            { runCatching(tick) }, // a throwing tick must not kill the schedule
            engine.tickMillis,
            engine.tickMillis,
            TimeUnit.MILLISECONDS,
        )
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
private class CommandHandler(private val engine: ApEngine) : ChannelInboundHandlerAdapter() {

    // ponytail: the queue is unbounded, so a client that pipelines without reading grows it
    // until the heap says no; Redis caps its own output buffer. A limit that closes the
    // connection past N pending replies is the repair when a real client misbehaves.
    private val parser = CommandParser()
    private val pending = ArrayDeque<CompletableFuture<Reply>>()

    /** The commands buffered since MULTI, or null when this connection is not in one. */
    private var buffered: MutableList<Command>? = null

    /** Whether a frame that failed to parse arrived while buffering; EXEC refuses the lot. */
    private var spoiled = false

    override fun channelRead(ctx: ChannelHandlerContext, message: Any) {
        @Suppress("UNCHECKED_CAST")
        val tokens = message as List<ByteArray>
        val answer = answer(tokens)
        pending.addLast(answer)
        answer.whenComplete { _, _ -> ctx.channel().eventLoop().execute { drain(ctx) } }
    }

    private fun answer(tokens: List<ByteArray>): CompletableFuture<Reply> {
        val name = tokens[0].toString(Charsets.ISO_8859_1).lowercase()
        if (name in TRANSACTION) {
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
            return evalScript(engine, parser, tokens.drop(1))
        }
        return when (val parsed = parser.parse(tokens)) {
            is Parsed.Ok -> buffered?.let { it += parsed.command; done(QUEUED) } ?: engine.submit(parsed.command)
            // Redis answers the error the moment the bad frame arrives and refuses the whole
            // transaction later, so the client learns which command was wrong.
            is Parsed.Failed -> {
                if (buffered != null) spoiled = true
                done(parsed.error)
            }
        }
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
        return engine
            .atomically<Reply>(commands.flatMap(::declaredKeys).distinct()) { ctx ->
                Reply.Array(commands.map(ctx::execute))
            }
            .orBatchError()
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

private val OK = Reply.Simple("OK")
private val QUEUED = Reply.Simple("QUEUED")
private val TRANSACTION = setOf("multi", "exec", "discard")

internal fun done(reply: Reply): CompletableFuture<Reply> = CompletableFuture.completedFuture(reply)

/**
 * A batch's answer, with C12's refusal turned back into the reply it carries. Both batches --
 * `EXEC` and `EVAL` -- end this way: `atomically` answers whatever its block returned, so a span
 * that was never allowed to run can only arrive as the future's failure.
 */
internal fun CompletableFuture<Reply>.orBatchError(): CompletableFuture<Reply> = exceptionally { failure ->
    val span = failure as? CrossPartitionBatch ?: failure.cause as? CrossPartitionBatch
    span?.error ?: Reply.Error("ERR", failure.cause?.message ?: failure.message ?: "internal error")
}

/** The keys a buffered command names, so EXEC declares the whole batch's span in one list. */
private fun declaredKeys(command: Command): List<Key> = when (command) {
    is Command.Keyed -> listOf(command.key)
    is Command.Fanned -> command.keys
    else -> emptyList()
}

/** The reply of a future already known to be done; a failed one answers rather than throwing. */
private fun CompletableFuture<Reply>.replyNow(): Reply =
    try {
        getNow(null)
    } catch (failed: Exception) {
        Reply.Error("ERR", failed.cause?.message ?: failed.message ?: "internal error")
    }

/**
 * `dynacache [port] [partitions]`, defaulting to Redis's own port and sixteen partitions.
 * The engine outlives nothing here: the shutdown hook closes both in order.
 */
fun main(args: Array<String>) {
    val port = args.getOrNull(0)?.toInt() ?: 6379
    val partitionCount = args.getOrNull(1)?.toInt() ?: 16
    val engine = ApEngine(partitionCount, Clock.systemUTC())
    val server = DynaCacheServer(port, engine)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            server.close()
            engine.close()
        },
    )
    server.start()
    println("DynaCache listening on ${server.boundPort} with $partitionCount partitions")
}
