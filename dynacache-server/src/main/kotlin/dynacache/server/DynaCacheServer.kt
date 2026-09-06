package dynacache.server

import dynacache.engine.ApEngine
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
 */
private class CommandHandler(private val engine: ApEngine) : ChannelInboundHandlerAdapter() {

    // ponytail: the queue is unbounded, so a client that pipelines without reading grows it
    // until the heap says no; Redis caps its own output buffer. A limit that closes the
    // connection past N pending replies is the repair when a real client misbehaves.
    private val parser = CommandParser()
    private val pending = ArrayDeque<CompletableFuture<Reply>>()

    override fun channelRead(ctx: ChannelHandlerContext, message: Any) {
        @Suppress("UNCHECKED_CAST")
        val tokens = message as List<ByteArray>
        val answer = when (val parsed = parser.parse(tokens)) {
            is Parsed.Ok -> engine.submit(parsed.command)
            is Parsed.Failed -> CompletableFuture.completedFuture<Reply>(parsed.error)
        }
        pending.addLast(answer)
        answer.whenComplete { _, _ -> ctx.channel().eventLoop().execute { drain(ctx) } }
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
