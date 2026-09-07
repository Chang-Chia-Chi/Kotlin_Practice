package dynacache.server

import dynacache.engine.BatchEngine
import dynacache.engine.Key
import dynacache.engine.PartitionContext
import dynacache.engine.Reply
import org.luaj.vm2.Globals
import org.luaj.vm2.LuaError
import org.luaj.vm2.LuaString
import org.luaj.vm2.LuaTable
import org.luaj.vm2.LuaValue
import org.luaj.vm2.Varargs
import org.luaj.vm2.lib.VarArgFunction
import org.luaj.vm2.lib.jse.JsePlatform
import java.io.ByteArrayInputStream
import java.util.concurrent.CompletableFuture

/**
 * A Lua environment a script cannot escape from and cannot ask a question whose answer differs
 * between two nodes (C11, I10). Everything that reads the OS, the filesystem, the network, the
 * clock or a random number generator is removed; what is left is arithmetic, strings, tables
 * and `redis`.
 *
 * A fresh one per `EVAL`, which is also why no global survives between calls (spec 5.7): the
 * environment a script writes into is thrown away with it.
 */
fun sandboxedGlobals(): Globals = JsePlatform.standardGlobals().apply {
    BANNED_GLOBALS.forEach { set(it, LuaValue.NIL) }
    // math survives; only its two non-deterministic members go.
    get("math").set("random", LuaValue.NIL)
    get("math").set("randomseed", LuaValue.NIL)
}

/**
 * `os` and `io` are C11's named doors. `luajava` is the widest of them all -- it reflects into
 * any JVM class -- and `require`, `package`, `dofile` and `loadfile` reach the filesystem.
 * `load` and `loadstring` compile text the sandbox never inspected, so a script could rebuild
 * what was taken away from a string; `debug` can reach a closure's upvalues and do the same.
 */
private val BANNED_GLOBALS = listOf(
    "os", "io", "luajava", "require", "package", "load", "loadstring", "dofile", "loadfile", "debug",
)

/**
 * `EVAL script numkeys key [key ...] arg [arg ...]` (spec 5.7). [args] is the client's frame
 * without the command name.
 *
 * The declared keys are the script's span, exactly as a MULTI/EXEC batch's are: the engine
 * refuses a span crossing partitions before a single line runs (C12), and inside the block the
 * script has the partition to itself, so nothing interleaves with it.
 */
// ponytail: a script that never returns holds its partition's only thread forever, and every
// key on that partition stops answering with it. Redis bounds this with a busy-script timeout
// and SCRIPT KILL; an instruction-count hook on the Globals is the repair here, and it wants
// its own ticket because killing a half-run script is a question about atomicity, not sandboxing.
fun evalScript(batch: BatchEngine, parser: CommandParser, args: List<ByteArray>): CompletableFuture<Reply> {
    if (args.size < 2) return done(Reply.Error("ERR", "wrong number of arguments for 'eval' command"))
    val declared = args[1].text().toIntOrNull()
        ?: return done(Reply.Error("ERR", "value is not an integer or out of range"))
    if (declared < 0) return done(Reply.Error("ERR", "Number of keys can't be negative"))
    if (declared > args.size - 2) {
        return done(Reply.Error("ERR", "Number of keys can't be greater than number of args"))
    }
    val keys = args.subList(2, 2 + declared).map(::Key)
    val argv = args.subList(2 + declared, args.size)
    return batch.runBatch(keys) { ctx -> runScript(ctx, parser, args[0], keys, argv) }
}

/**
 * The script itself, on the partition's thread. Every failure it can have is a reply: a script
 * that does not compile, one that raises, and one that `redis.call`s something the batch
 * refuses all leave through the same catch.
 */
private fun runScript(
    ctx: PartitionContext,
    parser: CommandParser,
    source: ByteArray,
    keys: List<Key>,
    argv: List<ByteArray>,
): Reply {
    val globals = sandboxedGlobals()
    globals.set("KEYS", luaArrayOf(keys.map { it.bytes }))
    globals.set("ARGV", luaArrayOf(argv))
    globals.set("redis", redisTable(ctx, parser))
    return try {
        // Mode "t": text only. A precompiled chunk would carry bytecode the sandbox never saw.
        val chunk = globals.load(ByteArrayInputStream(source), "@user_script", "t", globals)
        toReply(chunk.call())
    } catch (raised: LuaError) {
        // A redis.call refusal travels as a LuaError carrying the reply, so it arrives at the
        // client as the error the command itself gave rather than wrapped in a script failure.
        raised.messageObject?.errorReply() ?: Reply.Error("ERR", "Error running script: ${raised.message}")
    }
}

/**
 * The `redis` global: the script's only way to reach the cache, and only through this batch.
 * The two members differ in one thing, which is what the spec says about them: `call` raises
 * on an error reply and `pcall` hands the same error back as a table.
 */
private fun redisTable(ctx: PartitionContext, parser: CommandParser): LuaTable = LuaTable().apply {
    set("call", bridge(ctx, parser, raising = true))
    set("pcall", bridge(ctx, parser, raising = false))
}

/**
 * One `redis.call`. The arguments are the same token list a client would have sent, so
 * [CommandParser] decides what they mean and the batch's [PartitionContext] runs it -- which is
 * how a script inherits the batch's rules for free: an undeclared key and a command that spans
 * partitions are refused inside a script exactly as they are inside `MULTI`/`EXEC`.
 *
 * The parser is stateless apart from its clock, so sharing the connection's costs nothing even
 * though this runs on the partition's thread and the connection's parses run on the event loop.
 */
private fun bridge(ctx: PartitionContext, parser: CommandParser, raising: Boolean) = object : VarArgFunction() {

    override fun invoke(args: Varargs): Varargs {
        if (args.narg() < 1) return refuse(ARITY, raising)
        val tokens = (1..args.narg()).map { at ->
            args.arg(at).redisArgument() ?: return refuse(ARGUMENT_TYPE, raising)
        }
        val reply = when (val parsed = parser.parse(tokens)) {
            is Parsed.Ok -> ctx.execute(parsed.command)
            is Parsed.Failed -> parsed.error
        }
        return if (reply is Reply.Error) refuse(reply, raising) else reply.toLua()
    }
}

/** An error reply on its way out of `redis.call` (raised) or `redis.pcall` (returned). */
private fun refuse(error: Reply.Error, raising: Boolean): Varargs {
    val table = error.toLua()
    if (raising) throw LuaError(table)
    return table
}

/**
 * One argument of `redis.call` as the bytes a client would have sent, or null when it is
 * neither. Redis converts a Lua number to its string form here, so `redis.call('SET', k, 1)`
 * stores the two bytes `1` and not a number the wire has no room for.
 */
private fun LuaValue.redisArgument(): ByteArray? = when (type()) {
    LuaValue.TSTRING -> bytes()
    LuaValue.TNUMBER -> tojstring().toByteArray(Charsets.ISO_8859_1)
    else -> null
}

private val ARITY = Reply.Error("ERR", "Please specify at least one argument for redis.call()")
private val ARGUMENT_TYPE = Reply.Error("ERR", "Lua redis lib command arguments must be strings or integers")

// ---- Redis to Lua and back (spec 5.7) ----------------------------------------------------------

/**
 * What a command answered, as the script sees it: an integer is a number, a bulk string is a
 * string and a nil bulk is `false`, since Lua has no nil inside a table. `+OK` and an error are
 * the two replies with no Lua shape of their own, so they arrive as the tables that name them.
 */
private fun Reply.toLua(): LuaValue = when (this) {
    is Reply.Integer -> LuaValue.valueOf(value.toDouble())
    is Reply.Bulk -> bytes?.toLua() ?: LuaValue.FALSE
    is Reply.Array -> LuaTable().apply { items.forEachIndexed { at, item -> set(at + 1, item.toLua()) } }
    is Reply.Simple -> LuaTable().apply { set("ok", text) }
    is Reply.Error -> LuaTable().apply { set("err", "$kind $message") }
}

/** A Lua array table, 1-indexed, over binary-safe strings. */
private fun luaArrayOf(items: List<ByteArray>): LuaTable =
    LuaTable().apply { items.forEachIndexed { at, bytes -> set(at + 1, bytes.toLua()) } }

/**
 * What the script returned, as a reply. A number truncates to an integer, a string is a bulk
 * string, `true` is `:1` and `false` and `nil` are both the nil bulk. A table is an array of
 * whatever its 1..n run holds, unless it carries `err` or `ok`, which name the two replies an
 * array cannot express.
 */
private fun toReply(value: LuaValue): Reply = when (value.type()) {
    LuaValue.TNUMBER -> Reply.Integer(value.tolong())
    LuaValue.TSTRING -> Reply.Bulk(value.bytes())
    LuaValue.TBOOLEAN -> if (value.toboolean()) Reply.Integer(1) else NIL_BULK
    LuaValue.TTABLE -> value.errorReply()
        ?: value.get("ok").let { if (it.isnil()) arrayReply(value) else Reply.Simple(it.text()) }
    // nil, and anything else a script can hand back that Redis has no reply for.
    else -> NIL_BULK
}

/** The 1..n run of a table, stopping at the first hole, exactly as Redis reads a Lua array. */
private fun arrayReply(table: LuaValue): Reply.Array {
    val items = mutableListOf<Reply>()
    var at = 1
    while (true) {
        val item = table.get(at++)
        if (item.isnil()) return Reply.Array(items)
        items += toReply(item)
    }
}

/**
 * The error a `{err = ...}` table names, or null when this is not one. The kind is the leading
 * token, which is where a Redis client looks; a one-word error has no kind of its own and gets
 * the generic one, so the reply always has both halves the wire format needs.
 */
private fun LuaValue.errorReply(): Reply.Error? {
    if (!istable()) return null
    val message = get("err")
    if (message.isnil()) return null
    val text = message.text()
    val space = text.indexOf(' ')
    return if (space < 0) Reply.Error("ERR", text) else Reply.Error(text.take(space), text.drop(space + 1))
}

/**
 * Bytes in and out of Lua without a transcoding step. ISO-8859-1 is the byte-for-char mapping
 * this would otherwise be spelled with; going through the byte array directly says the same
 * thing and keeps a key's length inside Lua equal to its length on the wire.
 */
private fun ByteArray.toLua(): LuaValue = LuaString.valueUsing(this)

private fun LuaValue.bytes(): ByteArray = checkstring().let { it.m_bytes.copyOfRange(it.m_offset, it.m_offset + it.m_length) }

private fun LuaValue.text(): String = bytes().text()

private fun ByteArray.text(): String = toString(Charsets.ISO_8859_1)

private val NIL_BULK = Reply.Bulk(null)
