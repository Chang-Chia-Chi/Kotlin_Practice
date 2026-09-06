package dynacache.engine

// The skip list's entry, aliased because [Partition.Entry] is the store's own.
import dynacache.engine.ds.Entry as Scored
import dynacache.engine.ds.HashTable
import dynacache.engine.ds.SkipList
import dynacache.engine.ds.TimerWheel
import dynacache.engine.persist.RdbEntry
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors

/**
 * One partition: a single-thread executor and the store only that thread touches (C1 by
 * construction, ADR 0001). Everything below [submit] runs on the partition executor.
 */
internal class Partition(
    id: PartitionId,
    private val clock: Clock,
    private val random: Random,
    private val tickMillis: Long,
    /**
     * This partition's even share of the node's memory threshold. `Long.MAX_VALUE` is a node with
     * no threshold: a share nothing can cross is a partition that never evicts, with no second
     * branch to say so.
     */
    private val maxBytes: Long,
    /** Which key this partition gives up when it is over [maxBytes]; see [coldest]. */
    private val policy: EvictionPolicy,
    /**
     * The engine's write-ahead log hook: called with every command's reply, it answers the
     * durability of the entry it appended, or null when the command changed nothing (C14).
     */
    private val log: (Command, Reply, Instant) -> CompletableFuture<*>?,
) {

    private class Entry(val value: Value, val expiresAt: Instant?) {
        /** The String bytes, safe to read once the kind check in [execute] has passed. */
        val str: ByteArray get() = (value as Value.Str).bytes

        /**
         * When a command last read or wrote this entry, from that command's own reading of the
         * clock. The sampling policy of spec 2.7 evicts the oldest of the keys it draws.
         */
        var lastAccess: Instant = Instant.EPOCH

        /** What this entry last contributed to [usedBytes]; see [account]. */
        var accounted: Long = 0

        /** The one expiry rule: a key is readable through its deadline and gone after it. */
        fun expired(now: Instant): Boolean = expiresAt != null && now.isAfter(expiresAt)
    }

    private val store = HashTable<Key, Entry>()

    /**
     * What the store holds, by the estimate of [Value.approximateBytes]: the sum over the live
     * entries of the key's own bytes, [ENTRY_BYTES] for the entry itself and the value's payload.
     * Kept as a running total rather than recounted, so `INFO` costs nothing and [execute] can ask
     * after every command whether the partition is over its share.
     */
    private var usedBytes = 0L

    /**
     * The W-TinyLFU bookkeeping, or null under [EvictionPolicy.LRU], which needs none: sampling
     * reads [Entry.lastAccess] off keys it draws and remembers nothing between evictions. A null
     * here is the whole cost of the policy this partition did not choose.
     */
    private val tinyLfu: WindowTinyLfu? =
        if (policy == EvictionPolicy.W_TINYLFU) {
            WindowTinyLfu(maxBytes, random.nextLong()) { key -> store.get(key)?.accounted ?: 0 }
        } else {
            null
        }

    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "partition-${id.index}").apply { isDaemon = true }
    }

    /**
     * The active half of spec 5.4's belt and braces: the partition's own wheel deletes a key as
     * its deadline falls due, so nothing has to be read to be removed. Only ever touched from
     * this executor -- by [write] and [drop] on the command path, and by [tick] -- so its
     * callback needs no synchronisation of its own.
     *
     * Null until the first TTL, and born from that command's own reading of the clock: the
     * engine reads the clock once per command and never outside one, so a wheel cannot be built
     * in the constructor. A partition that has never held a TTL has no wheel to advance.
     */
    private var wheel: TimerWheel<Key>? = null

    private fun wheel(now: Instant): TimerWheel<Key> =
        wheel ?: TimerWheel<Key>(now, tickMillis) { key -> forget(key) }.also { wheel = it }

    /**
     * What the current task's appends still owe the disk. Reset by [task], grown by [execute];
     * only the executor touches it, so a task's reply waits for exactly its own entries.
     */
    private var durable: CompletableFuture<*> = DONE

    /**
     * One task on this partition's thread, whose future completes with [work]'s answer only once
     * every entry the task logged is durable: the reply-after-durable rule of C14.
     */
    private fun <R> task(work: () -> R): CompletableFuture<R> =
        CompletableFuture.supplyAsync({
            durable = DONE
            val result = work()
            durable.thenApply { result }
        }, executor).thenCompose { it }

    fun submit(command: Command): CompletableFuture<Reply> = task { execute(command) }

    /**
     * Advances the wheel to the clock's current reading. The server owns the scheduler that
     * calls this once per tick; the engine holds no thread of its own beyond the executors.
     */
    fun tick(): CompletableFuture<Void> =
        CompletableFuture.runAsync({ wheel?.advanceTo(clock.instant()) }, executor)

    /**
     * Runs [work] as one task on this partition's thread: the batch of CONTEXT.md, several
     * commands with nothing interleaved. C1 needs nothing more -- the executor is the one
     * thread, so a task that runs many commands already has the exclusion a batch asks for.
     */
    fun <R> inOneTask(work: () -> R): CompletableFuture<R> = task(work)

    /** One partition's share of a fanned-out command: one task, so those keys see no interleaving. */
    fun submitAll(commands: List<Command>): CompletableFuture<List<Reply>> = task { commands.map(::execute) }

    fun close() = executor.shutdown()

    /**
     * A point-in-time view of this partition's live keys at [now], taken as one task on the
     * executor so it sits between two commands or batches and never inside one (C9). The
     * entries are copied, not referenced: a Hash, List or Sorted Set is mutated in place by the
     * next command, and the writer serializes off this thread. A String's bytes are shared,
     * since no command mutates that array. The DVV is empty until replication stamps one (T22).
     * The task waits at [cut] first, so every partition's view is taken at the same moment.
     */
    fun snapshotView(now: Instant, cut: CyclicBarrier): CompletableFuture<List<RdbEntry>> =
        CompletableFuture.supplyAsync({
            cut.await()
            store.entries().filterNot { it.value.expired(now) }
                .map { RdbEntry(it.key, frozen(it.value.value), it.value.expiresAt, EMPTY) }
                .toList()
        }, executor)

    /**
     * Frozen copies of the live keys [holds] selects, as one task on the executor: what a
     * replica hashes and ships for one ring range (T28). A walk of the whole store, since keys
     * are not indexed by ring position; a range is a small slice of it.
     */
    fun view(holds: (Key) -> Boolean): CompletableFuture<List<Stored>> =
        CompletableFuture.supplyAsync({
            val now = clock.instant()
            store.entries().filter { holds(it.key) && !it.value.expired(now) }
                .map { Stored(it.key, frozen(it.value.value), it.value.expiresAt) }
                .toList()
        }, executor)

    /** Frozen copies of the live keys among [keys], as one task on the executor. */
    fun view(keys: Collection<Key>): CompletableFuture<List<Stored>> =
        CompletableFuture.supplyAsync({
            val now = clock.instant()
            keys.mapNotNull { key -> store.get(key)?.takeUnless { it.expired(now) }?.let { Stored(key, frozen(it.value), it.expiresAt) } }
        }, executor)

    /** Writes restored [entries] in, through the same funnel a command uses, skipping the already expired. */
    fun restore(entries: List<RdbEntry>): CompletableFuture<Void> =
        CompletableFuture.runAsync({
            val now = clock.instant()
            for (entry in entries) if (!entry.expired(now)) write(entry.key, now, Entry(entry.value, entry.expiresAt))
        }, executor)

    private fun frozen(value: Value): Value = when (value) {
        is Value.Str -> value
        is Value.Hash -> Value.Hash().also { copy -> value.fields.entries().forEach { copy.fields.put(it.key, it.value) } }
        is Value.List -> Value.List(ArrayDeque(value.items))
        is Value.ZSet -> Value.ZSet(SkipList(random.nextLong())).also { copy ->
            value.order.forward().forEach { copy.writeScore(it.score, it.member) }
        }
    }

    /**
     * This partition's share of a `SCAN`: the keys its walk found from [command]'s cursor, and
     * the cursor to continue from, 0 once the walk wrapped. The engine folds the partition into
     * the cursor the client sees.
     */
    fun scan(command: Command.Scan): CompletableFuture<Pair<Long, List<Reply>>> =
        CompletableFuture.supplyAsync({
            val now = clock.instant()
            val found = ArrayList<Reply>()
            val next = walk(store, command.cursor, command.count, { _, entry -> !entry.expired(now) }) { key, _ ->
                if (matches(command.pattern, key.bytes)) found += Reply.Bulk(key.bytes)
            }
            next to found
        }, executor)

    /** The clock is read exactly once per command, so a command sees one instant throughout. */
    fun execute(command: Command): Reply {
        val now = clock.instant()
        // C13: the kind is checked in front of every branch, so a wrong-type command answers
        // without a branch ever reaching the entry it would have corrupted.
        if (command is Command.Keyed && command.needs != null) {
            val held = live(command.key, now)?.value
            if (held != null && held.kind != command.needs) return WRONG_TYPE
        }
        val reply = run(command, now)
        log(command, reply, now)?.let { durable = CompletableFuture.allOf(durable, it) }
        // A command that grew or shrank an aggregate in place never passed through [write], so
        // one recount of the key it touched is what keeps the running total level with the store.
        if (command is Command.Keyed) {
            // One access per command, recorded here rather than inside [live] so a command that
            // reads its key twice -- the kind check and then the command itself -- counts once.
            // Recency needs no more than the single clock read; frequency is the sketch's.
            if (tinyLfu != null && store.get(command.key) != null) tinyLfu.touch(command.key)
            account(command.key)
        }
        // Spec 5.5: eviction runs when memory crosses the threshold, on the partition's own
        // thread, after the command that crossed it has finished with the store.
        if (usedBytes > maxBytes) evict(now)
        return reply
    }

    /** The command itself, once [execute] has settled the instant it runs at and its kind. */
    private fun run(command: Command, now: Instant): Reply {
        return when (command) {
            is Command.Fanned -> error("a partition never sees a multi-key command; ApEngine fans it out")
            is Command.Cp -> error("a partition never sees a CP command; the CP engine replicates it")
            is Command.Ping -> Reply.Simple("PONG")
            is Command.CommandTable -> Reply.Array(emptyList())

            is Command.Get -> Reply.Bulk(live(command.key, now)?.str)
            is Command.Set -> {
                val exists = live(command.key, now) != null
                val rejected = when (command.condition) {
                    null -> false
                    Command.Set.Condition.NX -> exists
                    Command.Set.Condition.XX -> !exists
                }
                if (rejected) {
                    NIL
                } else {
                    write(command.key, now, Entry(Value.Str(command.value), command.ttl?.let(now::plus)))
                    OK
                }
            }
            is Command.IncrBy -> {
                val current = live(command.key, now)
                val number = current?.str?.let(::asLong)
                if (current != null && number == null) NOT_AN_INTEGER else {
                    val next = try {
                        Math.addExact(number ?: 0L, command.delta)
                    } catch (overflow: ArithmeticException) {
                        return NOT_AN_INTEGER
                    }
                    write(command.key, now, Entry(Value.Str(next.toString().toByteArray()), current?.expiresAt))
                    Reply.Integer(next)
                }
            }
            is Command.Append -> {
                val current = live(command.key, now)
                val joined = (current?.str ?: EMPTY) + command.value
                write(command.key, now, Entry(Value.Str(joined), current?.expiresAt))
                Reply.Integer(joined.size.toLong())
            }
            is Command.StrLen -> Reply.Integer((live(command.key, now)?.str?.size ?: 0).toLong())

            is Command.HGet -> Reply.Bulk(hash(command.key, now)?.get(fieldName(command.field)))
            is Command.HSet -> Reply.Integer(put(command.key, now, command.entries))
            is Command.HMSet -> {
                put(command.key, now, command.entries)
                OK
            }
            is Command.HDel -> {
                val fields = hash(command.key, now)
                val removed = fields?.let { command.fields.count { f -> it.remove(fieldName(f)) != null } } ?: 0
                if (fields != null && fields.size == 0) drop(command.key)
                Reply.Integer(removed.toLong())
            }
            is Command.HGetAll -> Reply.Array(
                hash(command.key, now)?.entries().orEmpty().flatMap {
                    listOf(Reply.Bulk(fieldBytes(it.key)), Reply.Bulk(it.value))
                }.toList(),
            )
            is Command.HMGet -> {
                val fields = hash(command.key, now)
                Reply.Array(command.fields.map { Reply.Bulk(fields?.get(fieldName(it))) })
            }
            is Command.HExists ->
                if (hash(command.key, now)?.get(fieldName(command.field)) != null) ONE else ZERO
            is Command.HKeys -> Reply.Array(hash(command.key, now)?.entries().orEmpty().map { Reply.Bulk(fieldBytes(it.key)) }.toList())
            is Command.HVals -> Reply.Array(hash(command.key, now)?.entries().orEmpty().map { Reply.Bulk(it.value) }.toList())
            is Command.HLen -> Reply.Integer((hash(command.key, now)?.size ?: 0).toLong())
            is Command.HScan -> {
                val fields = hash(command.key, now) ?: return scanReply(0, emptyList())
                val found = ArrayList<Reply>()
                val next = walk(fields, command.cursor, command.count, { field, _ -> matches(command.pattern, fieldBytes(field)) }) { field, value ->
                    found += Reply.Bulk(fieldBytes(field))
                    found += Reply.Bulk(value)
                }
                scanReply(next, found)
            }
            is Command.Scan -> error("SCAN runs through Partition.scan, which hands the engine the cursor")

            is Command.Push -> {
                val items = items(command.key, now) ?: Value.List().also { write(command.key, now, Entry(it, null)) }.items
                for (value in command.values) if (command.end == Command.End.HEAD) items.addFirst(value) else items.addLast(value)
                Reply.Integer(items.size.toLong())
            }
            is Command.Pop -> {
                val items = items(command.key, now)
                if (items.isNullOrEmpty()) NIL else {
                    val popped = if (command.end == Command.End.HEAD) items.removeFirst() else items.removeLast()
                    dropIfEmpty(command.key, items)
                    Reply.Bulk(popped)
                }
            }

            is Command.LRange -> {
                val items = items(command.key, now).orEmpty()
                Reply.Array(span(command.start, command.stop, items.size).map { Reply.Bulk(items[it]) })
            }
            is Command.LLen -> Reply.Integer((items(command.key, now)?.size ?: 0).toLong())
            is Command.LIndex -> {
                val items = items(command.key, now).orEmpty()
                Reply.Bulk(items.getOrNull(at(command.index, items.size)))
            }
            is Command.LSet -> {
                val items = items(command.key, now) ?: return NO_SUCH_KEY
                val position = at(command.index, items.size)
                if (position !in items.indices) INDEX_OUT_OF_RANGE else {
                    items[position] = command.value
                    OK
                }
            }
            is Command.LRem -> {
                val items = items(command.key, now)
                val removed = items?.let { remove(it, command.count, command.value) } ?: 0
                if (items != null) dropIfEmpty(command.key, items)
                Reply.Integer(removed.toLong())
            }

            is Command.ZAdd -> {
                // Redis reads every score before it writes any, so one bad score leaves the
                // sorted set exactly as it was.
                val scored = command.entries.map { (score, member) ->
                    (parseScore(score) ?: return NOT_A_FLOAT) to member
                }
                if (scored.isEmpty()) return ZERO
                // XX writes only members that are already there, so on a missing key it writes
                // nothing -- and must not leave an empty sorted set behind for having looked.
                val zset = zset(command.key, now)
                    ?: if (command.condition == Command.Set.Condition.XX) return ZERO
                    else newZSet(command.key, now)
                var added = 0
                var moved = 0
                for ((score, member) in scored) {
                    val previous = zset.scores.get(fieldName(member))
                    when (command.condition) {
                        Command.Set.Condition.NX -> if (previous != null) continue
                        Command.Set.Condition.XX -> if (previous == null) continue
                        null -> {}
                    }
                    if (zset.writeScore(score, member)) added++ else if (previous != score) moved++
                }
                // CH counts what changed; without it Redis counts only what is new.
                Reply.Integer((if (command.changed) added + moved else added).toLong())
            }
            is Command.ZScore ->
                Reply.Bulk(scoreOf(command.key, now, command.member)?.let { scoreText(it).toByteArray() })
            is Command.ZCard -> Reply.Integer((zset(command.key, now)?.scores?.size ?: 0).toLong())
            is Command.ZRange -> {
                val order = zset(command.key, now)?.order
                val size = order?.size ?: 0
                // The window is Redis's LRANGE window, so `span` is the one place negative
                // indices are read. Reversed, position p counts back from the last entry.
                val window = span(command.start, command.stop, size)
                val found = when {
                    order == null || window.isEmpty() -> emptyList()
                    command.reverse ->
                        order.rangeByRank(size - 1 - window.last, size - 1 - window.first).asReversed()
                    else -> order.rangeByRank(window.first, window.last)
                }
                Reply.Array(members(found, command.withScores))
            }
            is Command.ZIncrBy -> {
                val delta = parseScore(command.delta) ?: return NOT_A_FLOAT
                val zset = zset(command.key, now)
                val moved = (zset?.scores?.get(fieldName(command.member)) ?: 0.0) + delta
                // inf + -inf: the one sum of two legal scores that is no score at all. Checked
                // before the key is created, so a refused increment leaves no empty sorted set.
                if (moved.isNaN()) return NAN_SCORE
                (zset ?: newZSet(command.key, now)).writeScore(moved, command.member)
                Reply.Bulk(scoreText(moved).toByteArray())
            }
            is Command.ZRangeByScore -> {
                val min = parseBound(command.min) ?: return NOT_A_RANGE
                val max = parseBound(command.max) ?: return NOT_A_RANGE
                val found = zset(command.key, now)?.order
                    ?.rangeByScore(min.score, max.score, min.inclusive, max.inclusive)
                    .orEmpty()
                Reply.Array(members(limit(found, command.offset, command.count), command.withScores))
            }
            is Command.ZRem -> {
                val zset = zset(command.key, now)
                // The score map says whether the member was there; the list is then told the same
                // thing. Counting off the map keeps one index from silently disagreeing with the
                // other about what was removed.
                val removed = zset?.let {
                    command.members.count { member ->
                        val score = it.scores.remove(fieldName(member)) ?: return@count false
                        it.order.remove(score, member)
                        true
                    }
                } ?: 0
                if (zset != null && zset.scores.size == 0) drop(command.key)
                Reply.Integer(removed.toLong())
            }
            is Command.ZScan -> {
                val scores = zset(command.key, now)?.scores ?: return scanReply(0, emptyList())
                val found = ArrayList<Reply>()
                val next = walk(scores, command.cursor, command.count, { member, _ -> matches(command.pattern, fieldBytes(member)) }) { member, score ->
                    found += Reply.Bulk(fieldBytes(member))
                    found += Reply.Bulk(scoreText(score).toByteArray())
                }
                scanReply(next, found)
            }
            is Command.ZRank -> {
                val zset = zset(command.key, now)
                val score = zset?.scores?.get(fieldName(command.member)) ?: return NIL
                val rank = zset.order.rank(score, command.member)
                Reply.Integer((if (command.reverse) zset.order.size - 1 - rank else rank).toLong())
            }

            is Command.DbSize -> {
                purgeExpired(now)
                Reply.Integer(store.size.toLong())
            }
            // The two numbers INFO joins across partitions: live keys, and the bytes they hold.
            is Command.Info -> {
                purgeExpired(now)
                Reply.Array(
                    listOf(
                        Reply.Integer(store.size.toLong()),
                        Reply.Integer(usedBytes),
                        Reply.Bulk(policy.info.toByteArray()),
                    ),
                )
            }
            is Command.Keys -> {
                purgeExpired(now)
                Reply.Array(store.entries().map { it.key }.filter { globMatches(command.pattern, it.bytes) }.map { Reply.Bulk(it.bytes) }.toList())
            }
            is Command.RandomKey -> {
                purgeExpired(now)
                Reply.Bulk(store.randomKey(random)?.bytes)
            }
            is Command.FlushDb -> {
                // Every key goes, so every deadline goes: the wheel is dropped whole.
                wheel = null
                store.clear()
                usedBytes = 0
                tinyLfu?.clear()
                OK
            }

            is Command.Del -> if (live(command.key, now) == null) ZERO else {
                drop(command.key)
                ONE
            }
            is Command.Exists -> if (live(command.key, now) == null) ZERO else ONE
            is Command.Type -> Reply.Simple(live(command.key, now)?.value?.kind?.text ?: "none")

            is Command.Expire -> {
                val entry = live(command.key, now) ?: return ZERO
                write(command.key, now, Entry(entry.value, command.deadline))
                ONE
            }
            is Command.Persist -> {
                val entry = live(command.key, now)
                if (entry?.expiresAt == null) ZERO else {
                    write(command.key, now, Entry(entry.value, null))
                    ONE
                }
            }
            is Command.Ttl -> {
                val entry = live(command.key, now)
                val deadline = entry?.expiresAt
                when {
                    entry == null -> NO_SUCH_KEY_TTL
                    deadline == null -> NO_TTL
                    else -> {
                        // Never negative: the key is still readable at its deadline, so the last
                        // millisecond of its life reports 0 rather than counting past it.
                        val millis = Duration.between(now, deadline).toMillis().coerceAtLeast(0L)
                        // Redis rounds seconds half up, so 9500 ms left is 10 and 9499 ms is 9.
                        Reply.Integer(if (command.precision == Command.Ttl.Precision.MILLIS) millis else (millis + 500) / 1000)
                    }
                }
            }
        }
    }

    /**
     * The one way an entry enters the store, and with it the one place a TTL reaches the wheel.
     * Every write goes through here, so no command can leave a deadline behind that would later
     * fire against a value it was never meant for.
     */
    private fun write(key: Key, now: Instant, entry: Entry) {
        entry.lastAccess = now
        // The displaced entry's bytes leave with it; [account] then charges for what replaced it.
        val displaced = store.put(key, entry)?.accounted ?: 0
        usedBytes -= displaced
        tinyLfu?.resized(key, -displaced)
        account(key)
        val deadline = entry.expiresAt
        // A write that carries no TTL clears the one the key had, wheel entry and all; a key
        // with no wheel entry has nothing to cancel, and so needs no wheel to be built.
        if (deadline == null) wheel?.cancel(key) else wheel(now).schedule(key, deadline)
    }

    /** The one way an entry leaves the store: its pending deadline leaves with it. */
    private fun drop(key: Key) {
        forget(key)
        wheel?.cancel(key)
    }

    /**
     * Takes the entry out of the store and its bytes off the total. The one accounting line the
     * wheel's own removal shares with [drop]: the wheel has already dropped its own entry by the
     * time its callback runs, so it cannot go through [drop], but the bytes still have to go.
     */
    private fun forget(key: Key): Entry? = store.remove(key)?.also {
        usedBytes -= it.accounted
        tinyLfu?.forgotten(key, it.accounted)
    }

    /**
     * Charges [usedBytes] for what the entry under [key] costs now. Idempotent: it books the
     * difference from what the entry was last charged, so calling it after a command that grew or
     * shrank an aggregate in place is what keeps the total honest without a third store path.
     */
    private fun account(key: Key) {
        val entry = store.get(key) ?: return
        val size = key.bytes.size + ENTRY_BYTES + entry.value.approximateBytes()
        usedBytes += size - entry.accounted
        tinyLfu?.resized(key, size - entry.accounted)
        entry.accounted = size
    }

    /**
     * One bounded eviction step, spec 5.5's order: every expired key goes first, and only then do
     * live keys, so no live key is ever taken while an expired one is still there (I6). The live
     * ones go by the sampling LRU of spec 2.7 -- [SAMPLE] keys drawn at random, the one accessed
     * longest ago evicted -- until the partition is back under its share or the store is empty.
     *
     * At most [MAX_EVICTIONS] keys go in one step, so no single command stalls on a threshold it
     * cannot reach in one pass; the command after it runs the next step. Eviction is local to this
     * partition and to this thread: nothing here reads or writes another partition.
     */
    private fun evict(now: Instant) {
        purgeExpired(now)
        var evicted = 0
        while (usedBytes > maxBytes && store.size > 0 && evicted < MAX_EVICTIONS) {
            drop(coldest() ?: return)
            evicted++
        }
    }

    /**
     * The one key this eviction takes, by whichever policy the partition was built with. The step
     * around it -- when to run, in what order, how many -- is the same either way, so the policy
     * is this function and nothing else.
     */
    private fun coldest(): Key? = tinyLfu?.victim() ?: sampledColdest()

    /**
     * Spec 2.7's sampling LRU: the least recently accessed of [SAMPLE] keys drawn at random. A
     * store no larger than the sample is taken whole -- drawing with replacement from it could
     * only miss a key that sampling "K random keys" was meant to include.
     */
    private fun sampledColdest(): Key? {
        val drawn =
            if (store.size <= SAMPLE) store.entries().map { it.key }.toList()
            else List(SAMPLE) { store.randomKey(random) ?: return null }
        return drawn.minByOrNull { store.get(it)!!.lastAccess }
    }

    /**
     * The entry under [key] if it is still alive at [now]; an expired one is deleted on this
     * access (spec 5.4's lazy check). A key is readable through its deadline and gone after it.
     */
    private fun live(key: Key, now: Instant): Entry? {
        val entry = store.get(key) ?: return null
        if (entry.expired(now)) {
            drop(key)
            return null
        }
        // Every read of an entry funnels through here, so this is where "recently used" is
        // written down, from the reading command's own instant.
        entry.lastAccess = now
        return entry
    }

    /** The fields under [key], or null when the key is absent. */
    private fun hash(key: Key, now: Instant): HashTable<String, ByteArray>? =
        (live(key, now)?.value as Value.Hash?)?.fields

    /**
     * The lazy check of [live], applied to the whole store at once: what a keyspace-wide command
     * sees afterwards is exactly the live keys, with no copy of the key set to filter.
     *
     * ponytail: O(n) in the keyspace, so `DBSIZE` costs a walk that Redis answers in O(1). The
     * wheel does not replace it: it removes a key at the first tick after its deadline, and a
     * keyspace-wide command asked in the gap before that tick must still not see the key. Only
     * a store that can find its expired keys without a walk would let this go.
     */
    private fun purgeExpired(now: Instant) {
        // Collected before anything is removed: the table must not be mutated while its
        // entries are being walked.
        store.entries().filter { it.value.expired(now) }.map { it.key }.toList().forEach(::drop)
    }

    /**
     * Redis's `SCAN` loop: buckets are walked until at least [count] entries came out of them or
     * the walk wrapped, then [keep] filters what came out. So a call may answer few keys, or
     * none, with a cursor that is not 0; the client keeps calling until it is.
     */
    private fun <K, V> walk(table: HashTable<K, V>, cursor: Long, count: Int, keep: (K, V) -> Boolean, emit: (K, V) -> Unit): Long {
        var next = cursor
        var visited = 0
        do {
            next = table.scan(next) { key, value ->
                visited++
                if (keep(key, value)) emit(key, value)
            }
        } while (next != 0L && visited < count)
        return next
    }

    /** `MATCH`: no pattern matches everything. */
    private fun matches(pattern: ByteArray?, bytes: ByteArray): Boolean = pattern == null || globMatches(pattern, bytes)

    /** The elements under [key], or null when the key is absent. */
    private fun items(key: Key, now: Instant): ArrayDeque<ByteArray>? =
        (live(key, now)?.value as Value.List?)?.items

    /** The sorted set under [key], or null when the key is absent. */
    private fun zset(key: Key, now: Instant): Value.ZSet? = live(key, now)?.value as Value.ZSet?

    /**
     * An empty sorted set under [key]. The skip list draws its levels from this partition's own
     * stream, so one seed on the engine still makes every list in it reproducible.
     */
    private fun newZSet(key: Key, now: Instant): Value.ZSet =
        Value.ZSet(SkipList(random.nextLong())).also { write(key, now, Entry(it, null)) }

    /** The reply shape every range command shares: the members, each followed by its score under `WITHSCORES`. */
    private fun members(found: List<Scored>, withScores: Boolean): List<Reply> =
        found.flatMap {
            if (withScores) listOf(Reply.Bulk(it.member), Reply.Bulk(scoreText(it.score).toByteArray()))
            else listOf(Reply.Bulk(it.member))
        }

    /**
     * Redis's `LIMIT offset count`: [count] below zero takes everything from [offset] on, and an
     * offset past the end takes nothing rather than erroring.
     */
    private fun limit(found: List<Scored>, offset: Long, count: Long): List<Scored> {
        if (offset < 0) return emptyList()
        if (offset == 0L && count < 0) return found
        val from = minOf(offset, found.size.toLong()).toInt()
        val to = if (count < 0) found.size else minOf(from + count, found.size.toLong()).toInt()
        return found.subList(from, to)
    }

    /** What [member] scores in [key]'s sorted set, or null when either is absent. */
    private fun scoreOf(key: Key, now: Instant, member: ByteArray): Double? =
        zset(key, now)?.scores?.get(fieldName(member))

    /**
     * A Redis list index as a position: a negative one counts back from the tail. An index the
     * list does not reach clamps to just outside it, which every caller treats as "not there";
     * the clamp is what keeps a wire-sized index inside an `Int`.
     */
    private fun at(index: Long, size: Int): Int =
        (if (index < 0) size + index else index).coerceIn(-1L, size.toLong()).toInt()

    /**
     * Redis's `LRANGE` bounds: a negative index counts back from the tail, a start before the
     * list starts at 0 and a stop past the end stops at the last element, so nothing errors.
     */
    private fun span(start: Long, stop: Long, size: Int): IntRange {
        val from = (if (start < 0) size + start else start).coerceAtLeast(0)
        val to = (if (stop < 0) size + stop else stop).coerceAtMost(size - 1L)
        return if (from > to) IntRange.EMPTY else from.toInt()..to.toInt()
    }

    /**
     * `LREM`'s count: the matches are collected in the direction the sign asks for and dropped
     * back to front, so the positions found stay valid while they are removed.
     */
    private fun remove(items: ArrayDeque<ByteArray>, count: Long, value: ByteArray): Int {
        val order = if (count < 0) items.indices.reversed() else items.indices
        // Zero means every match, and so does any magnitude the list cannot reach; clamping to
        // the size also disarms Long.MIN_VALUE, whose absolute value does not fit in a Long.
        val everyMatch = count == 0L || count >= items.size || count <= -items.size.toLong()
        val limit = if (everyMatch) items.size else Math.abs(count).toInt()
        val doomed = order.filter { items[it].contentEquals(value) }.take(limit)
        doomed.sortedDescending().forEach(items::removeAt)
        return doomed.size
    }

    /** Redis keeps no empty aggregate: the last element taken out takes the key with it. */
    private fun dropIfEmpty(key: Key, items: ArrayDeque<ByteArray>) {
        if (items.isEmpty()) drop(key)
    }

    /** Writes [entries] into [key]'s hash, creating it when absent; replies how many were new. */
    private fun put(key: Key, now: Instant, entries: List<Pair<ByteArray, ByteArray>>): Long {
        val fields = hash(key, now) ?: Value.Hash().also { write(key, now, Entry(it, null)) }.fields
        return entries.count { (field, value) -> fields.put(fieldName(field), value) == null }.toLong()
    }

    /**
     * The value as a Redis integer, or null when it is not one. Redis accepts an optional `-`
     * and decimal digits only, so a leading `+` or any padding is not an integer.
     */
    private fun asLong(value: ByteArray): Long? {
        val text = value.toString(Charsets.ISO_8859_1)
        return if (text.startsWith("+")) null else text.toLongOrNull()
    }

    internal companion object {
        /** The `SCAN` family's reply: the cursor as a bulk, then the array of what was found. */
        fun scanReply(cursor: Long, found: List<Reply>): Reply =
            Reply.Array(listOf(Reply.Bulk(cursor.toString().toByteArray()), Reply.Array(found)))

        /** What an entry costs beyond its key's bytes and its value's: the entry, the table's node, the deadline. */
        const val ENTRY_BYTES = 48L

        /** Spec 2.7's K: how many keys one eviction draws before taking the coldest of them. */
        const val SAMPLE = 5

        /** The bound on one eviction step, so no one write pays for a whole keyspace. */
        const val MAX_EVICTIONS = 32

        val EMPTY = ByteArray(0)

        /** A task that logged nothing owes the disk nothing. */
        val DONE: CompletableFuture<*> = CompletableFuture.completedFuture(null)
        val OK = Reply.Simple("OK")
        val NO_SUCH_KEY = Reply.Error("ERR", "no such key")
        val INDEX_OUT_OF_RANGE = Reply.Error("ERR", "index out of range")
        val NOT_AN_INTEGER = Reply.Error("ERR", "value is not an integer or out of range")
        val NAN_SCORE = Reply.Error("ERR", "resulting score is not a number (NaN)")
        val WRONG_TYPE = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        val NIL = Reply.Bulk(null)
        val ZERO = Reply.Integer(0)
        val ONE = Reply.Integer(1)

        /** Redis's two answers to `TTL` and `PTTL` that are not durations. */
        val NO_SUCH_KEY_TTL = Reply.Integer(-2)
        val NO_TTL = Reply.Integer(-1)
    }
}
