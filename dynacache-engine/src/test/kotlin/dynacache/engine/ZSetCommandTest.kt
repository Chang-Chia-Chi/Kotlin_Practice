package dynacache.engine

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.Random

/**
 * The Sorted Set on its dual index, driven through the `CommandEngine.submit` seam: a test says
 * only what a client says, so the score map and the skip list are never addressed directly and
 * only their agreement is observable.
 */
class ZSetCommandTest {

    private val engine = ApEngine(
        partitionCount = 4,
        clock = Clock.fixed(Instant.parse("2026-09-06T00:00:00Z"), ZoneOffset.UTC),
        random = Random(20260906),
    )

    @AfterEach
    fun close() = engine.close()

    private fun run(command: Command): Reply = engine.submit(command).get()

    /** `ZADD key score member [score member ...]`, scores written the way a client writes them. */
    private fun zadd(key: String, vararg pairs: Pair<String, String>): Reply =
        run(Command.ZAdd(Key(key), pairs.map { it.first.toByteArray(Charsets.ISO_8859_1) to it.second.toByteArray(Charsets.ISO_8859_1) }))

    private fun bulk(text: String?) = Reply.Bulk(text?.toByteArray(Charsets.ISO_8859_1))

    private fun bulks(vararg values: String): Reply = Reply.Array(values.map { bulk(it) })

    @Test
    fun zadd_scores_members_and_zscore_reads_them_back() {
        assertEquals(Reply.Integer(2), zadd("board", "100" to "alice", "200.5" to "bob"))
        assertEquals(bulk("100"), run(Command.ZScore(Key("board"), "alice".toByteArray(Charsets.ISO_8859_1))))
        assertEquals(bulk("200.5"), run(Command.ZScore(Key("board"), "bob".toByteArray(Charsets.ISO_8859_1))))
        assertEquals(bulk(null), run(Command.ZScore(Key("board"), "carol".toByteArray(Charsets.ISO_8859_1))))
        assertEquals(Reply.Integer(2), run(Command.ZCard(Key("board"))))
        assertEquals(Reply.Simple("zset"), run(Command.Type(Key("board"))))
    }

    @Test
    fun zadd_counts_only_new_members_and_updates_the_score_of_an_old_one() {
        zadd("board", "1" to "alice")
        assertEquals(Reply.Integer(1), zadd("board", "1" to "alice", "2" to "bob"))
        assertEquals(Reply.Integer(0), zadd("board", "9" to "alice"))
        assertEquals(bulk("9"), run(Command.ZScore(Key("board"), "alice".toByteArray(Charsets.ISO_8859_1))))
        assertEquals(Reply.Integer(2), run(Command.ZCard(Key("board"))))
    }

    private fun bytes(text: String) = text.toByteArray(Charsets.ISO_8859_1)

    /** `ZADD key [NX|XX] [CH] score member ...`, the flagged form. */
    private fun zadd(
        key: String,
        condition: Command.Set.Condition?,
        changed: Boolean,
        vararg pairs: Pair<String, String>,
    ): Reply = run(
        Command.ZAdd(Key(key), pairs.map { bytes(it.first) to bytes(it.second) }, condition, changed),
    )

    private fun zscore(key: String, member: String): Reply = run(Command.ZScore(Key(key), bytes(member)))

    @Test
    fun zadd_nx_xx_ch_flags() {
        val nx = Command.Set.Condition.NX
        val xx = Command.Set.Condition.XX

        // XX on a key that is not there writes nothing, and must leave no empty sorted set
        // behind for having looked.
        assertEquals(Reply.Integer(0), zadd("board", xx, false, "1" to "alice"))
        assertEquals(Reply.Integer(0), run(Command.Exists(Key("board"))))

        // NX creates the key, then refuses to move a member it already scored.
        assertEquals(Reply.Integer(1), zadd("board", nx, false, "1" to "alice"))
        assertEquals(Reply.Integer(0), zadd("board", nx, false, "9" to "alice"))
        assertEquals(bulk("1"), zscore("board", "alice"))

        // One ZADD, two members: NX adds the new one and leaves the old one where it was.
        assertEquals(Reply.Integer(1), zadd("board", nx, false, "9" to "alice", "2" to "bob"))
        assertEquals(bulk("1"), zscore("board", "alice"))
        assertEquals(bulk("2"), zscore("board", "bob"))

        // XX is the mirror: it moves the member that is there and ignores the one that is not.
        assertEquals(Reply.Integer(0), zadd("board", xx, false, "5" to "alice", "7" to "carol"))
        assertEquals(bulk("5"), zscore("board", "alice"))
        assertEquals(bulk(null), zscore("board", "carol"))
        assertEquals(Reply.Integer(2), run(Command.ZCard(Key("board"))))

        // CH counts members new or moved, where the plain reply counts only the new ones.
        assertEquals(Reply.Integer(2), zadd("board", null, true, "6" to "alice", "3" to "carol"))
        assertEquals(Reply.Integer(1), zadd("board", null, false, "4" to "dave"))
        // A write that changes no score changes nothing to count, CH or not.
        assertEquals(Reply.Integer(0), zadd("board", null, true, "6" to "alice"))
        assertEquals(Reply.Integer(4), run(Command.ZCard(Key("board"))))
    }

    @Test
    fun zadd_rejects_a_score_that_is_not_a_float_and_writes_nothing() {
        assertEquals(
            Reply.Error("ERR", "value is not a valid float"),
            zadd("board", "1" to "alice", "banana" to "bob"),
        )
        assertEquals(Reply.Integer(0), run(Command.ZCard(Key("board"))))
    }

    @Test
    fun zcard_and_zscore_answer_for_a_missing_key() {
        assertEquals(Reply.Integer(0), run(Command.ZCard(Key("absent"))))
        assertEquals(bulk(null), run(Command.ZScore(Key("absent"), "m".toByteArray(Charsets.ISO_8859_1))))
    }

    /** `ZRANGE key start stop [WITHSCORES]`, and its `ZREVRANGE` twin. */
    private fun zrange(key: String, start: Long, stop: Long, withScores: Boolean = false, reverse: Boolean = false) =
        run(Command.ZRange(Key(key), start, stop, withScores, reverse))

    @Test
    fun zrange_reads_positions_forwards_and_backwards_with_redis_negative_indices() {
        zadd("board", "1" to "alice", "3" to "carol", "2" to "bob")
        assertEquals(bulks("alice", "bob", "carol"), zrange("board", 0, -1))
        assertEquals(bulks("carol", "bob", "alice"), zrange("board", 0, -1, reverse = true))
        assertEquals(bulks("bob", "carol"), zrange("board", 1, 5))
        assertEquals(bulks("bob"), zrange("board", -2, -2))
        assertEquals(bulks("bob"), zrange("board", -2, -2, reverse = true))
        assertEquals(Reply.Array(emptyList()), zrange("board", 2, 1))
        assertEquals(Reply.Array(emptyList()), zrange("absent", 0, -1))
    }

    @Test
    fun zrange_withscores_pairs_every_member_with_its_score() {
        zadd("board", "1.5" to "alice", "2" to "bob")
        assertEquals(bulks("alice", "1.5", "bob", "2"), zrange("board", 0, -1, withScores = true))
        assertEquals(bulks("bob", "2", "alice", "1.5"), zrange("board", 0, -1, withScores = true, reverse = true))
    }

    private fun zrem(key: String, vararg members: String) =
        run(Command.ZRem(Key(key), members.map { it.toByteArray(Charsets.ISO_8859_1) }))

    private fun zrank(key: String, member: String, reverse: Boolean = false) =
        run(Command.ZRank(Key(key), member.toByteArray(Charsets.ISO_8859_1), reverse))

    @Test
    fun zrem_takes_members_out_of_both_indexes_and_the_last_one_takes_the_key() {
        zadd("board", "1" to "alice", "2" to "bob", "3" to "carol")
        assertEquals(Reply.Integer(2), zrem("board", "alice", "carol", "nobody"))
        assertEquals(bulk(null), run(Command.ZScore(Key("board"), "alice".toByteArray(Charsets.ISO_8859_1))))
        assertEquals(bulks("bob"), zrange("board", 0, -1))
        assertEquals(Reply.Integer(1), zrem("board", "bob"))
        assertEquals(Reply.Simple("none"), run(Command.Type(Key("board"))))
        assertEquals(Reply.Integer(0), zrem("board", "bob"))
    }

    @Test
    fun zrank_and_zrevrank_report_a_position_and_nil_for_a_member_that_is_not_there() {
        zadd("board", "1" to "alice", "2" to "bob", "3" to "carol")
        assertEquals(Reply.Integer(0), zrank("board", "alice"))
        assertEquals(Reply.Integer(2), zrank("board", "carol"))
        assertEquals(Reply.Integer(2), zrank("board", "alice", reverse = true))
        assertEquals(Reply.Integer(0), zrank("board", "carol", reverse = true))
        assertEquals(bulk(null), zrank("board", "nobody"))
        assertEquals(bulk(null), zrank("absent", "alice"))
    }

    /** `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`, bounds as a client writes them. */
    private fun byScore(
        key: String,
        min: String,
        max: String,
        withScores: Boolean = false,
        offset: Long = 0,
        count: Long = -1,
    ) = run(
        Command.ZRangeByScore(Key(key), min.toByteArray(Charsets.ISO_8859_1), max.toByteArray(Charsets.ISO_8859_1), withScores, offset, count),
    )

    @Test
    fun zrangebyscore_takes_inclusive_exclusive_and_infinite_bounds() {
        zadd("board", "1" to "alice", "2" to "bob", "3" to "carol", "4" to "dave")
        assertEquals(bulks("bob", "carol"), byScore("board", "2", "3"))
        assertEquals(bulks("carol"), byScore("board", "(2", "(4"))
        assertEquals(bulks("alice", "bob", "carol", "dave"), byScore("board", "-inf", "+inf"))
        assertEquals(bulks("alice", "bob"), byScore("board", "-inf", "2"))
        assertEquals(Reply.Array(emptyList()), byScore("board", "3", "2"))
        assertEquals(bulks("bob", "2"), byScore("board", "2", "2", withScores = true))
        assertEquals(Reply.Array(emptyList()), byScore("absent", "-inf", "+inf"))
    }

    @Test
    fun zrangebyscore_limit_offsets_and_truncates_and_a_bad_bound_errors() {
        zadd("board", "1" to "alice", "2" to "bob", "3" to "carol", "4" to "dave")
        assertEquals(bulks("bob", "carol"), byScore("board", "-inf", "+inf", offset = 1, count = 2))
        assertEquals(bulks("carol", "dave"), byScore("board", "-inf", "+inf", offset = 2, count = -1))
        assertEquals(Reply.Array(emptyList()), byScore("board", "-inf", "+inf", offset = 9, count = 2))
        assertEquals(
            Reply.Error("ERR", "min or max is not a float"),
            byScore("board", "banana", "+inf"),
        )
    }

    private fun zincrby(key: String, delta: String, member: String) =
        run(Command.ZIncrBy(Key(key), delta.toByteArray(Charsets.ISO_8859_1), member.toByteArray(Charsets.ISO_8859_1)))

    @Test
    fun zincrby_moves_a_member_and_creates_one_at_the_increment() {
        zadd("board", "10" to "alice", "1" to "bob")
        assertEquals(bulk("12.5"), zincrby("board", "2.5", "alice"))
        assertEquals(bulk("-5"), zincrby("board", "-5", "carol"))
        assertEquals(bulks("carol", "bob", "alice"), zrange("board", 0, -1))
        assertEquals(Reply.Error("ERR", "value is not a valid float"), zincrby("board", "banana", "alice"))
        assertEquals(bulk("12.5"), run(Command.ZScore(Key("board"), "alice".toByteArray(Charsets.ISO_8859_1))))
    }

    @Test
    fun zincrby_refuses_to_leave_a_member_scored_nan() {
        zadd("board", "+inf" to "alice")
        assertEquals(
            Reply.Error("ERR", "resulting score is not a number (NaN)"),
            zincrby("board", "-inf", "alice"),
        )
        assertEquals(bulk("inf"), run(Command.ZScore(Key("board"), "alice".toByteArray(Charsets.ISO_8859_1))))
    }

    /**
     * Walks `ZSCAN key` to its end, answering member to score text. The walk is the `SCAN`
     * family's: a call may answer nothing with a cursor that is not 0, so only 0 ends it (C15).
     */
    private fun zscanAll(key: String, pattern: String? = null, count: Int = 10): Map<String, String> {
        val found = HashMap<String, String>()
        var cursor = 0L
        do {
            val reply = run(Command.ZScan(Key(key), cursor, pattern?.toByteArray(Charsets.ISO_8859_1), count)) as Reply.Array
            cursor = (reply.items[0] as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1).toLong()
            val page = (reply.items[1] as Reply.Array).items.map { (it as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1) }
            page.chunked(2).forEach { (member, score) -> found[member] = score }
        } while (cursor != 0L)
        return found
    }

    @Test
    fun zscan_returns_all_members() {
        val expected = (0 until 200).associate { "m$it" to it.toString() }
        run(Command.ZAdd(Key("board"), expected.map { it.value.toByteArray(Charsets.ISO_8859_1) to it.key.toByteArray(Charsets.ISO_8859_1) }))

        assertEquals(expected, zscanAll("board", count = 7))
        assertEquals(expected, zscanAll("board", count = 1000))
        assertEquals(mapOf("m7" to "7"), zscanAll("board", pattern = "m7"))
        // `m1?` is three characters, so it matches m10 through m19 and not m1 itself.
        assertEquals((10..19).associate { "m$it" to it.toString() }, zscanAll("board", pattern = "m1?"))
        assertEquals(emptyMap<String, String>(), zscanAll("absent"))
    }

    /**
     * `ZRANGE key 0 -1 WITHSCORES` as pairs. Scores come back parsed, so an assertion about
     * order is about the order and not about how a score is spelled.
     */
    private fun scored(key: String): List<Pair<String, Double>> =
        (zrange(key, 0, -1, withScores = true) as Reply.Array).items
            .map { (it as Reply.Bulk).bytes!!.toString(Charsets.ISO_8859_1) }
            .chunked(2)
            .map { (member, score) -> member to score.toDouble() }

    /**
     * What the sorted set should read as: ascending by score, ties broken by the member bytes.
     * Members are decoded ISO-8859-1, one character per byte, so Kotlin's own string order over
     * them is the unsigned byte order Redis's `memcmp` gives, arrived at independently of the
     * skip list's comparator.
     */
    private fun ordered(model: Map<String, Double>): List<Pair<String, Double>> =
        model.entries.sortedWith(compareBy({ it.value }, { it.key })).map { it.key to it.value }

    @Test
    fun zset_ordering_invariant() {
        val random = Random(20260907)
        val names = (0 until 60).map { "m$it" }
        val model = HashMap<String, Double>()

        repeat(1500) { step ->
            val member = names[random.nextInt(names.size)]
            if (random.nextInt(100) < 35) {
                zrem("board", member)
                model.remove(member)
            } else {
                val score = (random.nextInt(2000) - 1000) / 4.0
                zadd("board", score.toString() to member)
                model[member] = score
            }
            if (step % 25 == 0) assertEquals(ordered(model), scored("board"), "after step $step")
        }
        assertEquals(ordered(model), scored("board"))
        assertEquals(Reply.Integer(model.size.toLong()), run(Command.ZCard(Key("board"))))
    }

    @Test
    fun zset_rank_consistency() {
        val random = Random(20260908)
        val model = HashMap<String, Double>()
        repeat(300) {
            val member = "m${random.nextInt(40)}"
            val score = random.nextInt(20).toDouble()
            zadd("board", score.toString() to member)
            model[member] = score
        }
        repeat(60) { gone ->
            zrem("board", "m$gone")
            model.remove("m$gone")
        }

        val listed = scored("board")
        assertEquals(ordered(model), listed)
        listed.forEachIndexed { position, (member, _) ->
            assertEquals(Reply.Integer(position.toLong()), zrank("board", member), member)
            assertEquals(
                Reply.Integer((listed.size - 1 - position).toLong()),
                zrank("board", member, reverse = true),
                member,
            )
        }
    }

    @Test
    fun zset_score_update() {
        zadd("board", "1" to "alice", "2" to "bob", "3" to "carol")

        assertEquals(Reply.Integer(0), zadd("board", "5" to "alice"))
        assertEquals(listOf("bob" to 2.0, "carol" to 3.0, "alice" to 5.0), scored("board"))
        assertEquals(Reply.Integer(3), run(Command.ZCard(Key("board"))))
        assertEquals(Reply.Integer(2), zrank("board", "alice"))

        assertEquals(Reply.Integer(0), zadd("board", "0" to "alice"))
        assertEquals(listOf("alice" to 0.0, "bob" to 2.0, "carol" to 3.0), scored("board"))
        assertEquals(Reply.Integer(0), zrank("board", "alice"))
    }
    @Test
    fun I3_zrange_sorted_with_lex_tiebreak() {
        // One score for six members, so only the member bytes can decide the order; 0x01 and
        // 0xff pin the comparison as unsigned, where a signed one would put 0xff first.
        val lowest = "\u0001"
        val highest = "\u00ff"
        listOf(highest, "b", lowest, "aa", "a", "B").forEach { zadd("board", "7" to it) }
        zadd("board", "1" to "first", "9" to "last")

        assertEquals(
            listOf("first", lowest, "B", "a", "aa", "b", highest, "last"),
            scored("board").map { it.first },
        )
        assertEquals(listOf(1.0) + List(6) { 7.0 } + 9.0, scored("board").map { it.second })
        assertEquals(Reply.Integer(1), zrank("board", lowest))
        assertEquals(Reply.Integer(6), zrank("board", highest))
    }

    @Test
    fun a_zset_command_on_a_string_key_is_wrongtype() {
        run(Command.Set(Key("s"), "v".toByteArray(Charsets.ISO_8859_1)))
        val wrongType = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        assertEquals(wrongType, zadd("s", "1" to "m"))
        assertEquals(wrongType, run(Command.ZCard(Key("s"))))
    }
}
