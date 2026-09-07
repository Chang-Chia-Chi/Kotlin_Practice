package dynacache.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

private fun bytes(text: String) = text.toByteArray(Charsets.ISO_8859_1)

/**
 * The one home for the `cp:` namespace rule (T71): which primitive owns a key, which Redis
 * commands the namespace answers, and what it says to the ones it does not.
 */
class CpNamespaceTest {

    private fun refusal(command: Command): Reply.Error =
        (CpNamespace.route(command) as CpRouting.Refused).error

    private fun verb(command: Command): Command.Cp =
        (CpNamespace.route(command) as CpRouting.Verb).command

    /**
     * CP spec 2: every sub-namespace of `cp:` names the state machine that owns the key. A `cp:`
     * key in none of them is [CpKind.UNTYPED], which the counter answers as it always has.
     */
    @Test
    fun cp_kind_lookup_covers_every_prefix() {
        val owners = mapOf(
            "cp:counter:x" to CpKind.COUNTER,
            "cp:lock:x" to CpKind.LOCK,
            "cp:sem:x" to CpKind.SEMAPHORE,
            "cp:latch:x" to CpKind.LATCH,
            "cp:ref:x" to CpKind.REFERENCE,
            "cp:session" to CpKind.SESSION,
            "cp:" to CpKind.UNTYPED,
            "cp:x" to CpKind.UNTYPED,
        )
        owners.forEach { (key, kind) -> assertEquals(kind, CpNamespace.kindOf(Key(key)), key) }
        // Every kind the enum declares is reachable from a key, so no prefix is written twice or
        // hidden behind a shorter one.
        assertEquals(CpKind.entries.toSet(), owners.values.toSet())
        assertNull(CpNamespace.kindOf(Key("plain")), "a plain key belongs to no CP primitive")
        assertNull(CpNamespace.kindOf(Key("cpx")), "cp: is a prefix, not a word")
        assertTrue(CpNamespace.owns(Key("cp:counter:x")) && !CpNamespace.owns(Key("plain")))
    }

    @Test
    fun C16_a_cp_verb_outside_the_cp_namespace_is_notcp() {
        assertEquals("NOTCP", refusal(Command.Cp.LongIncr(Key("plain"))).kind)
        assertEquals("NOTCP", CpNamespace.refusalFor(Command.Cp.LongIncr(Key("plain")))?.kind)
        assertNull(CpNamespace.refusalFor(Command.Cp.LongIncr(Key("cp:counter:x"))))
        // An untyped cp: key belongs to no primitive in particular, so no verb is a mismatch on it.
        assertNull(CpNamespace.refusalFor(Command.Cp.LongIncr(Key("cp:x"))))
        assertNull(CpNamespace.refusalFor(Command.Cp.SessionHeartbeat(1)))
        assertNull(CpNamespace.refusalFor(Command.Cp.Info))
    }

    /**
     * CP spec 6.8: `-WRONGTYPE` is "key exists as different primitive (e.g., LOCK on AtomicLong
     * key)". The sub-namespace of CP spec 2 is what says which primitive a key is, so a verb of
     * one kind on a key of another is that error, whether it is spelled as a CP verb or as the
     * Redis command the compat set maps to one.
     */
    @Test
    fun a_verb_of_one_kind_on_a_key_of_another_is_wrongtype() {
        assertEquals("WRONGTYPE", CpNamespace.refusalFor(Command.Cp.LongIncr(Key("cp:lock:x")))?.kind)
        assertEquals("WRONGTYPE", CpNamespace.refusalFor(Command.Cp.RefGet(Key("cp:sem:x")))?.kind)
        assertEquals("WRONGTYPE", CpNamespace.refusalFor(Command.Cp.LockState(Key("cp:counter:x")))?.kind)
        // CP spec 9.4: the lock's TTL is its lease, so EXPIRE on a cp:lock: key is rejected.
        assertEquals("WRONGTYPE", refusal(Command.Expire(Key("cp:lock:x"), java.time.Instant.EPOCH)).kind)
        assertEquals("WRONGTYPE", refusal(Command.Get(Key("cp:lock:x"))).kind)
        assertEquals("WRONGTYPE", refusal(Command.Set(Key("cp:latch:x"), bytes("1"))).kind)
        // INCR is the counter's alone: a reference holds bytes, not a number (CP spec 6.5).
        assertEquals("WRONGTYPE", refusal(Command.IncrBy(Key("cp:ref:x"), 1)).kind)
        assertEquals("WRONGTYPE", refusal(Command.Ttl(Key("cp:sem:x"), Command.Ttl.Precision.SECONDS)).kind)
        assertEquals("WRONGTYPE", refusal(Command.Persist(Key("cp:lock:x"))).kind)
    }

    @Test
    fun the_untyped_and_typed_counter_keys_read_the_same_verbs() {
        listOf("cp:counter:x", "cp:x").forEach { key ->
            assertEquals(Command.Cp.LongGet(Key(key)), verb(Command.Get(Key(key))))
            assertEquals(Command.Cp.LongIncr(Key(key)), verb(Command.IncrBy(Key(key), 1)))
            assertEquals(Command.Cp.LongDecr(Key(key)), verb(Command.IncrBy(Key(key), -1)))
            assertEquals(Command.Cp.LongIncrBy(Key(key), 5), verb(Command.IncrBy(Key(key), 5)))
            assertEquals(Command.Cp.LongPersist(Key(key)), verb(Command.Persist(Key(key))))
        }
    }

    @Test
    fun the_reference_answers_its_own_value_and_ttl_verbs() {
        val key = Key("cp:ref:r")
        assertEquals(Command.Cp.RefGet(key), verb(Command.Get(key)))
        assertEquals(Command.Cp.RefPersist(key), verb(Command.Persist(key)))
        assertEquals(
            Command.Cp.RefTtl(key, Command.Ttl.Precision.MILLIS),
            verb(Command.Ttl(key, Command.Ttl.Precision.MILLIS)),
        )
        assertEquals(
            Command.Cp.RefExpire(key, Duration.ofSeconds(10)),
            (CpNamespace.expiry(key, Duration.ofSeconds(10)) as CpRouting.Verb).command,
        )
        assertEquals(
            Command.Cp.LongExpire(Key("cp:counter:x"), Duration.ofSeconds(10)),
            (CpNamespace.expiry(Key("cp:counter:x"), Duration.ofSeconds(10)) as CpRouting.Verb).command,
        )
        assertEquals(CpRouting.Ap, CpNamespace.expiry(Key("plain"), Duration.ofSeconds(10)))
    }

    @Test
    fun a_plain_key_is_the_ap_engines() {
        assertEquals(CpRouting.Ap, CpNamespace.route(Command.Get(Key("plain"))))
        assertEquals(CpRouting.Ap, CpNamespace.route(Command.MGet(listOf(Key("a"), Key("b")))))
        assertEquals(CpRouting.Ap, CpNamespace.route(Command.Ping))
    }

    /**
     * A fanned command is CP-bound when any of its keys is, not only the first, and no primitive
     * answers one: half an MGET must not reach the AP engine with a `cp:` key in it (C16).
     */
    @Test
    fun C16_a_fanned_command_naming_a_cp_key_is_refused_whole() {
        assertEquals("NOTCP", refusal(Command.MGet(listOf(Key("plain"), Key("cp:counter:x")))).kind)
        assertEquals("NOTCP", refusal(Command.DelKeys(listOf(Key("cp:counter:x"), Key("plain")))).kind)
    }

    /**
     * CP spec 9.5 rule 2: the `cp:` namespace only accepts the compat set, so a command outside it
     * is `-NOTCP` whatever kind owns the key -- it is not aimed at the wrong primitive, it is
     * aimed at no primitive. The same holds for DEL, EXISTS and TYPE, which are in the compat set
     * and which no CP primitive answers yet.
     */
    @Test
    fun a_command_no_cp_primitive_answers_is_notcp_whatever_the_kind() {
        listOf("cp:counter:x", "cp:lock:x", "cp:sem:x", "cp:x").forEach { key ->
            assertEquals("NOTCP", refusal(Command.Del(Key(key))).kind, key)
            assertEquals("NOTCP", refusal(Command.Exists(Key(key))).kind, key)
            assertEquals("NOTCP", refusal(Command.Type(Key(key))).kind, key)
            assertEquals("NOTCP", refusal(Command.StrLen(Key(key))).kind, key)
            assertEquals(
                "NOTCP",
                refusal(Command.Push(Key(key), listOf(bytes("a")), Command.End.HEAD)).kind,
                key,
            )
        }
    }

    @Test
    fun a_counter_takes_a_number_or_the_error_redis_gives_for_one() {
        val key = Key("cp:counter:x")
        assertEquals(Command.Cp.LongSet(key, 5), verb(Command.Set(key, bytes("5"))))
        assertEquals("ERR", refusal(Command.Set(key, bytes("banana"))).kind)
        assertEquals(
            Command.Cp.RefSet(Key("cp:ref:r"), bytes("v"), null, Command.Set.Condition.NX),
            verb(Command.Set(Key("cp:ref:r"), bytes("v"), Command.Set.Condition.NX)),
        )
    }
}
