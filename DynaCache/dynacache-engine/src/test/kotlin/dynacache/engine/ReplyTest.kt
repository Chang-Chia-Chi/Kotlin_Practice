package dynacache.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

class ReplyTest {

    @Test
    fun `bulk replies compare by byte content, not by array identity`() {
        assertEquals(Reply.Bulk("hello".toByteArray()), Reply.Bulk("hello".toByteArray()))
        assertEquals(
            Reply.Bulk("hello".toByteArray()).hashCode(),
            Reply.Bulk("hello".toByteArray()).hashCode(),
        )
        assertNotEquals(Reply.Bulk("hello".toByteArray()), Reply.Bulk("world".toByteArray()))
    }

    @Test
    fun `a nil bulk equals only another nil bulk`() {
        assertEquals(Reply.Bulk(null), Reply.Bulk(null))
        assertNotEquals(Reply.Bulk(null), Reply.Bulk(ByteArray(0)))
        assertNotEquals(Reply.Bulk(ByteArray(0)), Reply.Bulk(null))
    }

    @Test
    fun `an array compares by its items, bulk bytes included`() {
        val mget = Reply.Array(listOf(Reply.Bulk("a".toByteArray()), Reply.Bulk(null)))
        assertEquals(mget, Reply.Array(listOf(Reply.Bulk("a".toByteArray()), Reply.Bulk(null))))
        assertEquals(
            mget.hashCode(),
            Reply.Array(listOf(Reply.Bulk("a".toByteArray()), Reply.Bulk(null))).hashCode(),
        )
        assertNotEquals(mget, Reply.Array(listOf(Reply.Bulk("b".toByteArray()), Reply.Bulk(null))))
    }

    @Test
    fun `simple, error and integer replies compare by value`() {
        assertEquals(Reply.Simple("OK"), Reply.Simple("OK"))
        assertNotEquals(Reply.Simple("OK"), Reply.Simple("PONG"))
        assertEquals(Reply.Integer(1), Reply.Integer(1))
        assertNotEquals(Reply.Integer(1), Reply.Integer(2))

        val wrongType = Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
        assertEquals("WRONGTYPE", wrongType.kind)
        assertEquals(wrongType, Reply.Error("WRONGTYPE", "Operation against a key holding the wrong kind of value"))
        assertNotEquals(wrongType, Reply.Error("ERR", "Operation against a key holding the wrong kind of value"))
    }

    @Test
    fun `replies of different shapes are never equal`() {
        assertNotEquals((Reply.Simple("1") as Reply), Reply.Bulk("1".toByteArray()))
        assertNotEquals((Reply.Integer(1) as Reply), Reply.Simple("1"))
    }
}
