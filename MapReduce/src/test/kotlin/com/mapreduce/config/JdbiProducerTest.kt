package com.mapreduce.config

import io.agroal.api.AgroalDataSource
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.sql.Connection

class JdbiProducerTest {

    @Test
    fun `jdbi creates instance with Kotlin plugins and boolean mapper`() {
        val ds = mock<AgroalDataSource>()
        val conn = org.h2.jdbcx.JdbcDataSource().apply {
            setURL("jdbc:h2:mem:jdbi_test_${System.nanoTime()};MODE=Oracle;DB_CLOSE_DELAY=-1")
        }.connection
        whenever(ds.connection).thenReturn(conn)

        val producer = JdbiProducer(ds)
        val jdbi = producer.jdbi()

        assertNotNull(jdbi)

        // Verify Kotlin mapper works by mapping a simple query
        jdbi.useHandle<Exception> { h ->
            h.execute("CREATE TABLE bool_test (id INT, flag NUMBER(1))")
            h.execute("INSERT INTO bool_test (id, flag) VALUES (1, 1)")
            h.execute("INSERT INTO bool_test (id, flag) VALUES (2, 0)")

            val trueVal = h.createQuery("SELECT flag FROM bool_test WHERE id = 1")
                .mapTo(Boolean::class.java).one()
            assertTrue(trueVal)

            val falseVal = h.createQuery("SELECT flag FROM bool_test WHERE id = 2")
                .mapTo(Boolean::class.java).one()
            assertFalse(falseVal)
        }
    }
}
