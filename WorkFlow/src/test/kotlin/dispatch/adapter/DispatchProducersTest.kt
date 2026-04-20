package com.workflow.dispatch.adapter

import com.workflow.dispatch.adapter.persistence.JdbiSimulationResultStore
import com.workflow.infrastructure.persistence.OracleTestContainer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.jdbi.v3.core.Jdbi
import kotlin.test.assertEquals
import kotlin.test.assertIs

class DispatchProducersTest {

    private val producer = DispatchProducers()

    @Test
    fun `prod env creates store backed by prod tables`() {
        val store = producer.simulationResultStore("prod", OracleTestContainer.jdbi)

        assertIs<JdbiSimulationResultStore>(store)
    }

    @Test
    fun `stg env creates store backed by stg tables`() {
        val store = producer.simulationResultStore("stg", OracleTestContainer.jdbi)

        assertIs<JdbiSimulationResultStore>(store)
    }

    @Test
    fun `unknown env throws IllegalArgumentException`() {
        assertThrows<IllegalArgumentException> {
            producer.simulationResultStore("unknown", mock<Jdbi>())
        }
    }

    @Test
    fun `dispatchPathBuilder for prod returns builder scoped to prod env`() {
        val builder = producer.dispatchPathBuilder("prod")
        assertEquals("env=prod/dispatch/result.parquet", builder.prodParquetPath())
    }

    @Test
    fun `dispatchPathBuilder for stg returns builder scoped to stg env`() {
        val builder = producer.dispatchPathBuilder("stg")
        assertEquals("env=stg/dispatch/20260403060000/result.parquet", builder.batchParquetPath("20260403060000"))
    }
}
