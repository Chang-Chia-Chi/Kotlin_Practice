package com.workflow.dispatch.adapter.persistence

import com.workflow.infrastructure.persistence.OracleTestContainer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.jdbi.v3.core.Jdbi
import kotlin.test.assertIs

class DispatchPersistenceProducerTest {

    private val producer = DispatchPersistenceProducer()

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
}
