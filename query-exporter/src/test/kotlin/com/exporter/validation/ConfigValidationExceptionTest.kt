package com.exporter.validation

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ConfigValidationExceptionTest {

    @Test
    fun `message includes error count`() {
        val ex = ConfigValidationException(listOf("err1", "err2", "err3"))
        assertThat(ex.message).contains("3 error(s)")
    }

    @Test
    fun `message includes all error details`() {
        val errors = listOf(
            "Datasource 'db1' not found.",
            "Query 'X' has ambiguous schedule.",
        )
        val ex = ConfigValidationException(errors)
        assertThat(ex.message).contains("Datasource 'db1' not found.")
        assertThat(ex.message).contains("ambiguous schedule")
    }

    @Test
    fun `errors list is accessible`() {
        val errors = listOf("e1", "e2")
        val ex = ConfigValidationException(errors)
        assertThat(ex.errors).containsExactly("e1", "e2")
    }

    @Test
    fun `single error formats correctly`() {
        val ex = ConfigValidationException(listOf("Only one problem"))
        assertThat(ex.message).contains("1 error(s)")
        assertThat(ex.message).contains("Only one problem")
    }
}
