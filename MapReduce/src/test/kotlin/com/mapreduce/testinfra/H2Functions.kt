package com.mapreduce.testinfra

/**
 * H2 user-defined functions that emulate Oracle-specific SQL functions.
 * Registered via CREATE ALIAS in h2-schema.sql.
 */
object H2Functions {

    /**
     * Emulates Oracle's NUMTODSINTERVAL(value, 'SECOND').
     * Returns fractional days so that CURRENT_TIMESTAMP + result works in H2.
     */
    @JvmStatic
    fun numToDsInterval(value: Long, unit: String): Double =
        when (unit.uppercase()) {
            "SECOND" -> value.toDouble() / 86400.0
            "MINUTE" -> value.toDouble() / 1440.0
            "HOUR" -> value.toDouble() / 24.0
            "DAY" -> value.toDouble()
            else -> throw IllegalArgumentException("Unsupported interval unit: $unit")
        }
}
