package infra.etl.pipe

import java.sql.Types

/**
 * The canonical value types of spec 4.1. Every [Row] value is an instance of the Kotlin type
 * named below, or null.
 *
 * | Constant   | Kotlin type     | DuckDB type ([duckDbType])   |
 * |------------|-----------------|------------------------------|
 * | [STRING]   | `String`        | `VARCHAR`                    |
 * | [BOOLEAN]  | `Boolean`       | `BOOLEAN`                    |
 * | [LONG]     | `Long`          | `BIGINT`                     |
 * | [DECIMAL]  | `BigDecimal`    | `DECIMAL`                    |
 * | [DOUBLE]   | `Double`        | `DOUBLE`                     |
 * | [DATE]     | `LocalDate`     | `DATE`                       |
 * | [DATETIME] | `LocalDateTime` | `TIMESTAMP`                  |
 * | [INSTANT]  | `Instant`       | `TIMESTAMP WITH TIME ZONE`   |
 * | [BYTES]    | `ByteArray`     | `BLOB`                       |
 *
 * @property duckDbType the natural DuckDB type for this canonical type. It is the mapping of
 *   spec 4.3 read backwards and nothing more: the nullable-column rule of spec 4.6, which
 *   forces a nullable column to VARCHAR, DECIMAL, or TIMESTAMP, belongs to DDL generation and
 *   overrides this value there.
 */
enum class CanonicalType(val duckDbType: String) {
    STRING("VARCHAR"),
    BOOLEAN("BOOLEAN"),
    LONG("BIGINT"),
    DECIMAL("DECIMAL"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    DATETIME("TIMESTAMP"),
    INSTANT("TIMESTAMP WITH TIME ZONE"),
    BYTES("BLOB");

    companion object {

        /**
         * The read seam of spec 4.3: a JDBC column type becomes a canonical type, or nothing.
         *
         * | JDBC / Oracle type          | Canonical type |
         * |-----------------------------|----------------|
         * | NUMBER, NUMERIC, DECIMAL    | [DECIMAL]      |
         * | INTEGER, BIGINT, SMALLINT   | [LONG]         |
         * | FLOAT, DOUBLE, BINARY_DOUBLE| [DOUBLE]       |
         * | VARCHAR2, CHAR, NVARCHAR2, CLOB | [STRING]   |
         * | Oracle DATE, TIMESTAMP (`Types.TIMESTAMP`) | [DATETIME] |
         * | DuckDB DATE (`Types.DATE`)  | [DATE]         |
         * | TIMESTAMP WITH TIME ZONE    | [INSTANT]      |
         * | BOOLEAN                     | [BOOLEAN]      |
         * | RAW, BLOB                   | [BYTES]        |
         * | anything else               | error          |
         *
         * DATE splits by type code, not by name. An Oracle DATE carries a time component and
         * reaches the driver as `Types.TIMESTAMP` (93), so it keeps that time as a
         * `LocalDateTime`. `Types.DATE` (91) only ever arrives from DuckDB, where a DATE has no
         * time, so it becomes a `LocalDate`.
         *
         * [typeName] is consulted only for the vendor type codes that are not in
         * [java.sql.Types] at all - Oracle reports BINARY_DOUBLE as 101 and TIMESTAMP WITH TIME
         * ZONE as -101.
         *
         * @throws IllegalArgumentException if the type is not in the table. The fix is a CAST in
         *   the source SQL; the framework does not guess. [RowMapper] adds the step and column.
         */
        fun fromJdbc(sqlType: Int, typeName: String): CanonicalType =
            bySqlType(sqlType)
                ?: byTypeName(typeName)
                ?: throw IllegalArgumentException(
                    "unsupported column type $typeName (JDBC type $sqlType). " +
                        "Add a CAST to a supported type in the source SQL.",
                )

        private fun bySqlType(sqlType: Int): CanonicalType? = when (sqlType) {
            Types.NUMERIC, Types.DECIMAL -> DECIMAL
            Types.INTEGER, Types.BIGINT, Types.SMALLINT -> LONG
            Types.FLOAT, Types.DOUBLE -> DOUBLE
            Types.CHAR, Types.VARCHAR, Types.NVARCHAR, Types.CLOB -> STRING
            Types.DATE -> DATE
            Types.TIMESTAMP -> DATETIME
            Types.TIMESTAMP_WITH_TIMEZONE -> INSTANT
            Types.BOOLEAN -> BOOLEAN
            Types.VARBINARY, Types.BLOB -> BYTES
            else -> null
        }

        /** Oracle's out-of-band type codes, which [bySqlType] cannot recognise. */
        private fun byTypeName(typeName: String): CanonicalType? =
            when (typeName.uppercase()) {
                "BINARY_DOUBLE" -> DOUBLE
                "TIMESTAMP WITH TIME ZONE" -> INSTANT
                else -> null
            }
    }
}
