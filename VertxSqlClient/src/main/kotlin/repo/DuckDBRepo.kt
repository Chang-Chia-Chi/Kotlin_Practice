package repo

import extension.withHandleFlow
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Named
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.duckdb.DuckDBConnection
import org.jdbi.v3.core.Jdbi
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties

@ApplicationScoped
class DuckDBRepo(
    @Named("duckdb") val jdbi: Jdbi,
) {
    final inline fun <reified T : Any> readParquetAsFlow(parquet: String): Flow<T> {
        val sql =
            """
            SELECT * FROM read_parquet(:parquet)
            """.trimIndent()
        return jdbi
            .withHandleFlow<T, Exception> { handle ->
                handle
                    .createQuery(sql)
                    .bind("parquet", parquet)
                    .mapTo(T::class.java)
            }.flowOn(Dispatchers.IO)
    }

    suspend fun copyTo(
        tableName: String,
        format: String,
        trgFilePath: String? = null,
    ): Path =
        withContext(Dispatchers.IO) {
            val filePath = trgFilePath ?: (UUID.randomUUID().toString() + ".$format")
            val sql =
                """
                COPY $tableName
                TO '$filePath'
                (FORMAT '$format')
                """.trimIndent()
            jdbi.withHandle<Int, Exception> { handle ->
                handle.execute(sql)
            }

            Paths.get(filePath)
        }

    suspend fun <T : Any> writeTo(
        tableName: String,
        records: List<T>,
        schema: String = DuckDBConnection.DEFAULT_SCHEMA,
    ): Int =
        withContext(Dispatchers.IO) {
            if (records.isEmpty()) return@withContext 0
            val klass: KClass<out T> = records.first()::class
            jdbi.useHandle<Exception> { handle ->
                handle.connection.use { conn ->
                    val duckConn = conn as DuckDBConnection

                    duckConn.createAppender(schema, tableName).use { appender ->
                        for (record in records) {
                            appender.beginRow()
                            for (prop in klass.memberProperties) {
                                when (val value = prop.getter.call(record)) {
                                    null -> throw IllegalArgumentException(
                                        "Null not supported for column '${prop.name}' in DuckDBAppender",
                                    )
                                    is Boolean -> appender.append(value)
                                    is Byte -> appender.append(value)
                                    is Short -> appender.append(value)
                                    is Int -> appender.append(value)
                                    is Long -> appender.append(value)
                                    is Float -> appender.append(value)
                                    is Double -> appender.append(value)
                                    is String -> appender.append(value)
                                    is LocalDateTime -> appender.append(Timestamp.valueOf(value))
                                    else -> appender.append(value.toString())
                                }
                            }
                            appender.endRow()
                        }
                        appender.flush()
                    }
                }
            }
            records.size
        }

    suspend fun <T : Any> createTableFromDataClass(
        tableName: String,
        klass: KClass<T>,
        schema: String = DuckDBConnection.DEFAULT_SCHEMA,
    ) = withContext(Dispatchers.IO) {
        val fields =
            klass.memberProperties.joinToString(", ") { prop ->
                "${prop.name} ${mapKotlinTypeToSql(prop.returnType)}"
            }

        val sql = "CREATE TABLE IF NOT EXISTS $schema.$tableName ($fields)"
        jdbi.useHandle<Exception> { handle ->
            handle.execute(sql)
        }
    }

    private fun mapKotlinTypeToSql(type: KType): String =
        when (type.classifier) {
            Boolean::class -> "BOOLEAN"
            Byte::class, Short::class,
            Int::class,
            -> "INTEGER"
            Long::class -> "BIGINT"
            Float::class -> "FLOAT"
            Double::class -> "DOUBLE"
            String::class -> "VARCHAR"
            java.time.LocalDate::class,
            java.sql.Date::class,
            -> "DATE"
            java.time.LocalDateTime::class,
            java.sql.Timestamp::class,
            -> "TIMESTAMP"
            else -> "VARCHAR"
        }
}
