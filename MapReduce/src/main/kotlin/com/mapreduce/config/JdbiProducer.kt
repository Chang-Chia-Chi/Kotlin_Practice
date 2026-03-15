package com.mapreduce.config

import io.agroal.api.AgroalDataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.statement.StatementContext
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin
import java.sql.ResultSet

@ApplicationScoped
class JdbiProducer(private val dataSource: AgroalDataSource) {

    @Produces
    @Singleton
    fun jdbi(): Jdbi = Jdbi.create(dataSource).apply {
        installPlugin(KotlinPlugin())
        installPlugin(KotlinSqlObjectPlugin())

        // Oracle stores booleans as NUMBER(1) — register mapper for Kotlin Boolean
        registerColumnMapper(Boolean::class.java, OracleNumberToBooleanMapper())
        registerColumnMapper(Boolean::class.javaObjectType, OracleNumberToBooleanMapper())
    }

    /** Maps Oracle NUMBER(1) columns to Kotlin Boolean (0 → false, non-zero → true). */
    private class OracleNumberToBooleanMapper : ColumnMapper<Boolean> {
        override fun map(rs: ResultSet, columnNumber: Int, ctx: StatementContext): Boolean =
            rs.getInt(columnNumber) != 0
    }
}
