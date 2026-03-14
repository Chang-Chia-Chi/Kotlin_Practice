package com.mapreduce.config

import io.agroal.api.AgroalDataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin

@ApplicationScoped
class JdbiProducer(private val dataSource: AgroalDataSource) {

    @Produces
    @Singleton
    fun jdbi(): Jdbi = Jdbi.create(dataSource).apply {
        installPlugin(KotlinPlugin())
        installPlugin(KotlinSqlObjectPlugin())
    }
}
