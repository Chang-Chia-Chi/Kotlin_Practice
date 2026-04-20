package com.workflow.infrastructure.persistence

import io.agroal.api.AgroalDataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.SqlObjectPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin

/**
 * CDI producer that exposes a singleton [Jdbi] backed by the Quarkus-managed
 * Agroal datasource. Installs Kotlin + SqlObject plugins so repositories can
 * use data classes and `@SqlObject` interfaces directly.
 */
@ApplicationScoped
class JdbiProducer {

    @Produces
    @ApplicationScoped
    fun jdbi(dataSource: AgroalDataSource): Jdbi =
        Jdbi.create(dataSource)
            .installPlugin(KotlinPlugin())
            .installPlugin(SqlObjectPlugin())
            .installPlugin(KotlinSqlObjectPlugin())
}
