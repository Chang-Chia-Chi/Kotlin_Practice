package com.taskqueue.queue

import io.agroal.api.AgroalDataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin

/**
 * CDI producer for [Jdbi]. Bridges Quarkus's managed DataSource (Agroal/HikariCP)
 * to JDBI so all DAO classes can inject `Jdbi` directly.
 *
 * The KotlinPlugin enables Kotlin data class mapping and named parameter binding.
 */
@ApplicationScoped
class JdbiProducer(private val dataSource: AgroalDataSource) {

    @Produces
    @Singleton
    fun jdbi(): Jdbi = Jdbi.create(dataSource).apply {
        installPlugin(KotlinPlugin())
    }
}
