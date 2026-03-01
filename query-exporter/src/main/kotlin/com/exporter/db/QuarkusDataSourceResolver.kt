package com.exporter.db

import io.quarkus.agroal.DataSource.DataSourceLiteral
import io.quarkus.arc.Arc
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Named
import org.jboss.logging.Logger
import javax.sql.DataSource

/**
 * Resolves named Quarkus Agroal DataSources via the Arc CDI container.
 *
 * Quarkus registers datasources as CDI beans:
 * - Default (unnamed): injectable with no qualifier.
 * - Named: injectable with `@DataSource("name")` qualifier.
 */
@ApplicationScoped
@Named("quarkusDataSourceResolver")
class QuarkusDataSourceResolver : DataSourceResolver {

    private val log = Logger.getLogger(QuarkusDataSourceResolver::class.java)

    override fun resolve(name: String): DataSource? {
        return try {
            val container = Arc.container() ?: return null
            if (name == "default") {
                container.instance(DataSource::class.java).get()
            } else {
                container.instance(DataSource::class.java, DataSourceLiteral(name)).get()
            }
        } catch (e: Exception) {
            log.debugf("Failed to resolve datasource '%s': %s", name, e.message)
            null
        }
    }

    override fun availableNames(): Set<String> {
        // Quarkus doesn't expose a registry of names directly.
        // In practice, validation checks each referenced name individually.
        return emptySet()
    }
}
