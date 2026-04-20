package com.workflow.infrastructure.queryexporter.adapter

import com.workflow.infrastructure.queryexporter.spi.DataSourceProvider
import io.agroal.api.AgroalDataSource
import io.quarkus.agroal.DataSource.DataSourceLiteral
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.spi.CDI
import javax.sql.DataSource

@ApplicationScoped
class QuarkusDataSourceProvider(
    private val defaultDataSource: AgroalDataSource,
) : DataSourceProvider {

    override fun resolve(name: String): DataSource {
        if (name == DEFAULT_DATASOURCE || name.isBlank()) {
            return defaultDataSource
        }
        return try {
            CDI.current()
                .select(AgroalDataSource::class.java, DataSourceLiteral(name))
                .get()
        } catch (e: Exception) {
            throw IllegalArgumentException("DataSource '$name' not found", e)
        }
    }

    companion object {
        const val DEFAULT_DATASOURCE = "default"
    }
}
