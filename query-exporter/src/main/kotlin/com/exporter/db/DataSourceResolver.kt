package com.exporter.db

import javax.sql.DataSource

/**
 * Abstraction for resolving named DataSources from the Quarkus/Agroal registry.
 * Extracted as an interface for testability — production impl uses Arc CDI container.
 */
interface DataSourceResolver {

    /**
     * Returns the DataSource for the given logical name, or null if not found.
     * The "default" name maps to the unnamed default datasource.
     */
    fun resolve(name: String): DataSource?
}
