package com.workflow.queryexporter.spi

import javax.sql.DataSource

fun interface DataSourceProvider {
    fun resolve(name: String): DataSource
}
