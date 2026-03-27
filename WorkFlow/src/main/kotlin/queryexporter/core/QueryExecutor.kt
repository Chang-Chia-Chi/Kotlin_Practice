package com.workflow.queryexporter.core

import javax.sql.DataSource

open class QueryExecutor {
    open fun execute(dataSource: DataSource, sql: String): List<Map<String, Any?>> {
        dataSource.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val meta = rs.metaData
                    val columns = (1..meta.columnCount).map { meta.getColumnLabel(it).lowercase() }
                    val rows = mutableListOf<Map<String, Any?>>()
                    while (rs.next()) {
                        rows += columns.mapIndexed { i, col -> col to rs.getObject(i + 1) }.toMap()
                    }
                    return rows
                }
            }
        }
    }
}
