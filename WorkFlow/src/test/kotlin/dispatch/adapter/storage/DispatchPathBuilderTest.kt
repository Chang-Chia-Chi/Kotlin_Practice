package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.model.BatchStatus
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DispatchPathBuilderTest {

    @Test
    fun `csvPath for prod normal`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.csvPath(BatchStatus.NORMAL, "20260403060000", "cfg1")
        assertEquals("env=prod/mode=normal/dispatch/20260403060000/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `csvPath for prod dryrun`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.csvPath(BatchStatus.DRYRUN, "abc-123", "cfg1")
        assertEquals("env=prod/mode=dryrun/dispatch/abc-123/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `csvPath for stg normal`() {
        val builder = DispatchPathBuilder("stg")
        val path = builder.csvPath(BatchStatus.NORMAL, "20260403060000", "cfg1")
        assertEquals("env=stg/mode=normal/dispatch/20260403060000/simulation/cfg1.csv.gz", path)
    }

    @Test
    fun `prodParquetPath returns fixed prod path`() {
        val builder = DispatchPathBuilder("prod")
        val path = builder.prodParquetPath()
        assertEquals("env=prod/dispatch/result.parquet", path)
    }

    @Test
    fun `batchParquetPath includes batchToken`() {
        val builder = DispatchPathBuilder("stg")
        val path = builder.batchParquetPath("20260403060000")
        assertEquals("env=stg/dispatch/20260403060000/result.parquet", path)
    }
}
