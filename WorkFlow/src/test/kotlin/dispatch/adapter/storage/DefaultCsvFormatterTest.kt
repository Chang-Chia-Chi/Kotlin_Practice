package com.workflow.dispatch.adapter.storage

import com.workflow.dispatch.adapter.storage.DefaultCsvFormatter
import com.workflow.dispatch.model.DispatchDecision
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DefaultCsvFormatterTest {

    private val formatter = DefaultCsvFormatter()

    @Test
    fun `formats decisions to CSV with header row`() {
        val decisions = listOf(
            DispatchDecision(
                dispatchOrder = 1, productId = "p1", sourceBomId = "bom1", qty = 5,
                targetSiteId = "A", targetBomId = "tgt1",
                siteGap = BigDecimal("-20"), bomGap = BigDecimal("-10"),
            ),
        )

        val csv = String(formatter.format("2026-03-29T06:00:00", "cfg1", decisions))
        val lines = csv.trim().lines()

        assertEquals(2, lines.size) // header + 1 row
        assertTrue(lines[0].contains("batch_token"))
        assertTrue(lines[0].contains("dispatch_order"))
        assertTrue(lines[1].contains("p1"))
        assertTrue(lines[1].contains("2026-03-29T06:00:00"))
    }

    @Test
    fun `null targetBomId and bomGap render as empty`() {
        val decisions = listOf(
            DispatchDecision(
                dispatchOrder = 1, productId = "p1", sourceBomId = "bom1", qty = 5,
                targetSiteId = "A", targetBomId = null,
                siteGap = BigDecimal("-20"), bomGap = null,
            ),
        )

        val csv = String(formatter.format("batch1", "cfg1", decisions))
        val dataLine = csv.trim().lines()[1]
        // target_bom_id and bom_gap columns should be empty
        assertTrue(dataLine.contains(",,") || dataLine.endsWith(","))
    }

    @Test
    fun `empty decisions list produces header only`() {
        val csv = String(formatter.format("batch1", "cfg1", emptyList()))
        val lines = csv.trim().lines()
        assertEquals(1, lines.size) // header only
    }
}
