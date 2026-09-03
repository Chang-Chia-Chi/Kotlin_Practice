package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.FakeProcessContext
import infra.shuttle.testkit.InMemoryTarget
import infra.shuttle.testkit.ScriptedFetcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/** Spec 6.4: attributes freeze when the chain ends and every notified channel's table is checked before the store. */
class AttributeFreezeTest {
    @TempDir lateinit var dir: Path
    private val clock = ClockFixture()
    private val fetcher = ScriptedFetcher(clock).file("in/123-order.csv", "a,b\n".toByteArray())
    private val downstream = MappingTable(listOf(MappingRow("fileId", field = "TRANSFER_ID"), MappingRow("orderNumber", attribute = "orderNumber")))
    private suspend fun input() = Payload(listOf(fetcher("in/123-order.csv", dir.resolve("input"), DigestAlgorithm.MD5)))

    @Test
    fun I15_attributes_never_change_after_the_chain_ends_and_mappings_are_checked_before_the_store() = runTest {
        val extract = processorFor(ProcessorSpec.Extract(ExtractFrom.FileName, regex = "(?<orderNumber>\\d+)-.*")) { null }
        FakeProcessContext(dir, fetcher, clock).use { ctx ->
            val result = ProcessingChain(listOf(extract), DigestAlgorithm.MD5).run(input(), ctx) as ChainResult.Done
            ctx.setAttribute("orderNumber", "changed after the chain")
            assertEquals(mapOf("orderNumber" to "123"), result.attributes)
            assertThrows(UnsupportedOperationException::class.java) { (result.attributes as MutableMap<String, String>)["x"] = "y" }
            ProcessingChain.checkMappings(result.attributes, listOf(downstream)) { false } // passes: every attribute row is satisfied
            val bad = MappingTable(listOf(MappingRow("order", provider = "orderDetails")))
            val failure = assertThrows(FreezeFailure::class.java) { ProcessingChain.checkMappings(result.attributes, listOf(downstream, bad)) { false } }
            assertEquals("row order: no bean named orderDetails", failure.message)
        }
    }

    @Test
    fun S21_an_attribute_extracted_from_the_file_name_is_available_to_the_mapping() = runTest {
        val extract = processorFor(ProcessorSpec.Extract(ExtractFrom.FileName, regex = "(?<orderNumber>\\d+)-.*")) { null }
        FakeProcessContext(dir, fetcher, clock).use { ctx ->
            val result = ProcessingChain(listOf(extract), DigestAlgorithm.MD5).run(input(), ctx) as ChainResult.Done
            ProcessingChain.checkMappings(result.attributes, listOf(downstream)) { false }
            val row = Transfer(TransferId(1), ctx.transfer.identity, TransferKind.OBJECT, TransferState.ACKED, attributes = result.attributes, firstSeenAt = clock.instant(), updatedAt = clock.instant())
            assertEquals("123", MappingRenderer().render(downstream, row, DeliveryMoment.ACKED).get("orderNumber").asText())
        }
    }

    @Test
    fun S26_missing_required_attribute_at_freeze_fails_before_the_store() = runTest {
        val target = InMemoryTarget("landing")
        FakeProcessContext(dir, fetcher, clock).use { ctx ->
            val result = ProcessingChain(emptyList(), DigestAlgorithm.MD5).run(input(), ctx) as ChainResult.Done
            val failure = assertThrows(FreezeFailure::class.java) { ProcessingChain.checkMappings(result.attributes, listOf(downstream)) { false } }
            assertEquals("mapping row orderNumber: attribute orderNumber is required and not set", failure.message)
            assertTrue(target.calls.isEmpty(), "nothing stored")
            // a default or `required: false` satisfies the row without the attribute
            val lenient = MappingTable(listOf(MappingRow("a", attribute = "orderNumber", default = "0"), MappingRow("b", attribute = "orderNumber", required = false)))
            ProcessingChain.checkMappings(result.attributes, listOf(lenient)) { false }
        }
    }

    @Test
    fun rule22_attribute_limits_are_enforced_when_the_chain_ends() = runTest {
        val greedy = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome { repeat(33) { ctx.setAttribute("a$it", "v") }; return Outcome.Continue(payload) }
        }
        val longName = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome { ctx.setAttribute("n".repeat(65), "v"); return Outcome.Continue(payload) }
        }
        val fat = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome { ctx.setAttribute("big", "x".repeat(1100)); return Outcome.Continue(payload) }
        }
        for ((processor, reason) in listOf(greedy to "rule 22: 33 attributes set; at most 32", longName to "rule 22: attribute name ${"n".repeat(65)} is longer than 64 characters", fat to "rule 22: attributes exceed 1 KB")) {
            FakeProcessContext(dir, fetcher, clock).use { ctx ->
                assertEquals(ChainResult.Rejected(reason), ProcessingChain(listOf(processor), DigestAlgorithm.MD5).run(input(), ctx))
            }
        }
    }
}
