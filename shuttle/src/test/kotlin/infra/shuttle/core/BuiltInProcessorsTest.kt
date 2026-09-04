package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.FakeProcessContext
import infra.shuttle.testkit.ScriptedFetcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

/** Spec 6.3: every built-in except expand and extraction from a message, on the fakes. */
class BuiltInProcessorsTest {
    @TempDir lateinit var dir: Path
    private val clock = ClockFixture()
    private val fetcher = ScriptedFetcher(clock).file("in/123-order.csv", "a,b\n1,2\n".toByteArray()).file("in/empty.csv", ByteArray(0))
    private fun ctx() = FakeProcessContext(dir, fetcher, clock)
    private suspend fun input(path: String = "in/123-order.csv") = Payload(listOf(fetcher(path, dir.resolve(path.substringAfterLast('/')), DigestAlgorithm.MD5)))
    private fun zipOf(vararg entries: Pair<String, String>): ByteArray = ByteArrayOutputStream().also { bytes ->
        ZipOutputStream(bytes).use { zip -> entries.forEach { (name, body) -> zip.putNextEntry(ZipEntry(name)); zip.write(body.toByteArray()); zip.closeEntry() } }
    }.toByteArray()
    private fun entriesOf(path: Path) = ZipInputStream(Files.newInputStream(path)).use { zip -> generateSequence { zip.nextEntry }.map { it.name to zip.readBytes().decodeToString() }.toList() }
    private suspend fun run(processor: Processor, payload: Payload, ctx: ProcessContext) =
        ProcessingChain(listOf(processor), DigestAlgorithm.MD5).run(payload, ctx)

    @Test
    fun quality_passes_a_file_that_satisfies_the_check_and_rejects_one_that_does_not() = runTest {
        val quality = processorFor(ProcessorSpec.Quality) { null }
        ctx().use { ctx ->
            assertTrue(run(quality, input(), ctx) is ChainResult.Done)
            assertEquals(ChainResult.Rejected("quality: empty.csv is empty"), run(quality, input("in/empty.csv"), ctx))
        }
    }

    @Test
    fun rename_applies_the_pattern_from_the_name_the_source_name_the_date_and_attributes_keeping_the_file() = runTest {
        val rename = processorFor(ProcessorSpec.Rename("{yyyyMMdd}-{orderNumber}-{name}.bak")) { null }
        ctx().use { ctx ->
            ctx.setAttribute("orderNumber", "123")
            val payload = input()
            val out = (run(rename, payload, ctx) as ChainResult.Done).payload.objects.single()
            assertEquals("20260101-123-123-order.csv.bak", out.name)
            assertEquals(payload.objects.single().path, out.path)
            assertEquals(payload.objects.single().digest, out.digest)
        }
    }

    @Test
    fun S20_rename_then_zip_yields_one_archive_under_the_renamed_key_with_a_different_digest() = runTest {
        val chain = ProcessingChain(listOf(processorFor(ProcessorSpec.Rename("{yyyyMMdd}-{name}")) { null }, processorFor(ProcessorSpec.Zip) { null }), DigestAlgorithm.MD5)
        ctx().use { ctx ->
            val payload = input()
            val result = chain.run(payload, ctx) as ChainResult.Done
            val archive = result.payload.objects.single()
            assertEquals("20260101-123-order.csv.zip", archive.name)       // STORED_NAME differs from SOURCE_NAME
            assertEquals(listOf(archive.path), ctx.createdFiles)             // created through the context
            assertNotEquals(payload.objects.single().digest, archive.digest) // SOURCE_DIGEST differs from DIGEST
            assertEquals(Digest.of(archive.path, DigestAlgorithm.MD5), archive.digest)
            assertEquals(listOf("20260101-123-order.csv" to "a,b\n1,2\n"), entriesOf(archive.path))
        }
    }

    @Test
    fun unzip_yields_one_object_per_entry_keeping_entry_paths_and_recomputing_digests() = runTest {
        fetcher.file("in/set.zip", zipOf("a/x.csv" to "1", "b/x.csv" to "22", "dir/" to ""))
        ctx().use { ctx ->
            val result = run(processorFor(ProcessorSpec.Unzip()) { null }, input("in/set.zip"), ctx) as ChainResult.Done
            assertEquals(listOf("a/x.csv", "b/x.csv"), result.payload.objects.map { it.name })
            assertEquals(listOf(1L, 2L), result.payload.objects.map { it.size })
            assertEquals(result.payload.objects.map { Digest.of(it.path, DigestAlgorithm.MD5) }, result.payload.objects.map { it.digest })
            assertEquals(2, ctx.createdFiles.size)
        }
    }

    @Test
    fun unzip_rejects_past_maxEntries_without_extracting_them_all_and_past_maxBytes() = runTest {
        fetcher.file("in/many.zip", zipOf("1" to "x", "2" to "x", "3" to "x", "4" to "x", "5" to "x"))
        fetcher.file("in/big.zip", zipOf("big" to "x".repeat(100)))
        ctx().use { ctx ->
            val rejected = run(processorFor(ProcessorSpec.Unzip(maxEntries = 2)) { null }, input("in/many.zip"), ctx) as ChainResult.Rejected
            assertEquals("unzip: many.zip has more than maxEntries 2 entries (3 seen)", rejected.reason)
            assertTrue(ctx.createdFiles.size <= 3, "stopped reading at the limit: ${ctx.createdFiles}")
        }
        ctx().use { ctx ->
            val rejected = run(processorFor(ProcessorSpec.Unzip(maxBytes = 50)) { null }, input("in/big.zip"), ctx) as ChainResult.Rejected
            assertEquals("unzip: big.zip exceeds maxBytes 50 uncompressed", rejected.reason)
        }
    }

    @Test
    fun extract_sets_attributes_from_the_file_name_the_source_path_and_json_content_and_rejects_a_non_match() = runTest {
        fetcher.file("in/order.json", """{"order":{"id":"77"},"tags":["a"]}""".toByteArray())
        ctx().use { ctx ->
            val payload = input()
            assertTrue(run(processorFor(ProcessorSpec.Extract(ExtractFrom.FileName, regex = "(?<orderNumber>\\d+)-.*\\.csv")) { null }, payload, ctx) is ChainResult.Done)
            // the fake context's listing path is "a.csv"; positional groups are named by `into`
            assertTrue(run(processorFor(ProcessorSpec.Extract(ExtractFrom.SourcePath, regex = "(\\w+)\\.(\\w+)", into = listOf("stem", "ext"))) { null }, payload, ctx) is ChainResult.Done)
            assertTrue(run(processorFor(ProcessorSpec.Extract(ExtractFrom.Content, json = mapOf("orderId" to "/order/id"))) { null }, input("in/order.json"), ctx) is ChainResult.Done)
            assertEquals(mapOf("orderNumber" to "123", "stem" to "a", "ext" to "csv", "orderId" to "77"), ctx.attributes)
            assertEquals(ChainResult.Rejected("extract: 123-order.csv does not match ^(?<x>[a-z]+)$"), run(processorFor(ProcessorSpec.Extract(ExtractFrom.FileName, regex = "^(?<x>[a-z]+)$")) { null }, payload, ctx))
            assertEquals(ChainResult.Rejected("extract: /order/nope is absent from order.json"), run(processorFor(ProcessorSpec.Extract(ExtractFrom.Content, json = mapOf("x" to "/order/nope"))) { null }, input("in/order.json"), ctx))
        }
    }

    @Test
    fun verifyDigest_passes_a_matching_expected_value_and_rejects_a_mismatch_or_a_missing_one() = runTest {
        val verify = processorFor(ProcessorSpec.VerifyDigest("expectedMd5")) { null }
        ctx().use { ctx ->
            val payload = input()
            assertEquals(ChainResult.Rejected("verifyDigest: attribute expectedMd5 is not set"), run(verify, payload, ctx))
            ctx.setAttribute("expectedMd5", payload.objects.single().digest.hex.uppercase())
            assertTrue(run(verify, payload, ctx) is ChainResult.Done)
            ctx.setAttribute("expectedMd5", "00")
            assertEquals(ChainResult.Rejected("verifyDigest: 123-order.csv digest ${payload.objects.single().digest.hex} does not match expected 00"), run(verify, payload, ctx))
        }
    }

    @Test
    fun a_custom_processor_resolves_by_name_and_an_unknown_name_fails_at_construction() {
        val mine = object : Processor {
            override val produces = setOf("x")
            override suspend fun process(payload: Payload, ctx: ProcessContext) = Outcome.Continue(payload)
        }
        assertEquals(mine, processorFor(ProcessorSpec.Custom("mine")) { if (it.name == "mine") mine else null })
        assertThrows(IllegalArgumentException::class.java) { processorFor(ProcessorSpec.Custom("nope")) { null } }
    }

    private fun messageCtx(body: String) = FakeProcessContext(dir, fetcher, clock, source = SourceView("images", body.toByteArray()))

    @Test
    fun extract_from_message_sets_attributes_from_the_message_body_by_regex_or_json_and_rejects_a_message_without_one() = runTest {
        messageCtx("""{"batchId":"b7","subject":"images.ready"}""").use { ctx ->
            val payload = input()
            assertTrue(run(processorFor(ProcessorSpec.Extract(ExtractFrom.Message, json = mapOf("batchId" to "/batchId"))) { null }, payload, ctx) is ChainResult.Done)
            assertTrue(run(processorFor(ProcessorSpec.Extract(ExtractFrom.Message, regex = "\"subject\":\"(?<subject>[^\"]+)\"")) { null }, payload, ctx) is ChainResult.Done)
            assertEquals(mapOf("batchId" to "b7", "subject" to "images.ready"), ctx.attributes)
            assertEquals(ChainResult.Rejected("extract: /nope is absent from the message"), run(processorFor(ProcessorSpec.Extract(ExtractFrom.Message, json = mapOf("x" to "/nope"))) { null }, payload, ctx))
        }
        ctx().use { ctx ->
            assertEquals(ChainResult.Rejected("extract: the message has no body"), run(processorFor(ProcessorSpec.Extract(ExtractFrom.Message, json = mapOf("x" to "/x"))) { null }, input(), ctx))
        }
    }

    @Test
    fun expand_fetches_one_child_per_path_listed_in_a_json_metadata_file_or_in_the_message_and_rejects_an_absent_or_empty_list() = runTest {
        fetcher.file("sets/set.json", """{"images":[{"path":"img/1.png"},{"path":"img/2.png"}],"none":[]}""".toByteArray())
            .file("img/1.png", "one".toByteArray()).file("img/2.png", "two".toByteArray())
        ctx().use { ctx ->
            val done = run(processorFor(ProcessorSpec.Expand(ExpandFormat.Json, "/images[*].path", "minio")) { null }, input("sets/set.json"), ctx) as ChainResult.Done
            assertEquals(listOf("1.png", "2.png"), done.payload.objects.map { it.name })
            assertEquals(listOf("one", "two"), done.payload.objects.map { Files.readString(it.path) })
            assertEquals(done.payload.objects.map { it.path }, ctx.createdFiles, "each child is a file the context owns")
            assertEquals(done.payload.objects.map { Digest.of(it.path, DigestAlgorithm.MD5) }, done.payload.objects.map { it.digest })
            assertEquals(ChainResult.Rejected("expand: /none[*].path lists no paths in set.json"), run(processorFor(ProcessorSpec.Expand(ExpandFormat.Json, "/none[*].path", "minio")) { null }, input("sets/set.json"), ctx))
            assertEquals(ChainResult.Rejected("expand: /nope is absent from set.json or is not a path"), run(processorFor(ProcessorSpec.Expand(ExpandFormat.Json, "/nope", "minio")) { null }, input("sets/set.json"), ctx))
        }
        messageCtx("""{"paths":["img/2.png"]}""").use { ctx ->
            val done = run(processorFor(ProcessorSpec.Expand(ExpandFormat.Message, "/paths", "minio")) { null }, input(), ctx) as ChainResult.Done
            assertEquals(listOf("2.png"), done.payload.objects.map { it.name })
        }
    }
}
