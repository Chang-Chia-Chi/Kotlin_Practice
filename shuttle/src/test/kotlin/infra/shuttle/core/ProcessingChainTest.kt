package infra.shuttle.core

import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.FakeProcessContext
import infra.shuttle.testkit.ScriptedFetcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/** Spec 6.2: the chain runner and its four re-run rules, against the test kit. */
class ProcessingChainTest {
    @TempDir lateinit var dir: Path
    private val clock = ClockFixture()
    private val fetcher = ScriptedFetcher(clock).file("in/123-order.csv", "a,b\n1,2\n".toByteArray())
    private fun ctx() = FakeProcessContext(dir, fetcher, clock)
    private suspend fun input() = Payload(listOf(fetcher("in/123-order.csv", dir.resolve("input"), DigestAlgorithm.MD5)))

    /** A processor that records its position and passes the payload on. */
    private fun step(log: MutableList<String>, name: String) = object : Processor {
        override val produces = emptySet<String>()
        override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome { log += name; return Outcome.Continue(payload) }
    }

    @Test
    fun a_chain_of_three_runs_in_order_and_returns_the_payload_with_frozen_attributes() = runTest {
        val log = mutableListOf<String>()
        val chain = ProcessingChain(listOf(step(log, "a"), step(log, "b"), step(log, "c")), DigestAlgorithm.MD5)
        ctx().use { ctx ->
            ctx.setAttribute("k", "v")
            val result = chain.run(input(), ctx) as ChainResult.Done
            assertEquals(listOf("a", "b", "c"), log)
            assertEquals(1, result.payload.objects.size)
            assertEquals(mapOf("k" to "v"), result.attributes)
        }
    }

    @Test
    fun a_reject_ends_the_chain_with_its_reason_and_later_processors_never_run() = runTest {
        val log = mutableListOf<String>()
        val rejecting = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext) = Outcome.Reject("bad header")
        }
        val chain = ProcessingChain(listOf(step(log, "a"), rejecting, step(log, "c")), DigestAlgorithm.MD5)
        ctx().use { ctx ->
            assertEquals(ChainResult.Rejected("bad header"), chain.run(input(), ctx))
            assertEquals(listOf("a"), log)
        }
    }

    @Test
    fun a_processor_throwing_is_a_retryable_stage_error_carrying_the_cause() = runTest {
        val boom = IOException("disk")
        val throwing = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = throw boom
        }
        ctx().use { ctx ->
            val error = try {
                ProcessingChain(listOf(throwing), DigestAlgorithm.MD5).run(input(), ctx)
                throw AssertionError("expected a StageError")
            } catch (e: StageError) {
                e
            }
            assertSame(boom, error.cause)
        }
    }

    @Test
    fun I18_a_processor_never_modifies_an_input_and_every_created_file_is_deleted_with_staging() = runTest {
        val copying = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
                val out = ctx.newStagedFile("copy.csv")
                Files.write(out, Files.readAllBytes(payload.objects.single().path) + "3,4\n".toByteArray())
                return Outcome.Continue(Payload(listOf(payload.objects.single().copy(name = "copy.csv", path = out))))
            }
        }
        val writing = object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
                Files.writeString(payload.objects.single().path, "x,y\n9,9\n")
                return Outcome.Continue(payload)
            }
        }
        val payload = input()
        val ctx = ctx()
        ctx.snapshot(payload)
        val result = ProcessingChain(listOf(copying), DigestAlgorithm.MD5).run(payload, ctx) as ChainResult.Done
        assertTrue(ctx.inputsUntouched())
        val created = ctx.createdFiles.single()
        // the new file's digest and size are the pipeline's job, recomputed from its bytes
        assertEquals(Files.size(created), result.payload.objects.single().size)
        assertEquals(Digest.of(created, DigestAlgorithm.MD5), result.payload.objects.single().digest)
        assertFalse(result.payload.objects.single().digest == payload.objects.single().digest)
        ctx.close()
        assertFalse(Files.exists(created))

        val other = ctx()
        other.snapshot(payload)
        ProcessingChain(listOf(writing), DigestAlgorithm.MD5).run(payload, other)
        assertFalse(other.inputsUntouched(), "the kit detects a processor writing into its input")
        assertInstanceOf(Path::class.java, payload.objects.single().path)
    }
}
