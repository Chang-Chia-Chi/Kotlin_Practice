package infra.shuttle.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import kotlin.coroutines.cancellation.CancellationException

/** A processor threw (spec 11): a retryable stage error the pipeline turns into `failedAttempt`. */
class StageError(stage: String, cause: Throwable) : RuntimeException("$stage: ${cause.message}", cause)

/** Spec 6.4: at attribute freeze a notified channel's table could not be satisfied; FAILED with no retry until re-drive. */
class FreezeFailure(message: String) : RuntimeException(message)

/**
 * What one step of the chain did, seen from outside: its position, the attributes it set or changed, and
 * its outcome. `shuttle try` prints these (D35); a running route observes nothing, which is the default.
 */
fun interface StepObserver {
    fun stepped(index: Int, set: Map<String, String>, outcome: Outcome)
}

sealed interface ChainResult {
    /** The final payload, digests recomputed for new files, and the attributes frozen (I15). */
    data class Done(val payload: Payload, val attributes: Map<String, String>) : ChainResult
    data class Rejected(val reason: String) : ChainResult
}

/**
 * Spec 6.2: runs the processors in order under the four re-run rules. Inputs are never touched here
 * (the kit proves a processor did not either); nothing leaves staging; every object whose file the
 * chain created gets its digest and size recomputed from the bytes; the final cardinality is the
 * caller's to turn into rows. Rule 22 is enforced on the frozen attributes.
 *
 * Spec 3.3 and plan 2.5: [io] is the module's one bounded view of `Dispatchers.IO`, sized to the sum of
 * route parallelism, and the whole run happens on it - archive writing, the `unzip` and `extract` reads,
 * and the digest and size recomputed at the end. D52: a processor is *called* on that view and may block
 * where it stands; a custom processor must not hop to a dispatcher of its own for blocking work, because
 * rule 9's arithmetic only bounds the module's blocking calls while every one of them is on this view.
 */
class ProcessingChain(
    private val processors: List<Processor>,
    private val algorithm: DigestAlgorithm,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {

    suspend fun run(payload: Payload, ctx: ProcessContext, observe: StepObserver = NOTHING): ChainResult = withContext(io) {
        val inputs = payload.objects.map { it.path }.toSet()
        var current = payload
        for ((index, processor) in processors.withIndex()) {
            val before = ctx.attributes.toMap()
            val outcome = try {
                processor.process(current, ctx)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                throw StageError(processor::class.simpleName ?: "processor", e)
            }
            observe.stepped(index, ctx.attributes.filter { (k, v) -> before[k] != v }, outcome)
            when (outcome) {
                is Outcome.Reject -> return@withContext ChainResult.Rejected(outcome.reason)
                is Outcome.Continue -> current = outcome.payload
            }
        }
        val attributes = java.util.Collections.unmodifiableMap(LinkedHashMap(ctx.attributes)) // frozen (I15)
        attributeLimitBroken(attributes)?.let { return@withContext ChainResult.Rejected(it) }
        val objects = current.objects.map { if (it.path in inputs) it else it.copy(size = Files.size(it.path), digest = Digest.of(it.path, algorithm)) }
        ChainResult.Done(Payload(objects), attributes)
    }

    /** Rule 22 at run time: at most 32 names, each at most 64 characters, 1 KB in all (spec 6.4). */
    private fun attributeLimitBroken(attributes: Map<String, String>): String? = when {
        attributes.size > 32 -> "rule 22: ${attributes.size} attributes set; at most 32"
        attributes.keys.any { it.length > 64 } -> "rule 22: attribute name ${attributes.keys.first { it.length > 64 }} is longer than 64 characters"
        attributes.entries.sumOf { it.key.length + it.value.length } > 1024 -> "rule 22: attributes exceed 1 KB"
        else -> null
    }

    companion object {
        private val NOTHING = StepObserver { _, _, _ -> }

        /**
         * Spec 6.4, at attribute freeze: every notified channel's table checked against the frozen
         * attributes. A row reading an attribute that is not set, with no default and `required`,
         * fails the transfer before the store, naming the row and the attribute; the boot-time row
         * checks (rules 15, 16, 18, 19, 21) are repeated so a table changed since boot fails here too.
         */
        fun checkMappings(attributes: Map<String, String>, tables: Collection<MappingTable>, providerExists: (String) -> Boolean) {
            for (table in tables) {
                MappingRenderer.check(table, null, providerExists).firstOrNull()?.let { throw FreezeFailure(it.message) }
                table.rows.firstOrNull { it.attribute != null && it.required && it.default == null && attributes[it.attribute].isNullOrBlank() }
                    ?.let { throw FreezeFailure("mapping row ${it.path}: attribute ${it.attribute} is required and not set") }
            }
        }
    }
}

/** Streams a staged file through the algorithm (spec 6.5); the fetch adapters and the chain share it. */
fun Digest.Companion.of(path: Path, algorithm: DigestAlgorithm): Digest {
    val jca = when (algorithm) { DigestAlgorithm.MD5 -> "MD5"; DigestAlgorithm.SHA256 -> "SHA-256"; DigestAlgorithm.SHA1 -> "SHA-1" }
    val md = java.security.MessageDigest.getInstance(jca)
    Files.newInputStream(path).use { input ->
        val buffer = ByteArray(64 * 1024)
        while (true) { val n = input.read(buffer); if (n < 0) break; md.update(buffer, 0, n) }
    }
    return Digest(algorithm, java.util.HexFormat.of().formatHex(md.digest()))
}
