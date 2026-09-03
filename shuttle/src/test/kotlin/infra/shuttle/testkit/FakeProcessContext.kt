package infra.shuttle.testkit

import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.Fetcher
import infra.shuttle.core.Payload
import infra.shuttle.core.ProcessContext
import infra.shuttle.core.RouteName
import infra.shuttle.core.SourceView
import infra.shuttle.core.StagedObject
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferView
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock

/**
 * Spec 6.2 over a test's temp directory. `newStagedFile` allocates inside [dir] and tracks the path in
 * [createdFiles]; `close` deletes them (I18's second half). `snapshot` fingerprints the inputs before a
 * chain runs and `inputsUntouched` tells whether a processor wrote into one (I18's first half).
 */
class FakeProcessContext(
    private val dir: Path,
    private val fetcher: Fetcher,
    override val clock: Clock,
    override val transfer: TransferView = TransferView(TransferId(1), RouteName("drop"), ScriptedSource.identity("a.csv"), "a.csv", clock.instant(), null),
    override val source: SourceView = SourceView(transfer.sourcePath),
    private val algorithm: DigestAlgorithm = DigestAlgorithm.MD5,
) : ProcessContext, AutoCloseable {
    override val attributes = LinkedHashMap<String, String>()
    val createdFiles = mutableListOf<Path>()
    private var inputs = emptyMap<Path, Pair<Long, String>>()

    override fun setAttribute(name: String, value: String) {
        attributes[name] = value
    }

    override fun newStagedFile(name: String): Path = dir.resolve("${createdFiles.size}-$name").also { createdFiles.add(it) }

    override suspend fun fetch(store: String, path: String): StagedObject = fetcher(path, newStagedFile(path.substringAfterLast('/')), algorithm)

    fun snapshot(payload: Payload) {
        inputs = payload.objects.associate { it.path to fingerprint(it.path) }
    }

    fun inputsUntouched(): Boolean = inputs.all { (path, before) -> fingerprint(path) == before }

    override fun close() = createdFiles.forEach { Files.deleteIfExists(it) }

    private fun fingerprint(path: Path) = Files.readAllBytes(path).let { it.size.toLong() to digestOf(it, DigestAlgorithm.MD5).hex }
}
