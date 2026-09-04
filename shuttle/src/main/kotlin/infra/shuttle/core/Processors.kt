package infra.shuttle.core

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

/** One mapper for every built-in that reads JSON: extract, expand and the pipeline's `fetch.path`. */
internal val JSON = ObjectMapper()

/**
 * Spec 6.3: configuration to behaviour. `custom` resolves through the injected lookup (CDI in the host,
 * a map in tests); an unknown name is a configuration error here, which rule 15 has already refused at boot.
 */
fun processorFor(spec: ProcessorSpec, custom: (ProcessorSpec.Custom) -> Processor?): Processor = when (spec) {
    ProcessorSpec.Quality -> QualityProcessor { if (Files.size(it.path) == 0L) "${it.name} is empty" else null }
    is ProcessorSpec.Rename -> RenameProcessor(spec)
    ProcessorSpec.Zip -> ZipProcessor()
    is ProcessorSpec.Unzip -> UnzipProcessor(spec)
    is ProcessorSpec.Extract -> ExtractProcessor(spec)
    is ProcessorSpec.Expand -> ExpandProcessor(spec)
    is ProcessorSpec.VerifyDigest -> VerifyDigestProcessor(spec)
    is ProcessorSpec.Custom -> custom(spec) ?: throw IllegalArgumentException("no custom processor named ${spec.name}")
}

/** Spec 6.3 `quality` (D11): the check answers a reason to reject, or null to pass. */
class QualityProcessor(private val check: (StagedObject) -> String?) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome =
        payload.objects.firstNotNullOfOrNull(check)?.let { Outcome.Reject("quality: $it") } ?: Outcome.Continue(payload)
}

/** Spec 6.3 `rename`: `{name}`, `{sourceName}`, `{yyyyMMdd}` (any date pattern) and attribute names; same file, new name. */
class RenameProcessor(spec: ProcessorSpec.Rename) : Processor {
    private val pattern = spec.pattern
    override val produces = emptySet<String>()

    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome = Outcome.Continue(Payload(payload.objects.map { o ->
        o.copy(name = expandPattern(pattern, o.name, ctx.transfer.identity.sourceName, ctx.attributes, ctx.clock))
    }))
}

/** Rule 13's vocabulary, shared by `rename` and the target key: `{name}`, `{sourceName}`, a date pattern in UTC, or an attribute. */
fun expandPattern(pattern: String, name: String, sourceName: String, attributes: Map<String, String>, clock: java.time.Clock): String =
    TOKEN.replace(pattern) { m ->
        val token = m.groupValues[1]
        when {
            token == "name" -> name
            token == "sourceName" -> sourceName
            DATE.matches(token) -> DateTimeFormatter.ofPattern(token).format(clock.instant().atOffset(ZoneOffset.UTC))
            else -> attributes[token] ?: throw IllegalStateException("pattern $pattern: attribute $token is not set")
        }
    }

/**
 * Spec 7.1: the key one object of the final payload is stored under. A `bucket` or a `directory` is
 * *where* the store puts it, never part of the key - the pipeline and `shuttle try` both resolve it here,
 * so an operator's offline key is the key the target gets (D35).
 */
fun targetKey(target: Target?, name: String, sourceName: String, attributes: Map<String, String>, clock: java.time.Clock): String =
    expandPattern(target?.key ?: "{name}", name, sourceName, attributes, clock)

/**
 * Rule 13 at run time. The rule judges the *pattern* at boot, and `{name}` is the one part of a key
 * that the pattern does not carry: an unzip entry named `../../escaped.txt`, a rename, or an attribute
 * holding such a segment puts it into the resolved key instead. A `..` segment names a path outside the
 * target directory, which a file system target would happily write to (an S3 key is opaque, so it is a
 * no-op there).
 */
fun keyLeavesTarget(key: String): Boolean = key.split('/').any { it == ".." }

private val TOKEN = Regex("""\{([^}]+)}""")
private val DATE = Regex("[yMdHmsS]+")

/** Spec 6.3 `zip`: every object into one archive named after the first, created through the context. */
class ZipProcessor : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
        val first = payload.objects.first()
        val archive = ctx.newStagedFile("${first.name}.zip")
        ZipOutputStream(Files.newOutputStream(archive)).use { zip ->
            for (o in payload.objects) { zip.putNextEntry(ZipEntry(o.name)); Files.copy(o.path, zip); zip.closeEntry() }
        }
        return Outcome.Continue(Payload(listOf(first.copy(name = "${first.name}.zip", path = archive, size = Files.size(archive), mtime = ctx.clock.instant(), contentType = "application/zip"))))
    }
}

/** Spec 6.3 `unzip` (D41): one object per entry; past `maxEntries` or `maxBytes` uncompressed the read stops and the transfer is rejected. */
class UnzipProcessor(private val spec: ProcessorSpec.Unzip) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
        val archive = payload.objects.single()
        val out = mutableListOf<StagedObject>()
        var total = 0L
        ZipInputStream(Files.newInputStream(archive.path)).use { zip ->
            while (true) {
                val entry = zip.nextEntry ?: break
                if (entry.isDirectory) continue
                if (out.size == spec.maxEntries) return Outcome.Reject("unzip: ${archive.name} has more than maxEntries ${spec.maxEntries} entries (${out.size + 1} seen)")
                val file = ctx.newStagedFile(entry.name.substringAfterLast('/'))
                Files.newOutputStream(file).use { sink ->
                    val buffer = ByteArray(64 * 1024)
                    while (true) {
                        val n = zip.read(buffer); if (n < 0) break
                        total += n
                        if (total > spec.maxBytes) return Outcome.Reject("unzip: ${archive.name} exceeds maxBytes ${spec.maxBytes} uncompressed")
                        sink.write(buffer, 0, n)
                    }
                }
                out += archive.copy(name = entry.name, path = file, size = Files.size(file), mtime = entry.lastModifiedTime?.toInstant() ?: ctx.clock.instant(), contentType = null)
            }
        }
        return Outcome.Continue(Payload(out))
    }
}

/** Spec 6.3 `extract` from the file name, the source path, the content or the message body; named or positional regex groups, or JSON pointers. */
class ExtractProcessor(private val spec: ProcessorSpec.Extract) : Processor {
    private val regex = spec.regex?.let(::Regex)
    override val produces = spec.produces

    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
        val o = payload.objects.first()
        val subject = when (spec.from) {
            ExtractFrom.FileName -> o.name
            ExtractFrom.SourcePath -> ctx.transfer.sourcePath
            ExtractFrom.Content -> Files.readString(o.path)
            ExtractFrom.Message -> ctx.source.body?.decodeToString() ?: return Outcome.Reject("extract: the message has no body")
        }
        val where = when (spec.from) { ExtractFrom.Content -> o.name; ExtractFrom.Message -> "the message"; else -> subject }
        if (regex != null) {
            val match = regex.find(subject) ?: return Outcome.Reject("extract: $where does not match ${spec.regex}")
            val names = spec.into ?: produces.toList()
            val values = if (spec.into != null) match.groupValues.drop(1) else names.map { match.groups[it]?.value ?: "" }
            names.zip(values).forEach { (n, v) -> ctx.setAttribute(n, v) }
        }
        spec.json?.let { pointers ->
            val tree = JSON.readTree(subject)
            for ((name, pointer) in pointers) {
                val node = tree.at(pointer)
                if (node.isMissingNode || node.isNull) return Outcome.Reject("extract: $pointer is absent from $where")
                ctx.setAttribute(name, if (node.isValueNode) node.asText() else node.toString())
            }
        }
        return Outcome.Continue(payload)
    }
}

/**
 * Spec 6.3 `expand`: one child per listed path, each fetched from `from` through the context. `format: json`
 * reads the current object, `format: message` the subscription message; `files` is a JSON pointer whose one
 * `[*]` walks an array (`/images[*].path`, `/paths[*]`, or `/paths` for an array of strings). Nothing listed,
 * or a pointer that lands on something other than a string, is a Reject: the metadata is the bad input.
 */
class ExpandProcessor(private val spec: ProcessorSpec.Expand) : Processor {
    private val pointer = expandPointer(spec.files)
    override val produces = emptySet<String>()

    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
        val (bytes, where) = when (spec.format) {
            ExpandFormat.Json -> payload.objects.single().let { Files.readAllBytes(it.path) to it.name }
            ExpandFormat.Message -> (ctx.source.body ?: return Outcome.Reject("expand: the message has no body")) to "the message"
        }
        val (head, tail) = pointer
        val listed = JSON.readTree(bytes).at(head)
        val nodes = if (listed.isArray) listed.toList() else listOf(listed)
        val paths = nodes.map { n -> n.at(tail).takeIf { it.isTextual }?.asText() ?: return Outcome.Reject("expand: ${spec.files} is absent from $where or is not a path") }
        if (paths.isEmpty()) return Outcome.Reject("expand: ${spec.files} lists no paths in $where")
        return Outcome.Continue(Payload(paths.map { ctx.fetch(spec.from, it) }))
    }
}

/** `expand.files` split at its `[*]`: the pointer to the array and the pointer into each element (`.path` reads as `/path`); rule 14 checks both. */
internal fun expandPointer(files: String): Pair<String, String> =
    files.substringBefore("[*]") to files.substringAfter("[*]", "").let { if (it.startsWith(".")) it.replace('.', '/') else it }

/** Spec 6.5 `verifyDigest`: the expected value comes from an attribute; the transport computed, the application compares. */
class VerifyDigestProcessor(private val spec: ProcessorSpec.VerifyDigest) : Processor {
    override val produces = emptySet<String>()
    override suspend fun process(payload: Payload, ctx: ProcessContext): Outcome {
        val expected = ctx.attributes[spec.attribute] ?: return Outcome.Reject("verifyDigest: attribute ${spec.attribute} is not set")
        payload.objects.firstOrNull { !it.digest.hex.equals(expected, ignoreCase = true) }
            ?.let { return Outcome.Reject("verifyDigest: ${it.name} digest ${it.digest.hex} does not match expected $expected") }
        return Outcome.Continue(payload)
    }
}
