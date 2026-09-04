package infra.shuttle.quarkus

import com.fasterxml.jackson.databind.ObjectMapper
import infra.shuttle.core.ChainResult
import infra.shuttle.core.Digest
import infra.shuttle.core.Fetcher
import infra.shuttle.core.MappingFailure
import infra.shuttle.core.MappingRenderer
import infra.shuttle.core.Outcome
import infra.shuttle.core.Payload
import infra.shuttle.core.ProcessingChain
import infra.shuttle.core.ProcessorSpec
import infra.shuttle.core.RouteName
import infra.shuttle.core.SftpStore
import infra.shuttle.core.Source
import infra.shuttle.core.SourceIdentity
import infra.shuttle.core.SourceKind
import infra.shuttle.core.SourceView
import infra.shuttle.core.StageError
import infra.shuttle.core.StagedObject
import infra.shuttle.core.StagingContext
import infra.shuttle.core.TargetRef
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferId
import infra.shuttle.core.TransferKind
import infra.shuttle.core.TransferState
import infra.shuttle.core.TransferView
import infra.shuttle.core.keyLeavesTarget
import infra.shuttle.core.of
import infra.shuttle.core.processorFor
import infra.shuttle.core.targetKey
import infra.shuttle.yaml.YamlLoader
import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import kotlinx.coroutines.runBlocking
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import infra.shuttle.core.HttpChannel as HttpChannelConfig

/**
 * Spec 12.2: `shuttle validate <files>`. Steps 1 and 5 of startup and nothing else: the files become a
 * configuration, every rule is judged, every named bean is asked for. It holds no client of any kind,
 * so it cannot connect to anything. Exit code 0 only when the report is clean.
 */
class ValidateCommand(
    private val files: List<Path>,
    private val env: Map<String, String>,
    private val beans: (String) -> Set<String>?,
    private val out: PrintStream,
) {
    fun run(): Int {
        val report = YamlLoader.validate(files, env, beans)
        report.errors.forEach { out.println("error: $it") }
        report.violations.forEach { out.println("rule ${it.rule}: ${it.message}") }
        if (report.ok) out.println("ok: ${files.joinToString()}")
        return if (report.ok) 0 else 1
    }
}

/**
 * Spec 12.2 and D35: `shuttle try --route <name> --file-name <name> [--source-path <path>] [--content <file>]
 * [--message <file>]`. Validates, then runs the route's chain over the sample in a temp directory, printing the
 * attributes each step set, the key the target would use, and the body rendered for every channel the route
 * notifies. What runs is the deployment's own `ProcessingChain` and `StagingContext`, observed step by step,
 * so rule 22, the digests recomputed for the files the chain created and the target key are all the running
 * route's; the two verdicts the pipeline reaches between the chain and the store are printed here too.
 * Rule 17 is judged twice: statically at validate, and again against the attributes the chain actually set,
 * which is where a sample name the regex does not match shows up. Connects to nothing, stores nothing:
 * `expand` fetches its children from the sample files beside `--content`, and a custom processor that reaches
 * a network fails here first, which is the rule anyway.
 */
class TryCommand(
    private val files: List<Path>,
    private val env: Map<String, String>,
    private val beans: NamedBeans,
    private val out: PrintStream,
    private val clock: Clock,
    private val route: String,
    private val fileName: String,
    private val sourcePath: String? = null,
    private val content: Path? = null,
    private val message: Path? = null,
) {
    fun run(): Int {
        val report = YamlLoader.validate(files, env, beans::produces)
        report.errors.forEach { out.println("error: $it") }
        report.violations.forEach { out.println("rule ${it.rule}: ${it.message}") }
        if (report.errors.isNotEmpty()) return 1
        val config = YamlLoader.load(files.map { Files.readString(it) }, env)
        val route = config.routes.firstOrNull { it.name == route } ?: run { out.println("no route named $route"); return 1 }
        val dir = Files.createTempDirectory("shuttle-try")
        try {
            val algorithm = route.digest ?: config.digest
            val staged = dir.resolve(fileName.substringAfterLast('/'))
            if (content != null) Files.copy(content, staged) else Files.createFile(staged)
            val sample = StagedObject(staged.fileName.toString(), staged, Files.size(staged), clock.instant(), Digest.of(staged, algorithm), null)
            val sourceRef = when (val s = route.source) { is Source.Poll -> "${s.store}:${s.directory}"; is Source.Subscribe -> "${s.channel}:${s.subject}"; null -> "" }
            val path = sourcePath ?: "${(route.source as? Source.Poll)?.directory.orEmpty()}/$fileName"
            val identity = SourceIdentity(RouteName(route.name), if (route.source is Source.Subscribe) SourceKind.NATS else SourceKind.SFTP, sourceRef, fileName, sample.size, sample.mtime)
            // every declared store fetches from the samples beside the sample content: `expand` works, nothing connects
            val samples = (content ?: message)?.toAbsolutePath()?.parent ?: Path.of("")
            val ctx = StagingContext(
                TransferView(TransferId(0), RouteName(route.name), identity, path, clock.instant(), null),
                SourceView(path, message?.let { Files.readAllBytes(it) }),
                dir, clock, algorithm, sampleFetcher(samples).let { f -> config.objectStores.associate { it.name to f } },
            )

            val chain = ProcessingChain(route.process.map { spec -> processorFor(spec) { beans.processor(it.name) } }, algorithm)
            var stepRejected = false
            val result = try {
                runBlocking {
                    chain.run(Payload(listOf(sample)), ctx) { i, set, outcome ->
                        val step = "step ${i + 1} ${route.process[i].word()}"
                        when (outcome) {
                            is Outcome.Reject -> { out.println("$step: REJECT ${outcome.reason}"); stepRejected = true }
                            is Outcome.Continue -> out.println("$step: attributes $set" + outcome.payload.objects.map { it.name }.let { if (it != listOf(sample.name)) " objects $it" else "" })
                        }
                    }
                }
            } catch (e: StageError) {
                out.println("error: ${e.message}")
                return 1
            }
            val done = when (result) {
                is ChainResult.Rejected -> { if (!stepRejected) out.println("reject: ${result.reason}"); return 1 }
                is ChainResult.Done -> result
            }

            val bodies = config.channels.filterIsInstance<HttpChannelConfig>().associateBy { it.name }
            val notified = route.notify.mapNotNull { n -> bodies[n.channel]?.let { n to it } }
            val violations = notified.flatMap { (n, channel) ->
                MappingRenderer.check(channel.body, done.attributes.keys) { beans.provider(it) != null }.filter { it.rule == 17 }.map { "rule ${it.rule}: channel ${n.channel} ${it.message}" }
            }
            violations.forEach(out::println)

            // the two verdicts the pipeline reaches between the chain and the store (spec 6.1 and rule 13's run-time half)
            val objects = done.payload.objects
            if (objects.isEmpty()) { out.println("reject: process: the chain left no object to store"); return 1 }
            val keys = objects.map { targetKey(route.target, it.name, identity.sourceName, done.attributes, clock) }
            keys.firstOrNull(::keyLeavesTarget)?.let { out.println("reject: key: $it leaves the target directory"); return 1 }

            val target = route.target
            val kind = if (config.objectStores.firstOrNull { it.name == target?.store } is SftpStore) "sftp" else "s3"
            val renderer = MappingRenderer(beans::provider)
            for ((i, stored) in objects.withIndex()) {
                out.println("key: ${keys[i]}")
                val transfer = Transfer(
                    id = TransferId(0), identity = identity, kind = if (route.source is Source.Subscribe) TransferKind.MESSAGE else TransferKind.OBJECT,
                    state = TransferState.ACKED, sourceDigest = sample.digest, digest = stored.digest, storedName = stored.name,
                    storedMtime = stored.mtime, attributes = done.attributes,
                    target = TargetRef(kind, target?.bucket ?: target?.directory ?: "", keys[i], null, stored.size),
                    firstSeenAt = clock.instant(), updatedAt = clock.instant(), ackedAt = clock.instant(),
                )
                for ((n, channel) in notified) {
                    out.println("body ${n.channel} (${n.on.name.lowercase()}):")
                    out.println(runCatching { runBlocking { json.writerWithDefaultPrettyPrinter().writeValueAsString(renderer.render(channel.body, transfer, n.on)) } }
                        .getOrElse { if (it is MappingFailure) "FAILED: ${it.message}" else throw it })
                }
            }
            return if (report.ok && violations.isEmpty()) 0 else 1
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    private fun ProcessorSpec.word() = this::class.simpleName!!.lowercase()

    /**
     * Try mode connects to nothing, so `expand`'s listed paths are looked up by file name in [samples] - the
     * directory the sample content came from. A path with no sample beside it is the operator's missing input.
     */
    private fun sampleFetcher(samples: Path): Fetcher = { path, into, algorithm ->
        val file = samples.resolve(path.substringAfterLast('/'))
        if (!Files.isRegularFile(file)) throw IllegalStateException("no sample file $file for $path")
        Files.copy(file, into)
        StagedObject(file.fileName.toString(), into, Files.size(into), clock.instant(), Digest.of(into, algorithm), null)
    }

    companion object {
        private val json = ObjectMapper()

        /** `--route`, `--file-name`, `--source-path`, `--content`, `--message` in any order. */
        fun parse(args: List<String>, env: Map<String, String>, beans: NamedBeans, out: PrintStream, clock: Clock): TryCommand? {
            val options = HashMap<String, String>()
            val files = ArrayList<Path>()
            var i = 0
            while (i < args.size) {
                val a = args[i]
                if (a.startsWith("--")) { options[a.removePrefix("--")] = args.getOrNull(i + 1) ?: return null; i += 2 } else { files.add(Path.of(a)); i++ }
            }
            val route = options["route"] ?: return null
            val fileName = options["file-name"] ?: return null
            return TryCommand(files, env, beans, out, clock, route, fileName, options["source-path"], options["content"]?.let(Path::of), options["message"]?.let(Path::of))
        }
    }
}

/**
 * The process entry point: `shuttle validate <files>`, `shuttle try <files> --route ... --file-name ...`, or nothing
 * for the service. The mode is put where `ShuttleLifecycle` reads it before Quarkus boots, so a validate never
 * starts the host, and the container is up for both commands because rule 15 asks it what a name resolves to.
 */
@QuarkusMain
class ShuttleMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            System.setProperty("shuttle.mode", args.firstOrNull() ?: "serve")
            Quarkus.run(ShuttleApp::class.java, *args)
        }
    }
}

class ShuttleApp : QuarkusApplication {
    override fun run(vararg args: String): Int = when (args.firstOrNull()) {
        "validate" -> ValidateCommand(args.drop(1).map(Path::of), environment(), cdiBeans()::produces, System.out).run()
        "try" -> TryCommand.parse(args.drop(1), environment(), cdiBeans(), System.out, Clock.systemUTC())?.run()
            ?: run { System.err.println("usage: shuttle try <files> --route <name> --file-name <name> [--source-path <path>] [--content <file>] [--message <file>]"); 2 }
        else -> { Quarkus.waitForExit(); 0 }
    }
}
