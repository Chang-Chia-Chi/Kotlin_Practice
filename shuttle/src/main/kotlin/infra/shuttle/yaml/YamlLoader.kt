package infra.shuttle.yaml

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import infra.shuttle.core.AckAction
import infra.shuttle.core.Backoff
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryPolicy
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.ExtractFrom
import infra.shuttle.core.FileReadiness
import infra.shuttle.core.HostKey
import infra.shuttle.core.MappingRow
import infra.shuttle.core.MappingTable
import infra.shuttle.core.MappingType
import infra.shuttle.core.ProcessorSpec
import infra.shuttle.core.channel
import infra.shuttle.core.HttpAuth
import infra.shuttle.core.HttpChannelBuilder
import infra.shuttle.core.HttpMethod
import infra.shuttle.core.Readiness
import infra.shuttle.core.Report
import infra.shuttle.core.RouteBuilder
import infra.shuttle.core.Rules
import infra.shuttle.core.S3Credentials
import infra.shuttle.core.S3StoreBuilder
import infra.shuttle.core.S3Timeouts
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStoreBuilder
import infra.shuttle.core.ShuttleBuilder
import infra.shuttle.core.ShuttleConfig
import infra.shuttle.core.Target
import infra.shuttle.core.objectStore
import infra.shuttle.core.shuttle
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration

/** A document that is not a configuration: every problem found, each naming its YAML path. */
class YamlLoadException(val errors: List<String>) : RuntimeException(errors.joinToString("; "))

/** Spec 13.1 onto the spec 13.2 builders, so defaults live in one place. `${VAR}` comes from `env`; nothing here connects. */
object YamlLoader {
    private val yaml = YAMLMapper()

    fun load(text: String, env: Map<String, String>): ShuttleConfig = load(listOf(text), env)

    /** Several files deep-merge in order, later keys winning, so a site file can complete a base file. */
    fun load(texts: List<String>, env: Map<String, String>): ShuttleConfig {
        val merged = texts.map { yaml.readTree(BARE_REFERENCE.replace(it, "\"$1\"")) }.reduce(::merge)
        val errors = mutableListOf<String>()
        val root = Node(merged, "", env, errors)
        val config = shuttle { root.obj("shuttle")?.let { read(it) } }
        root.done()
        if (errors.isNotEmpty()) throw YamlLoadException(errors)
        return config
    }

    /** Spec 12.2, the pure half: load errors alone when the document never became a configuration, rule numbers otherwise. */
    fun validate(files: List<Path>, env: Map<String, String>, beans: (String) -> Set<String>? = { null }): Report =
        try {
            Rules.validate(load(files.map { Files.readString(it) }, env), beans)
        } catch (e: YamlLoadException) {
            Report(emptyList(), e.errors)
        }

    /**
     * YAML forbids `{` in a plain scalar inside a flow mapping, so spec 13.1's `auth: { user: ${SFTP_USER} }` is not
     * YAML as written; a bare `${VAR}` standing as a whole value is quoted before parsing so the document loads verbatim.
     */
    private val BARE_REFERENCE = Regex("""(?<=[:\[,]\s)(\$\{[A-Za-z_][A-Za-z0-9_]*})(?=\s*[,}\]]|\s*$)""", RegexOption.MULTILINE)

    private fun merge(base: JsonNode, over: JsonNode): JsonNode {
        if (base !is ObjectNode || over !is ObjectNode) return over
        over.fields().forEach { (key, value) -> base.set<JsonNode>(key, base.get(key)?.let { merge(it, value) } ?: value) }
        return base
    }

    private fun ShuttleBuilder.read(n: Node) {
        n.obj("shuttleStateStore")?.obj("oracle")?.str("datasource")?.let { ds -> shuttleStateStore { oracle(ds) } }
        n.obj("notifier")?.let { c -> notifier { c.int("workers")?.let { workers = it }; c.int("batch")?.let { batch = it }; c.dur("sweepEvery")?.let { sweepEvery = it } } }
        n.obj("supervision")?.let { c ->
            supervision {
                c.obj("restartBackoff")?.let { b -> b.dur("initial")?.let { i -> b.dur("max")?.let { m -> restartBackoff(i, m) } } }
                c.word("readiness", "all-routes-down" to Readiness.AllRoutesDown, "any-route-down" to Readiness.AnyRouteDown)?.let { readiness = it }
            }
        }
        n.digest("digest")?.let { digest = it }
        n.dur("drainTimeout")?.let { drainTimeout = it }
        n.obj("objectStores")?.entries()?.forEach { (name, store) ->
            store.one("sftp" to { objectStores { sftp(name) { read(it) } } }, "s3" to { objectStores { s3(name) { read(it) } } })
        }
        n.obj("channels")?.entries()?.forEach { (name, channel) ->
            channel.one(
                "http" to { channels { http(name) { read(it) } } },
                "nats" to { c -> channels { nats(name) { url = c.str("url"); credentials = c.secret("credentials"); subject = c.str("subject") } } },
            )
        }
        n.obj("routes")?.entries()?.forEach { (name, route) -> route(name) { read(route) } }
    }

    private fun SftpStoreBuilder.read(n: Node) {
        n.str("host")?.let { h -> endpoint { host = h; n.int("port")?.let { port = it } } }
        n.obj("auth")?.let { a -> a.secret("user")?.let { u -> a.secret("password")?.let { p -> auth { password(u, p) } } } }
        n.child("hostKey")?.let { k -> if (k.isScalar()) k.word(null, "acceptAll" to HostKey.AcceptAll)?.let { hostKey = it } else k.path("knownHosts")?.let { hostKey = HostKey.Strict(it) } }
        n.dur("keepAlive")?.let { keepAlive = it }
        n.dur("idleTimeout")?.let { idleTimeout = it }
        n.dur("idleCutoff")?.let { idleCutoff = it }
        n.dur("drainTimeout")?.let { drainTimeout = it }
        n.dur("cancelGrace")?.let { cancelGrace = it }
        n.obj("pool")?.let { p -> pool { p.int("maxSize")?.let { maxSize = it }; p.int("maxConcurrentTransfers")?.let { maxConcurrentTransfers = it } } }
        n.obj("staging")?.let { s -> staging { dir = s.path("dir"); s.bytes("minFree")?.let { minFree = it } } }
    }

    private fun S3StoreBuilder.read(n: Node) {
        endpoint = n.str("endpoint")
        n.str("region")?.let { region = it }
        n.bool("pathStyle")?.let { pathStyle = it }
        n.obj("credentials")?.let { c -> c.secret("accessKey")?.let { a -> c.secret("secretKey")?.let { s -> credentials = S3Credentials(a, s) } } }
        n.obj("timeouts")?.let { t ->
            timeouts = S3Timeouts(t.dur("connect") ?: timeouts.connect, t.dur("socket") ?: timeouts.socket, t.dur("apiCall") ?: timeouts.apiCall)
        }
        n.obj("pool")?.let { p -> pool { p.int("maxSize")?.let { maxSize = it }; p.int("maxConcurrentTransfers")?.let { maxConcurrentTransfers = it } } }
    }

    private fun HttpChannelBuilder.read(n: Node) {
        n.word("method", *HttpMethod.entries.map { it.name to it }.toTypedArray())?.let { method = it }
        url = n.str("url")
        n.obj("auth")?.one(
            "bearer" to { b -> b.secretSelf()?.let { auth = HttpAuth.Bearer(it) } },
            "basic" to { b -> b.secret("user")?.let { u -> b.secret("password")?.let { p -> auth = HttpAuth.Basic(u, p) } } },
            "header" to { h -> h.str("name")?.let { name -> h.secret("value")?.let { v -> auth = HttpAuth.Header(name, v) } } },
        )
        n.dur("timeout")?.let { timeout = it }
        n.obj("response")?.let { r -> response { r.statuses("success")?.let { success = it }; r.statuses("retry")?.let { retry = it }; reference = r.str("reference") } }
        n.obj("policy")?.let { p ->
            val backoff = p.obj("backoff")?.let { b -> Backoff(b.dur("initial") ?: policy.backoff.initial, b.dur("max") ?: policy.backoff.max) } ?: policy.backoff
            policy = DeliveryPolicy(p.int("maxAttempts") ?: policy.maxAttempts, p.dur("giveUpAfter") ?: policy.giveUpAfter, backoff)
        }
        n.items("body")?.let { rows -> body = MappingTable(rows.mapNotNull { it.row() }) }
    }

    /** Spec 9.6: one row in the table's own keys; `type` is a word, everything else a string or a flag. */
    private fun Node.row(): MappingRow? = str("path")?.let { path ->
        MappingRow(
            path, str("field"), str("attribute"), str("provider"), str("select"), str("value"),
            word("type", *MappingType.entries.map { it.name.lowercase() to it }.toTypedArray()) ?: MappingType.STRING,
            str("format"), str("default"), bool("trim") ?: false, bool("upper") ?: false, bool("lower") ?: false, bool("required") ?: true, str("digest"),
        )
    }

    private fun RouteBuilder.read(n: Node) {
        n.obj("source")?.one(
            "poll" to { p ->
                p.str("store")?.let { store ->
                    p.str("directory")?.let { directory ->
                        source = poll(objectStore(store), directory) {
                            p.dur("every")?.let { every = it }
                            p.items("readiness")?.mapNotNull { it.readiness() }?.let { readiness = it }
                            p.ack("onAck")?.let { onAck = it }
                            p.ack("onNack")?.let { onNack = it }
                        }
                    }
                }
            },
            "subscribe" to { s ->
                s.str("channel")?.let { ch ->
                    s.str("subject")?.let { subject ->
                        source = subscribe(channel(ch), subject) {
                            s.ack("onAck")?.let { onAck = it }
                            s.ack("onNack")?.let { onNack = it }
                            s.dur("inProgressEvery")?.let { inProgressEvery = it }
                        }
                    }
                }
            },
        )
        n.obj("fetch")?.let { f -> f.str("store")?.let { store -> f.str("path")?.let { path -> fetch(objectStore(store), path) } } }
        n.items("process")?.let { steps -> process = steps.mapNotNull { it.processor() } }
        n.obj("target")?.let { t ->
            t.str("store")?.let { store -> target = Target(store, t.str("bucket"), t.str("directory"), t.str("key") ?: "{name}") }
        }
        n.items("notify")?.forEach { e ->
            e.word("on", *DeliveryMoment.entries.map { it.name.lowercase() to it }.toTypedArray())?.let { on -> e.str("channel")?.let { notify(on, channel(it)) } }
        }
        n.int("parallelism")?.let { parallelism = it }
        n.int("maxAttempts")?.let { maxAttempts = it }
        n.dur("stuckAfter")?.let { stuckAfter = it }
        n.digest("digest")?.let { digest = it }
        n.dur("recheckFinished")?.let { recheckFinished = it }
    }

    private fun Node.digest(key: String) = word(key, *DigestAlgorithm.entries.map { it.name.lowercase() to it }.toTypedArray())

    /** Spec 5.1: `{ sizeStable: { checks, interval } }` or `{ minAge: 1m }`. */
    private fun Node.readiness(): FileReadiness? {
        var check: FileReadiness? = null
        one(
            "sizeStable" to { s -> check = FileReadiness.SizeStable().let { d -> FileReadiness.SizeStable(s.int("checks") ?: d.checks, s.dur("interval") ?: d.interval) } },
            "minAge" to { m -> check = m.durSelf()?.let(FileReadiness::MinAge) },
        )
        return check
    }

    /** Spec 6.3 as one step: `{ rename: { pattern } }`, `{ zip: {} }`, `{ custom: name, config: { .. } }`. */
    private fun Node.processor(): ProcessorSpec? {
        var spec: ProcessorSpec? = null
        one(
            "quality" to { spec = ProcessorSpec.Quality },
            "rename" to { r -> spec = r.str("pattern")?.let(ProcessorSpec::Rename) },
            "zip" to { spec = ProcessorSpec.Zip },
            "unzip" to { u -> spec = ProcessorSpec.Unzip().let { d -> ProcessorSpec.Unzip(u.int("maxEntries") ?: d.maxEntries, u.bytes("maxBytes") ?: d.maxBytes) } },
            "extract" to { e ->
                spec = e.word("from", "fileName" to ExtractFrom.FileName, "sourcePath" to ExtractFrom.SourcePath, "content" to ExtractFrom.Content, "message" to ExtractFrom.Message)
                    ?.let { from -> ProcessorSpec.Extract(from, e.str("regex"), e.items("into")?.mapNotNull { it.scalar() }, e.obj("json")?.entries()?.mapNotNull { (k, v) -> v.scalar()?.let { k to it } }?.toMap()) }
            },
            "expand" to { x -> spec = x.str("format")?.let { f -> x.str("files")?.let { files -> x.str("from")?.let { ProcessorSpec.Expand(f, files, it) } } } },
            "verifyDigest" to { v -> spec = v.str("attribute")?.let(ProcessorSpec::VerifyDigest) },
            "custom" to { c -> spec = c.scalar()?.let { name -> ProcessorSpec.Custom(name, free("config")?.let { yaml.convertValue(it, Map::class.java) }?.mapKeys { it.key.toString() } ?: emptyMap()) } },
        )
        return spec
    }

    /** Spec 5.3: a bare word, or a one-key object such as `move: temp/`. */
    private fun Node.ack(key: String): AckAction? {
        val node = child(key) ?: return null
        if (node.isScalar()) return node.word(null, "delete" to AckAction.Delete, "none" to AckAction.None, "ack" to AckAction.Ack, "term" to AckAction.Term, "nak" to AckAction.Nak)
        var action: AckAction? = null
        node.one(
            "move" to { action = it.scalar()?.let(AckAction::Move) },
            "callback" to { action = it.scalar()?.let(AckAction::Callback) },
            "tag" to { t -> action = t.scalar()?.let { kv -> kv.split("=", limit = 2).takeIf { it.size == 2 }?.let { AckAction.Tag(it[0], it[1]) } ?: t.fail("$kv is not key=value") } },
        )
        return action
    }
}

/**
 * One node of the document with its dotted path; every key read is remembered, so `done()` can name each
 * key nobody asked for. Scalars resolve `${VAR}` from the environment before they are parsed.
 */
private class Node(private val json: JsonNode, private val path: String, private val env: Map<String, String>, private val errors: MutableList<String>) {
    private val used = mutableSetOf<String>()
    private val children = mutableListOf<Node>()

    fun <T> fail(message: String): T? {
        errors += "${path.ifEmpty { "document" }}: $message"
        return null
    }

    fun isScalar() = json.isValueNode
    fun child(key: String): Node? = json.get(key)?.let { used += key; Node(it, if (path.isEmpty()) key else "$path.$key", env, errors).also(children::add) }
    fun obj(key: String): Node? = child(key)?.let { if (it.json.isObject) it else it.fail("expected a mapping") }
    fun entries(): List<Pair<String, Node>> = json.fieldNames().asSequence().map { it to child(it)!! }.toList()
    fun items(key: String): List<Node>? = child(key)?.let { c ->
        if (!c.json.isArray) c.fail("expected a list")
        else c.json.mapIndexed { i, item -> Node(item, "${c.path}[$i]", env, errors).also(c.children::add) }
    }

    /** The one key of this mapping that names a kind (`sftp:`/`s3:`, an ack action, a processor step). */
    fun one(vararg kinds: Pair<String, (Node) -> Unit>) {
        val present = kinds.filter { json.has(it.first) }
        when (present.size) {
            1 -> present.single().let { (key, read) -> child(key)?.let(read) }
            0 -> fail<Unit>("expected one of ${kinds.joinToString { it.first }}")
            else -> fail<Unit>("${present.joinToString { it.first }} cannot be combined")
        }
    }

    /** A free-form subtree the bean owns (`custom.config`): taken whole, no key of it is unknown. */
    fun free(key: String): JsonNode? = json.get(key)?.also { used += key }
    fun scalar(): String? = if (json.isValueNode) resolve(json.asText()) else fail("expected a value")
    fun durSelf(): Duration? = scalar()?.let { s -> Duration.parseOrNull(s) ?: fail("$s is not a duration such as 30s, 15m or 1h") }
    fun str(key: String): String? = child(key)?.scalar()
    fun path(key: String): Path? = str(key)?.let(Path::of)
    fun int(key: String): Int? = child(key)?.let { c -> c.scalar()?.toIntOrNull() ?: c.fail("expected a whole number") }
    fun bool(key: String): Boolean? = child(key)?.let { c -> c.scalar()?.toBooleanStrictOrNull() ?: c.fail("expected true or false") }
    fun dur(key: String): Duration? = child(key)?.durSelf()
    fun bytes(key: String): Long? = child(key)?.let { c ->
        c.scalar()?.let { s -> BYTES.matchEntire(s.trim())?.let { m -> m.groupValues[1].toLong() shl (10 * "kmgt".indexOf(m.groupValues[2].lowercase()).coerceAtLeast(-1).plus(1)) } ?: c.fail("$s is not a size such as 512m or 1g") }
    }

    fun <T> word(key: String?, vararg options: Pair<String, T>): T? {
        val node = if (key == null) this else child(key) ?: return null
        val text = node.scalar() ?: return null
        return options.firstOrNull { it.first == text }?.second ?: node.fail("$text is not one of ${options.joinToString { it.first }}")
    }

    /** `[200-299]` and `[408, 429, 500-599]`: whole numbers and inclusive ranges. */
    fun statuses(key: String): Set<Int>? = items(key)?.flatMap { item ->
        item.scalar()?.let { s ->
            s.toIntOrNull()?.let { listOf(it) } ?: RANGE.matchEntire(s)?.let { (it.groupValues[1].toInt()..it.groupValues[2].toInt()).toList() }
                ?: item.fail("$s is not a status or a range such as 500-599")
        } ?: emptyList()
    }?.toSet()

    /** Rule 25 is judged on the result: a `${VAR}` reference is `Env`, anything else `Literal`. */
    fun secret(key: String): Secret? = child(key)?.secretSelf()
    fun secretSelf(): Secret? {
        val raw = if (json.isValueNode) json.asText() else return fail("expected a value")
        return REFERENCE.matchEntire(raw)?.let { m -> variable(m.groupValues[1])?.let { Secret.Env(it) } } ?: Secret.Literal(raw)
    }

    private fun variable(name: String): String? = if (name in env) name else fail("\${$name} is not set in the environment")
    private fun resolve(text: String): String? {
        var missing = false
        val out = REFERENCE_ANYWHERE.replace(text) { m -> env[m.groupValues[1]] ?: run { variable(m.groupValues[1]); missing = true; "" } }
        return if (missing) null else out
    }

    fun done() {
        if (json.isObject) json.fieldNames().asSequence().filter { it !in used }.forEach { fail<Unit>("unknown key ${if (path.isEmpty()) it else "$path.$it"}") }
        children.forEach { it.done() }
    }

    private companion object {
        val REFERENCE = Regex("""^\$\{([A-Za-z_][A-Za-z0-9_]*)}$""")
        val REFERENCE_ANYWHERE = Regex("""\$\{([A-Za-z_][A-Za-z0-9_]*)}""")
        val BYTES = Regex("""(\d+)\s*([kKmMgGtT]?)[bB]?""")
        val RANGE = Regex("""(\d+)\s*-\s*(\d+)""")
    }
}
