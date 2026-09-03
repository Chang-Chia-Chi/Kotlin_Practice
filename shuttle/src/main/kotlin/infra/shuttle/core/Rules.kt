package infra.shuttle.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException

data class Violation(val rule: Int, val message: String)

data class Report(val violations: List<Violation>) {
    val ok get() = violations.isEmpty()
}

/**
 * Spec 13.3: the twenty-five rules, every violation collected and reported with its number.
 * `beans` answers what a named bean produces, or null when no bean has that name (rules 15 and 17);
 * validate mode passes the host's lookup, tests pass a map.
 */
object Rules {
    fun validate(config: ShuttleConfig, beans: (String) -> Set<String>? = { null }): Report =
        Report(Run(config, beans).apply { all() }.found)

    private class Run(val config: ShuttleConfig, val beans: (String) -> Set<String>?) {
        val found = mutableListOf<Violation>()
        val stores = config.objectStores.associateBy { it.name }
        val channels = config.channels.associateBy { it.name }

        fun fail(rule: Int, message: String) {
            found += Violation(rule, message)
        }

        fun all() {
            names()
            timeouts()
            stores()
            config.channels.forEach { channel(it) }
            secrets()
            if (config.supervision.restartBackoff.let { it.initial > it.max }) fail(24, "restartBackoff.initial exceeds max")
            config.routes.forEach { route(it) }
        }

        /** Rule 4. */
        fun names() {
            (config.objectStores.map { it.name } + config.channels.map { it.name }).duplicates()
                .forEach { fail(4, "store or channel name $it is declared twice") }
            config.routes.map { it.name }.duplicates().forEach { fail(4, "route name $it is declared twice") }
        }

        /** Rule 3. */
        fun timeouts() {
            val drain = config.drainTimeout
            config.objectStores.forEach {
                when (it) {
                    is S3Store -> if (it.timeouts.apiCall >= drain) fail(3, "store ${it.name}: apiCall ${it.timeouts.apiCall} is not below drainTimeout $drain")
                    is SftpStore -> if (it.drainTimeout + it.cancelGrace >= drain) fail(3, "store ${it.name}: drain plus cancel grace is not below drainTimeout $drain")
                }
            }
            config.channels.filterIsInstance<HttpChannel>()
                .filter { it.timeout >= drain }
                .forEach { fail(3, "channel ${it.name}: timeout ${it.timeout} is not below drainTimeout $drain") }
        }

        /** Rules 9, 10, 11. */
        fun stores() {
            val stagings = mutableMapOf<Path, String>()
            for (store in config.objectStores) {
                val pool = store.pool
                if (pool != null) {
                    if (pool.maxConcurrentTransfers > pool.maxSize) fail(9, "store ${store.name}: maxConcurrentTransfers exceeds maxSize")
                    val sessions = config.routes.sumOf { it.parallelism * it.rolesOn(store.name) } +
                        config.routes.count { (it.source as? Source.Poll)?.store == store.name }
                    if (sessions > pool.maxSize) fail(9, "store ${store.name}: routes need $sessions sessions, pool.maxSize is ${pool.maxSize}")
                }
                if (store is SftpStore) {
                    if (store.keepAlive >= store.idleCutoff) fail(10, "store ${store.name}: keepAlive is not below idleCutoff")
                    if (store.idleTimeout >= store.idleCutoff) fail(10, "store ${store.name}: idleTimeout is not below idleCutoff")
                    if ((store.staging?.minFree ?: 0) < 0) fail(7, "store ${store.name}: staging.minFree must be >= 0")
                    val staging = store.staging?.dir?.toAbsolutePath()?.normalize()
                    when {
                        staging == null -> fail(11, "store ${store.name}: no staging directory")
                        !Files.isDirectory(staging) || !Files.isWritable(staging) -> fail(11, "store ${store.name}: staging $staging is not a writable directory")
                        Files.getFileStore(staging).type().lowercase() in NETWORK_FILESYSTEMS -> fail(11, "store ${store.name}: staging $staging is not local disk")
                        else -> stagings.put(staging, store.name)?.let { fail(11, "stores $it and ${store.name} share staging $staging") }
                    }
                }
            }
        }

        /** Rules 15 (providers), 16, 18, 19, 20, 21. */
        fun channel(channel: Channel) {
            if (channel !is HttpChannel) return
            if ((channel.response.success intersect channel.response.retry).isNotEmpty()) fail(20, "channel ${channel.name}: success and retry overlap")
            MappingRenderer.check(channel.body, declaredAttributes = null) { beans(it) != null }
                .forEach { fail(it.rule, "channel ${channel.name} ${it.message}") }
        }

        /** Rule 25. */
        fun secrets() {
            val declared = config.objectStores.flatMap {
                when (it) {
                    is SftpStore -> listOf(it.user, it.password)
                    is S3Store -> listOf(it.credentials?.accessKey, it.credentials?.secretKey)
                }
            } + config.channels.flatMap {
                when (it) {
                    is NatsChannel -> listOf(it.credentials)
                    is HttpChannel -> when (val auth = it.auth) {
                        is HttpAuth.Bearer -> listOf(auth.token)
                        is HttpAuth.Basic -> listOf(auth.user, auth.password)
                        is HttpAuth.Header -> listOf(auth.value)
                        null -> emptyList()
                    }
                }
            }
            if (declared.any { it is Secret.Literal }) fail(25, "a secret is given as a literal; use a \${VAR} reference")
        }

        /** Rules 1, 2, 5, 6, 7, 8, 12, 13, 14, 15, 17, 22, 23. */
        fun route(route: Route) {
            val name = route.name
            val source = route.source
            if (source == null) fail(5, "route $name has no source")
            if (route.target == null) fail(5, "route $name has no target")
            when (source) {
                is Source.Poll -> {
                    reference(name, source.store, "poll", stores) { it is SftpStore }
                    if (route.fetch != null) fail(6, "route $name polls and has a fetch")
                    ack(name, source.onAck, source.onNack, (stores[source.store] as? S3Store)?.let { S3_ACKS } ?: SFTP_ACKS, setOf(AckAction.None))
                    val move = source.onAck as? AckAction.Move
                    if (move != null && Path.of(source.directory).resolve(move.folder).normalize() == Path.of(source.directory).normalize()) {
                        fail(23, "route $name moves acked files into the polled directory itself")
                    }
                }
                is Source.Subscribe -> {
                    reference(name, source.channel, "subscribe", channels) { it is NatsChannel }
                    if (route.fetch == null) fail(6, "route $name subscribes without a fetch")
                    if (!source.inProgressEvery.isPositive()) fail(7, "route $name: inProgressEvery must be > 0")
                    ack(name, source.onAck, source.onNack, setOf(AckAction.Ack, AckAction.Term), setOf(AckAction.Nak))
                }
                null -> Unit
            }
            route.fetch?.let { reference(name, it.store, "fetch", stores) { true } }
            route.target?.let { reference(name, it.store, "target", stores) { true } }
            route.notify.forEach { reference(name, it.channel, "notify", channels) { true } }
            if (route.parallelism < 1) fail(7, "route $name: parallelism must be >= 1")
            if (route.maxAttempts < 1) fail(7, "route $name: maxAttempts must be >= 1")
            route.stuckAfter?.let { if (!it.isPositive()) fail(7, "route $name: stuckAfter must be > 0") }
            if (route.recheckFinished.isNegative()) fail(7, "route $name: recheckFinished must be >= 0")
            route.notify.map { it.on to it.channel }.duplicates().forEach { fail(8, "route $name notifies ${it.second} on ${it.first.name.lowercase()} twice") }

            val declared = route.process.flatMap { if (it is ProcessorSpec.Custom) beans(it.name).orEmpty() else it.produces }.toSet()
            if (declared.size > 32) fail(22, "route $name declares ${declared.size} attributes; at most 32")
            declared.filter { it.length > 64 }.forEach { fail(22, "route $name: attribute name $it is longer than 64 characters") }
            val placeholders = PLACEHOLDERS + declared
            route.target?.let { target ->
                listOfNotNull(target.key, target.directory).forEach { pattern(13, "route $name target", it, placeholders) }
            }
            route.process.forEach { processor(name, it, source is Source.Subscribe, placeholders) }
            route.notify.mapNotNull { channels[it.channel] as? HttpChannel }.distinct().forEach { channel ->
                MappingRenderer.check(channel.body, declared) { true }.filter { it.rule == 17 }
                    .forEach { fail(17, "route $name: channel ${channel.name} ${it.message}") }
            }
        }

        /** Rule 12. */
        private fun ack(route: String, onAck: AckAction?, onNack: AckAction?, acks: Set<AckAction>, nacks: Set<AckAction>) {
            when (onAck) {
                null -> fail(12, "route $route states no onAck")
                is AckAction.Callback -> if (onAck.channel !in channels) fail(12, "route $route: callback names $${onAck.channel}, which is no channel offering notify")
                else -> if (acks.none { it::class == onAck::class }) fail(12, "route $route: onAck $onAck is not in the trigger's vocabulary")
            }
            if (onNack != null && nacks.none { it::class == onNack::class }) fail(12, "route $route: onNack $onNack is not in the trigger's vocabulary")
        }

        /** Rules 14 and 15 (custom processors). */
        private fun processor(route: String, spec: ProcessorSpec, subscribed: Boolean, placeholders: Set<String>) {
            val at = "route $route processor ${spec::class.simpleName}"
            when (spec) {
                is ProcessorSpec.Rename -> pattern(14, at, spec.pattern, placeholders)
                is ProcessorSpec.Extract -> {
                    if (spec.from == ExtractFrom.Message && !subscribed) fail(14, "$at: from message on a route that does not subscribe")
                    val compiled = spec.regex?.let { runCatching { Pattern.compile(it) }.getOrElse { e -> fail(14, "$at: ${(e as? PatternSyntaxException)?.description ?: e.message}"); null } }
                    if (compiled != null) {
                        val groups = compiled.matcher("").groupCount()
                        when {
                            spec.into != null && spec.into.size != groups -> fail(14, "$at: into names ${spec.into.size} groups, the regex has $groups")
                            spec.into == null && spec.produces.isEmpty() -> fail(14, "$at: the regex has no named groups and no into list")
                        }
                    }
                    spec.json?.values?.filterNot { it.isJsonPointer() }?.forEach { fail(14, "$at: $it is not a JSON pointer") }
                    if (spec.regex == null && spec.json == null) fail(14, "$at: neither regex nor json")
                }
                is ProcessorSpec.Expand -> {
                    if (spec.from !in stores) fail(14, "$at: from ${spec.from} names no object store")
                    if (spec.files.isBlank()) fail(14, "$at: files is blank")
                }
                is ProcessorSpec.Custom -> if (beans(spec.name) == null) fail(15, "$at: no bean named ${spec.name}")
                is ProcessorSpec.VerifyDigest -> if (spec.attribute.isBlank()) fail(14, "$at: attribute is blank")
                is ProcessorSpec.Unzip -> {
                    if (spec.maxEntries < 1) fail(14, "$at: maxEntries must be >= 1")
                    if (spec.maxBytes <= 0) fail(14, "$at: maxBytes must be > 0")
                }
                ProcessorSpec.Quality, ProcessorSpec.Zip -> Unit
            }
        }

        /** Rules 1 and 2 for one reference. */
        private fun <T> reference(route: String, ref: String, use: String, declared: Map<String, T>, offers: (T) -> Boolean) {
            val target = declared[ref]
            when {
                target != null -> if (!offers(target)) fail(2, "route $route: $ref does not offer $use")
                ref in stores || ref in channels -> fail(2, "route $route: $ref cannot be used for $use")
                else -> fail(1, "route $route references $ref, which is not declared")
            }
        }

        private fun pattern(rule: Int, at: String, pattern: String, allowed: Set<String>) {
            if (".." in pattern) fail(rule, "$at: pattern $pattern yields ..")
            PLACEHOLDER.findAll(pattern).map { it.groupValues[1] }.filter { it !in allowed }
                .forEach { fail(rule, "$at: pattern $pattern uses {$it}, which is not a placeholder or a declared attribute") }
        }

        private fun Route.rolesOn(store: String) =
            listOf((source as? Source.Poll)?.store, fetch?.store, target?.store).count { it == store }

        private fun <T> List<T>.duplicates() = groupingBy { it }.eachCount().filterValues { it > 1 }.keys
        private fun String.isJsonPointer() = isEmpty() || startsWith("/")
    }

    private val PLACEHOLDER = Regex("""\{([^}]*)}""")
    private val PLACEHOLDERS = setOf("name", "sourceName", "yyyyMMdd")
    private val SFTP_ACKS = setOf<AckAction>(AckAction.Move(""), AckAction.Delete, AckAction.None)
    private val S3_ACKS = SFTP_ACKS + AckAction.Tag("", "")
    private val NETWORK_FILESYSTEMS = setOf("nfs", "nfs4", "cifs", "smbfs", "smb", "fuse.sshfs", "afs")
}
