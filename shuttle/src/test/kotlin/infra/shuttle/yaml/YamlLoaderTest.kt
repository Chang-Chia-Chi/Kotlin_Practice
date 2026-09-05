package infra.shuttle.yaml

import infra.shuttle.core.AckAction
import infra.shuttle.core.Backoff
import infra.shuttle.core.DeliveryPolicy
import infra.shuttle.core.ExtractFrom
import infra.shuttle.core.Field
import infra.shuttle.core.HttpChannel
import infra.shuttle.core.NatsChannel
import infra.shuttle.core.ProcessorSpec
import infra.shuttle.core.Report
import infra.shuttle.core.ResponseSpec
import infra.shuttle.core.Rules
import infra.shuttle.core.S3Store
import infra.shuttle.core.S3Timeouts
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStore
import infra.shuttle.core.Source
import infra.shuttle.core.Staging
import infra.shuttle.core.StateStoreConfig
import infra.shuttle.core.Violation
import infra.shuttle.core.bucket
import infra.shuttle.core.channel
import infra.shuttle.core.env
import infra.shuttle.core.extract
import infra.shuttle.core.mapping
import infra.shuttle.core.move
import infra.shuttle.core.objectStore
import infra.shuttle.core.rename
import infra.shuttle.core.shuttle
import infra.shuttle.core.then
import infra.shuttle.core.zip
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 13.1 becomes the configuration spec 13.2 builds; every problem names its YAML path. */
class YamlLoaderTest {

    private val env = mapOf("SFTP_USER" to "u", "SFTP_PASSWORD" to "p")

    @Test
    fun the_smallest_document_becomes_a_configuration() {
        val config = YamlLoader.load(
            """
            shuttle:
              shuttleStateStore:
                oracle: { datasource: shuttle }
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} } }
              routes:
                mirror:
                  source: { poll: { store: vendor, directory: /outbound, every: 1h, onAck: delete } }
                  target: { store: vendor, directory: /incoming }
            """.trimIndent(),
            env,
        )
        assertEquals(StateStoreConfig("shuttle"), config.stateStore)
        assertEquals("sftp.example", (config.objectStores.single() as SftpStore).host)
        val route = config.routes.single()
        assertEquals("mirror", route.name)
        assertEquals(Source.Poll("vendor", "/outbound", 1.hours, onAck = AckAction.Delete), route.source)
        assertEquals("/incoming", route.target?.directory)
    }

    @Test
    fun a_missing_environment_variable_is_a_load_error_naming_it() {
        val e = assertThrows(YamlLoadException::class.java) { YamlLoader.load(minimal, env - "SFTP_PASSWORD") }
        assertEquals(listOf("shuttle.objectStores.vendor.sftp.auth.password: \${SFTP_PASSWORD} is not set in the environment"), e.errors)
    }

    @Test
    fun an_unknown_key_is_an_error_naming_its_path() {
        val e = assertThrows(YamlLoadException::class.java) {
            YamlLoader.load(minimal.replace("target:", "paralellism: 4\n      target:"), env)
        }
        assertEquals(listOf("shuttle.routes.mirror: unknown key shuttle.routes.mirror.paralellism"), e.errors)
    }

    /**
     * Retired rule 16: `field` is read into the [infra.shuttle.core.Field] vocabulary as the document is
     * parsed, so an unknown name never becomes a row and is reported where every other bad word is.
     */
    @Test
    fun an_unknown_mapping_field_is_a_load_error_naming_the_row() {
        val e = assertThrows(YamlLoadException::class.java) { YamlLoader.load(spec131().replace("field: EVENT", "field: MOMENT"), specEnv) }
        assertEquals(1, e.errors.size, e.errors.toString())
        assertTrue(e.errors.single().startsWith("shuttle.channels.downstream.http.body[9].field: MOMENT is not one of "), e.errors.single())
    }

    /** Ticket 38, the same reading as the retired rule 16: `expand.format` is a word, so an unknown one never becomes a step. */
    @Test
    fun an_unknown_expand_format_is_a_load_error_naming_the_step() {
        val e = assertThrows(YamlLoadException::class.java) { YamlLoader.load(spec131().replace("format: json", "format: lines"), specEnv) }
        assertEquals(1, e.errors.size, e.errors.toString())
        assertEquals("shuttle.routes.image-sets.process[1].expand.format: lines is not one of json, message", e.errors.single())
    }

    @Test
    fun durations_byte_sizes_and_status_ranges_parse() {
        val config = YamlLoader.load(
            """
            shuttle:
              drainTimeout: 60s
              objectStores:
                vendor:
                  sftp: { host: h, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, idleCutoff: 5m, staging: { dir: /tmp/stage, minFree: 512m } }
                minio:
                  s3: { endpoint: https://minio.internal, credentials: { accessKey: ${'$'}{A}, secretKey: ${'$'}{B} }, timeouts: { connect: 5s, socket: 30s, apiCall: 45s } }
              channels:
                downstream:
                  http:
                    url: https://downstream.internal/api/files
                    timeout: 10s
                    response: { success: [200-299], retry: [408, 429, 500-599], reference: /requestId }
                    policy: { maxAttempts: 50, giveUpAfter: 24h, backoff: { initial: 5s, max: 15m } }
            """.trimIndent(),
            env + mapOf("A" to "a", "B" to "b"),
        )
        assertEquals(60.seconds, config.drainTimeout)
        val vendor = config.objectStores.filterIsInstance<SftpStore>().single()
        assertEquals(5.minutes, vendor.idleCutoff)
        assertEquals(Staging(Path.of("/tmp/stage"), 512L * 1024 * 1024), vendor.staging)
        assertEquals(S3Timeouts(5.seconds, 30.seconds, 45.seconds), config.objectStores.filterIsInstance<S3Store>().single().timeouts)
        val http = config.channels.single() as HttpChannel
        assertEquals(ResponseSpec((200..299).toSet(), setOf(408, 429) + (500..599), "/requestId"), http.response)
        assertEquals(DeliveryPolicy(maxAttempts = 50, giveUpAfter = 24.hours, backoff = Backoff(5.seconds, 15.minutes)), http.policy)
    }

    /**
     * G15: `subject` is what the channel publishes on when a route notifies through it, and `body` is the
     * mapping table it publishes, which spec 9.6 gives every channel whatever its kind. The DSL says the same.
     */
    @Test
    fun a_nats_channel_reads_its_url_credentials_subject_and_body() {
        val config = YamlLoader.load(
            """
            shuttle:
              channels:
                events:
                  nats:
                    url: nats://events.internal:4222
                    credentials: ${'$'}{NATS_CREDS}
                    subject: files.stored
                    body:
                      - { path: fileId, field: TRANSFER_ID }
            """.trimIndent(),
            mapOf("NATS_CREDS" to "creds"),
        )
        val table = mapping { "fileId" from Field.TRANSFER_ID }
        assertEquals(NatsChannel("events", "nats://events.internal:4222", Secret.Env("NATS_CREDS"), "files.stored", table), config.channels.single())
        val dsl = shuttle {
            channels { nats("events") { url = "nats://events.internal:4222"; credentials = env("NATS_CREDS"); subject = "files.stored"; body = table } }
        }
        assertEquals(dsl.channels.single(), config.channels.single())
    }

    @TempDir
    lateinit var stage: Path

    /** Spec 13.1 verbatim, with its staging paths pointed at a real directory so rule 11 can look. */
    private fun spec131(): String {
        Files.createDirectories(stage.resolve("vendor"))
        Files.createDirectories(stage.resolve("partner"))
        return YamlLoaderTest::class.java.getResource("/spec-13-1.yaml")!!.readText().replace("/var/shuttle/stage", stage.toString().replace('\\', '/'))
    }

    private val specEnv = env + listOf("PARTNER_USER", "PARTNER_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY", "DOWNSTREAM_TOKEN", "UPSTREAM_KEY", "NATS_CREDS").associateWith { "x" }
    private val specBeans = mapOf("orderDetails" to emptySet(), "imageResizer" to setOf("orderNumber"))

    @Test
    fun the_spec_13_1_document_loads_passes_every_rule_and_equals_the_dsl_build_for_vendor_drop() {
        val config = YamlLoader.load(spec131(), specEnv)
        assertEquals(emptyList<Violation>(), Rules.validate(config) { specBeans[it] }.violations)
        val dsl = shuttle {
            route("vendor-drop") {
                source = poll(objectStore("vendor"), directory = "/inbox") { every = 1.hours; onAck = move("temp/") }
                process = extract(from = ExtractFrom.FileName, regex = "(?<orderNumber>\\d+)-.*\\.csv") then rename("{yyyyMMdd}-{name}") then zip()
                target = objectStore("minio").bucket("landing") { key = "vendor/{name}" }
                notify(on = Acked, channel("downstream"))
                parallelism = 4
                maxAttempts = 5
                stuckAfter = 3.hours
                recheckFinished = 24.hours
            }
        }
        assertEquals(dsl.routes.single(), config.routes.first { it.name == "vendor-drop" })
        assertEquals(listOf("vendor-drop", "mirror", "image-sets"), config.routes.map { it.name })
    }

    private fun rules(text: String, beans: Map<String, Set<String>> = emptyMap()) =
        Rules.validate(YamlLoader.load(text, specEnv)) { beans[it] }.violations.map { it.rule }.distinct()

    @Test
    fun rule9_counts_every_role_and_a_route_without_parallelism_as_one() {
        // vendor pool 4: `mirror` takes poll 1 + target 1 + its lister; `copy` states no parallelism and adds a lister, which
        // fills the pool exactly if an omitted parallelism counted as 0, and overflows it by two counted as 1 (D36).
        val text = minimal.replace("sftp: { host: sftp.example,", "sftp: { pool: { maxSize: 4 }, staging: { dir: $stageDir }, host: sftp.example,") + "\n" + """
            |    copy:
            |      source: { poll: { store: vendor, directory: /elsewhere, every: 1h, onAck: none } }
            |      target: { store: vendor, directory: /copies }
        """.trimMargin()
        assertEquals(listOf(9), rules(text))
    }

    @Test
    fun rule25_a_literal_secret_fails() =
        assertEquals(listOf(25), rules(minimal.replace("password: \${SFTP_PASSWORD}", "password: hunter2").replace("sftp: { host", "sftp: { staging: { dir: $stageDir }, host")))

    @Test
    fun S25_validate_mode_reports_five_rule_numbers_in_one_report() {
        val file = stage.resolve("shuttle.yaml")
        Files.writeString(
            file,
            """
            shuttle:
              drainTimeout: 60s
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: hunter2 }, staging: { dir: $stageDir } }
              channels:
                downstream:
                  http: { url: https://downstream.internal, timeout: 61s }
              routes:
                mirror:
                  source: { poll: { store: vendor, directory: /outbound, every: 1h } }
                  target: { store: nowhere, directory: /incoming }
                  notify: [ { on: acked, channel: downstream } ]
                  parallelism: 0
            """.trimIndent(),
        )
        val report = YamlLoader.validate(listOf(file), env)
        assertEquals(emptyList<String>(), report.errors)
        assertEquals(listOf(1, 3, 7, 12, 25), report.violations.map { it.rule }.distinct().sorted())
    }

    @Test
    fun validate_mode_reports_load_errors_when_the_document_is_not_a_configuration() {
        val file = stage.resolve("broken.yaml")
        Files.writeString(file, minimal.replace("every: 1h", "every: soon"))
        assertEquals(Report(emptyList(), listOf("shuttle.routes.mirror.source.poll.every: soon is not a duration such as 30s, 15m or 1h")), YamlLoader.validate(listOf(file), env))
    }

    @Test
    fun v0_4_knobs_load_with_the_spec_defaults_when_omitted() {
        val config = YamlLoader.load(minimal.replace("sftp: { host", "sftp: { staging: { dir: /tmp/stage }, host").replace("target:", "process: [ { unzip: {} } ]\n      target:"), env)
        val route = config.routes.single()
        assertEquals(24.hours, route.recheckFinished)
        assertEquals(Staging(Path.of("/tmp/stage"), 1L shl 30), (config.objectStores.single() as SftpStore).staging)
        assertEquals(ProcessorSpec.Unzip(10_000, 10L shl 30), route.process.single())
        val explicit = YamlLoader.load(
            minimal.replace("sftp: { host", "sftp: { staging: { dir: /tmp/stage, minFree: 2g }, host").replace("target:", "process: [ { unzip: { maxEntries: 5, maxBytes: 1m } } ]\n      recheckFinished: 0s\n      target:"),
            env,
        ).routes.single()
        assertEquals(0.seconds, explicit.recheckFinished)
        assertEquals(ProcessorSpec.Unzip(5, 1L shl 20), explicit.process.single())
    }

    private val stageDir get() = stage.toString().replace('\\', '/')

    /**
     * Spec 6.2 and 13.1's own `custom: imageResizer, config: { maxWidth: 2048 }`. The keys inside `config`
     * are the bean's, but the values in them are the operator's: a `${VAR}` there is the same reference as
     * anywhere else in the document, and it never reaches the bean literal (ticket 43).
     */
    @Test
    fun a_custom_configs_environment_references_expand_like_every_other_value() {
        val config = YamlLoader.load(withResizer, env + ("RESIZE_TOKEN" to "t0ken"))

        assertEquals(
            ProcessorSpec.Custom(
                "imageResizer",
                mapOf("maxWidth" to 2048, "token" to "t0ken", "sizes" to listOf("small", "u"), "nested" to mapOf("of" to "u")),
            ),
            config.routes.single().process.single(),
        )
    }

    @Test
    fun a_missing_environment_variable_in_a_custom_config_is_a_load_error_naming_the_step() {
        val e = assertThrows(YamlLoadException::class.java) { YamlLoader.load(withResizer, env) }
        assertEquals(listOf("shuttle.routes.mirror.process[0]: \${RESIZE_TOKEN} is not set in the environment"), e.errors)
    }

    private val minimal = """
        shuttle:
          shuttleStateStore:
            oracle: { datasource: shuttle }
          objectStores:
            vendor:
              sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} } }
          routes:
            mirror:
              source: { poll: { store: vendor, directory: /outbound, every: 1h, onAck: delete } }
              target: { store: vendor, directory: /incoming }
    """.trimIndent()

    /** A `custom` step whose config carries a reference at the top, inside a list and inside a nested mapping. */
    private val withResizer = minimal.replace(
        "      target:",
        "      process: [ { custom: imageResizer, config: { maxWidth: 2048, token: \${RESIZE_TOKEN}," +
            " sizes: [ small, \${SFTP_USER} ], nested: { of: \${SFTP_USER} } } } ]\n      target:",
    )
}
