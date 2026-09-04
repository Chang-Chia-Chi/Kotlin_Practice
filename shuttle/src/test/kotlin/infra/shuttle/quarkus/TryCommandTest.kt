package infra.shuttle.quarkus

import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.targetKey
import infra.shuttle.testkit.ClockFixture
import infra.shuttle.testkit.digestOf
import infra.shuttle.yaml.YamlLoader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/** Spec 12.2 and S31: `shuttle try` runs one route's chain over a sample and prints what the deployment would do, connecting to nothing. */
class TryCommandTest {
    @TempDir lateinit var dir: Path

    private val env = mapOf("SFTP_USER" to "u", "SFTP_PASSWORD" to "p", "S3_KEY" to "k", "S3_SECRET" to "s", "TOKEN" to "t")
    private val clock = ClockFixture()

    private fun vendorDrop(attribute: String = "orderNumber", group: String = "orderNumber") = dir.resolve("shuttle.yaml").also {
        Files.writeString(
            it,
            """
            shuttle:
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, staging: { dir: $dir } }
                minio:
                  s3: { endpoint: https://minio.internal, credentials: { accessKey: ${'$'}{S3_KEY}, secretKey: ${'$'}{S3_SECRET} } }
              channels:
                downstream:
                  http:
                    url: https://downstream.internal/api/files
                    auth: { bearer: ${'$'}{TOKEN} }
                    body:
                      - { path: fileId, field: TRANSFER_ID }
                      - { path: file.name, field: STORED_NAME }
                      - { path: orderNumber, attribute: $attribute }
                      - { path: source, value: vendor-drop }
              routes:
                vendor-drop:
                  source: { poll: { store: vendor, directory: /inbox, every: 1h, onAck: { move: temp/ } } }
                  process:
                    - { extract: { from: fileName, regex: "(?<$group>\\d+)-.*\\.csv" } }
                    - { rename: { pattern: "{yyyyMMdd}-{name}" } }
                  target: { store: minio, bucket: landing, key: "vendor/{name}" }
                  notify: [ { on: acked, channel: downstream } ]
            """.trimIndent(),
        )
    }

    private fun run(file: Path, route: String = "vendor-drop", fileName: String = "123-order.csv", content: Path? = null): Pair<Int, String> {
        val bytes = ByteArrayOutputStream()
        val code = TryCommand(listOf(file), env, NamedBeans.none, PrintStream(bytes, true), clock, route, fileName, content = content).run()
        return code to bytes.toString()
    }

    @Test
    fun S31_prints_the_attributes_per_step_the_key_and_one_body_per_notified_channel() {
        val (code, out) = run(vendorDrop())

        assertEquals(0, code, out)
        assertTrue(out.contains("step 1 extract: attributes {orderNumber=123}"), out)
        assertTrue(out.contains("step 2 rename: attributes {} objects [20260101-123-order.csv]"), out)
        assertTrue(out.contains("key: vendor/20260101-123-order.csv"), out)
        assertTrue(out.contains("body downstream (acked):"), out)
        assertTrue(out.contains("\"orderNumber\" : \"123\""), out)
        assertTrue(out.contains("\"name\" : \"20260101-123-order.csv\""), out)
        assertTrue(out.contains("\"source\" : \"vendor-drop\""), out)
    }

    @Test
    fun S31_a_mapping_naming_an_attribute_the_regex_does_not_produce_is_reported_by_rule_17() {
        val (code, out) = run(vendorDrop(attribute = "orderNo"))

        assertEquals(1, code)
        assertTrue(out.contains("rule 17:"), out)
        assertTrue(out.contains("orderNo"), out)
    }

    @Test
    fun a_sample_name_the_regex_does_not_match_is_the_extract_step_rejecting_it() {
        val (code, out) = run(vendorDrop(), fileName = "order.txt")

        assertEquals(1, code)
        assertTrue(out.contains("step 1 extract: REJECT"), out)
        assertTrue(out.contains("order.txt does not match"), out)
    }

    @Test
    fun an_unknown_route_exits_non_zero() {
        val (code, out) = run(vendorDrop(), route = "nope")
        assertEquals(1, code)
        assertTrue(out.contains("no route named nope"), out)
    }

    /**
     * Spec 13.1's `image-sets` shape, offline: a metadata file lists the children `expand` fetches. Every
     * store's fetcher reads the sample files sitting beside the sample content, so nothing is connected to.
     */
    private fun imageSets() = dir.resolve("image-sets.yaml").also {
        Files.writeString(
            it,
            """
            shuttle:
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, staging: { dir: $dir } }
              channels:
                downstream:
                  http:
                    url: https://downstream.internal/api/files
                    auth: { bearer: ${'$'}{TOKEN} }
                    body:
                      - { path: file.name, field: STORED_NAME }
                      - { path: file.digest, field: DIGEST }
              routes:
                image-sets:
                  source: { poll: { store: vendor, directory: /inbox, every: 1h, onAck: delete } }
                  process:
                    - { expand: { format: json, files: "/images[*].path", from: vendor } }
                  target: { store: vendor, directory: /incoming, key: "sets/{name}" }
                  notify: [ { on: acked, channel: downstream } ]
            """.trimIndent(),
        )
    }

    /** The metadata file an operator tries, with the two children it lists sitting beside it. */
    private fun sampleSet() = dir.resolve("set-1.json").also {
        Files.writeString(it, """{"images":[{"path":"/inbox/a.png"},{"path":"/inbox/b.png"}]}""")
        Files.writeString(dir.resolve("a.png"), "aaa")
        Files.writeString(dir.resolve("b.png"), "bb")
    }

    /** A route whose only step is `unzip`, for the two verdicts the pipeline reaches after the chain (ticket 25). */
    private fun unzipDrop() = dir.resolve("unzip.yaml").also {
        Files.writeString(
            it,
            """
            shuttle:
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, staging: { dir: $dir } }
                minio:
                  s3: { endpoint: https://minio.internal, credentials: { accessKey: ${'$'}{S3_KEY}, secretKey: ${'$'}{S3_SECRET} } }
              routes:
                unzip-drop:
                  source: { poll: { store: vendor, directory: /inbox, every: 1h, onAck: delete } }
                  process:
                    - { unzip: {} }
                  target: { store: minio, bucket: landing, key: "vendor/{name}" }
            """.trimIndent(),
        )
    }

    /** Review finding Spec 7: D35 promised the real chain, and a route with `expand` could not be tried at all. */
    @Test
    fun SPEC7_a_route_with_expand_prints_one_key_and_one_body_per_child_fetched_from_the_sample_files() {
        val metadata = sampleSet()

        val (code, out) = run(imageSets(), route = "image-sets", fileName = "set-1.json", content = metadata)

        assertEquals(0, code, out)
        assertTrue(out.contains("key: sets/a.png"), out)
        assertTrue(out.contains("key: sets/b.png"), out)
        assertEquals(2, out.split("body downstream (acked):").size - 1, out)
        assertTrue(out.contains("\"name\" : \"a.png\""), out)
        assertTrue(out.contains("\"name\" : \"b.png\""), out)
        // each child's digest is its own, recomputed by the chain from the bytes it fetched
        assertTrue(out.contains(digestOf("aaa".toByteArray(), DigestAlgorithm.MD5).hex), out)
        assertTrue(out.contains(digestOf("bb".toByteArray(), DigestAlgorithm.MD5).hex), out)
    }

    /**
     * Review finding Spec 7: the key an operator sees is `TransferPipeline`'s own, resolved by the function
     * both call. `try` used to prepend an SFTP target's `directory`, which the pipeline never puts in a key.
     */
    @Test
    fun SPEC7_the_printed_key_is_the_one_the_pipeline_would_store_under() {
        val yaml = imageSets()
        val target = YamlLoader.load(listOf(Files.readString(yaml)), env).routes.single().target

        val (code, out) = run(yaml, route = "image-sets", fileName = "set-1.json", content = sampleSet())

        assertEquals(0, code, out)
        assertTrue(out.contains("key: " + targetKey(target, "a.png", "set-1.json", emptyMap(), clock)), out)
        assertFalse(out.contains("/incoming"), out)
    }

    /** Rule 22 is the chain's, so try mode judges it exactly as a running route does (spec 6.4). */
    @Test
    fun rule22_an_attribute_name_over_64_characters_is_rejected_in_try_mode_too() {
        val long = "o".repeat(65)

        val (code, out) = run(vendorDrop(attribute = long, group = long))

        assertEquals(1, code)
        assertTrue(out.contains("reject: rule 22: attribute name $long is longer than 64 characters"), out)
    }

    /** Spec 6.1 and ticket 25: a payload of nothing is the pipeline's reject, and try mode says so instead of printing a key. */
    @Test
    fun SPEC7_a_chain_that_leaves_no_object_is_the_verdict_the_pipeline_would_reach() {
        val zip = dir.resolve("empty.zip")
        ZipOutputStream(Files.newOutputStream(zip)).close()

        val (code, out) = run(unzipDrop(), route = "unzip-drop", fileName = "empty.zip", content = zip)

        assertEquals(1, code)
        assertTrue(out.contains("reject: process: the chain left no object to store"), out)
    }

    /** Rule 13's run-time half (ticket 25): a resolved key with a `..` segment is refused before any store. */
    @Test
    fun SPEC7_a_key_that_leaves_the_target_directory_is_the_verdict_the_pipeline_would_reach() {
        val zip = dir.resolve("escape.zip")
        ZipOutputStream(Files.newOutputStream(zip)).use {
            it.putNextEntry(ZipEntry("../escaped.csv"))
            it.write("a,b\n".toByteArray())
            it.closeEntry()
        }

        val (code, out) = run(unzipDrop(), route = "unzip-drop", fileName = "escape.zip", content = zip)

        assertEquals(1, code)
        assertTrue(out.contains("reject: key: vendor/../escaped.csv leaves the target directory"), out)
    }
}
