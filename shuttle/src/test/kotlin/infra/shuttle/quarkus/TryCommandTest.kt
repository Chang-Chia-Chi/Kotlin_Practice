package infra.shuttle.quarkus

import infra.shuttle.testkit.ClockFixture
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path

/** Spec 12.2 and S31: `shuttle try` runs one route's chain over a sample and prints what the deployment would do, connecting to nothing. */
class TryCommandTest {
    @TempDir lateinit var dir: Path

    private val env = mapOf("SFTP_USER" to "u", "SFTP_PASSWORD" to "p", "S3_KEY" to "k", "S3_SECRET" to "s", "TOKEN" to "t")
    private val clock = ClockFixture()

    private fun vendorDrop(attribute: String = "orderNumber") = dir.resolve("shuttle.yaml").also {
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
                    - { extract: { from: fileName, regex: "(?<orderNumber>\\d+)-.*\\.csv" } }
                    - { rename: { pattern: "{yyyyMMdd}-{name}" } }
                  target: { store: minio, bucket: landing, key: "vendor/{name}" }
                  notify: [ { on: acked, channel: downstream } ]
            """.trimIndent(),
        )
    }

    private fun run(file: Path, route: String = "vendor-drop", fileName: String = "123-order.csv"): Pair<Int, String> {
        val bytes = ByteArrayOutputStream()
        val code = TryCommand(listOf(file), env, NamedBeans.none, PrintStream(bytes, true), clock, route, fileName).run()
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
}
