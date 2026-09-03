package infra.shuttle.quarkus

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path

/** Spec 12.2: `shuttle validate <files>` prints every violation with its rule number and exits non-zero; it holds no client of any kind. */
class ValidateCommandTest {
    @TempDir lateinit var dir: Path

    private val env = mapOf("SFTP_USER" to "u", "SFTP_PASSWORD" to "p")

    private fun run(vararg files: Path, beans: (String) -> Set<String>? = { null }): Pair<Int, String> {
        val bytes = ByteArrayOutputStream()
        val code = ValidateCommand(files.toList(), env, beans, PrintStream(bytes, true)).run()
        return code to bytes.toString()
    }

    @Test
    fun S25_five_violations_print_five_rule_numbers_and_exit_non_zero() {
        val file = dir.resolve("shuttle.yaml")
        Files.writeString(
            file,
            """
            shuttle:
              drainTimeout: 60s
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: hunter2 }, staging: { dir: $dir } }
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

        val (code, out) = run(file)

        assertEquals(1, code)
        val rules = Regex("^rule (\\d+):", RegexOption.MULTILINE).findAll(out).map { it.groupValues[1].toInt() }.toList()
        assertEquals(listOf(1, 3, 7, 12, 25), rules.distinct().sorted(), out)
    }

    @Test
    fun S24_rule_9_is_reported_in_validate_mode() {
        val file = dir.resolve("shuttle.yaml")
        Files.writeString(
            file,
            """
            shuttle:
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, pool: { maxSize: 2 }, staging: { dir: $dir } }
              routes:
                mirror:
                  source: { poll: { store: vendor, directory: /outbound, every: 1h, onAck: delete } }
                  target: { store: vendor, directory: /incoming }
                  parallelism: 4
            """.trimIndent(),
        )

        val (code, out) = run(file)

        assertEquals(1, code)
        assertTrue(out.contains("rule 9:"), out)
    }

    @Test
    fun a_document_that_is_not_a_configuration_prints_its_load_errors_and_exits_non_zero() {
        val file = dir.resolve("broken.yaml")
        Files.writeString(file, "shuttle:\n  routes:\n    mirror:\n      source: { poll: { store: vendor, directory: /x, every: soon } }\n")

        val (code, out) = run(file)

        assertEquals(1, code)
        assertTrue(out.contains("every: soon is not a duration"), out)
    }

    @Test
    fun a_valid_document_exits_zero() {
        val file = dir.resolve("ok.yaml")
        Files.writeString(
            file,
            """
            shuttle:
              objectStores:
                vendor:
                  sftp: { host: sftp.example, auth: { user: ${'$'}{SFTP_USER}, password: ${'$'}{SFTP_PASSWORD} }, staging: { dir: $dir } }
                minio:
                  s3: { endpoint: http://minio, credentials: { accessKey: ${'$'}{SFTP_USER}, secretKey: ${'$'}{SFTP_PASSWORD} } }
              routes:
                mirror:
                  source: { poll: { store: vendor, directory: /outbound, every: 1h, onAck: delete } }
                  target: { store: minio, bucket: landing }
            """.trimIndent(),
        )

        assertEquals(0, run(file).first)
    }
}
