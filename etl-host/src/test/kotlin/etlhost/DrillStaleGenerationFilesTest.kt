package etlhost

import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.security.TestSecurity
import io.restassured.RestAssured.given
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Day-2 drill 4: the pod was OOMKilled mid-build and restarted with generation files on disk.
 *
 * Plants what a crash leaves behind - a promoted generation, a half-written `.tmp`, a `.wal`, and a
 * file for a group this deploy no longer serves - then boots the host over them. snapshotcache spec
 * 10.1 step 1 says every one of them is unowned, because the current pointer is never persisted.
 */
class StaleFilesFixture : HostFixture() {

    override fun start(): Map<String, String> {
        val overrides = super.start()
        val storage = Path.of(overrides.getValue("etl-host.cache.storage-path"))
        val served = Files.createDirectories(storage.resolve(GROUP))
        val dropped = Files.createDirectories(storage.resolve("a-group-nobody-serves-any-more"))

        planted = listOf(
            served.resolve("gen_0000000007.db"),
            served.resolve("gen_0000000008.db.tmp"),
            served.resolve("gen_0000000008.db.wal"),
            dropped.resolve("gen_0000000042.db"),
            // The flat layout that predates the per-group directories.
            storage.resolve("gen_0000000003.db"),
            // Not a generation file. Nothing may touch it.
            served.resolve("operator-notes.txt"),
        )
        planted.forEach { Files.writeString(it, "not a real duckdb file") }
        return overrides
    }

    companion object {
        lateinit var planted: List<Path>
    }
}

@QuarkusTest
@WithTestResource(StaleFilesFixture::class)
class DrillStaleGenerationFilesTest {

    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `startup wipes every leftover generation file, including groups no longer served`() {
        val (generations, other) = StaleFilesFixture.planted.partition { it.fileName.toString().startsWith("gen_") }

        assertThat(generations)
            .withFailMessage(
                "spec 10.1 step 1 says every gen_* file under the cache directory is deleted; these " +
                    "survived: %s",
                generations.filter { Files.exists(it) },
            )
            .allSatisfy { assertThat(Files.exists(it)).isFalse() }

        assertThat(other).allSatisfy {
            assertThat(Files.exists(it))
                .withFailMessage("the wipe deleted a non-generation file: %s", it)
                .isTrue()
        }
    }

    /**
     * And the numbering. With the wipe on (the default), numbering restarts at 1 regardless of the
     * leftover `gen_0000000007.db` - which is only safe *because* the wipe removed it, and is the
     * pairing spec 5.4 spells out.
     */
    @Test
    @TestSecurity(user = "ops", roles = ["etl-admin"])
    fun `numbering restarts at 1 after a wipe`() {
        val body = given().get("/admin/etl/snapshot/${HostFixture.GROUP}")
            .then().statusCode(200).extract().body().asString()
        println("=== DRILL 4 stale-boot snapshot ===")
        println(body)
        println("=== end ===")
        assertThat(body).contains(""""generation":1""")
    }
}
