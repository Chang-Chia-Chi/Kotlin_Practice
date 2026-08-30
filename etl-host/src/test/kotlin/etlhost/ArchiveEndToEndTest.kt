package etlhost

import infra.snapshotarchive.ArchiveStatus
import infra.snapshotarchive.ManifestDao
import infra.snapshotarchive.ManifestEntry
import infra.snapshotarchive.ManifestSchema
import infra.snapshotarchive.ObjectStore
import infra.snapshotcache.api.GroupId
import infra.snapshotcache.bootstrap.ManagedSnapshotCache
import io.minio.MakeBucketArgs
import io.minio.MinioClient
import io.quarkus.test.common.WithTestResource
import io.quarkus.test.junit.QuarkusTest
import io.quarkus.test.junit.TestProfile
import jakarta.inject.Inject
import java.nio.file.Path
import java.sql.DriverManager
import java.time.Clock
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MinIOContainer

/**
 * The Oracle host of [OracleSource], plus the two things the archive layer refuses to create for
 * itself: the bucket and the manifest table.
 *
 * Both refusals are deliberate upstream. `ObjectStore` will not auto-create a bucket, because a
 * bucket made by whichever pod started first is exactly the ambient side effect the layer's
 * ordering guarantees exist to avoid; `ManifestSchema.DDL` is applied by the DBA. A fixture is the
 * test-shaped equivalent of both, and `docker-compose.staging.yml` is the deployment-shaped one.
 */
class ArchiveSource : OracleSource() {

    private lateinit var minio: MinIOContainer

    override fun startSource(root: Path): Map<String, String> {
        val oracle = super.startSource(root)

        DriverManager.getConnection(
            oracle.getValue("etl-host.source.url"),
            oracle.getValue("etl-host.source.username"),
            oracle.getValue("etl-host.source.password"),
        ).use { connection ->
            connection.createStatement().use { st -> ManifestSchema.DDL.forEach(st::execute) }
        }

        // Pinned to the tag this environment already has, exactly as the archive layer's own
        // ObjectStoreTest pins it; the Testcontainers module's default image is not present.
        minio = MinIOContainer("minio/minio:RELEASE.2024-10-02T17-50-41Z")
        minio.start()
        client(minio.s3URL, minio.userName, minio.password)
            .makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build())

        return oracle + mapOf(
            "etl-host.archive.enabled" to "true",
            "etl-host.archive.endpoint" to minio.s3URL,
            "etl-host.archive.bucket" to BUCKET,
            "etl-host.archive.access-key" to minio.userName,
            "etl-host.archive.secret-key" to minio.password,
            "etl-host.archive.temp-directory" to root.resolve("archive").toString(),
            // Everything but the newest COMPLETE version is past retention the moment it lands.
            // The window is the deployment's number; what this test is about is the rule that
            // survives any window - the last good baseline is never reclaimed.
            "etl-host.archive.retention" to "PT0S",
        )
    }

    override fun stop() {
        super.stop()
        if (this::minio.isInitialized) minio.stop()
    }

    companion object {

        const val BUCKET = "snapshot-archive"

        fun client(endpoint: String, accessKey: String, secretKey: String): MinioClient =
            MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build()
    }
}

/**
 * **snapshotcache spec 18 hosted** - the half of M3 that was built, tested against its own fakes
 * and containers, and then never run by an application.
 *
 * A published generation is exported to Parquet under a lease, uploaded, and committed as a
 * manifest row; a second generation becomes a second version; and a maintenance sweep enforces
 * retention against both. Three separate claims, and only the third of them can be made by a test
 * that owns a real bucket *and* a real manifest at once:
 *
 * 1. the COMPLETE row exists and its object really landed at the size the inventory promised;
 * 2. a second run publishes a strictly newer version rather than overwriting the first;
 * 3. the purge reclaims the aged version's row **and its objects**, and leaves the newest COMPLETE
 *    alone - D34's keep-newest rule, which is what stops a stopped archiver from having its last
 *    good baseline aged out from under every consuming ETL.
 *
 * `archive.submit(...).get()` rather than waiting for the hourly `@Scheduled` tick, for the same
 * reason [CacheTick]'s logic is called directly: that Quarkus fires an `@Scheduled` method is
 * Quarkus's property. What is under test here is what happens inside one.
 *
 * `@Tag("oracle")` and excluded by default, the same convention as [HostEndToEndOracleTest] - the
 * manifest's SQL is Oracle (`NEXTVAL`, `FETCH FIRST`), so this class costs an Oracle container and
 * a MinIO one. Opt in with **`-DexcludedGroups=none`**, never `-Dgroups=oracle`:
 *
 * ```
 * mvn -pl etl-host -am test -Dtest=ArchiveEndToEndTest -DexcludedGroups=none \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * ```
 */
@QuarkusTest
@WithTestResource(ArchiveSource::class)
@TestProfile(OracleProfile::class)
@Tag("oracle")
class ArchiveEndToEndTest {

    @Inject
    lateinit var archive: ArchiveWiring

    @Inject
    lateinit var managed: ManagedSnapshotCache

    @Inject
    lateinit var config: HostConfig

    private val group = GroupId(HostFixture.GROUP)

    @Test
    fun `a published generation is archived, the manifest row appears, and retention prunes it`() {
        val manifest = manifestDao()
        val objects = objectStore()

        // 1. The generation the startup refresh published, checkpointed.
        archive.submit(group)!!.get(180, TimeUnit.SECONDS)

        val first = requireNotNull(manifest.newestComplete(group)) {
            "no COMPLETE manifest row after an archive run; the layer is wired but not working"
        }
        assertThat(first.status).isEqualTo(ArchiveStatus.COMPLETE)
        assertThat(first.uriPrefix).isEqualTo("${ArchiveSource.BUCKET}/snapshots/${HostFixture.GROUP}/v${first.version}/")
        assertThat(objects.sizeOf(keyOf(first)))
            .withFailMessage("the manifest says v%d is COMPLETE but its object is not in the bucket", first.version)
            .isNotNull()

        // 2. A second generation is a second version, never an overwrite. `data_as_of` is the only
        //    join key between the ephemeral generation numbering and the durable manifest (D31),
        //    and it is what the monotonicity guard compares.
        managed.admin.triggerRefresh(group)
        archive.submit(group)!!.get(180, TimeUnit.SECONDS)

        val second = requireNotNull(manifest.newestComplete(group))
        assertThat(second.version).isGreaterThan(first.version)
        assertThat(second.dataAsOf).isAfter(first.dataAsOf)

        // 3. Retention. Both versions are past a zero-length window; exactly one survives it,
        //    because the newest COMPLETE is never reclaimed however old it is.
        archive.sweep()

        assertThat(manifest.find(group, first.version))
            .withFailMessage("v%d survived a zero retention window", first.version)
            .isNull()
        assertThat(objects.sizeOf(keyOf(first)))
            .withFailMessage(
                "v%d's row is gone but its object is still in the bucket - objects are deleted " +
                    "before the row precisely so this cannot happen",
                first.version,
            )
            .isNull()

        assertThat(manifest.find(group, second.version))
            .withFailMessage("the newest COMPLETE version was reclaimed; D34's keep-newest rule is not holding")
            .isNotNull()
        assertThat(objects.sizeOf(keyOf(second))).isNotNull()
    }

    /** The one table this group archives, at the key `uri_prefix` implies. */
    private fun keyOf(entry: ManifestEntry): String =
        entry.uriPrefix.removePrefix("${config.archiveBucket}/") + "${HostFixture.GROUP}.parquet"

    private fun manifestDao(): ManifestDao = ManifestDao(
        Jdbi.create(config.sourceUrl, config.sourceUsername.orElse(null), config.sourcePassword.orElse(null)),
        config.archiveBucket,
        Clock.systemUTC(),
    )

    private fun objectStore(): ObjectStore = ObjectStore(
        ArchiveSource.client(config.archiveEndpoint, config.archiveAccessKey, config.archiveSecretKey),
        config.archiveBucket,
    )
}
