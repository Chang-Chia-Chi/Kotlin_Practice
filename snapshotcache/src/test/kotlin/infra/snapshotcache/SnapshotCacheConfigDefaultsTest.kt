package infra.snapshotcache

import infra.snapshotcache.api.SnapshotCacheConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.Duration

/** Spec 13's table, asserted verbatim. A default that drifts from the spec fails here. */
class SnapshotCacheConfigDefaultsTest {

    private val config = SnapshotCacheConfig(
        storagePath = Path.of("/data/cache"),
        tempDirectory = Path.of("/data/tmp"),
    )

    @Test
    fun `defaults match spec section 13`() {
        assertThat(config.refreshInterval).isEqualTo(Duration.ofMinutes(10))
        assertThat(config.allowOverlap).isFalse()
        assertThat(config.maxLiveGenerations).isEqualTo(3)
        assertThat(config.defaultWaitBudget).isEqualTo(Duration.ofSeconds(30))
        assertThat(config.leaseDeadline).isEqualTo(Duration.ofMinutes(5))
        assertThat(config.jdbcFetchSize).isEqualTo(2000)
        assertThat(config.servingMemoryLimit).isEqualTo("3GB")
        assertThat(config.consumerMemoryLimit).isEqualTo("1GB")
        assertThat(config.clearStaleFilesOnStartup).isTrue()
        assertThat(config.leaseDrainTimeout).isEqualTo(Duration.ofSeconds(30))
    }

    @Test
    fun `verify defaults match spec section 13`() {
        val verify = config.verify
        assertThat(verify.nonEmpty).isTrue()
        assertThat(verify.readable).isTrue()
        assertThat(verify.keyUnique).isTrue()
        assertThat(verify.requiredNonNull).isEmpty()
        assertThat(verify.consecutiveFailureThreshold).isEqualTo(3)
        assertThat(verify.rowCountDelta.enabled).isFalse()
        assertThat(verify.rowCountDelta.maxDecreaseRatio).isEqualTo(0.20)
        assertThat(verify.rowCountDelta.maxIncreaseRatio).isEqualTo(1.00)
    }
}
