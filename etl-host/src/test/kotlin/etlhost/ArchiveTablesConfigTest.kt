package etlhost

import infra.snapshotcache.api.GroupId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The archive layer's one piece of host-side validation, checked without a container.
 *
 * `etl-host.archive.tables`' keys are group names, and nothing else in the configuration says so.
 * Unvalidated, a typo reaches `Archiver.publish`'s `requireNotNull(tables[group])` and takes out
 * one archive run **per hour**, logged as an archiver failure rather than as the configuration
 * mistake it is - so the pod looks healthy while it checkpoints nothing.
 */
class ArchiveTablesConfigTest {

    @Test
    fun `a group's comma-separated table list is split and trimmed`() {
        val tables = ArchiveWiring.tablesFor(
            mapOf("wip" to "wip, wip_history ", "equipment" to "equipment"),
            setOf("wip", "equipment"),
        )
        assertThat(tables).isEqualTo(
            mapOf(
                GroupId("wip") to listOf("wip", "wip_history"),
                GroupId("equipment") to listOf("equipment"),
            ),
        )
    }

    @Test
    fun `a key that is not a group is rejected, and the message names both sets`() {
        assertThatThrownBy { ArchiveWiring.tablesFor(mapOf("wpi" to "wip"), setOf("wip", "equipment")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("wpi")
            .hasMessageContaining("wip")
            .hasMessageContaining("equipment")
    }
}
