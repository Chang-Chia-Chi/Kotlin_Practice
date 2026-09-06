package dynacache.engine.persist

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/** The file adapter of the dot ceiling (T51): what a node reads back after a restart. */
class DotCeilingStoreTest {

    @TempDir
    lateinit var dir: Path

    @Test
    fun a_fresh_node_has_no_ceiling() {
        assertEquals(0L, DotCeilingStore.inFile(dir.resolve("node-1").resolve("dots")).load())
    }

    @Test
    fun the_last_reserved_ceiling_is_what_a_restart_loads() {
        val file = dir.resolve("node-1").resolve("dots")
        val before = DotCeilingStore.inFile(file)
        before.reserve(1000)
        before.reserve(2000)

        assertEquals(2000L, DotCeilingStore.inFile(file).load())
        assertFalse(Files.exists(file.resolveSibling("dots.tmp")), "the temporary file is renamed away")
    }

    @Test
    fun a_ceiling_that_cannot_be_read_fails_loudly_rather_than_starting_over() {
        val file = dir.resolve("dots")
        Files.writeString(file, "not a number")

        assertThrows(NumberFormatException::class.java) { DotCeilingStore.inFile(file).load() }
    }

    @Test
    fun in_memory_survives_only_its_own_instance() {
        val ceilings = DotCeilingStore.inMemory()
        ceilings.reserve(500)

        assertEquals(500L, ceilings.load())
        assertEquals(0L, DotCeilingStore.inMemory().load())
    }
}
