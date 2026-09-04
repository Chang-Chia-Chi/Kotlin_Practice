package infra.shuttle.jdbi

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class StateStoreSchemaTest {

    @Test
    fun the_DDL_text_matches_spec_8_1_verbatim() {
        val spec = Files.readString(fileAbove("docs/shuttle/spec.md")).replace("\r\n", "\n") // the checkout may be CRLF; the constant is not
        val section = spec.substringAfter("### 8.1 Tables").substringBefore("### 8.2")
        val block = section.substringAfter("```sql\n").substringBefore("\n```")
        assertEquals(block.trimEnd(), StateStoreSchema.DDL.trimEnd())
    }

    /** The quickstart's Oracle container applies this file at first start; it is the same DDL or the example lies. */
    @Test
    fun the_example_schema_file_is_the_same_DDL() {
        val schema = Files.readString(fileAbove("shuttle/examples/schema.sql")).replace("\r\n", "\n")
        assertEquals(StateStoreSchema.DDL.trimEnd(), schema.trimEnd())
    }

    @Test
    fun the_statements_are_the_two_sequences_two_tables_and_three_indexes_without_comments() {
        val s = StateStoreSchema.statements()
        assertEquals(7, s.size)
        assertTrue(s.none { it.contains("--") || it.contains(";") })
        assertTrue(s[0].startsWith("CREATE SEQUENCE file_transfer_seq"))
        assertTrue(s[2].startsWith("CREATE TABLE file_transfer"))
    }

    private fun fileAbove(relative: String): Path {
        var dir: Path? = Paths.get("").toAbsolutePath()
        while (dir != null) {
            val candidate = dir.resolve(relative)
            if (Files.exists(candidate)) return candidate
            dir = dir.parent
        }
        error("$relative not found above ${Paths.get("").toAbsolutePath()}")
    }
}
