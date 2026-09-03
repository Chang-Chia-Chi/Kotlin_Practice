package infra.shuttle.core

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

/** Spec 9.6: the mapping table as a pure function from the row plus attributes to a JSON tree. */
class MappingRendererTest {

    private val mapper = ObjectMapper()
    private val seen = Instant.parse("2026-09-03T10:00:00Z")
    private val transfer = Transfer(
        id = TransferId(42),
        identity = SourceIdentity(RouteName("vendor-drop"), SourceKind.SFTP, "vendor:/inbox", "123-order.csv", 2048, seen),
        kind = TransferKind.OBJECT,
        state = TransferState.ACKED,
        sourceDigest = Digest(DigestAlgorithm.MD5, "aa"),
        digest = Digest(DigestAlgorithm.MD5, "bb"),
        storedName = "20260903-123-order.csv.zip",
        storedMtime = seen.plusSeconds(60),
        attributes = mapOf("orderNumber" to "  123 ", "empty" to ""),
        target = TargetRef("s3", "landing", "vendor/20260903-123-order.csv.zip", "v1", 1024),
        firstSeenAt = seen,
        updatedAt = seen.plusSeconds(120),
        ackedAt = seen.plusSeconds(90),
    )

    private fun table(vararg rows: MappingRow) = MappingTable(rows.toList())
    private fun render(vararg rows: MappingRow, providers: Map<String, Provider> = emptyMap()) =
        runBlocking { MappingRenderer { providers[it] }.render(table(*rows), transfer, DeliveryMoment.ACKED, 3) }

    @Test
    fun field_rows_read_the_transfer_row_and_dotted_paths_nest() {
        val body = render(
            MappingRow("fileId", field = "TRANSFER_ID"),
            MappingRow("file.name", field = "STORED_NAME"),
            MappingRow("file.size", field = "TARGET_SIZE"),
            MappingRow("event", field = "EVENT"),
            MappingRow("attempt", field = "ATTEMPT"),
        )
        assertEquals(
            mapper.readTree("""{"fileId":"42","file":{"name":"20260903-123-order.csv.zip","size":"1024"},"event":"acked","attempt":"3"}"""),
            body,
        )
    }

    @Test
    fun attribute_and_value_rows() {
        val body = render(MappingRow("orderNumber", attribute = "orderNumber"), MappingRow("source", value = "vendor-drop"))
        assertEquals(mapper.readTree("""{"orderNumber":"  123 ","source":"vendor-drop"}"""), body)
    }

    @Test
    fun a_provider_mounts_whole_and_select_picks_a_piece() {
        val order = Provider { mapper.readTree("""{"id":7,"lines":[{"sku":"A"}]}""") }
        val body = render(
            MappingRow("order", provider = "orderDetails"),
            MappingRow("firstSku", provider = "orderDetails", select = "/lines/0/sku"),
            providers = mapOf("orderDetails" to order),
        )
        assertEquals(mapper.readTree("""{"order":{"id":7,"lines":[{"sku":"A"}]},"firstSku":"A"}"""), body)
    }

    @Test
    fun I22_a_provider_selected_by_three_rows_is_invoked_once() {
        var invocations = 0
        val order = Provider { invocations++; mapper.readTree("""{"id":7,"customer":"acme","total":9.5}""") }
        val body = render(
            MappingRow("order.id", provider = "orderDetails", select = "/id"),
            MappingRow("order.customer", provider = "orderDetails", select = "/customer"),
            MappingRow("order.total", provider = "orderDetails", select = "/total"),
            providers = mapOf("orderDetails" to order),
        )
        assertEquals(1, invocations)
        assertEquals(mapper.readTree("""{"order":{"id":7,"customer":"acme","total":9.5}}"""), body)
    }

    @Test
    fun a_name_with_quotes_and_backslashes_survives_serialisation() {
        val odd = transfer.copy(storedName = """he said "hi" \ back\slash""")
        val body = runBlocking { MappingRenderer().render(table(MappingRow("name", field = "STORED_NAME")), odd, DeliveryMoment.ACKED) }
        val text = mapper.writeValueAsString(body)
        assertTrue(text.contains("""\"hi\""""), text)
        assertEquals("""he said "hi" \ back\slash""", mapper.readTree(text).get("name").textValue())
    }

    @Test
    fun type_coerces_to_number_and_boolean() {
        val body = render(
            MappingRow("size", field = "TARGET_SIZE", type = MappingType.NUMBER),
            MappingRow("attempt", field = "ATTEMPT", type = MappingType.NUMBER),
            MappingRow("flag", value = "true", type = MappingType.BOOLEAN),
            MappingRow("price", value = "9.50", type = MappingType.NUMBER),
        )
        assertEquals(mapper.readTree("""{"size":1024,"attempt":3,"flag":true,"price":9.50}"""), body)
    }

    @Test
    fun format_renders_an_instant_and_defaults_to_ISO_INSTANT() {
        val body = render(
            MappingRow("receivedAt", field = "SOURCE_MTIME", format = "ISO_INSTANT"),
            MappingRow("day", field = "SOURCE_MTIME", format = "yyyy-MM-dd"),
            MappingRow("acked", field = "ACKED_AT"),
        )
        assertEquals(mapper.readTree("""{"receivedAt":"2026-09-03T10:00:00Z","day":"2026-09-03","acked":"2026-09-03T10:01:30Z"}"""), body)
    }

    @Test
    fun default_applies_before_required_and_required_false_omits_the_path() {
        val body = render(
            MappingRow("parent", field = "PARENT_ID", default = "none"),
            MappingRow("missing", attribute = "nope", required = false),
            MappingRow("present", attribute = "orderNumber", default = "unused"),
        )
        assertEquals(mapper.readTree("""{"parent":"none","present":"  123 "}"""), body)
        assertFalse(body.has("missing"))
    }

    @Test
    fun a_missing_required_value_reports_the_row() {
        val failure = assertThrows(MappingFailure::class.java) { render(MappingRow("order.number", attribute = "nope")) }
        assertEquals("order.number", failure.path)
        assertTrue(failure.message!!.contains("nope"), failure.message)
    }

    @Test
    fun trim_upper_and_lower_transform_the_value() {
        val body = render(
            MappingRow("trimmed", attribute = "orderNumber", trim = true),
            MappingRow("upper", field = "ROUTE", upper = true),
            MappingRow("lower", value = "ACME", lower = true),
            MappingRow("empty", attribute = "empty", trim = true, default = "n/a"),
        )
        assertEquals(mapper.readTree("""{"trimmed":"123","upper":"VENDOR-DROP","lower":"acme","empty":"n/a"}"""), body)
    }

    @Test
    fun check_rejects_each_bad_row_by_rule_number() {
        val table = table(
            MappingRow("a", attribute = "undeclared"),
            MappingRow("b", field = "NOPE"),
            MappingRow("c", provider = "unregistered"),
            MappingRow("d", provider = "orderDetails", select = "no-slash"),
            MappingRow("e", field = "SOURCE_MTIME", format = "yyyy-{"),
            MappingRow("f", field = "TRANSFER_ID", value = "v"),
            MappingRow("g", field = "DIGEST", digest = "crc32"),
            MappingRow("ok", attribute = "orderNumber"),
        )
        val violations = MappingRenderer.check(table, declaredAttributes = setOf("orderNumber")) { it == "orderDetails" }
        assertEquals(mapOf("a" to 17, "b" to 16, "c" to 15, "d" to 18, "e" to 18, "f" to 19, "g" to 21), violations.associate { it.message.substringAfter("row ").substringBefore(":") to it.rule })
    }

    @Test
    fun check_without_declared_attributes_skips_rule_17() {
        val table = table(MappingRow("a", attribute = "anything"), MappingRow("b", field = "NOPE"))
        assertEquals(listOf(16), MappingRenderer.check(table, declaredAttributes = null) { true }.map { it.rule })
    }
}
