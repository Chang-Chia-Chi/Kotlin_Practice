package infra.shuttle.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** A required row had no value, or a value the row cannot coerce (spec 6.4): the transfer fails at attribute freeze, naming the row. */
class MappingFailure(val path: String, detail: String) : RuntimeException("mapping row $path: $detail")

/**
 * Spec 9.6: a pure function from a transfer row plus its frozen attributes to a JSON tree, rendered at
 * send time (D19). `providers` resolves a named bean; each is invoked at most once per rendering (I22).
 * A value is missing when its source has none, a `select` points at nothing, or the text is blank after
 * `trim`; `default` fills a missing value before `required` is judged.
 */
class MappingRenderer(private val providers: (String) -> Provider? = { null }) {

    suspend fun render(table: MappingTable, transfer: Transfer, moment: DeliveryMoment, attempt: Int = 1): JsonNode {
        val root = mapper.createObjectNode()
        val provided = HashMap<String, JsonNode>()
        for (row in table.rows) {
            val raw: Any? = when {
                row.field != null -> field(Field.valueOf(row.field), transfer, moment, attempt, row.digest)
                row.attribute != null -> transfer.attributes[row.attribute]
                row.provider != null -> provided.getOrPut(row.provider) {
                    providers(row.provider)?.provide(transfer) ?: throw MappingFailure(row.path, "no provider named ${row.provider}")
                }.let { if (row.select == null) it else it.at(row.select) }
                else -> row.value
            }
            // A provider's node keeps its own JSON type unless the row transforms or coerces it.
            val untouched = !row.trim && !row.upper && !row.lower && row.type == MappingType.STRING
            val node = if (raw is JsonNode && !raw.isMissingNode && !raw.isNull && (raw.isContainerNode || untouched)) raw else scalar(row, raw)
            if (node != null) put(root, row.path, node)
        }
        return root
    }

    private fun scalar(row: MappingRow, raw: Any?): JsonNode? {
        var text = when (raw) {
            null -> null
            is JsonNode -> if (raw.isMissingNode || raw.isNull) null else raw.asText()
            is Instant -> format(row.format, raw)
            else -> raw.toString()
        }
        if (row.trim) text = text?.trim()
        if (row.upper) text = text?.uppercase()
        if (row.lower) text = text?.lowercase()
        val value = text?.takeIf { it.isNotEmpty() } ?: row.default
            ?: if (row.required) throw MappingFailure(row.path, "no value for ${row.field ?: row.attribute ?: row.provider}") else return null
        return when (row.type) {
            MappingType.STRING -> TextNode(value)
            MappingType.NUMBER -> runCatching { mapper.readTree(value) }.getOrNull()?.takeIf { it.isNumber }
                ?: throw MappingFailure(row.path, "$value is not a number")
            MappingType.BOOLEAN -> value.toBooleanStrictOrNull()?.let(BooleanNode::valueOf)
                ?: throw MappingFailure(row.path, "$value is not a boolean")
        }
    }

    private fun format(format: String?, instant: Instant): String = when (format) {
        null, "ISO_INSTANT" -> instant.toString()
        else -> formatter(format).format(instant.atOffset(ZoneOffset.UTC))
    }

    /** `asked` is the row's `digest: <algo>`: a `DIGEST` row asking for an algorithm the transfer does not carry is missing (spec 9.6, D49). */
    private fun field(field: Field, t: Transfer, moment: DeliveryMoment, attempt: Int, asked: String?): Any? = when (field) {
        Field.TRANSFER_ID -> t.id.value
        Field.PARENT_ID -> t.parentId?.value
        Field.ROUTE -> t.identity.route.value
        Field.KIND -> t.kind.name.lowercase()
        Field.SOURCE_KIND -> t.identity.sourceKind.name.lowercase()
        Field.SOURCE_REF -> t.identity.sourceRef
        Field.SOURCE_NAME -> t.identity.sourceName
        Field.SOURCE_PATH -> "${t.identity.sourceRef}/${t.identity.sourceName}"
        Field.SOURCE_SIZE -> t.identity.sourceSize
        Field.SOURCE_MTIME -> t.identity.sourceMtime
        Field.SOURCE_DIGEST -> t.sourceDigest?.hex
        Field.STORED_NAME -> t.storedName
        Field.STORED_MTIME -> t.storedMtime
        Field.DIGEST -> t.digest?.takeIf { asked == null || it.algorithm.name.equals(asked, ignoreCase = true) }?.hex
        Field.DIGEST_ALGO -> t.digest?.algorithm?.name?.lowercase()
        Field.TARGET_KIND -> t.target?.kind
        Field.TARGET_LOCATION -> t.target?.location
        Field.TARGET_KEY -> t.target?.key
        Field.TARGET_REF -> t.target?.ref
        Field.TARGET_SIZE -> t.target?.size
        Field.FIRST_SEEN_AT -> t.firstSeenAt
        Field.ACKED_AT -> t.ackedAt
        Field.EVENT -> moment.name.lowercase()
        Field.ATTEMPT -> attempt
    }

    private fun put(root: ObjectNode, path: String, value: JsonNode) {
        val segments = path.split('.')
        val parent = segments.dropLast(1).fold(root) { node, seg -> node.get(seg) as? ObjectNode ?: node.putObject(seg) }
        parent.set<JsonNode>(segments.last(), value)
    }

    companion object {
        private val mapper = ObjectMapper()

        /**
         * Spec 9.6 boot checks, rules 15 to 19 and 21, one violation per bad row. `declaredAttributes` is the
         * route's frozen attribute names (rule 17); null skips that rule for a table checked outside any route.
         * Called by [Rules] at boot and by the pipeline at attribute freeze (spec 6.4).
         */
        fun check(table: MappingTable, declaredAttributes: Set<String>?, providerExists: (String) -> Boolean): List<Violation> = buildList {
            for (row in table.rows) {
                val at = "row ${row.path}"
                fun fail(rule: Int, message: String) = add(Violation(rule, "$at: $message"))
                if (listOfNotNull(row.field, row.attribute, row.provider, row.value).size != 1) fail(19, "exactly one of field, attribute, provider, value")
                if (row.field != null && Field.entries.none { it.name == row.field }) fail(16, "${row.field} is not in the vocabulary")
                if (row.attribute != null && declaredAttributes != null && row.attribute !in declaredAttributes) fail(17, "reads attribute ${row.attribute}, which no processor declares")
                if (row.provider != null && !providerExists(row.provider)) fail(15, "no bean named ${row.provider}")
                if (row.select != null && runCatching { com.fasterxml.jackson.core.JsonPointer.compile(row.select) }.isFailure) fail(18, "select ${row.select} is not a JSON pointer")
                if (row.format != null && runCatching { formatter(row.format) }.isFailure) fail(18, "format ${row.format} does not parse")
                if (row.digest != null && DigestAlgorithm.entries.none { it.name.equals(row.digest, ignoreCase = true) }) fail(21, "digest ${row.digest} is not md5, sha256 or sha1")
            }
        }

        /** An `ISO_*` constant of [DateTimeFormatter], or a pattern; throws for neither (rule 18 rejects those at boot). */
        fun formatter(format: String): DateTimeFormatter =
            if (format.startsWith("ISO_") || format == "BASIC_ISO_DATE") DateTimeFormatter::class.java.getField(format).get(null) as DateTimeFormatter
            else DateTimeFormatter.ofPattern(format)
    }
}
