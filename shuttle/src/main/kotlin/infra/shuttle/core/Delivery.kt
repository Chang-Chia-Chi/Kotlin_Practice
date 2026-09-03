package infra.shuttle.core

import com.fasterxml.jackson.databind.JsonNode
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 9.1: `on_state`, the moment a notification announces, fixed for ever. */
enum class DeliveryMoment { FETCHED, STORED, ACKED }

/** Spec 9.1: `notification_state`, how far the sending has got. */
enum class DeliveryState { PENDING, DELIVERED, FAILED }

/** What a transition asks the state store to create (spec 8.2). */
data class DeliveryRequest(val moment: DeliveryMoment, val channel: ChannelName)

/** One row of `delivery_outbox` (spec 8.1). */
data class Delivery(
    val id: DeliveryId,
    val transferId: TransferId,
    val moment: DeliveryMoment,
    val channel: ChannelName,
    val state: DeliveryState,
    val attempts: Int,
    val nextAttemptAt: Instant,
    val lastStatus: String? = null,
    val lastError: String? = null,
    val reference: String? = null,
    val createdAt: Instant,
    val deliveredAt: Instant? = null,
)

/** What a channel receives: the body is rendered at send time from the row (D19). */
data class DeliveryEvent(val transferId: TransferId, val moment: DeliveryMoment, val channel: ChannelName, val attempt: Int, val body: JsonNode)

sealed interface DeliveryOutcome {
    data class Delivered(val reference: String?) : DeliveryOutcome
    data class Retry(val status: String?, val reason: String) : DeliveryOutcome
    data class Reject(val status: String?, val reason: String) : DeliveryOutcome
}

/** Exponential backoff from `initial` to `max`; shared by delivery policy (spec 9.3) and supervision (spec 10). */
data class Backoff(val initial: Duration, val max: Duration, val factor: Double = 2.0)

/** Spec 9.3 defaults. */
data class DeliveryPolicy(
    val maxAttempts: Int = 50,
    val giveUpAfter: Duration = 24.hours,
    val backoff: Backoff = Backoff(initial = 5.seconds, max = 15.minutes),
    val fullJitter: Boolean = true,
    val timeout: Duration = 10.seconds,
)

/** Spec 9.6: one row of a channel's body, in the table's own keys so YAML and the DSL meet here. */
data class MappingRow(
    val path: String,
    val field: String? = null,
    val attribute: String? = null,
    val provider: String? = null,
    val select: String? = null,
    val value: String? = null,
    val type: MappingType = MappingType.STRING,
    val format: String? = null,
    val default: String? = null,
    val trim: Boolean = false,
    val upper: Boolean = false,
    val lower: Boolean = false,
    val required: Boolean = true,
    val digest: String? = null,
)

enum class MappingType { STRING, NUMBER, BOOLEAN }

data class MappingTable(val rows: List<MappingRow>)

/** Spec 9.6 vocabulary. */
enum class Field {
    TRANSFER_ID, PARENT_ID, ROUTE, KIND, SOURCE_KIND, SOURCE_REF, SOURCE_NAME, SOURCE_PATH, SOURCE_SIZE, SOURCE_MTIME,
    SOURCE_DIGEST, STORED_NAME, STORED_MTIME, DIGEST, DIGEST_ALGO, TARGET_KIND, TARGET_LOCATION, TARGET_KEY, TARGET_REF,
    TARGET_SIZE, FIRST_SEEN_AT, ACKED_AT, EVENT, ATTEMPT,
}
