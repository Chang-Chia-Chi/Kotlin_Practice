package infra.shuttle.http

import com.fasterxml.jackson.databind.ObjectMapper
import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryChannel
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.HttpAuth
import infra.shuttle.core.HttpChannel as HttpChannelConfig
import infra.shuttle.core.Secret
import kotlinx.coroutines.suspendCancellableCoroutine
import org.jboss.logging.Logger
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64
import java.util.concurrent.CompletionException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.toJavaDuration

/**
 * Spec 9.2 over `java.net.http`: one request per `deliver`, the body already rendered on the event (D19),
 * the outcome read from the channel's `response` section: a success status is `Delivered` with the
 * reference the pointer finds in the body (null, with a WARN, when it finds nothing), a retry status is
 * `Retry`, any other status is `Reject`; connection failures and a timeout are `Retry` (spec 11).
 *
 * The request goes through `sendAsync` bridged into a cancellable suspension, so no thread blocks and
 * cancelling the coroutine cancels the request at once; `CancellationException` is never caught.
 * Secrets resolve from `env` at construction, so a missing variable fails at boot, not at the first send.
 */
class HttpChannel(
    private val config: HttpChannelConfig,
    private val http: HttpClient,
    env: (String) -> String?,
) : DeliveryChannel {

    override val name = ChannelName(config.name)
    override val policy = config.policy
    private val url = URI(requireNotNull(config.url) { "channel ${config.name} has no url" })

    private val authHeader: Pair<String, String>? = run {
        fun Secret.resolve(): String = when (this) {
            is Secret.Env -> env(variable) ?: throw IllegalStateException("channel ${config.name}: environment variable $variable is not set")
            is Secret.Literal -> value
        }
        when (val auth = config.auth) {
            null -> null
            is HttpAuth.Bearer -> "Authorization" to "Bearer ${auth.token.resolve()}"
            is HttpAuth.Basic -> "Authorization" to "Basic " + Base64.getEncoder().encodeToString("${auth.user.resolve()}:${auth.password.resolve()}".encodeToByteArray())
            is HttpAuth.Header -> auth.name to auth.value.resolve()
        }
    }

    override suspend fun deliver(event: DeliveryEvent): DeliveryOutcome {
        val request = HttpRequest.newBuilder(url)
            .timeout(config.timeout.toJavaDuration())
            .header("Content-Type", "application/json")
            .apply { authHeader?.let { (name, value) -> header(name, value) } }
            .method(config.method.name, HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(event.body)))
            .build()
        val outcome = try {
            classify(send(request), event)
        } catch (e: IOException) {
            // Refused, reset, and HttpTimeoutException alike: the endpoint was not reached, so try again later.
            DeliveryOutcome.Retry(null, e.toString())
        }
        log.infof(
            "delivery transfer=%d event=%s channel=%s attempt=%d status=%s reference=%s",
            event.transferId.value, event.moment.name.lowercase(), config.name, event.attempt,
            when (outcome) { is DeliveryOutcome.Delivered -> "delivered"; is DeliveryOutcome.Retry -> outcome.status ?: "unreachable"; is DeliveryOutcome.Reject -> outcome.status },
            (outcome as? DeliveryOutcome.Delivered)?.reference,
        )
        return outcome
    }

    private suspend fun send(request: HttpRequest): HttpResponse<String> = suspendCancellableCoroutine { cont ->
        val future = http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        future.whenComplete { response, error ->
            if (error == null) cont.resume(response) else cont.resumeWithException((error as? CompletionException)?.cause ?: error)
        }
        cont.invokeOnCancellation { future.cancel(true) }
    }

    private fun classify(response: HttpResponse<String>, event: DeliveryEvent): DeliveryOutcome {
        val status = response.statusCode()
        return when (status) {
            in config.response.success -> DeliveryOutcome.Delivered(reference(response.body(), event))
            in config.response.retry -> DeliveryOutcome.Retry(status.toString(), response.body().take(200))
            else -> DeliveryOutcome.Reject(status.toString(), response.body().take(200))
        }
    }

    private fun reference(body: String, event: DeliveryEvent): String? {
        val pointer = config.response.reference ?: return null
        val found = runCatching { mapper.readTree(body).at(pointer) }.getOrNull()?.takeUnless { it.isMissingNode || it.isNull }?.asText()
        if (found == null) log.warnf("channel %s answered success for transfer %d but %s resolved nothing in its body", config.name, event.transferId.value, pointer)
        return found
    }

    private companion object {
        val log: Logger = Logger.getLogger(HttpChannel::class.java)
        val mapper = ObjectMapper()
    }
}
