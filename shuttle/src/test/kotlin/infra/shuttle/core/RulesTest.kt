package infra.shuttle.core

import infra.shuttle.core.ExtractFrom.FileName
import infra.shuttle.core.Field.DIGEST
import infra.shuttle.core.Field.TRANSFER_ID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 13.3: every rule rejects a configuration that violates only it, by number (I14). */
class RulesTest {

    @TempDir
    lateinit var staging: Path

    /** Spec 13.2's vendor-drop build, valid under every rule; each test bends one thing. */
    private fun config(
        vendor: SftpStoreBuilder.() -> Unit = {},
        downstream: HttpChannelBuilder.() -> Unit = {},
        vendorDrop: RouteBuilder.() -> Unit = {},
        more: ShuttleBuilder.() -> Unit = {},
    ) = shuttle {
        shuttleStateStore { oracle(datasource = "shuttle") }
        notifier { workers = 4; batch = 50; sweepEvery = 30.seconds }
        supervision { restartBackoff(30.seconds, max = 15.minutes); readiness = Readiness.AllRoutesDown }
        digest = Digest.MD5
        drainTimeout = 60.seconds
        objectStores {
            sftp("vendor") {
                endpoint { host = "sftp.example" }; auth { password(env("SFTP_USER"), env("SFTP_PASSWORD")) }
                pool { maxSize = 20; maxConcurrentTransfers = 16 }; staging { dir = this@RulesTest.staging }
                vendor()
            }
            s3("minio") { endpoint = "https://minio.internal"; pathStyle = true; credentials = fromEnvironment("S3_ACCESS_KEY", "S3_SECRET_KEY") }
        }
        channels {
            http("downstream") {
                method = HttpMethod.POST; url = "https://downstream.internal/api/files"; auth = bearer(env("DOWNSTREAM_TOKEN"))
                response { success = 200..299; retry = setOf(408, 429) + (500..599); reference = "/requestId" }
                body = mapping {
                    "fileId" from TRANSFER_ID
                    "file.md5" from DIGEST
                    "orderNumber" fromAttribute "orderNumber"
                    "source" value "vendor-drop"
                }
                downstream()
            }
        }
        route("vendor-drop") {
            source = poll(objectStore("vendor"), directory = "/inbox") { every = 1.hours; onAck = move("temp/") }
            process = extract(from = FileName, regex = "(?<orderNumber>\\d+)-.*\\.csv") then rename("{yyyyMMdd}-{name}") then zip()
            target = objectStore("minio").bucket("landing") { key = "vendor/{name}" }
            notify(on = Acked, channel("downstream"))
            parallelism = 4
            vendorDrop()
        }
        more()
    }

    private fun violated(config: ShuttleConfig, beans: Map<String, Set<String>> = emptyMap()) =
        Rules.validate(config) { beans[it] }.violations.map { it.rule }.distinct()

    @Test
    fun rule11_every_staging_directory_exists_is_writable_local_and_unshared() =
        assertEquals(listOf(11), violated(config(vendor = { staging { dir = this@RulesTest.staging.resolve("missing") } })))

    @Test
    fun rule12_onAck_is_explicit_and_in_the_trigger_kinds_vocabulary() =
        assertEquals(listOf(12), violated(config(vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") })))

    /** A nats channel without a subject is declared but offers no notify role (rule 2's reading), so a callback may not name it. */
    @Test
    fun rule12_a_callback_names_a_channel_offering_the_notify_role() =
        assertEquals(
            listOf(12),
            violated(config(
                more = { channels { nats("events") { url = "nats://events.internal:4222" } } },
                vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") { onAck = callback(channel("events")) } },
            )),
        )

    @Test
    fun rule12_a_callback_may_name_a_channel_offering_notify() =
        assertEquals(
            emptyList<Int>(),
            violated(config(vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") { onAck = callback(channel("downstream")) } })),
        )

    @Test
    fun rule13_key_and_directory_patterns_use_only_known_placeholders() =
        assertEquals(listOf(13), violated(config(vendorDrop = { target = objectStore("minio").bucket("landing") { key = "vendor/{nope}" } })))

    @Test
    fun rule14_every_built_in_processor_configuration_parses() =
        assertEquals(listOf(14), violated(config(vendorDrop = { process = listOf(extract(from = FileName, regex = "(?<orderNumber>\\d+")) })))

    @Test
    fun rule14_expand_format_is_json_or_message_with_message_only_on_a_subscribed_route_and_files_a_pointer() {
        assertEquals(listOf(14), violated(config(vendorDrop = { process = process then expand("lines", "/images[*].path", objectStore("minio")) })))
        assertEquals(listOf(14), violated(config(vendorDrop = { process = process then expand("message", "/images[*].path", objectStore("minio")) })))
        assertEquals(listOf(14), violated(config(vendorDrop = { process = process then expand("json", "images[*].path", objectStore("minio")) })))
        assertEquals(emptyList<Int>(), violated(config(vendorDrop = { process = process then expand("json", "/images[*].path", objectStore("minio")) })))
    }

    @Test
    fun rule14_unzip_maxEntries_is_at_least_one() =
        assertEquals(listOf(14), violated(config(vendorDrop = { process = process then unzip(maxEntries = 0) })))

    @Test
    fun rule14_unzip_maxBytes_is_positive() =
        assertEquals(listOf(14), violated(config(vendorDrop = { process = process then unzip(maxBytes = 0) })))

    @Test
    fun rule15_every_custom_processor_and_provider_resolves_to_a_bean() =
        assertEquals(listOf(15), violated(config(vendorDrop = { process = process then custom("imageResizer") })))

    @Test
    fun rule17_every_mapping_attribute_is_declared_by_a_processor_in_that_route() =
        assertEquals(listOf(17), violated(config(downstream = { body = mapping { "orderNumber" fromAttribute "orderNo" } })))

    /** The callback channel's body is rendered for the route too, so rule 17 reads it although the route does not notify it. */
    @Test
    fun rule17_reads_the_body_of_a_callback_channel() =
        assertEquals(
            listOf(17),
            violated(config(
                more = { channels { http("upstream") { method = HttpMethod.POST; url = "https://upstream.internal/ack"; body = mapping { "orderNumber" fromAttribute "orderNo" } } } },
                vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") { onAck = callback(channel("upstream")) } },
            )),
        )

    @Test
    fun rule18_every_select_is_a_json_pointer_and_every_format_parses() =
        assertEquals(
            listOf(18),
            violated(config(downstream = { body = mapping { "order" by provider("orderDetails", select = "requestId") } }), beans = mapOf("orderDetails" to emptySet())),
        )

    @Test
    fun rule19_a_mapping_row_has_exactly_one_source() =
        assertEquals(listOf(19), violated(config(downstream = { body = mapping { row(MappingRow("x", field = Field.TRANSFER_ID, value = "v")) } })))

    @Test
    fun rule20_success_and_retry_status_sets_are_disjoint() =
        assertEquals(listOf(20), violated(config(downstream = { response { success = 200..299; retry = setOf(204) } })))

    @Test
    fun rule21_digest_is_md5_sha256_or_sha1() =
        assertEquals(listOf(21), violated(config(downstream = { body = mapping { row(MappingRow("x", field = Field.DIGEST, digest = "crc32")) } })))

    /** D49: one digest per route, so a row asking for another algorithm can only ever render missing; rule 26 says so at boot. */
    @Test
    fun rule26_a_mapping_digest_row_asks_for_the_algorithm_its_route_computes() =
        assertEquals(listOf(26), violated(config(downstream = { body = mapping { row(MappingRow("x", field = Field.DIGEST, digest = "sha256")) } })))

    @Test
    fun rule26_accepts_the_row_when_the_route_overrides_the_process_default() =
        assertEquals(
            emptyList<Int>(),
            violated(config(
                downstream = { body = mapping { row(MappingRow("x", field = Field.DIGEST, digest = "sha256")) } },
                vendorDrop = { digest = Digest.SHA256 },
            )),
        )

    @Test
    fun rule22_attribute_names_are_at_most_32_and_64_characters_each() =
        assertEquals(listOf(22), violated(config(vendorDrop = { process = process then extract(from = FileName, regex = "(?<${"a".repeat(65)}>.*)") })))

    @Test
    fun rule23_a_move_target_is_not_the_polled_directory() =
        assertEquals(listOf(23), violated(config(vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") { onAck = move("/inbox") } })))

    @Test
    fun rule24_readiness_is_known_and_restartBackoff_initial_is_at_most_max() =
        assertEquals(listOf(24), violated(config(more = { supervision { restartBackoff(15.minutes, max = 30.seconds) } })))

    @Test
    fun rule25_a_secret_appears_only_as_an_environment_reference() =
        assertEquals(listOf(25), violated(config(vendor = { auth { password(env("SFTP_USER"), Secret.Literal("hunter2")) } })))

    @Test
    fun the_baseline_passes_every_rule() = assertEquals(emptyList<Int>(), violated(config()))

    @Test
    fun rule1_every_referenced_name_exists() =
        assertEquals(listOf(1), violated(config(vendorDrop = { target = objectStore("nope").bucket("landing") })))

    @Test
    fun rule2_the_referenced_declaration_offers_the_role_used() =
        assertEquals(listOf(2), violated(config(vendorDrop = { source = poll(objectStore("minio"), directory = "/inbox") { onAck = AckAction.Delete } })))

    /** G15: a nats channel offers the notify role only once it says which subject to publish on. */
    @Test
    fun rule2_a_nats_channel_notified_on_states_a_subject() =
        assertEquals(
            listOf(2),
            violated(config(more = { channels { nats("events") { url = "nats://events.internal:4222" } } }, vendorDrop = { notify(on = Acked, channel("events")) })),
        )

    @Test
    fun rule2_a_nats_channel_with_a_subject_may_be_notified_on() =
        assertEquals(
            emptyList<Int>(),
            violated(config(more = { channels { nats("events") { url = "nats://events.internal:4222"; subject = "files.stored" } } }, vendorDrop = { notify(on = Acked, channel("events")) })),
        )

    @Test
    fun rule3_every_timeout_is_below_drainTimeout() =
        assertEquals(listOf(3), violated(config(downstream = { timeout = 61.seconds })))

    @Test
    fun rule4_names_are_unique_across_routes_stores_and_channels() =
        assertEquals(listOf(4), violated(config(more = { channels { nats("vendor") { url = "nats://events.internal:4222" } } })))

    @Test
    fun rule6_only_a_subscribe_source_has_a_fetch() =
        assertEquals(listOf(6), violated(config(vendorDrop = { fetch(objectStore("minio"), "/metadata.path") })))

    /** Ticket 14's addendum: `S3Fetcher` needs a bucket and an S3 store declares none, so the fetch states it (spec 13.1's image-sets). */
    @Test
    fun rule6_a_subscribe_source_fetching_from_an_S3_store_states_a_bucket() {
        val events: ShuttleBuilder.() -> Unit = { channels { nats("events") { url = "nats://events.internal:4222" } } }
        fun subscribed(bucket: String?): RouteBuilder.() -> Unit = {
            source = subscribe(channel("events"), "images.ready") { onAck = AckAction.Ack }
            fetch(objectStore("minio"), "/metadata.path", bucket)
        }
        assertEquals(listOf(6), violated(config(more = events, vendorDrop = subscribed(bucket = null))))
        assertEquals(emptyList<Int>(), violated(config(more = events, vendorDrop = subscribed(bucket = "images"))))
        assertEquals(emptyList<Int>(), violated(config(more = events, vendorDrop = { source = subscribe(channel("events"), "images.ready") { onAck = AckAction.Ack }; fetch(objectStore("vendor"), "/metadata.path") })))
    }

    @Test
    fun rule7_parallelism_maxAttempts_stuckAfter_and_inProgressEvery_are_positive() =
        assertEquals(listOf(7), violated(config(vendorDrop = { parallelism = 0 })))

    /** B8: a zero-permit semaphore parks the notifier for ever, so validate mode refuses it. */
    @Test
    fun rule7_notifier_workers_is_positive() =
        assertEquals(listOf(7), violated(config(more = { notifier { workers = 0 } })))

    /** B8: a zero sweep interval turns the notifier's wait into a hot loop over the outbox. */
    @Test
    fun rule7_notifier_sweepEvery_is_positive() =
        assertEquals(listOf(7), violated(config(more = { notifier { sweepEvery = 0.seconds } })))

    /** B8: `every = 0s` makes the connector throw, so the route is down at every start. */
    @Test
    fun rule7_poll_every_is_positive() =
        assertEquals(
            listOf(7),
            violated(config(vendorDrop = { source = poll(objectStore("vendor"), directory = "/inbox") { every = 0.seconds; onAck = move("temp/") } })),
        )

    /** B8: a zero batch claims no rows and never waits, so the sweep spins on the outbox. */
    @Test
    fun rule7_notifier_batch_is_positive() =
        assertEquals(listOf(7), violated(config(more = { notifier { batch = 0 } })))

    /** B8: a zero initial delay stays zero however often it doubles, so a failing route restarts flat out. */
    @Test
    fun rule7_restartBackoff_initial_is_positive() =
        assertEquals(listOf(7), violated(config(more = { supervision { restartBackoff(0.seconds, max = 15.minutes) } })))

    /** B8: a pool of zero sessions or zero transfer permits parks the first acquire for ever. */
    @Test
    fun rule7_pool_sizes_are_positive() {
        assertEquals(listOf(7), violated(config(vendor = { pool { maxSize = 20; maxConcurrentTransfers = 0 } })))
        assertEquals(listOf(7, 9), violated(config(vendor = { pool { maxSize = 0; maxConcurrentTransfers = 0 } })))
    }

    /** B8: each of these makes the connector's readiness check throw, so the route is down at every start. */
    @Test
    fun rule7_readiness_checks_and_intervals_are_positive() {
        fun polling(vararg checks: FileReadiness): RouteBuilder.() -> Unit =
            { source = poll(objectStore("vendor"), directory = "/inbox") { every = 1.hours; onAck = move("temp/"); readiness = checks.toList() } }
        assertEquals(listOf(7), violated(config(vendorDrop = polling(FileReadiness.SizeStable(checks = 0, interval = 10.seconds)))))
        assertEquals(listOf(7), violated(config(vendorDrop = polling(FileReadiness.SizeStable(checks = 2, interval = 0.seconds)))))
        assertEquals(listOf(7), violated(config(vendorDrop = polling(FileReadiness.MinAge(0.seconds)))))
        assertEquals(emptyList<Int>(), violated(config(vendorDrop = polling(FileReadiness.SizeStable(), FileReadiness.MinAge(1.minutes)))))
    }

    /** B8: a zero first delay leaves every retry due at once, so a failing channel is retried at sweep rate. */
    @Test
    fun rule7_delivery_policy_attempts_and_backoff_are_positive() {
        assertEquals(listOf(7), violated(config(downstream = { policy = DeliveryPolicy(maxAttempts = 0) })))
        assertEquals(listOf(7), violated(config(downstream = { policy = DeliveryPolicy(backoff = Backoff(initial = 0.seconds, max = 15.minutes)) })))
    }

    @Test
    fun rule7_recheckFinished_is_not_negative() =
        assertEquals(listOf(7), violated(config(vendorDrop = { recheckFinished = (-1).seconds })))

    @Test
    fun rule7_staging_minFree_is_not_negative() =
        assertEquals(listOf(7), violated(config(vendor = { staging { dir = this@RulesTest.staging; minFree = -1 } })))

    @Test
    fun rule8_a_state_and_channel_pair_appears_once_per_route() =
        assertEquals(listOf(8), violated(config(vendorDrop = { notify(on = Acked, channel("downstream")) })))

    @Test
    fun rule9_pool_arithmetic_per_object_store() =
        assertEquals(listOf(9), violated(config(vendorDrop = { parallelism = 20 })))

    @Test
    fun rule10_sftp_keepAlive_and_idleTimeout_are_below_idleCutoff() =
        assertEquals(listOf(10), violated(config(vendor = { idleCutoff = 20.seconds })))

    @Test
    fun rule5_a_route_has_exactly_one_source_and_one_target() =
        assertEquals(listOf(5), violated(config(vendorDrop = { target = null })))
}
