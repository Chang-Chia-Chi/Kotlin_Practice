package infra.shuttle.acceptance

import infra.shuttle.core.DeliveryState
import infra.shuttle.core.HookPoint
import infra.shuttle.core.ShuttleMetrics
import infra.shuttle.core.TransferState
import infra.shuttle.quarkus.NamedBeans
import infra.shuttle.quarkus.ShuttleHost
import infra.shuttle.s3.Minio
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.ObjectVersion
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.exists
import kotlin.io.path.writeText

/**
 * Spec 18.2, milestone 1: S1 to S26 through the real `ShuttleHost` over the fixture's adapters (`AcceptanceFixture`):
 * the vendor-drop and mirror routes, poll on the embedded SSHD, the S3 target on MinIO with versioning, the HTTP
 * channel on the loopback server, the crash matrix replayed by closing and reopening a host.
 */
@Tag("acceptance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class M1AcceptanceTest : AcceptanceFixture() {

    // ---- spec 13.1 at test scale ----

    private fun vendorDrop(
        notify: String = "      notify:\n        - { on: acked, channel: downstream }\n",
        process: String = EXTRACT + RENAME + ZIP,
        store: String = "vendor",
        readiness: String = "[ { sizeStable: { checks: 1, interval: 1ms } } ]",
        recheckFinished: String = "24h",
        maxAttempts: Int = 5,
    ) = "    vendor-drop:\n" +
        "      source:\n" +
        "        poll:\n" +
        "          store: $store\n" +
        "          directory: /inbox\n" +
        "          every: 5s\n" +
        "          readiness: $readiness\n" +
        "          onAck: { move: temp/ }\n" +
        (if (process.isEmpty()) "" else "      process:\n$process") +
        "      target: { store: minio, bucket: $bucket, key: \"vendor/{name}\" }\n" +
        notify +
        "      parallelism: 4\n" +
        "      maxAttempts: $maxAttempts\n" +
        "      stuckAfter: 3h\n" +
        "      recheckFinished: $recheckFinished\n"

    /** Spec 13.1's mirror route; milestone 1 has S3 as its only target, so it mirrors into a second key prefix of the bucket. */
    private fun mirror(store: String = "vendor") =
        "    mirror:\n" +
            "      source: { poll: { store: $store, directory: /outbound, every: 5s, readiness: [ { sizeStable: { checks: 1, interval: 1ms } } ], onAck: delete } }\n" +
            "      target: { store: minio, bucket: $bucket, key: \"mirror/{name}\" }\n"

    private fun versions(key: String): List<ObjectVersion> =
        Minio.client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucket).prefix(key).build()).versions()

    private fun head(key: String, versionId: String? = null): HeadObjectResponse =
        Minio.client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).versionId(versionId).build())

    // ---- S1 to S26 ----

    /** I1, I2, I3, I10, I11, I15, I20 on real adapters: the whole vendor-drop route, one file, one channel. */
    @Test
    fun S1_vendor_drop_happy_path_one_file_one_channel() = runBlocking {
        boot(yaml(vendorDrop(), channels = downstream(rows = BODY + ORDER_ROW)))
        seed(inbox, "123-order.csv")

        val row = awaitState(TransferState.DONE)

        // The chain ran: extract set the attribute, rename+zip produced the object stored under the renamed key.
        val key = "vendor/20260101-123-order.csv.zip"
        assertEquals(key, row.target!!.key, "the object is stored under the extract+rename+zip key")
        assertEquals(mapOf("orderNumber" to "123"), row.attributes)
        val stored = versions(key).single()
        assertEquals(row.target!!.ref, stored.versionId(), "the row's reference is the one current version")

        // The S3 object carries the PROCESSED object's digest and name in its metadata (spec 7.1), independently checked.
        val bytes = Minio.client.getObjectAsBytes(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray()
        assertEquals(listOf("20260101-123-order.csv"), zipEntryNames(bytes), "the stored object is the zip of the renamed file")
        val metadata = head(key).metadata().mapKeys { it.key.lowercase() }
        assertEquals(md5Hex(bytes), metadata["digest"], "the stored object's own MD5 is in its metadata")
        assertEquals("md5", metadata["digest-algorithm"])
        assertEquals("20260101-123-order.csv.zip", metadata["source-name"], "the processed object's name is on the object")
        assertEquals("123", metadata["attr-ordernumber"])
        assertEquals(row.id.value.toString(), metadata["transfer-id"])

        // Ticket 45 (closes D43): the row carries the STORED object's name and digest; the source's stay under source_*.
        assertEquals("20260101-123-order.csv.zip", row.storedName, "the row's stored_name is the archive's name")
        assertEquals(md5Hex(bytes), row.digest!!.hex, "the row's digest is the archive's own")
        assertEquals("123-order.csv", row.identity.sourceName)
        assertTrue(row.sourceDigest != row.digest, "SOURCE_DIGEST and DIGEST differ after zip")

        assertTrue(inbox.resolve("temp/123-order.csv").exists(), "the source file was moved to temp/ (D6, after the store)")
        assertTrue(!inbox.resolve("123-order.csv").exists())

        val delivery = reads.outbox().single()
        assertEquals(DeliveryState.DELIVERED, delivery.state)
        assertEquals("r-1", delivery.reference, "the reference the loopback server returned")
        val request = received.single()
        assertEquals("/api/files", request.path)
        assertEquals(row.id.value, request.body.get("fileId").asLong())
        assertEquals("123", request.body.get("orderNumber").asText())
        assertEquals("acked", request.body.get("event").asText())
        assertEquals(key, request.body.at("/location/key").asText())
        assertEquals(bucket, request.body.at("/location/bucket").asText())
        assertEquals("20260101-123-order.csv.zip", request.body.at("/file/name").asText(), "STORED_NAME in the body is the archive's name (ticket 45)")
        assertEquals(md5Hex(bytes), request.body.at("/file/md5").asText(), "DIGEST in the body is the archive's own")
    }

    private fun zipEntryNames(bytes: ByteArray): List<String> =
        java.util.zip.ZipInputStream(bytes.inputStream()).use { zip ->
            generateSequence { zip.nextEntry }.map { it.name }.toList()
        }

    /** A route with no processing, so a crash-matrix or channel scenario is not slowed by rename+zip. */
    private fun plainRoute(notify: String = "", onAck: String = "{ move: temp/ }", maxAttempts: Int = 5, recheckFinished: String = "24h") =
        vendorDrop(notify = notify, process = "", recheckFinished = recheckFinished, maxAttempts = maxAttempts)
            .replace("onAck: { move: temp/ }", "onAck: $onAck")

    private val notifyBlock = "      notify:\n        - { on: acked, channel: downstream }\n"


    // ---- S2 to S6: the crash matrix on real adapters (spec 4.4) ----

    /** I6, I8, S2: a crash after the store and before the ledger; the next poll stores again, one current copy and one non-current version. */
    @Test
    fun S2_crash_after_store_before_ledger_stores_again_leaving_one_current_and_one_non_current_version() = runBlocking {
        hook.pauseAt(HookPoint.afterStore)
        val host = bootR(plainRoute())
        seed(inbox, "a.csv")
        crash(host, HookPoint.afterStore)
        val key = "vendor/a.csv"
        assertEquals(1, versions(key).size, "one copy stored before the crash; the ledger never saw it")
        assertEquals(TransferState.PROCESSED, reads.transfers().single().state)

        bootR(plainRoute())
        val row = awaitState(TransferState.DONE)
        assertEquals(2, versions(key).size, "the next poll stored again: the first is now the non-current version")
        assertEquals(row.target!!.ref, versions(key).single { it.isLatest }.versionId())
        assertTrue(Minio.client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucket).prefix(key).build()).deleteMarkers().isEmpty(), "nothing was ever deleted (I6)")
        // The crash *inside* store, between PUT and HEAD, is the S3 adapter's own contract, proven on MinIO in
        // S3TargetTest.I6_three_stores_read_back_the_newest_by_key_a_crash_between_PUT_and_HEAD_is_repaired_by_the_next_store.
    }

    /** I8, S3: a crash after the ledger STORED write; the next poll verifies the copy and acks with no second store. */
    @Test
    fun S3_crash_after_ledger_STORED_verifies_and_acks_with_no_second_store() = runBlocking {
        hook.pauseAt(HookPoint.afterLedgerStored)
        val host = bootR(plainRoute())
        seed(inbox, "a.csv")
        crash(host, HookPoint.afterLedgerStored)
        assertEquals(TransferState.STORED, reads.transfers().single().state)
        assertTrue(inbox.resolve("a.csv").exists(), "the file was never moved")

        bootR(plainRoute())
        val row = awaitState(TransferState.DONE)
        assertEquals(1, versions("vendor/a.csv").size, "verify true: no second store")
        assertTrue(inbox.resolve("temp/a.csv").exists(), "the ack moved it on recovery")
        assertEquals(row.target!!.ref, versions("vendor/a.csv").single().versionId())
    }

    /** I8, S4: a crash after the move and before the ledger ACKED; reconciliation on the next complete poll writes ACKED and its deliveries. */
    @Test
    fun S4_crash_after_the_move_before_ledger_ACKED_is_repaired_by_reconciliation() = runBlocking {
        hook.pauseAt(HookPoint.afterAck)
        val host = bootR(plainRoute(notify = notifyBlock), downstream())
        seed(inbox, "a.csv")
        crash(host, HookPoint.afterAck)
        assertEquals(TransferState.STORED, reads.transfers().single().state)
        assertTrue(inbox.resolve("temp/a.csv").exists(), "the move happened; the ledger did not record ACKED")
        assertTrue(reads.outbox().isEmpty(), "no delivery yet")

        // Reconciliation acts on STORED rows whose updated_at is older than the poll's start: advance the clock so the recovery poll starts later.
        clock.advance(kotlin.time.Duration.parse("1h"))
        bootR(plainRoute(notify = notifyBlock), downstream())
        val row = awaitState(TransferState.DONE)
        assertTrue(counter(ShuttleMetrics.RECONCILED, "route", "vendor-drop") >= 1.0, "the row was reconciled, not re-run")
        assertEquals(1, versions("vendor/a.csv").size, "nothing stored on recovery")
        assertEquals(DeliveryState.DELIVERED, reads.outbox().single { it.transferId == row.id }.state)
    }

    /** I8, S5: a crash after the channel answered and before the ledger; the notifier of the next host delivers again, two calls with one transfer id, the row DELIVERED once. */
    @Test
    fun S5_crash_after_delivery_sent_before_ledger_delivers_again_two_calls_one_row_DELIVERED_once() = runBlocking {
        hook.pauseAt(HookPoint.afterDeliverySent)
        val host = bootR(plainRoute(notify = notifyBlock), downstream())
        seed(inbox, "a.csv")
        val id = crash(host, HookPoint.afterDeliverySent)
        assertEquals(1, received.size, "the channel was called once before the crash")
        assertEquals(DeliveryState.PENDING, reads.outbox().single().state, "but the ledger never recorded it")

        bootR(plainRoute(notify = notifyBlock), downstream())
        await("the redelivery") { received.size == 2 }
        await("the row DONE") { reads.transfers().first { it.id == id }.state == TransferState.DONE }
        assertEquals(DeliveryState.DELIVERED, reads.outbox().single().state)
        assertEquals(setOf(id.value), received.map { it.body.get("fileId").asLong() }.toSet(), "two calls, one transfer id")
    }

    /** I1, S6: a STORED row whose copy has gone is fully re-run on the same row and reaches DONE. */
    @Test
    fun S6_copy_missing_at_STORED_is_stored_again_on_the_same_row_and_reaches_DONE() = runBlocking {
        hook.pauseAt(HookPoint.afterLedgerStored)
        val host = bootR(plainRoute())
        seed(inbox, "a.csv")
        val id = crash(host, HookPoint.afterLedgerStored)
        val ref = reads.transfers().single().target!!
        // The copy is expired by hand, the way the lifecycle rule eventually would: verify will now be false.
        Minio.client.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder().bucket(bucket).key(ref.key).versionId(ref.ref).build())

        bootR(plainRoute())
        val row = awaitState(TransferState.DONE)
        assertEquals(id, row.id, "the same row")
        assertTrue(row.target!!.ref != ref.ref, "a fresh version was stored")
    }

    // ---- S7 to S9, S17: the channel (spec 9.3) ----

    /** S7: downstream answers 503 twice then 200; two retries with backoff, then DELIVERED at attempt 3. */
    @Test
    fun S7_downstream_503_twice_then_200_delivers_at_the_third_attempt() = runBlocking {
        respond = { n, _ -> if (n <= 2) 503 to "busy" else 200 to """{"requestId":"ok-$n"}""" }
        bootR(plainRoute(notify = notifyBlock), downstream())
        seed(inbox, "a.csv")
        withClockTicking { await("the delivery to succeed") { reads.outbox().any { it.state == DeliveryState.DELIVERED } } }
        val delivery = reads.outbox().single()
        assertEquals(DeliveryState.DELIVERED, delivery.state)
        assertEquals(3, delivery.attempts, "two retries then the success")
        assertEquals(3, received.size)
    }

    /** S8: downstream answers 400; the delivery is FAILED (rejected), the transfer stays ACKED and is counted. */
    @Test
    fun S8_downstream_400_fails_the_delivery_and_leaves_the_transfer_ACKED() = runBlocking {
        respond = { _, _ -> 400 to "bad request" }
        bootR(plainRoute(notify = notifyBlock), downstream())
        seed(inbox, "a.csv")
        await("the delivery to be rejected") { reads.outbox().any { it.state == DeliveryState.FAILED } }
        assertEquals(TransferState.ACKED, reads.transfers().single().state, "a failed delivery never fails the transfer (D9)")
        assertEquals(1.0, counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "rejected"))
    }

    /** S9: downstream stays down past giveUpAfter; the delivery is FAILED with gave_up, and an admin re-drive delivers it once downstream is back. */
    @Test
    fun S9_downstream_down_past_giveUpAfter_is_FAILED_and_a_redrive_delivers_it() = runBlocking {
        respond = { _, _ -> 503 to "down" }
        bootR(plainRoute(notify = notifyBlock), downstream(policy = "{ maxAttempts: 50, giveUpAfter: 3s, backoff: { initial: 1s, max: 1s } }"))
        seed(inbox, "a.csv")
        withClockTicking { await("the delivery to give up") { reads.outbox().any { it.state == DeliveryState.FAILED } } }
        assertEquals(1.0, counter(ShuttleMetrics.DELIVERIES, "channel", "downstream", "event", "acked", "outcome", "gave_up"))

        respond = { n, _ -> 200 to """{"requestId":"back-$n"}""" }
        val delivery = reads.outbox().single()
        assertEquals(ShuttleHost.Outcome.DONE, hosts.single().redriveDelivery(delivery.id))
        withClockTicking { await("the re-driven delivery") { reads.outbox().single().state == DeliveryState.DELIVERED } }
    }

    /** S17: two channels on acked, one always 503; the other delivers, the transfer stays ACKED, the failing one keeps retrying. */
    @Test
    fun S17_two_channels_on_acked_one_always_503_the_other_delivers() = runBlocking {
        respond = { n, path -> if (path.endsWith("/bad")) 503 to "down" else 200 to """{"requestId":"g-$n"}""" }
        val notify = "      notify:\n        - { on: acked, channel: downstream }\n        - { on: acked, channel: always503 }\n"
        bootR(plainRoute(notify = notify), downstream() + downstream(name = "always503", path = "/bad", rows = "          - { path: id, field: TRANSFER_ID }\n"))
        seed(inbox, "a.csv")
        withClockTicking { await("the good channel to deliver") { reads.outbox().any { it.channel.value == "downstream" && it.state == DeliveryState.DELIVERED } } }
        assertEquals(TransferState.ACKED, reads.transfers().single().state, "one channel still failing keeps the transfer ACKED, not DONE")
        val bad = reads.outbox().single { it.channel.value == "always503" }
        assertEquals(DeliveryState.PENDING, bad.state, "the failing channel keeps retrying")
        assertTrue(bad.attempts >= 1)
    }

    // ---- S10, S12, S16, S19 to S22, S26: scenarios proven on fakes, re-proven at the adapter level ----

    /** S10: a processor Reject leaves the transfer REJECTED, nothing stored, the object in place; a re-drive re-runs from fetch. */
    @Test
    fun S10_processor_Reject_is_REJECTED_nothing_stored_and_the_object_stays() = runBlocking {
        // An extract whose regex cannot match the name is a Reject before any store (spec 6.3).
        val process = "        - { extract: { from: fileName, regex: \"(?<n>ZZZ)\" } }\n"
        bootR(vendorDrop(notify = "", process = process))
        seed(inbox, "a.csv")
        val row = awaitState(TransferState.REJECTED)
        assertTrue(versions("vendor/a.csv").isEmpty(), "nothing stored")
        assertTrue(inbox.resolve("a.csv").exists(), "the object stays in place; nothing is deleted")
        assertEquals(1.0, counter(ShuttleMetrics.TRANSFERS, "route", "vendor-drop", "outcome", "rejected"))
        assertEquals(TransferState.REJECTED, row.state)
    }

    /** S12: the same identity re-dropped after DONE with the same digest is verified and acked again as `reacked`, with no store and no delivery. */
    @Test
    fun S12_same_identity_re_dropped_after_DONE_is_reacked_with_no_store_and_no_delivery() = runBlocking {
        bootR(plainRoute(notify = notifyBlock, onAck = "none", recheckFinished = "0s"), downstream())
        seed(inbox, "a.csv")
        val done = awaitState(TransferState.DONE)
        val deliveries = reads.outbox().size
        // onAck is `none`, so the file stays and is re-listed; recheckFinished 0s digests it every poll.
        await("the file to be re-acked") { counter(ShuttleMetrics.TRANSFERS, "route", "vendor-drop", "outcome", "reacked") >= 1.0 }
        assertEquals(1, reads.transfers().size, "no new revision: the same digest is the same content")
        assertEquals(1, versions("vendor/a.csv").size, "no second store")
        assertEquals(deliveries, reads.outbox().size, "no new delivery")
        assertEquals(done.id, reads.transfers().single().id)
    }

    /** S16: with the state store unavailable for one poll, nothing completes; the next poll, once it is back, completes the transfer. */
    @Test
    fun S16_state_store_unavailable_for_one_poll_then_completes() = runBlocking {
        bootR(plainRoute())
        storeDown = true
        seed(inbox, "a.csv")
        await("a poll to have run while the store was down") { counter(ShuttleMetrics.POLLS, "route", "vendor-drop", "result", "completed") >= 1.0 }
        storeDown = false
        awaitState(TransferState.DONE)
        assertEquals(1, versions("vendor/a.csv").size)
    }

    /** I17, S19: the mirror route notifies nobody, so it goes ACKED to DONE in one transaction and creates no outbox row. */
    @Test
    fun S19_mirror_route_with_no_notifications_goes_to_DONE_and_creates_no_outbox_row() = runBlocking {
        bootR(mirror())
        seed(outbound, "m.csv")
        val row = awaitState(TransferState.DONE)
        assertEquals("mirror/m.csv", row.target!!.key)
        assertEquals(1, versions("mirror/m.csv").size)
        assertTrue(reads.outbox().isEmpty(), "no outbox row for a route that notifies nobody")
        assertTrue(!outbound.resolve("m.csv").exists(), "onAck delete removed the source")
    }

    /** S20: rename then zip stores one archive under the renamed key; the S3 object's digest is the archive's, differing from the source digest. */
    @Test
    fun S20_rename_then_zip_stores_one_archive_under_the_renamed_key_with_a_different_digest() = runBlocking {
        val process = RENAME + ZIP
        bootR(vendorDrop(notify = "", process = process))
        seed(inbox, "data.csv")
        val row = awaitState(TransferState.DONE)
        val key = "vendor/20260101-data.csv.zip"
        assertEquals(key, row.target!!.key, "STORED under the renamed, zipped key")
        val bytes = Minio.client.getObjectAsBytes(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray()
        assertEquals(listOf("20260101-data.csv"), zipEntryNames(bytes))
        val storedDigest = head(key).metadata().mapKeys { it.key.lowercase() }["digest"]
        assertEquals(md5Hex(bytes), storedDigest, "the object carries the archive's own digest")
        assertTrue(storedDigest != row.sourceDigest!!.hex, "SOURCE_DIGEST and the stored object's DIGEST differ")
        assertEquals(storedDigest, row.digest!!.hex, "the row's digest is the archive's own (ticket 45)")
        assertEquals("20260101-data.csv.zip", row.storedName, "the row's stored_name is the archive's name")
    }

    /** S21: an attribute extracted from the file name is carried in the body; a mapping naming an undeclared attribute is rejected by rule 17 at load. */
    @Test
    fun S21_an_extracted_attribute_reaches_the_body_and_an_undeclared_one_fails_rule_17() = runBlocking {
        bootR(vendorDrop(notify = notifyBlock), downstream(rows = BODY + ORDER_ROW))
        seed(inbox, "777-order.csv")
        await("the delivery") { received.isNotEmpty() }
        assertEquals("777", received.first().body.get("orderNumber").asText(), "the body carries the extracted attribute")

        val badBody = BODY + "          - { path: missing, attribute: notDeclared }\n"
        val failure = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
            load(yaml(vendorDrop(notify = notifyBlock), downstream(rows = badBody)))
        }
        assertTrue(failure.message!!.contains("rule 17"), failure.message)
    }

    /** S22: one provider selected by three body rows is invoked once per rendering and fills three paths in the delivered body (I22). */
    @Test
    fun S22_one_provider_selected_by_three_rows_is_invoked_once_and_fills_three_paths() = runBlocking {
        val invocations = AtomicInteger()
        val provider = infra.shuttle.core.Provider { _ -> invocations.incrementAndGet(); mapper.readTree("""{"a":1,"b":2,"c":3}""") }
        val beans = NamedBeans { if (it == "details") provider else null }
        val rows = "          - { path: fileId, field: TRANSFER_ID }\n" +
            "          - { path: x, provider: details, select: /a }\n" +
            "          - { path: y, provider: details, select: /b }\n" +
            "          - { path: z, provider: details, select: /c }\n"
        bootR(plainRoute(notify = notifyBlock), downstream(rows = rows), beans = beans)
        seed(inbox, "a.csv")
        await("the delivery") { received.isNotEmpty() }
        val body = received.first().body
        assertEquals(1, body.get("x").asInt()); assertEquals(2, body.get("y").asInt()); assertEquals(3, body.get("z").asInt())
        assertEquals(1, invocations.get(), "the provider was invoked once for the three rows (I22)")
    }

    /** S26: a required attribute missing at freeze fails before the store; the reason names the row and nothing is stored. */
    @Test
    fun S26_missing_required_attribute_at_freeze_fails_before_the_store() = runBlocking {
        // The extract declares `orderNumber` (so rule 17 passes) but a digit-less name leaves it blank; the body
        // requires it, so the freeze check fails the transfer before the store (a required attribute, not a Reject).
        val process = "        - { extract: { from: fileName, regex: \"(?<orderNumber>\\\\d*)no\" } }\n"
        bootR(vendorDrop(notify = notifyBlock, process = process), downstream(rows = BODY + ORDER_ROW))
        seed(inbox, "no-number.csv")
        val row = awaitState(TransferState.FAILED)
        assertTrue(row.lastError!!.contains("orderNumber"), "the reason names the row: ${row.lastError}")
        assertTrue(versions("vendor/no-number.csv").isEmpty(), "nothing stored")
    }

    // ---- S18, S23, S24, S25: host-level scenarios ----

    /** S18: a wrong SFTP password leaves the route down and supervised-restarted with backoff, the process alive; readiness follows the rule. */
    @Test
    fun S18_a_wrong_password_leaves_the_route_down_and_restarted_with_backoff_the_process_alive() = runBlocking {
        val route = vendorDrop(notify = "", process = "", store = "bad")
        val host = boot(yaml(route, stores = sftpStore("bad", "\${WRONG}")))
        val before = server.authAttempts
        await("supervised restarts") { counter(ShuttleMetrics.ROUTE_RESTARTS, "route", "vendor-drop") >= 2.0 }
        // The route flaps up/down as the supervisor retries; under all-routes-down the pod is unready while the only route is down.
        await("readiness false while the only route is down") { !host.ready() }
        assertTrue(server.authAttempts > before, "each restart tried the password again")
    }

    /** S23: two routes, one dead; the healthy one keeps completing and readiness stays true under all-routes-down. */
    @Test
    fun S23_two_routes_one_dead_the_other_keeps_completing_and_readiness_stays_true() = runBlocking {
        val routes = plainRoute(notify = "") + mirror(store = "bad")
        val host = boot(yaml(routes, stores = sftpStore("bad", "\${WRONG}")))
        seed(inbox, "a.csv")
        awaitState(TransferState.DONE)
        await("the dead route restarting") { counter(ShuttleMetrics.ROUTE_RESTARTS, "route", "mirror") >= 1.0 }
        assertTrue(host.ready(), "all-routes-down: a partially healthy pod keeps serving")
    }

    /** S24: pool arithmetic exceeded is rejected by rule 9 at load (the same rule validate mode reports). */
    @Test
    fun S24_pool_arithmetic_exceeded_is_rejected_by_rule_9() {
        val failure = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
            load(yaml(plainRoute().replace("parallelism: 4", "parallelism: 9")))
        }
        assertTrue(failure.message!!.contains("rule 9:"), failure.message)
    }

    /** S25: validate mode on a file with five violations lists five rule numbers, exits non-zero, and opens no connection. */
    @Test
    fun S25_validate_mode_on_a_file_with_five_violations_lists_five_rule_numbers_and_exits_non_zero() {
        val badYaml = "shuttle:\n" +
            "  drainTimeout: 60s\n" +
            "  objectStores:\n" +
            "    vendor:\n" +
            "      sftp: { host: sftp.example, auth: { user: \${SFTP_USER}, password: hunter2 }, staging: { dir: ${staging.toString().replace('\\', '/')} } }\n" +
            "  channels:\n" +
            "    downstream:\n" +
            "      http: { url: https://downstream.internal, timeout: 61s }\n" +
            "  routes:\n" +
            "    mirror:\n" +
            "      source: { poll: { store: vendor, directory: /outbound, every: 1h } }\n" +
            "      target: { store: nowhere, directory: /incoming }\n" +
            "      notify: [ { on: acked, channel: downstream } ]\n" +
            "      parallelism: 0\n"
        val file = files.resolve("bad-validate.yaml").also { it.writeText(badYaml) }
        val out = java.io.ByteArrayOutputStream()
        val code = infra.shuttle.quarkus.ValidateCommand(listOf(file), env, { null }, java.io.PrintStream(out, true)).run()
        assertEquals(1, code)
        val rules = Regex("^rule (\\d+):", RegexOption.MULTILINE).findAll(out.toString()).map { it.groupValues[1].toInt() }.toList()
        assertTrue(rules.size >= 5, "five violations expected, got $rules")
        assertEquals(0, received.size, "the command opened no connection")
    }

    // ---- S13: the load scenario, tagged `load` (spec 18.2 S13) ----

    /** S13 at a scaled-down volume: all DONE, in-flight never above `parallelism`, staging bounded, and no poll skipped at the next tick. */
    @Test
    @Tag("load")
    fun S13_a_batch_of_files_all_reach_DONE_with_in_flight_bounded_and_staging_bounded() = runBlocking {
        val n = 200
        val sizeBytes = 64 * 1024 // 64 KiB each; see the progress entry for the full-scale extrapolation.
        val content = "x".repeat(sizeBytes)
        repeat(n) { seed(inbox, "load-$it.csv", content) }
        // A long poll interval so one tick lists the whole batch and drains it before the next tick fires: the
        // in-flight bound, not the poll, is the backpressure, so no tick is skipped at the next interval.
        bootR(plainRoute(notify = "").replace("every: 5s", "every: 60s"))
        val parallelism = 4

        var maxInflight = 0
        var maxStagingFiles = 0
        withTimeout(300_000) {
            while (reads.transfers().count { it.state == TransferState.DONE } < n) {
                maxInflight = maxOf(maxInflight, registry.find(ShuttleMetrics.INFLIGHT).tag("route", "vendor-drop").gauge()?.value()?.toInt() ?: 0)
                maxStagingFiles = maxOf(maxStagingFiles, Files.list(staging).use { it.count().toInt() })
                delay(50)
            }
        }
        assertEquals(n, reads.transfers().count { it.state == TransferState.DONE }, "every file reached DONE")
        assertEquals(n, versions("vendor/").filter { it.isLatest }.size, "one current version per file")
        assertTrue(maxInflight <= parallelism, "in-flight never above parallelism: peak $maxInflight")
        assertTrue(maxStagingFiles <= parallelism, "staging bounded to at most parallelism run directories: peak $maxStagingFiles")
        assertEquals(0.0, counter(ShuttleMetrics.POLLS, "route", "vendor-drop", "result", "skipped"), "no poll skipped at the next tick")
    }

    private companion object {
        const val EXTRACT = "        - { extract: { from: fileName, regex: \"(?<orderNumber>\\\\d+)-.*\\\\.csv\" } }\n"
        const val RENAME = "        - { rename: { pattern: \"{yyyyMMdd}-{name}\" } }\n"
        const val ZIP = "        - { zip: {} }\n"
        /** The order-number attribute row; only a route whose chain declares `orderNumber` (the extract) may carry it (rule 17). */
        const val ORDER_ROW = "          - { path: orderNumber,     attribute: orderNumber }\n"
    }
}
