package infra.shuttle.acceptance

import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryState
import infra.shuttle.core.HookPoint
import infra.shuttle.core.Outcome
import infra.shuttle.core.Payload
import infra.shuttle.core.ProcessContext
import infra.shuttle.core.Processor
import infra.shuttle.core.ShuttleMetrics
import infra.shuttle.core.Transfer
import infra.shuttle.core.TransferKind
import infra.shuttle.core.TransferState
import infra.shuttle.nats.NatsBroker
import infra.shuttle.quarkus.NamedBeans
import infra.shuttle.quarkus.ShuttleHost
import infra.shuttle.s3.Minio
import io.nats.client.Connection
import io.nats.client.JetStreamManagement
import io.nats.client.Nats
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.PublishAck
import io.nats.client.api.StorageType
import io.nats.client.api.StreamConfiguration
import io.nats.client.impl.Headers
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectories
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

/**
 * Spec 18.2, milestone 2: S27 to S30 and S32 through the real `ShuttleHost` over the fixture's adapters
 * (`AcceptanceFixture`) plus NATS JetStream on Testcontainers: spec 13.1's image-sets route, subscribe on NATS,
 * fetch and expand from MinIO, the SFTP target on the embedded SSHD as the partner server, `fetched` and `acked`
 * notifications and the callback ack on the loopback server. The stream and the durable consumer are the
 * operator's (spec 17 item 9), created here per scenario with a two second ack wait so a redelivery happens
 * inside a test's patience; `inProgressEvery` stays below it (D38).
 */
@Tag("acceptance")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class M2AcceptanceTest : AcceptanceFixture() {

    override val env = super.env + ("UPSTREAM_KEY" to "k3y")

    private lateinit var nats: Connection
    private lateinit var management: JetStreamManagement
    private lateinit var stream: String
    private lateinit var subject: String
    private lateinit var incoming: Path
    private var scenarios = 0

    /** Spec 13.1's `custom: imageResizer`, a bean that passes the payload through: what it would do to an image is not this suite's. */
    private val beans = NamedBeans { name ->
        if (name != "imageResizer") null else object : Processor {
            override val produces = emptySet<String>()
            override suspend fun process(payload: Payload, ctx: ProcessContext) = Outcome.Continue(payload)
        }
    }

    @BeforeAll
    fun startBroker() {
        nats = Nats.connect(NatsBroker.url)
        management = nats.jetStreamManagement()
    }

    @AfterAll
    fun stopBroker() {
        nats.close()
    }

    /** The operator's provisioning (D15): one stream and the route's durable consumer, ack wait 2 s, per scenario. */
    @BeforeEach
    fun freshStream() {
        stream = "images-${++scenarios}"
        subject = "images.ready.$scenarios"
        management.addStream(
            StreamConfiguration.builder().name(stream).storageType(StorageType.Memory).subjects(subject).duplicateWindow(Duration.ofMillis(500)).build(),
        )
        management.addOrUpdateConsumer(stream, ConsumerConfiguration.builder().durable(ROUTE).filterSubject(subject).ackWait(ACK_WAIT).build())
        incoming = root.resolve("incoming").also { it.toFile().deleteRecursively(); it.createDirectories() }
    }

    // ---- spec 13.1's image-sets route at test scale ----

    private fun imageSets(
        onAck: String = "ack",
        parallelism: Int = 2,
        maxAttempts: Int = 5,
        notify: String = "      notify:\n        - { on: fetched, channel: upstream-receipt }\n        - { on: acked,   channel: downstream }\n",
    ) = "    $ROUTE:\n" +
        "      source:\n" +
        "        subscribe: { channel: events, subject: $subject, onAck: $onAck, inProgressEvery: 500ms }\n" +
        "      fetch: { store: minio, bucket: $bucket, path: /metadata/path }\n" +
        "      process:\n" +
        "        - { extract: { from: message, json: { batchId: /batchId } } }\n" +
        "        - { expand: { format: json, files: \"/images[*].path\", from: minio } }\n" +
        "        - { custom: imageResizer, config: { maxWidth: 2048 } }\n" +
        "      target: { store: partner, directory: /incoming }\n" +
        notify +
        "      parallelism: $parallelism\n" +
        "      maxAttempts: $maxAttempts\n"

    private val events get() = "    events:\n      nats: { url: ${NatsBroker.url} }\n"

    /** Spec 13.1's `upstream-receipt` shape: an API key header, the source path and digest; the same shape serves S30's callback channel. */
    private fun upstream(name: String, path: String) =
        "    $name:\n" +
            "      http:\n" +
            "        method: POST\n" +
            "        url: http://127.0.0.1:${http.address.port}$path\n" +
            "        auth: { header: { name: X-Api-Key, value: \${UPSTREAM_KEY} } }\n" +
            "        timeout: 4s\n" +
            "        response: { success: [200-299], retry: [500-599] }\n" +
            "        body:\n" +
            "          - { path: object, field: SOURCE_PATH }\n" +
            "          - { path: md5,    field: SOURCE_DIGEST }\n" +
            "          - { path: event,  field: EVENT }\n"

    private val upstreamReceipt get() = upstream("upstream-receipt", "/api/received")

    /** Boot spec 13.1's image-sets route with the partner store, the three channels and the `imageResizer` bean. */
    private fun bootImageSets(route: String = imageSets(), channels: String = downstream(rows = M2_BODY)) =
        boot(yaml(route, channels = events + upstreamReceipt + channels, stores = sftpStore("partner", "\${SFTP_PASSWORD}")), beans)

    // ---- the message, the metadata file and the images ----

    private fun putObject(key: String, content: String) =
        Minio.client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromString(content))

    /** A batch of [images] on MinIO with its metadata file, the way the upstream would leave them before publishing. */
    private fun batch(id: String, vararg images: String) {
        images.forEach { putObject("img/$it", "bytes of $it") }
        putObject("sets/$id.json", """{"images":[${images.joinToString(",") { "{\"path\":\"img/$it\"}" }}]}""")
    }

    private fun message(id: String) = """{"batchId":"$id","metadata":{"path":"sets/$id.json"}}"""

    /** The upstream's publish: the batch's message on the subject, with a `Nats-Msg-Id` when the publisher sets one (spec 5.2). */
    private fun publish(id: String, msgId: String? = null): PublishAck {
        val headers = msgId?.let { Headers().put("Nats-Msg-Id", it) }
        val message = NatsMessage.builder().subject(subject).headers(headers).data(message(id).encodeToByteArray()).build()
        return nats.jetStream().publish(message)
    }

    // ---- observation: the ledger, the partner's disk, the broker, the loopback server ----

    private suspend fun parent(): Transfer = reads.transfers().single { it.kind == TransferKind.MESSAGE }

    private suspend fun awaitParent(state: TransferState): Transfer {
        await("the parent in $state") { reads.transfers().any { it.kind == TransferKind.MESSAGE && it.state == state } }
        return parent()
    }

    private fun onPartner(): List<String> = incoming.listDirectoryEntries().map { it.name }.sorted()

    private fun consumer(): ConsumerInfo = management.getConsumerInfo(stream, ROUTE)

    private fun requests(path: String) = received.filter { it.path == path }

    // ---- S27 to S32 ----

    /**
     * S27, I10, I16, I20 on real adapters: one message, one metadata file, N images. The parent is FETCHED and told
     * upstream, the children land on the partner in parallel, the parent is acked once at the broker and downstream
     * told once, with the metadata file's name and digest in the body (a parent stores nothing itself; ticket 45).
     */
    @Test
    fun S27_image_sets_happy_path_children_stored_on_the_partner_message_acked_once_fetched_and_acked_delivered_once_each() = runBlocking {
        bootImageSets()
        batch("b-1", "1.png", "2.png", "3.png")
        publish("b-1")

        val parent = awaitParent(TransferState.DONE)

        val children = reads.childrenOf(parent.id)
        assertEquals(3, children.size)
        assertTrue(children.all { it.state == TransferState.DONE }, "every child DONE: ${children.map { it.state }}")
        assertEquals(listOf("1.png", "2.png", "3.png"), onPartner(), "N children on the partner, and no partial file")
        assertEquals(listOf("bytes of 1.png", "bytes of 2.png", "bytes of 3.png"), onPartner().map { Files.readString(incoming.resolve(it)) })
        assertEquals(mapOf("batchId" to "b-1"), parent.attributes, "extract from: message")
        assertTrue(server.liveSessions <= 2, "the partner saw at most the route's parallelism in sessions (rule 9): ${server.liveSessions}")

        val consumer = consumer()
        assertEquals(0L, consumer.numAckPending, "the message is acked")
        assertEquals(1L, consumer.delivered.consumerSequence, "and was delivered once")
        assertEquals(1L, consumer.ackFloor.consumerSequence)

        val outbox = reads.outbox()
        assertEquals(setOf(DeliveryMoment.FETCHED to DeliveryState.DELIVERED, DeliveryMoment.ACKED to DeliveryState.DELIVERED), outbox.map { it.moment to it.state }.toSet())
        assertTrue(outbox.all { it.transferId == parent.id }, "children never notify (D28)")
        val fetched = requests("/api/received").single().body
        assertEquals("events:$subject/1", fetched.get("object").asText(), "SOURCE_PATH of a message is channel:subject/id")
        assertEquals(parent.sourceDigest!!.hex, fetched.get("md5").asText())
        val acked = requests("/api/files").single().body
        assertEquals(parent.id.value, acked.get("fileId").asLong())
        assertEquals("acked", acked.get("event").asText())
        assertEquals("b-1", acked.get("batchId").asText())
        assertEquals("message", acked.get("kind").asText())
        // Ticket 45: a parent stores nothing itself, so its stored_name and digest stay the fetched metadata file's; each child carries its own.
        assertEquals("b-1.json", acked.get("file").get("name").asText())
        assertEquals(parent.sourceDigest!!.hex, acked.get("file").get("md5").asText())
        assertEquals(listOf("1.png", "2.png", "3.png"), children.mapNotNull { it.storedName }.sorted(), "each child's stored_name is the image it stored")
        assertTrue(acked.get("location") == null, "a parent has no target of its own; TARGET_* rows are required: false")
    }

    /**
     * S28, I8, I16 on real adapters: the process dies with the first child STORED and the second not started (one upload
     * at a time). The broker redelivers after its ack wait; the redelivery keeps the child rows, verifies the stored
     * child on the partner by size and mtime (ticket 18), stores the rest, and acks the message once.
     */
    @Test
    fun S28_crash_with_half_the_children_stored_the_redelivery_verifies_them_stores_the_rest_and_acks_once() = runBlocking {
        hook.pauseAt(HookPoint.afterLedgerStored)
        val host = bootImageSets(imageSets(parallelism = 1))
        batch("b-2", "1.png", "2.png")
        publish("b-2")
        val first = crash(host, HookPoint.afterLedgerStored)

        val parent = parent()
        assertEquals(TransferState.PROCESSED, parent.state)
        val before = reads.childrenOf(parent.id)
        assertEquals(setOf(TransferState.STORED, TransferState.FETCHED), before.map { it.state }.toSet(), "half the children stored: ${before.map { it.state }}")
        val stored = before.single { it.state == TransferState.STORED }
        assertEquals(first, stored.id)
        assertEquals(listOf(stored.storedName), onPartner(), "one copy on the partner, no partial file")
        val landed = Files.getLastModifiedTime(incoming.resolve(stored.storedName!!))
        assertEquals(1L, consumer().numAckPending, "the message is still the broker's to redeliver")

        bootImageSets(imageSets(parallelism = 1))
        awaitParent(TransferState.DONE)

        val after = reads.childrenOf(parent.id)
        assertEquals(before.map { it.id }, after.map { it.id }, "the redelivery kept the child rows")
        assertEquals(stored.target, after.single { it.id == stored.id }.target, "the stored child was verified, not stored again")
        assertEquals(landed, Files.getLastModifiedTime(incoming.resolve(stored.storedName!!)), "the partner's copy was not touched")
        assertEquals(listOf("1.png", "2.png"), onPartner())
        val consumer = consumer()
        assertEquals(2L, consumer.delivered.consumerSequence, "delivered twice: the redelivery under the same message id (spec 5.2)")
        assertEquals(0L, consumer.numAckPending, "acked once, at the end")
        assertEquals(listOf(DeliveryState.DELIVERED, DeliveryState.DELIVERED), reads.outbox().map { it.state }, "fetched and acked, once each")
        assertEquals(1, requests("/api/files").size)
    }

    /**
     * S32, I23 on real adapters: the process dies after ledger ACKED and before the broker ack. The broker redelivers
     * the same message; the row is ACKED already, so every child is verified on the partner, nothing is fetched or
     * stored, the broker is acked (`reacked`), and the outbox holds exactly the rows the ledger wrote before the crash.
     */
    @Test
    fun S32_crash_after_ledger_ACKED_before_the_broker_ack_the_redelivery_reacks_with_children_verified_and_no_new_outbox_rows() = runBlocking {
        // Downstream is down until the crash, so the acked row is PENDING with one attempt under the frozen clock.
        respond = { n, path -> if (path == "/api/files") 503 to "down" else 200 to """{"requestId":"r-$n"}""" }
        hook.pauseAt(HookPoint.afterLedgerAcked)
        val host = bootImageSets()
        batch("b-3", "1.png", "2.png")
        publish("b-3")
        val id = crash(host, HookPoint.afterLedgerAcked)

        val parent = parent()
        assertEquals(id, parent.id)
        assertEquals(TransferState.ACKED, parent.state)
        val outbox = reads.outbox()
        assertEquals(DeliveryState.PENDING, outbox.single { it.moment == DeliveryMoment.ACKED }.state)
        assertEquals(1L, consumer().numAckPending, "the broker was never acked")
        val landed = onPartner().map { Files.getLastModifiedTime(incoming.resolve(it)) }

        respond = { n, _ -> 200 to """{"requestId":"r-$n"}""" }
        bootImageSets()
        withClockTicking {
            await("the redelivery to be reacked") { counter(ShuttleMetrics.TRANSFERS, "route", ROUTE, "outcome", "reacked") == 1.0 }
            awaitParent(TransferState.DONE)
        }

        assertEquals(outbox.map { it.id }, reads.outbox().map { it.id }, "exactly the outbox rows the ledger wrote before the crash")
        assertTrue(reads.outbox().all { it.state == DeliveryState.DELIVERED })
        assertEquals(landed, onPartner().map { Files.getLastModifiedTime(incoming.resolve(it)) }, "every child verified, nothing stored again")
        val consumer = consumer()
        assertEquals(2L, consumer.delivered.consumerSequence, "delivered twice")
        assertEquals(0L, consumer.numAckPending, "acked on the redelivery")
    }

    /**
     * S29, I16 on real adapters: the partner holds a folder where one child's key must land, so every rename over it is
     * refused. Five redeliveries later the child and the parent are FAILED, the other child STORED, nothing acked
     * downstream. The message is termed at the fifth failure (ticket 16), so after the operator's fix and re-drive the
     * trigger is the upstream's republish under the same `Nats-Msg-Id` (spec 5.2), outside the stream's duplicate window.
     */
    @Test
    fun S29_one_child_failing_five_times_fails_the_parent_the_message_is_not_acked_and_a_redrive_reruns_the_chain() = runBlocking {
        bootImageSets(imageSets(parallelism = 1))
        batch("b-4", "1.png", "2.png")
        incoming.resolve("2.png").createDirectories()
        publish("b-4", msgId = "b-4")

        val parent = awaitParent(TransferState.FAILED)
        // The row flips FAILED inside the child's attempt; the count and the term follow on the parent's failure path.
        await("the failure to be counted and the message termed") {
            counter(ShuttleMetrics.TRANSFERS, "route", ROUTE, "outcome", "failed") == 1.0 && consumer().numAckPending == 0L
        }
        val children = reads.childrenOf(parent.id)
        val failed = children.single { it.state == TransferState.FAILED }
        assertEquals("2.png", failed.storedName)
        assertEquals(5, failed.attempts, "the child's attempts, one per redelivery")
        assertEquals(0, parent.attempts, "the parent is failed by its child, not charged")
        assertEquals(TransferState.STORED, children.single { it.storedName == "1.png" }.state, "the sibling was stored once and verified after")
        assertEquals(5L, consumer().delivered.consumerSequence, "four naks then a term: five deliveries, none of them acked")
        assertTrue(requests("/api/files").isEmpty(), "nothing told downstream")

        // The operator clears the folder and re-drives; the upstream publishes the message again.
        assertTrue(incoming.resolve("2.png").toFile().delete())
        assertEquals(ShuttleHost.Outcome.DONE, hosts.single().redrive(parent.id))
        assertEquals(TransferState.SEEN, reads.transfers().single { it.id == parent.id }.state)
        // Inside the stream's duplicate window a republish under the same id is dropped as a duplicate (D46): try until it is stored.
        await("the stream's duplicate window to pass") { !publish("b-4", msgId = "b-4").isDuplicate }

        awaitParent(TransferState.DONE)
        val replaced = reads.childrenOf(parent.id)
        assertTrue(replaced.none { r -> children.any { it.id == r.id } }, "the re-drive re-ran the chain and replaced its children (spec 4.5)")
        assertEquals(listOf("1.png", "2.png"), onPartner())
        assertEquals(1, requests("/api/files").size, "told downstream once, after the re-drive")
        assertEquals(1, requests("/api/received").size, "the fetched row already existed, so upstream was not told again (I20)")
    }

    /**
     * S30 on real adapters: `onAck: callback` on a subscribed route. The first callback is held, so the transfer is seen
     * STORED with the message still the broker's; it then answers 500, which is a failed attempt and a nak; the
     * redelivery verifies the children, calls again, gets 200, and only then is the row ACKED, the broker acked and
     * downstream told once.
     */
    @Test
    fun S30_a_callback_ack_answering_500_then_200_keeps_the_transfer_STORED_through_the_failure_and_ACKED_after_with_one_acked_delivery() = runBlocking {
        val callbacks = AtomicInteger()
        respond = { n, path ->
            if (path == "/api/ack" && callbacks.incrementAndGet() == 1) { release.await(); 500 to "not yet" } else 200 to """{"requestId":"r-$n"}"""
        }
        bootImageSets(imageSets(onAck = "{ callback: upstream-ack }"), channels = upstream("upstream-ack", "/api/ack") + downstream(rows = M2_BODY))
        batch("b-5", "1.png", "2.png")
        publish("b-5")

        await("the first callback to be in flight") { requests("/api/ack").size == 1 }
        val held = parent()
        assertEquals(TransferState.STORED, held.state, "STORED through the callback")
        assertEquals(0, held.attempts)
        assertTrue(reads.outbox().none { it.moment == DeliveryMoment.ACKED }, "no acked row before the callback succeeds")
        assertEquals(1L, consumer().numAckPending, "the message is not acked before upstream answers")
        release.countDown()

        val parent = awaitParent(TransferState.DONE)
        assertEquals(1, parent.attempts, "one failed attempt, the 500")
        assertEquals(listOf("acked", "acked"), requests("/api/ack").map { it.body.get("event").asText() }, "500 then 200, both for the acked moment")
        assertEquals(DeliveryState.DELIVERED, reads.outbox().single { it.moment == DeliveryMoment.ACKED }.state)
        assertEquals(1, requests("/api/files").size, "one acked delivery")
        val consumer = consumer()
        assertEquals(2L, consumer.delivered.consumerSequence, "the nak's redelivery")
        assertEquals(0L, consumer.numAckPending)
    }

    private companion object {
        const val ROUTE = "image-sets"
        val ACK_WAIT: Duration = Duration.ofSeconds(2)

        /**
         * Spec 13.1's `downstream` body for the image-sets route. A message parent has no size, mtime or target of its
         * own (spec 5.2, D28), so the rows that read them are `required: false` here; the vendor-drop rows are M1's `BODY`.
         */
        const val M2_BODY =
            "          - { path: fileId,          field: TRANSFER_ID }\n" +
                "          - { path: kind,            field: KIND }\n" +
                "          - { path: file.name,       field: STORED_NAME }\n" +
                "          - { path: file.md5,        field: DIGEST }\n" +
                "          - { path: file.size,       field: TARGET_SIZE,     required: false }\n" +
                "          - { path: location.bucket, field: TARGET_LOCATION, required: false }\n" +
                "          - { path: location.key,    field: TARGET_KEY,      required: false }\n" +
                "          - { path: receivedAt,      field: SOURCE_MTIME,    format: ISO_INSTANT, required: false }\n" +
                "          - { path: batchId,         attribute: batchId }\n" +
                "          - { path: event,           field: EVENT }\n" +
                "          - { path: source,          value: image-sets }\n"
    }
}
