package infra.shuttle.sftp

import infra.shuttle.core.AckAction
import infra.shuttle.core.DigestAlgorithm
import infra.shuttle.core.FileReadiness
import infra.shuttle.core.HostKey
import infra.shuttle.core.Pool
import infra.shuttle.core.Secret
import infra.shuttle.core.SftpStore
import infra.shuttle.core.Source
import infra.shuttle.core.Staging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import sftp.connector.config.AuthMethod
import sftp.connector.config.HostKeyPolicy
import sftp.connector.config.OverlapPolicy
import sftp.connector.config.PostAction
import sftp.connector.source.AllOf
import sftp.connector.source.MinAge
import sftp.connector.source.SizeStable
import java.nio.file.Path
import sftp.connector.config.Digest as ConnectorDigest
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Spec 13.1's `sftp` store and one route's `poll` as the connector's own configuration. No server. */
class SftpConnectorConfigTest {

    @TempDir lateinit var stage: Path

    private fun store(
        pool: Pool = Pool(maxSize = 20, maxConcurrentTransfers = 16),
        idleCutoff: kotlin.time.Duration = 3.minutes,
    ) = SftpStore(
        name = "vendor", host = "sftp.example", port = 2222,
        user = Secret.Env("SFTP_USER"), password = Secret.Env("SFTP_PASSWORD"),
        hostKey = HostKey.AcceptAll, idleTimeout = 2.minutes, keepAlive = 20.seconds,
        idleCutoff = idleCutoff, pool = pool, staging = Staging(stage),
    )

    private fun poll(
        onAck: AckAction? = AckAction.Move("temp/"),
        readiness: List<FileReadiness> = listOf(FileReadiness.SizeStable(3, 5.seconds), FileReadiness.MinAge(2.minutes)),
    ) = Source.Poll(store = "vendor", directory = "/inbox", every = 1.minutes, readiness = readiness, onAck = onAck)

    private fun configOf(
        store: SftpStore = store(),
        poll: Source.Poll = poll(),
        algorithm: DigestAlgorithm = DigestAlgorithm.MD5,
    ) = sftpConnectorConfig(store, poll, algorithm) { (it as Secret.Env).variable + "-value" }

    @Test
    fun the_store_and_the_poll_reach_the_connectors_config() {
        val config = configOf()

        assertEquals("vendor", config.name)
        assertEquals("sftp.example:2222", config.endpoint.address)
        assertEquals(HostKeyPolicy.AcceptAll, config.hostKey)
        assertEquals("SFTP_USER-value", (config.auth as AuthMethod.Password).user, "the secret is resolved by the caller")
        assertEquals(20, config.pool.maxSize)
        assertEquals(16, config.resilience.maxConcurrentTransfers)
        assertEquals(3.minutes, config.pool.idleCutoff)
        assertEquals(2.minutes, config.pool.idleTimeout)
        assertEquals(20.seconds, config.pool.keepAlive)
        assertEquals(listOf("/inbox"), config.polling.directories)
        assertEquals(OverlapPolicy.SKIP, config.polling.overlap, "spec 5.1: one listing of a directory at a time")
        assertEquals(stage, config.polling.staging.dir)
        assertEquals(ConnectorDigest.MD5, config.polling.staging.digest, "so the download's own sum is the one the pipeline wants")
        assertEquals(1000, config.polling.maxFilesPerPoll, "no store knob for it yet; the connector's own default stands")
    }

    @Test
    fun the_readiness_checks_reach_the_connector_in_order() {
        val checks = assertInstanceOf(AllOf::class.java, configOf().polling.readiness).checks

        val sizeStable = assertInstanceOf(SizeStable::class.java, checks[0])
        assertEquals(3, sizeStable.checks)
        assertEquals(5.seconds, sizeStable.interval)
        assertEquals(2.minutes, assertInstanceOf(MinAge::class.java, checks[1]).duration)
    }

    @Test
    fun a_store_that_declares_no_readiness_hands_every_listed_file_over() {
        assertEquals(emptyList<Any>(), assertInstanceOf(AllOf::class.java, configOf(poll = poll(readiness = emptyList())).polling.readiness).checks)
    }

    /**
     * Spec 5.3: `move`, `delete` and `none` are the whole poll vocabulary, and the nack is always
     * none. `callback` is an ack action of any trigger and asks nothing of the file: the pipeline
     * makes the call itself, so the connector does what `none` does (ticket 22).
     */
    @Test
    fun the_ack_vocabulary_maps_onto_the_connectors_post_actions() {
        assertEquals(PostAction.Move("temp/"), configOf(poll = poll(onAck = AckAction.Move("temp/"))).polling.onAck)
        assertEquals(PostAction.Delete, configOf(poll = poll(onAck = AckAction.Delete)).polling.onAck)
        assertEquals(PostAction.Noop, configOf(poll = poll(onAck = AckAction.None)).polling.onAck)
        assertEquals(PostAction.Noop, configOf(poll = poll(onAck = null)).polling.onAck)
        assertEquals(PostAction.Noop, configOf(poll = poll(onAck = AckAction.Callback("upstream"))).polling.onAck)
        assertEquals(PostAction.Noop, configOf().polling.onNack, "a polled file's redelivery is the next poll")
    }

    @Test
    fun rule12_an_ack_action_of_another_trigger_is_not_something_a_poll_can_do() {
        val refused = assertThrows(IllegalArgumentException::class.java) { configOf(poll = poll(onAck = AckAction.Term)) }
        assertEquals(true, refused.message!!.contains("onAck"), refused.message)
    }

    @Test
    fun sha1_has_no_name_in_the_connector_so_its_downloads_are_summed_with_sha256() {
        assertEquals(ConnectorDigest.SHA256, configOf(algorithm = DigestAlgorithm.SHA1).polling.staging.digest)
        assertEquals(ConnectorDigest.SHA256, configOf(algorithm = DigestAlgorithm.SHA256).polling.staging.digest)
    }
}
