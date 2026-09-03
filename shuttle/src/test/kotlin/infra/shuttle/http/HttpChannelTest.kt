package infra.shuttle.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import infra.shuttle.core.ChannelName
import infra.shuttle.core.DeliveryEvent
import infra.shuttle.core.DeliveryMoment
import infra.shuttle.core.DeliveryOutcome
import infra.shuttle.core.HttpAuth
import infra.shuttle.core.HttpChannel as HttpChannelConfig
import infra.shuttle.core.ResponseSpec
import infra.shuttle.core.Secret
import infra.shuttle.core.TransferId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.http.HttpClient
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** The seam is `DeliveryChannel.deliver`, observed through a loopback JDK server on port 0. */
class HttpChannelTest {

    private class Received(val headers: Map<String, List<String>>, val body: String)

    private lateinit var server: HttpServer
    private val received = LinkedBlockingQueue<Received>()
    private val mapper = ObjectMapper()

    /** What the server answers once it has recorded the request; tests swap it. */
    private var respond: (HttpExchange) -> Unit = { it.answer(200, """{"requestId":"r-1"}""") }

    @BeforeEach
    fun start() {
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            received.add(Received(exchange.requestHeaders, exchange.requestBody.readAllBytes().decodeToString()))
            try { respond(exchange) } finally { exchange.close() }
        }
        server.start()
    }

    @AfterEach
    fun stop() = server.stop(0)

    private fun HttpExchange.answer(status: Int, body: String) {
        val bytes = body.encodeToByteArray()
        sendResponseHeaders(status, bytes.size.toLong())
        responseBody.write(bytes)
    }

    private fun config(
        auth: HttpAuth? = null,
        response: ResponseSpec = ResponseSpec(success = (200..299).toSet(), retry = setOf(408, 429) + (500..599), reference = "/requestId"),
        port: Int = server.address.port,
    ) = HttpChannelConfig(name = "downstream", url = "http://127.0.0.1:$port/api/files", auth = auth, timeout = 1.seconds, response = response)

    private fun channel(config: HttpChannelConfig = config(), env: Map<String, String> = emptyMap()) =
        HttpChannel(config, HttpClient.newHttpClient(), env::get)

    private fun event(body: String = """{"fileId":"42"}""") =
        DeliveryEvent(TransferId(42), DeliveryMoment.ACKED, ChannelName("downstream"), attempt = 1, body = mapper.readTree(body))

    @Test
    fun `200 with the reference pointer resolving yields Delivered with the reference`() = runBlocking {
        val outcome = channel().deliver(event())
        assertEquals(DeliveryOutcome.Delivered("r-1"), outcome)
    }

    @Test
    fun `200 without the pointer resolving yields Delivered with a null reference`() = runBlocking {
        respond = { it.answer(200, """{"id":"elsewhere"}""") }
        assertEquals(DeliveryOutcome.Delivered(null), channel().deliver(event()))
    }

    @Test
    fun `a retry status yields Retry and any other status yields Reject`() = runBlocking {
        for (status in listOf(503, 429)) {
            respond = { it.answer(status, "busy") }
            val outcome = channel().deliver(event())
            assertTrue(outcome is DeliveryOutcome.Retry && outcome.status == status.toString(), "status $status gave $outcome")
        }
        respond = { it.answer(400, """{"error":"bad"}""") }
        val outcome = channel().deliver(event())
        assertTrue(outcome is DeliveryOutcome.Reject && outcome.status == "400", "status 400 gave $outcome")
    }

    @Test
    fun `connection refused yields Retry`() = runBlocking {
        val closedPort = ServerSocket(0).use { it.localPort }
        val outcome = channel(config(port = closedPort)).deliver(event())
        assertTrue(outcome is DeliveryOutcome.Retry && outcome.status == null, "refused gave $outcome")
    }

    @Test
    fun `auth modes bearer basic and header set the header the server sees`() = runBlocking {
        val env = mapOf("TOKEN" to "t-1", "USER" to "u", "PASS" to "p:w", "KEY" to "k-1")
        channel(config(auth = HttpAuth.Bearer(Secret.Env("TOKEN"))), env).deliver(event())
        assertEquals("Bearer t-1", received.take().headers["Authorization"]?.single())
        channel(config(auth = HttpAuth.Basic(Secret.Env("USER"), Secret.Env("PASS"))), env).deliver(event())
        assertEquals("Basic " + Base64.getEncoder().encodeToString("u:p:w".encodeToByteArray()), received.take().headers["Authorization"]?.single())
        channel(config(auth = HttpAuth.Header("X-Api-Key", Secret.Env("KEY"))), env).deliver(event())
        assertEquals("k-1", received.take().headers["X-api-key"]?.single())
    }

    @Test
    fun `a body value with quotes and backslashes arrives escaped and parses back`() = runBlocking {
        val name = """a "quoted" \ back\slash.csv"""
        channel().deliver(event(mapper.writeValueAsString(mapOf("file" to mapOf("name" to name)))))
        val body = received.take().body
        assertTrue("\\\"" in body && "\\\\" in body, "not escaped: $body")
        assertEquals(name, mapper.readTree(body).at("/file/name").asText())
    }

    @Test
    fun `CancellationException propagates unchanged and produces no outcome`() = runBlocking {
        val release = CountDownLatch(1)
        respond = { release.await(); it.answer(200, "{}") }
        var thrown: Throwable? = null
        var outcome: DeliveryOutcome? = null
        try {
            val job = launch(Dispatchers.Default) {
                try { outcome = channel(config().copy(timeout = 30.seconds)).deliver(event()) } catch (e: CancellationException) { thrown = e; throw e }
            }
            received.take()   // the request is on the wire and the handler is stalled
            job.cancelAndJoin()
        } finally {
            release.countDown()
        }
        assertTrue(thrown is CancellationException, "got $thrown")
        assertNull(outcome)
    }

    @Test
    fun `a stall past the timeout yields Retry`() = runBlocking {
        val release = CountDownLatch(1)
        respond = { release.await(); it.answer(200, "{}") }
        try {
            val outcome = channel(config().copy(timeout = 200.milliseconds)).deliver(event())
            assertTrue(outcome is DeliveryOutcome.Retry && outcome.status == null, "stall gave $outcome")
        } finally {
            release.countDown()
        }
    }
}
