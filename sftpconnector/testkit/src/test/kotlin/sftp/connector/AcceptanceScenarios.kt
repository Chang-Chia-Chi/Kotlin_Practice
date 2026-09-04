package sftp.connector

import org.junit.platform.suite.api.SelectMethod
import org.junit.platform.suite.api.Suite
import sftp.connector.source.SftpWatchTest
import sftp.connector.source.SourceAgainstServerTest
import sftp.connector.source.WatchUnderOpenBreakerTest
import sftp.connector.testkit.ReadPathAgainstServerTest
import sftp.connector.testkit.ResilienceAgainstServerTest

/**
 * The scenario table, S1 to S12, as one suite: `mvn -Dtest=AcceptanceScenarios` runs the twelve
 * and nothing else. Each scenario is selected from the class that proves it end to end, which is
 * where the ticket that built the behaviour left it; nothing is duplicated here, and the
 * selection is checked on every build by `AcceptanceScenariosTest`.
 *
 * S4, S8 and S12 are about what the watch and the pool do with a tick, which the fake transport
 * on virtual time proves deterministically and a real server could only prove with a timer. S3's
 * expected outcome is a sequence of ticks, and lives on the fake for the same reason; its breaker
 * is proven against a real server and a refusing proxy in `ResilienceAgainstServerTest.S3_`.
 */
@Suite
@SelectMethod(type = ResilienceAgainstServerTest::class, name = "S1_a session killed mid-download is replaced and the download completes, with the file seen once")
@SelectMethod(type = ResilienceAgainstServerTest::class, name = "S2_a stall past the keepalive poisons the session and the call is retried on a fresh one")
@SelectMethod(type = WatchUnderOpenBreakerTest::class, name = "S3_an open breaker skips every tick without dialling, until the probe after the wait closes it")
@SelectMethod(type = SftpWatchTest::class, name = "S4_a full pool fails the tick, and the watch continues")
@SelectMethod(type = SourceAgainstServerTest::class, name = "S5_a file removed between the listing and the download answers null, not an error")
@SelectMethod(type = StartupAgainstServerTest::class, name = "S6_a move target on another filesystem stops the connector from starting")
@SelectMethod(type = SourceAgainstServerTest::class, name = "S7_an ack without a download runs the move and transfers nothing")
@SelectMethod(type = SftpWatchTest::class, name = "S8_under SKIP a tick that comes round while the last is still running is skipped, and no second listing is sent")
@SelectMethod(type = ShutdownAgainstServerTest::class, name = "S9_closing during a download leaves no partial file, releases the lease, and returns within the bound")
@SelectMethod(type = ResilienceAgainstServerTest::class, name = "S10_a wrong password is refused once, never retried, and never held against the server")
@SelectMethod(type = ReadPathAgainstServerTest::class, name = "S11_a hundred thousand entries with a limit of a thousand stops after a thousand")
@SelectMethod(type = SftpWatchTest::class, name = "S12_a file listed again by a tick running alongside is handed over once")
class AcceptanceScenarios
