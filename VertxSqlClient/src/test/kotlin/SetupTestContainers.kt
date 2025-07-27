import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import org.testcontainers.containers.OracleContainer

class SetupTestContainers : QuarkusTestResourceLifecycleManager {
    private val oracle: OracleContainer =
        OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
            .withUsername("test")
            .withPassword("test")
            .withExposedPorts(1521)

    override fun start(): MutableMap<String, String> {
        oracle.start()
        oracle.createConnection("").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE SALE_INFO (PRODUCT_ID INTEGER PRIMARY KEY, SALE_UNIT INTEGER)")
            }
            conn.prepareStatement("INSERT INTO SALE_INFO (PRODUCT_ID, SALE_UNIT) VALUES (?, ?)").use { ps ->
                for (i in 1..100000) {
                    ps.setInt(1, i)
                    ps.setLong(2, i.toLong())
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
        return mutableMapOf(
            "datasource.host" to oracle.host,
            "datasource.port" to oracle.oraclePort.toString(),
            "datasource.schema" to oracle.databaseName,
            "datasource.username" to oracle.username,
            "datasource.password" to oracle.password,
        )
    }

    override fun stop() {
        oracle.stop()
    }
}
