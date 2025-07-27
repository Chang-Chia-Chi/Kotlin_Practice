package repo

import org.duckdb.DuckDBConnection
import java.io.PrintWriter
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import java.util.logging.Logger
import javax.sql.DataSource

class SharedDuckDbDataSource(
    private val url: String = "jdbc:duckdb:",
    private val properties: Properties = Properties(),
) : DataSource {
    companion object {
        fun of(
            url: String,
            properties: Properties = Properties(),
        ): SharedDuckDbDataSource = SharedDuckDbDataSource(url, properties)
    }

    private val baseConnection: DuckDBConnection by lazy {
        DriverManager.getConnection(url, properties) as DuckDBConnection
    }

    override fun getLogWriter(): PrintWriter = DriverManager.getLogWriter()

    override fun setLogWriter(out: PrintWriter?) = DriverManager.setLogWriter(out)

    override fun setLoginTimeout(seconds: Int) = DriverManager.setLoginTimeout(seconds)

    override fun getLoginTimeout(): Int = DriverManager.getLoginTimeout()

    override fun getParentLogger(): Logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

    override fun <T> unwrap(iface: Class<T>?): T = throw SQLException("Not a wrapper for $iface")

    override fun isWrapperFor(iface: Class<*>?): Boolean = false

    override fun getConnection(): Connection = baseConnection.duplicate()

    override fun getConnection(
        username: String?,
        password: String?,
    ): Connection = getConnection()
}
