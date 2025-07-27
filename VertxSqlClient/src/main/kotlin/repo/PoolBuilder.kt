package repo

import io.vertx.mutiny.core.Vertx
import io.vertx.oracleclient.OracleBuilder
import io.vertx.oracleclient.OracleConnectOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.context.Dependent

@Dependent
class PoolBuilder(
    private val vertx: Vertx,
    private val config: DbConfig,
) {
    @ApplicationScoped
    fun createOraclePool(): Pool {
        val connectOptions =
            OracleConnectOptions()
                .setPort(config.port)
                .setHost(config.host)
                .setDatabase(config.schema)
                .setUser(config.username)
                .setPassword(config.password)

        val poolOptions: PoolOptions = PoolOptions().setMaxSize(config.maxSize)

        return OracleBuilder
            .pool()
            .with(poolOptions)
            .connectingTo(connectOptions)
            .using(vertx.delegate)
            .build()
    }
}
