package com.example.gauntlet.infrastructure

import com.example.gauntlet.domain.AnalyticsRepository
import com.example.gauntlet.domain.OrderRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.nio.file.Files
import java.nio.file.Path

/**
 * 唯一知道「用什麼資料庫」的地方。domain 與 application 都只認介面。
 */
@ApplicationScoped
class PersistenceProducer(
    @ConfigProperty(name = "gauntlet.sqlite.path", defaultValue = "data/gauntlet.db")
    private val sqlitePath: String,
    @ConfigProperty(name = "gauntlet.duckdb.url", defaultValue = "jdbc:duckdb:")
    private val duckdbUrl: String,
    @ConfigProperty(name = "gauntlet.duckdb.memory-limit", defaultValue = "1GB")
    private val duckdbMemoryLimit: String,
    @ConfigProperty(name = "gauntlet.duckdb.threads", defaultValue = "2")
    private val duckdbThreads: Int,
) {

    @Produces
    @Singleton
    fun orderRepository(): OrderRepository {
        val file = Path.of(sqlitePath)
        file.parent?.let { Files.createDirectories(it) }
        val jdbi = SqliteJdbiProvider.create(file)
        SqliteJdbiProvider.migrate(jdbi)
        return SqliteOrderRepository(jdbi)
    }

    @Produces
    @Singleton
    fun duckDbProvider(): DuckDbJdbiProvider =
        DuckDbJdbiProvider.create(duckdbUrl, duckdbMemoryLimit, duckdbThreads)
            .also { it.migrate() }

    @Produces
    @Singleton
    fun analyticsRepository(provider: DuckDbJdbiProvider): AnalyticsRepository =
        DuckDbAnalyticsRepository(provider.jdbi)

    fun closeDuckDb(@Disposes provider: DuckDbJdbiProvider) {
        provider.close()
    }
}
