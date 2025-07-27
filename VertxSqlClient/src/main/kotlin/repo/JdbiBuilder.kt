package repo

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.context.Dependent
import jakarta.inject.Named
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.sqlobject.kotlin.KotlinSqlObjectPlugin

@Dependent
class JdbiBuilder {
    @Named("duckdb")
    @ApplicationScoped
    fun createDuckDbJdbi(): Jdbi {
        val jdbi = Jdbi.create(SharedDuckDbDataSource.of())
        jdbi.installPlugin(KotlinPlugin())
        jdbi.installPlugin(KotlinSqlObjectPlugin())
        return jdbi
    }
}
