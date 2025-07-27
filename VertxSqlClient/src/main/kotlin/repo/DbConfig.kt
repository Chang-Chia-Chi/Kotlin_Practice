package repo

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithName

@ConfigMapping(prefix = "datasource")
interface DbConfig {
    @get:WithName("host")
    val host: String

    @get:WithName("port")
    val port: Int

    @get:WithName("schema")
    val schema: String

    @get:WithName("username")
    val username: String

    @get:WithName("password")
    val password: String

    @get:WithName("maxSize")
    val maxSize: Int
}
