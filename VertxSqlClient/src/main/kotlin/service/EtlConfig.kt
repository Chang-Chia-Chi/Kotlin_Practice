package service

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault

@ConfigMapping(prefix = "etl")
interface EtlConfig {
    @WithDefault("etl-test")
    fun s3BucketName(): String

    @WithDefault("tmp")
    fun s3Prefix(): String
}
