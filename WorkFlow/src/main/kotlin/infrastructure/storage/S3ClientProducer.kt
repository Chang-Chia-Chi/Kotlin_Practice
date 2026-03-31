package com.workflow.infrastructure.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.net.URI

@ApplicationScoped
class S3ClientProducer {

    @Produces
    @ApplicationScoped
    fun s3AsyncClient(
        @ConfigProperty(name = "storage.endpoint") endpoint: String,
        @ConfigProperty(name = "storage.region") region: String,
        @ConfigProperty(name = "storage.access-key") accessKey: String,
        @ConfigProperty(name = "storage.secret-key") secretKey: String,
    ): S3AsyncClient = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
        )
        .forcePathStyle(true)
        .build()
}
