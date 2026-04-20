package com.workflow.infrastructure.storage

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager

class MinioTestResource : QuarkusTestResourceLifecycleManager {
    override fun start(): Map<String, String> {
        MinioTestContainer.s3Client  // triggers container start + bucket creation
        return mapOf("storage.endpoint" to MinioTestContainer.endpoint)
    }

    override fun stop() {}
}
