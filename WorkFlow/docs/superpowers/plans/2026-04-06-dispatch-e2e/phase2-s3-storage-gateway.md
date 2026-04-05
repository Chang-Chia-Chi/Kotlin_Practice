# Phase 2: S3StorageGateway Implementation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `S3StorageGateway` (the production `StorageGateway` adapter) using the Kotlin AWS SDK, with CDI wiring via `DispatchProducers`.

**Architecture:** Uses `aws.sdk.kotlin:s3` (already in POM). The `S3Client` is produced by CDI and configured via `storage.*` properties in `application.properties`. The gateway delegates to `putObject` for both CSV and Parquet uploads.

**Tech Stack:** Kotlin AWS SDK (`aws.sdk.kotlin:s3` v1.3.31), Quarkus CDI

---

### Task 1: Create the S3Client CDI Producer

**Files:**
- Modify: `src/main/kotlin/dispatch/adapter/DispatchProducers.kt`

- [ ] **Step 1: Add S3Client producer to DispatchProducers**

Add the following imports and method to the existing `DispatchProducers` class:

```kotlin
import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.net.url.Url
```

Add this method inside the class body:

```kotlin
    @Produces
    @ApplicationScoped
    fun s3Client(
        @ConfigProperty(name = "storage.endpoint") endpoint: String,
        @ConfigProperty(name = "storage.region") region: String,
        @ConfigProperty(name = "storage.access-key") accessKey: String,
        @ConfigProperty(name = "storage.secret-key") secretKey: String,
    ): S3Client = S3Client {
        this.region = region
        endpointUrl = Url.parse(endpoint)
        credentialsProvider = aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider(
            aws.sdk.kotlin.runtime.auth.credentials.Credentials(accessKey, secretKey)
        )
        forcePathStyle = true
    }
```

- [ ] **Step 2: Verify the project compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/DispatchProducers.kt
git commit -m "feat: add S3Client CDI producer in DispatchProducers"
```

---

### Task 2: Implement S3StorageGateway

**Files:**
- Create: `src/main/kotlin/dispatch/adapter/storage/S3StorageGateway.kt`

- [ ] **Step 1: Create the S3StorageGateway implementation**

```kotlin
package com.workflow.dispatch.adapter.storage

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.asByteStream
import com.workflow.dispatch.usecase.port.outbound.storage.StorageGateway
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.io.File

@ApplicationScoped
class S3StorageGateway(
    private val s3Client: S3Client,
    @ConfigProperty(name = "storage.bucket") private val bucket: String,
) : StorageGateway {

    private val log = LoggerFactory.getLogger(S3StorageGateway::class.java)

    override suspend fun uploadCsv(path: String, file: File) {
        s3Client.putObject(
            PutObjectRequest {
                this.bucket = this@S3StorageGateway.bucket
                key = path
                contentType = "application/gzip"
            },
        ) {
            body = file.asByteStream()
        }
        log.debug("Uploaded CSV to s3://{}/{} ({} bytes)", bucket, path, file.length())
    }

    override suspend fun uploadParquet(path: String, content: ByteArray) {
        s3Client.putObject(
            PutObjectRequest {
                this.bucket = this@S3StorageGateway.bucket
                key = path
                contentType = "application/octet-stream"
            },
        ) {
            body = ByteStream.fromBytes(content)
        }
        log.debug("Uploaded Parquet to s3://{}/{} ({} bytes)", bucket, path, content.size)
    }
}
```

- [ ] **Step 2: Verify the project compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn compile -pl WorkFlow`
Expected: `BUILD SUCCESS`

- [ ] **Step 3: Run the full test suite to verify no regressions**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test -pl WorkFlow`
Expected: All tests PASS. The new `S3StorageGateway` should be picked up by CDI as the `StorageGateway` bean (there was no prior implementation).

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/dispatch/adapter/storage/S3StorageGateway.kt
git commit -m "feat: implement S3StorageGateway for MinIO/S3 uploads"
```
