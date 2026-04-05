# Phase 4: Test Infrastructure (MinIO Container + Fixture Data)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create shared MinIO test container singleton and representative fixture data for the E2E integration tests.

**Architecture:** `MinioTestContainer` follows the same singleton object pattern as `OracleTestContainer`. Provides a pre-configured `S3Client` and a pre-created test bucket. Fixture data is a JSON file loaded by tests.

**Tech Stack:** Testcontainers (GenericContainer), MinIO, AWS Kotlin SDK S3, Jackson

---

### Task 1: Add MinIO Testcontainer (if not already available)

The existing `org.testcontainers:testcontainers` dependency (already in POM) covers `GenericContainer`. No new dependency needed.

**Files:**
- Verify: `pom.xml` has `org.testcontainers:testcontainers` in test scope

- [ ] **Step 1: Verify testcontainers dependency exists**

Run: `grep -A2 "testcontainers</artifactId>" pom.xml`
Expected: At least one `testcontainers` entry with `<scope>test</scope>`.

- [ ] **Step 2: Commit (skip if no changes needed)**

No commit needed if dependency already exists.

---

### Task 2: Create MinioTestContainer Singleton

**Files:**
- Create: `src/test/kotlin/infrastructure/storage/MinioTestContainer.kt`

- [ ] **Step 1: Create the MinioTestContainer object**

```kotlin
package com.workflow.infrastructure.storage

import aws.sdk.kotlin.runtime.auth.credentials.Credentials
import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

object MinioTestContainer {

    private const val ACCESS_KEY = "minioadmin"
    private const val SECRET_KEY = "minioadmin"
    const val BUCKET = "dispatch-test"

    private val container = GenericContainer("minio/minio:latest")
        .withCommand("server /data")
        .withExposedPorts(9000)
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .waitingFor(
            HttpWaitStrategy()
                .forPort(9000)
                .forPath("/minio/health/ready"),
        )
        .apply { start() }

    val endpoint: String get() = "http://${container.host}:${container.getMappedPort(9000)}"

    val s3Client: S3Client by lazy {
        val client = S3Client {
            region = "us-east-1"
            endpointUrl = Url.parse(endpoint)
            credentialsProvider = StaticCredentialsProvider(
                Credentials(ACCESS_KEY, SECRET_KEY),
            )
            forcePathStyle = true
        }
        runBlocking {
            client.createBucket(CreateBucketRequest { bucket = BUCKET })
        }
        client
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add src/test/kotlin/infrastructure/storage/MinioTestContainer.kt
git commit -m "test: add MinioTestContainer singleton for S3 integration tests"
```

---

### Task 3: Create Fixture Data File

**Files:**
- Create: `src/test/resources/fixtures/dispatch-e2e-fixture.json`

The fixture contains 3 configs: Config A (QTY mode, 3 sites, BOM mappings for fan-out),
Config B (RATIO mode, 2 sites, no BOM mappings), and Config C (QTY mode, 2 sites, minimal).

- [ ] **Step 1: Create the fixture JSON**

```json
{
  "configs": [
    {
      "id": "CFG-A",
      "mode": "QTY",
      "algorithmId": "default",
      "sourceBomPrefix": "BOM-A",
      "siteTargets": [
        { "siteId": "SITE-1", "target": 100 },
        { "siteId": "SITE-2", "target": 80 },
        { "siteId": "SITE-3", "target": 60 }
      ],
      "bomMappings": {
        "SITE-1": {
          "sourceBomId": "BOM-A-001",
          "targetAllocations": [
            { "targetBomId": "TBOM-X", "target": 50 },
            { "targetBomId": "TBOM-Y", "target": 50 }
          ]
        },
        "SITE-2": {
          "sourceBomId": "BOM-A-002",
          "targetAllocations": [
            { "targetBomId": "TBOM-X", "target": 80 }
          ]
        }
      }
    },
    {
      "id": "CFG-B",
      "mode": "RATIO",
      "algorithmId": "default",
      "sourceBomPrefix": "BOM-B",
      "siteTargets": [
        { "siteId": "SITE-1", "target": 60 },
        { "siteId": "SITE-2", "target": 40 }
      ],
      "bomMappings": null
    },
    {
      "id": "CFG-C",
      "mode": "QTY",
      "algorithmId": "default",
      "sourceBomPrefix": "BOM-C",
      "siteTargets": [
        { "siteId": "SITE-1", "target": 50 },
        { "siteId": "SITE-3", "target": 50 }
      ],
      "bomMappings": null
    }
  ],
  "candidates": {
    "CFG-A": [
      { "productId": "PROD-A1", "sourceBomId": "BOM-A-001", "qty": 5 },
      { "productId": "PROD-A2", "sourceBomId": "BOM-A-001", "qty": 3 },
      { "productId": "PROD-A3", "sourceBomId": "BOM-A-002", "qty": 8 },
      { "productId": "PROD-A4", "sourceBomId": "BOM-A-001", "qty": 2 },
      { "productId": "PROD-A5", "sourceBomId": "BOM-A-002", "qty": 10 },
      { "productId": "PROD-A6", "sourceBomId": "BOM-A-001", "qty": 4 },
      { "productId": "PROD-A7", "sourceBomId": "BOM-A-002", "qty": 6 }
    ],
    "CFG-B": [
      { "productId": "PROD-B1", "sourceBomId": "BOM-B-001", "qty": 4 },
      { "productId": "PROD-B2", "sourceBomId": "BOM-B-001", "qty": 7 },
      { "productId": "PROD-B3", "sourceBomId": "BOM-B-001", "qty": 2 },
      { "productId": "PROD-B4", "sourceBomId": "BOM-B-001", "qty": 5 },
      { "productId": "PROD-B5", "sourceBomId": "BOM-B-001", "qty": 3 }
    ],
    "CFG-C": [
      { "productId": "PROD-C1", "sourceBomId": "BOM-C-001", "qty": 6 },
      { "productId": "PROD-C2", "sourceBomId": "BOM-C-001", "qty": 4 },
      { "productId": "PROD-C3", "sourceBomId": "BOM-C-001", "qty": 8 },
      { "productId": "PROD-C4", "sourceBomId": "BOM-C-001", "qty": 3 },
      { "productId": "PROD-C5", "sourceBomId": "BOM-C-001", "qty": 5 }
    ]
  },
  "baselines": {
    "CFG-A": {
      "siteAllocations": { "SITE-1": 20, "SITE-2": 15, "SITE-3": 10 },
      "bomAllocations": {
        "SITE-1:TBOM-X": 10,
        "SITE-1:TBOM-Y": 10,
        "SITE-2:TBOM-X": 15
      }
    },
    "CFG-B": {
      "siteAllocations": { "SITE-1": 30, "SITE-2": 20 },
      "bomAllocations": {}
    },
    "CFG-C": {
      "siteAllocations": { "SITE-1": 10, "SITE-3": 10 },
      "bomAllocations": {}
    }
  }
}
```

- [ ] **Step 2: Create a fixture loader utility for tests**

Create `src/test/kotlin/dispatch/DispatchE2EFixture.kt`:

```kotlin
package com.workflow.dispatch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.Baseline
import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import java.math.BigDecimal

object DispatchE2EFixture {

    private val mapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val root: JsonNode by lazy {
        val stream = DispatchE2EFixture::class.java.classLoader
            .getResourceAsStream("fixtures/dispatch-e2e-fixture.json")!!
        mapper.readTree(stream)
    }

    fun configs(): List<DispatchConfig> = root["configs"].map { node ->
        DispatchConfig(
            id = node["id"].asText(),
            mode = DispatchMode.valueOf(node["mode"].asText()),
            algorithmId = node["algorithmId"].asText(),
            sourceBomPrefix = node["sourceBomPrefix"].asText(),
            siteTargets = node["siteTargets"].map { st ->
                SiteTarget(st["siteId"].asText(), BigDecimal(st["target"].asText()))
            },
            bomMappings = node["bomMappings"]?.takeIf { !it.isNull }?.let { bm ->
                bm.fields().asSequence().associate { (siteId, mapping) ->
                    siteId to BomMapping(
                        sourceBomId = mapping["sourceBomId"].asText(),
                        targetAllocations = mapping["targetAllocations"].map { ta ->
                            TargetBomAllocation(ta["targetBomId"].asText(), BigDecimal(ta["target"].asText()))
                        },
                    )
                }
            },
        )
    }

    fun candidates(configId: String): List<CandidateProduct> =
        root["candidates"][configId].map { node ->
            CandidateProduct(
                productId = node["productId"].asText(),
                sourceBomId = node["sourceBomId"].asText(),
                qty = node["qty"].asInt(),
            )
        }

    fun baseline(configId: String): Baseline {
        val bl = root["baselines"][configId]
        val siteAlloc = bl["siteAllocations"].fields().asSequence()
            .associate { (k, v) -> k to BigDecimal(v.asText()) }
        val bomAlloc = bl["bomAllocations"].fields().asSequence()
            .associate { (k, v) ->
                val (siteId, bomId) = k.split(":")
                SiteBomKey(siteId, bomId) to BigDecimal(v.asText())
            }
        return Baseline(siteAlloc, bomAlloc)
    }

    fun configIds(): List<String> = configs().map { it.id }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn test-compile -pl WorkFlow`
Expected: `BUILD SUCCESS`

- [ ] **Step 4: Commit**

```bash
git add src/test/resources/fixtures/dispatch-e2e-fixture.json
git add src/test/kotlin/dispatch/DispatchE2EFixture.kt
git commit -m "test: add E2E fixture data and fixture loader for dispatch integration tests"
```
