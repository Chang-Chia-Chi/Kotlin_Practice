# SFTP Ingest - Design Spec

Version: v0.1 (agreed in design review, not yet implemented)
Scope: the first application built on the SFTP connector; one process, one replica, hourly
Status: ready for a phase plan
Depends on: `docs/sftpconnector/spec.md` v0.1 (the connector), which is itself unimplemented

---

## 1. Background and Goals

### 1.1 Problem

An upstream party drops files into one directory on an SFTP server. Every hour this service
must take every complete file from that directory, put it into a MinIO bucket, tell downstream
systems where it landed, and move the file into a temp folder on the same server that downstream
purges on its own schedule. A file must never be lost, never be reported before it is safely in
the bucket, and never block the rest of the batch when it is broken. Files are small, about 450
an hour today and under 10 MB each, but the design must not need rewriting at ten times that.

The connector spec owns everything below the file event: sessions, pool, listing, readiness,
download staging, ack and nack, retry against the server. This spec owns everything above it:
what to do with a file once it is seen, how to remember what was done, how to survive a crash at
every step, and how to tell downstream.

### 1.2 Goals

- **At-least-once, converging to exactly-once in effect.** Every file reaches the bucket and
  every configured channel hears about it. A crash at any point is redone from a durable ledger
  with at most one extra upload and one extra notification, never a lost one.
- **A general-purpose ledger.** The tables record file transfers and deliveries for any source,
  target and channel, so the next application with the same shape reuses them unchanged.
- **Channels as a seam.** One HTTP channel today; a second channel is a configuration entry
  and one adapter class, never a change to the pipeline.
- **Data quality as a seam.** A check runs on every complete file before upload; today it
  passes everything.
- **Bounded, observable, safe under shutdown.** Every stage has a timeout below the shutdown
  drain, every queue is bounded, and a pod restart at any moment is survivable.
- **Source and target as vocabulary, not as a framework.** A route moves a file from a
  source to a target and tells channels. The DSL says exactly that; the pipeline sees a source
  only as events and a download function, and a target only as "store one copy at this key,
  verify it, probe it". What kind of place either one is stays inside its adapter (D21).
- **Framework-free core.** The pipeline, ledger contract, target contract, channel contract
  and relay know nothing of Quarkus, JDBI, the AWS SDK or HTTP; each of those is an adapter
  package.

### 1.3 Non-goals

- No exactly-once notification. Downstream deduplicates on the file id in the body (Sec 7.6).
- No multi-replica coordination. One process per route (D13); the seam is named in Sec 14.2.
- No streaming or resume. Files stage on local disk through the connector (its D11).
- No per-attempt delivery history table. Attempts are traced through logs (D3).
- No parsing of file content for the notification body. Every body field derives from the
  file's metadata and the upload result (Sec 16, item 3).
- No creation of the bucket or the tables. Both are provisioned ahead of the process (D15).

---

## 2. Terminology

| Term | Definition |
|---|---|
| Route | One source, one target and a list of channels. Today the source is one watched directory on one connector and the target is one S3 bucket. |
| Source | Where files come from. The pipeline sees it as a flow of `IngestEvent` plus a `Downloader`; the SFTP connector is the one implementation, the test kit's scripted source the second. |
| Target | Where copies go. The pipeline sees it as `store`, `verify` and `probe`; the S3 adapter is the one implementation, the test kit's in-memory target the second. |
| File identity | Source system, directory, name, size and mtime together. Two listings with the same five values are the same file (D2). |
| Transfer | The ledger's record of one file identity moving through the pipeline. One row in `file_transfer`. |
| Delivery | The ledger's record of one transfer being announced on one channel. One row in `delivery_outbox`. |
| Stage | One step of the per-file pipeline: download, quality, store, ack. |
| Ack | The connector's ack, which runs the configured move into the temp folder. Always the pipeline's call, after the store. |
| Channel | An adapter that announces a transfer to one downstream party and reports delivered, retry or reject. |
| Relay | The coroutine that turns PENDING deliveries into channel calls. |
| Reconciliation | The end-of-poll pass that repairs a transfer whose file vanished from the source between the move and the ledger write. |
| Re-drive | An operator action that puts a REJECTED or FAILED transfer, or a FAILED delivery, back into play. |

---

## 3. Overall Model

### 3.1 Layers

```
 connector   watch(dir, every = 1h): Flow<SftpEvent>      (docs/sftpconnector/spec.md)
   │  FileSeen(file, ack, nack)
   ▼
 consumer    one collector per route; decides from the ledger what each FileSeen needs;
   │         launches bounded per-file pipelines; reconciles at PollCompleted
   ▼
 pipeline    download -> quality -> target.store (S3: PUT, HEAD, prune) -> ledger UPLOADED
   │         -> ack (move) -> ledger ACKED + outbox rows           (one file, one coroutine)
   ▼
 ledger      file_transfer, delivery_outbox                        (Oracle through JDBI)
   │
 relay       cold Flow over PENDING deliveries -> channel.deliver -> ledger DELIVERED / retry
   │
 channels    HttpChannel (JDK HttpClient), fakes in tests; more later
```

Three pieces of state exist and each has one owner. The connector holds its in-flight set of
files. The ledger holds every transfer and delivery, durably. The relay holds the ids of the
deliveries currently inside a worker (Sec 7.4). Nothing else remembers anything.

### 3.2 Module and packages

One Maven module, `sftp-ingest`, a Quarkus application modelled on `etl-host`. Package
boundaries are dependency sentences enforced by ArchUnit (D18):

| Package | Holds | May import |
|---|---|---|
| `infra.sftpingest.pipeline` | consumer, per-file pipeline, states, `IngestEvent`, `Downloader`, `Ledger`, `Target`, `DeliveryChannel`, `QualityCheck`, relay, config DSL, metrics names | kotlin-stdlib, coroutines, micrometer-core, jboss-logging |
| `infra.sftpingest.sftp` | the binding from the connector's `SftpEvent` flow and `download` to `IngestEvent` and the `Downloader` function | `pipeline`, the connector core |
| `infra.sftpingest.jdbi` | Oracle ledger | `pipeline`, JDBI |
| `infra.sftpingest.s3` | the S3 target over the AWS SDK, including the version prune | `pipeline`, AWS SDK v2 |
| `infra.sftpingest.http` | HTTP channel | `pipeline`, `java.net.http`, Jackson |
| `infra.sftpingest.quarkus` | CDI producers, property mapping, readiness, admin resource, shutdown | everything above, Quarkus, the connector's Quarkus adapter |

Rules: nothing in `pipeline` imports an adapter; each adapter imports only `pipeline` and its
own technology; only `quarkus` imports Quarkus; only `sftp` and `quarkus` import the connector.
The pipeline consumes the source through two small types it owns, a sealed `IngestEvent`
(`Seen(file, ack, nack)`, `PollCompleted(listed, truncated)`, `PollFailed`, `PollSkipped`,
`RouteDown(error)`) and a `Downloader` function from a file identity and a staging directory to
a `LocalFile` with its digest. The `sftp` package maps the connector's events onto them (D20). Every seam in `pipeline` has a second
implementation in the test tree (in-memory ledger, in-memory target, recording channel),
which is what keeps the pipeline suite free of containers and sub-second.

Logging is `org.jboss.logging.Logger` in every package, for the reasons the sibling frameworks
record. Time is an injected `java.time.Clock`.

### 3.3 Thread model

Everything above the adapters is `suspend`. Blocking calls run on a bounded view of
`Dispatchers.IO` owned by this module, sized to the route's parallelism: JDBI statements, the
synchronous S3 client, and `HttpClient.send`. The connector owns its own bounded dispatcher for
JSch. Per-file pipelines run inside one `SupervisorJob` scope per route, so one file's failure
never cancels its siblings, and the relay runs inside one scope per process. Shutdown cancels
those scopes in the order of Sec 11.2.

---

## 4. Pipeline

### 4.1 Per-file stages

One coroutine per `FileSeen`, at most `parallelism` at once per route (default 4, which is the
connector's `maxConcurrentTransfers` under the five-session cap, D21 of its spec).

| # | Stage | Does | Ledger after |
|---|---|---|---|
| 0 | Decide | Look up the transfer by identity; choose an entry point (Sec 4.3) | SEEN (row created or reused) |
| 1 | Download | `connector.download(file, staging)`; the connector verifies size and returns the digest | DOWNLOADED (digest recorded) |
| 2 | Quality | `QualityCheck.check(localFile, meta)`; Pass continues, Fail stops (Sec 8) | REJECTED on Fail |
| 3 | Store | `target.store(key, file, metadata)`; the adapter guarantees that afterwards exactly one copy exists at the key and returns a reference to it (Sec 6.1) | UPLOADED (key, target ref) |
| 4 | Ack | `ack()`: the connector moves the file into the temp folder | ACKED, plus one PENDING delivery per channel, in one transaction |
| 5 | Deliver | Owned by the relay, not this coroutine (Sec 7) | DONE when every delivery is DELIVERED |

Local staging is deleted after stage 3 succeeds and on every failure path. What "exactly one
copy" costs is the adapter's business: the S3 adapter does a PUT, a HEAD that checks the content
length because the SDK's own checksums are off (D4), and a prune of every other version of the
key (Sec 6.3). The pipeline never learns that versions exist.

### 4.2 Transfer states

```
            ┌────────────────────────── re-drive ──────────────────────────┐
            ▼                                                              │
 (none) → SEEN → DOWNLOADED → UPLOADED → ACKED → DONE                 REJECTED
                     │            │                                        │
                     └─ Fail ─────┼──────────────────────────────────→ REJECTED
                                  │
   any stage error, attempts < max ─→ stays, nack(redeliver = true), next poll retries
   any stage error, attempts = max ─→ FAILED, nack(redeliver = false)
```

- `attempts` counts pipeline attempts that ended in an error. Default `maxAttempts = 5`.
- REJECTED and FAILED are terminal until an operator re-drives (Sec 12.3). The connector is
  told `nack(reason, redeliver = false)` so it stops emitting the file until restart; after a
  restart the ledger answers for it (Sec 4.3).
- DONE is terminal. A DONE row is never touched again except by retention.

### 4.3 Entry points, decided from the ledger

The connector emits a `FileSeen` for every file in the directory that passes readiness, every
poll, until the file is moved. The consumer therefore sees the same file again after any
failure or crash, and the ledger decides how much work is left:

| Ledger state | Action |
|---|---|
| none, SEEN, DOWNLOADED | Full pipeline from stage 1. A staged file from an earlier process is not trusted (D17). |
| UPLOADED | `target.verify(ref)` on the recorded reference. True: skip to stage 4. False: full pipeline from stage 1 on the same row. |
| ACKED, DONE | The file is back in the inbox although it was moved. Same rule as UPLOADED: verify, then ack again. Logged at WARN and counted, because it means someone put a file back. |
| REJECTED, FAILED | `nack(redeliver = false)`, no work, no log beyond DEBUG. |

The connector's spec permits ack without a preceding download for exactly the UPLOADED case.

### 4.4 Crash matrix

Every row of this table is a scenario in Sec 17.2, driven by a hook point named in the
`Hook` interface (`afterDownload`, `afterQuality`, `afterStore`, `afterLedgerUploaded`,
`afterMove`, `afterLedgerAcked`, `afterDeliverySent`). A crash inside `store` itself, for S3
between the PUT and the prune, is the adapter's contract to survive and is replayed in the
adapter's own test tier (Sec 6.3, I6).

| Crash after | Source | Bucket | Ledger | Next poll does | Extra effects |
|---|---|---|---|---|---|
| download | file in inbox | nothing | SEEN or DOWNLOADED | full pipeline | none |
| store, before ledger | in inbox | 1 copy | DOWNLOADED | full pipeline: store again | one extra upload |
| ledger UPLOADED | in inbox | 1 copy | UPLOADED | verify true, ack | none |
| move, before ledger | in temp | 1 copy | UPLOADED | reconciliation marks ACKED and creates deliveries (Sec 4.5) | delivery delayed to the next poll |
| ledger ACKED | in temp | 1 copy | ACKED, PENDING | relay delivers | none |
| delivery sent, before ledger | in temp | 1 copy | ACKED, PENDING | relay delivers again | one duplicate notification, deduplicated downstream |

The invariant the table proves: **at any crash point, at most one extra upload and at most one
extra delivery per channel, and never a lost one** (I8).

### 4.5 Reconciliation

At `PollCompleted` for a route, when the listing was complete, meaning it ended before
`maxFilesPerPoll`: every transfer of that route in UPLOADED whose `updated_at` is older than the
poll's start and whose identity was not listed is transitioned to ACKED with its deliveries
created, through the same function stage 4 uses. The source no longer has the file and the
ledger already proves the object exists, so the only missing fact is the ack, and the move is
the ack. A truncated listing proves nothing about absence, so reconciliation skips that poll and
counts it.

---

## 5. Ledger

### 5.1 Tables

DDL is applied by the DBA (D15); `LedgerSchema.DDL` in the code is the reference text, the way
the archive layer's manifest is. Oracle types shown; the JDBI adapter is the only SQL in the
module.

```sql
CREATE TABLE file_transfer (
  id                NUMBER(19)     NOT NULL,           -- sequence
  source_system     VARCHAR2(64)   NOT NULL,           -- route name
  source_dir        VARCHAR2(1024) NOT NULL,
  file_name         VARCHAR2(512)  NOT NULL,
  file_size         NUMBER(19)     NOT NULL,
  file_mtime        TIMESTAMP      NOT NULL,
  digest            VARCHAR2(128),                     -- hex
  digest_algo       VARCHAR2(16),                      -- SHA-256 | MD5
  state             VARCHAR2(16)   NOT NULL,           -- SEEN..DONE, REJECTED, FAILED
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  last_error        VARCHAR2(2000),
  target_kind       VARCHAR2(16),                      -- S3
  target_bucket     VARCHAR2(255),
  target_key        VARCHAR2(1024),
  target_ref        VARCHAR2(512),                     -- adapter-defined; S3: the version id
  first_seen_at     TIMESTAMP      NOT NULL,
  updated_at        TIMESTAMP      NOT NULL,
  completed_at      TIMESTAMP,
  CONSTRAINT pk_file_transfer PRIMARY KEY (id),
  CONSTRAINT uq_file_transfer_identity
    UNIQUE (source_system, source_dir, file_name, file_size, file_mtime)
);
CREATE INDEX ix_file_transfer_state ON file_transfer (source_system, state, updated_at);

CREATE TABLE delivery_outbox (
  id                NUMBER(19)     NOT NULL,           -- sequence
  file_transfer_id  NUMBER(19)     NOT NULL,
  channel           VARCHAR2(64)   NOT NULL,
  state             VARCHAR2(16)   NOT NULL,           -- PENDING, DELIVERED, FAILED
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  next_attempt_at   TIMESTAMP      NOT NULL,
  last_status       VARCHAR2(64),                      -- e.g. HTTP 503
  last_error        VARCHAR2(2000),
  reference         VARCHAR2(255),                     -- downstream's id for the delivered call
  created_at        TIMESTAMP      NOT NULL,
  delivered_at      TIMESTAMP,
  CONSTRAINT pk_delivery_outbox PRIMARY KEY (id),
  CONSTRAINT fk_delivery_transfer FOREIGN KEY (file_transfer_id) REFERENCES file_transfer (id),
  CONSTRAINT uq_delivery_channel UNIQUE (file_transfer_id, channel)
);
CREATE INDEX ix_delivery_due ON delivery_outbox (state, next_attempt_at);
```

Two tables because one transfer has many deliveries and each delivery has its own attempts,
retry time and reference; one row per file with a column set per channel cannot survive a
second channel (D3). There is no payload column: the body is rendered at send time from the
transfer row (D19). There is no attempt table: every attempt logs its file id, channel, attempt
number, status and reference at INFO, which is the trace (D3).

### 5.2 The `Ledger` seam

```kotlin
interface Ledger {
    suspend fun find(identity: FileIdentity): Transfer?
    suspend fun seen(identity: FileIdentity): Transfer                     // insert or reuse
    suspend fun downloaded(id: TransferId, digest: Digest)
    suspend fun uploaded(id: TransferId, target: TargetRef)
    suspend fun acked(id: TransferId, channels: List<ChannelName>)        // ACKED + PENDING rows, one txn
    suspend fun rejected(id: TransferId, reason: String)
    suspend fun failedAttempt(id: TransferId, error: String, maxAttempts: Int): Transfer  // increments; may flip to FAILED
    suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<FileIdentity>): List<TransferId>
    suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int): List<Delivery>
    suspend fun delivered(id: DeliveryId, reference: String?)             // may flip transfer to DONE
    suspend fun retryLater(id: DeliveryId, at: Instant, status: String?, error: String)
    suspend fun deliveryFailed(id: DeliveryId, status: String?, error: String)
    suspend fun redrive(id: TransferId)                                   // REJECTED/FAILED -> SEEN, attempts = 0
    suspend fun redriveDelivery(id: DeliveryId)                           // FAILED -> PENDING, attempts = 0
}
```

Every method is one transaction. `acked` and `delivered` are the two that touch both tables and
must be atomic (I11). The in-memory implementation in the test tree is the second adapter.

---

## 6. Target

### 6.1 The `Target` seam

```kotlin
interface Target {
    suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef
    //  contract: afterwards exactly one copy exists at key, and it is the one just written
    suspend fun verify(ref: TargetRef): Boolean          // the copy the ref names exists with the recorded size
    suspend fun probe()                                  // fails startup when the target is unreachable or missing
}

data class TargetRef(val kind: String, val bucket: String, val key: String, val ref: String?, val size: Long)
```

Three methods because the pipeline needs exactly three facts: the copy is there, it is still
there, the place exists. How an adapter keeps the "exactly one copy" promise is its own
business: S3 with versioning prunes, an SFTP target would upload under a temporary name and
rename over, a local directory would write and move. The test kit's in-memory target is the
second implementation (D21).

### 6.2 S3 client

AWS SDK for Java v2, synchronous `S3Client` over the Apache HTTP client (D4). Configuration
that is not optional against MinIO: endpoint override, path-style access, a fixed placeholder
region, static credentials from the environment (D14). Request and response checksum
calculation set to when-required, so no CRC32 reaches a server that may not accept it; the
MinIO version is an open item (Sec 16). Timeouts: connect, socket and API call, with the API
call timeout required to be below the shutdown drain timeout (Sec 11.2), because a socket read
cannot be interrupted and a stage parked inside the SDK drains when that timeout fires.

### 6.3 The S3 target: key, metadata, versions

`store` is PUT, then HEAD comparing the content length, then the prune below; `verify` is a HEAD
of the key and version id; `probe` is a HEAD of the bucket. The version id the PUT returns is
the `TargetRef.ref`.

- The key is a pure function of file identity, configured per route (default
  `<prefix>/<file name>`), so a retry overwrites instead of creating a sibling (D5).
- Metadata on every object: `x-amz-meta-digest`, `x-amz-meta-digest-algo`,
  `x-amz-meta-source-mtime`, `x-amz-meta-source-name`, `x-amz-meta-transfer-id`.
- Versioning is on bucket-wide and cannot be changed, so every successful PUT is followed by
  a prune: list versions for that exact key, delete every version id except the one the PUT
  returned (D5). The prune also removes a version left by a crash between an earlier PUT and
  its prune, or between the prune and the ledger write, because the retry is a PUT on the same
  key followed by the same prune; the adapter's own tests replay both crashes (I6). A
  lifecycle rule for non-current versions is worth requesting from the bucket owner as a safety
  net; the design does not depend on it.

---

## 7. Delivery

### 7.1 The `DeliveryChannel` seam

```kotlin
interface DeliveryChannel {
    val name: ChannelName
    val policy: DeliveryPolicy
    suspend fun deliver(event: DeliveryEvent): DeliveryOutcome
}

sealed interface DeliveryOutcome {
    data class Delivered(val reference: String?) : DeliveryOutcome
    data class Retry(val status: String?, val reason: String) : DeliveryOutcome
    data class Reject(val status: String?, val reason: String) : DeliveryOutcome
}

data class DeliveryEvent(          // the fixed vocabulary every body is built from
    val transferId: Long, val route: String,
    val fileName: String, val fileSize: Long, val fileMtime: Instant,
    val digest: String, val digestAlgo: String,
    val bucket: String, val key: String, val targetRef: String?,
    val firstSeenAt: Instant, val ackedAt: Instant,
    val attempt: Int,
)
```

The relay knows this interface and nothing else. `CancellationException` is never caught or
converted into an outcome.

### 7.2 Delivery policy

Per channel, in the DSL:

| Knob | Default | Meaning |
|---|---|---|
| `maxAttempts` | 50 | after this, the delivery is FAILED |
| `giveUpAfter` | 24 hours from `created_at` | after this, FAILED regardless of attempts |
| `backoff` | exponential from 5 s, factor 2, cap 15 min, full jitter | `next_attempt_at` |
| `timeout` | 10 s | per call; below the drain timeout |

A FAILED delivery does not change the transfer's state: the file is safe in the bucket and in
temp, and the transfer stays ACKED with a metric and a log line pointing at the delivery. The
transfer becomes DONE only when every delivery is DELIVERED (D9). This is how webhook systems
such as Stripe and GitHub behave: each endpoint independently, bounded backoff, then a
dead-letter with a redeliver action.

### 7.3 Relay

One coroutine per process, a cold flow over the ledger:

```kotlin
fun due(): Flow<Delivery> = flow {
    while (currentCoroutineContext().isActive) {
        val batch = ledger.due(clock.instant(), excluding = inFlight.snapshot(), limit = batchSize)
        if (batch.isEmpty()) { wake.awaitOrTimeout(sweepInterval); continue }
        batch.forEach { inFlight += it.id; emit(it) }
    }
}

due().buffer(batchSize)
    .flatMapMerge(concurrency = parallelism) { d -> flow { emit(d to deliverOnce(d)) } }
    .collect { (d, outcome) -> try { record(d, outcome) } finally { inFlight -= d.id } }
```

- `emit` suspends when the buffer is full, so the next query runs only after the previous
  batch has drained: at most `batchSize + parallelism` deliveries are in memory (I5).
- `wake` is a conflated signal the pipeline sends after every `acked` transaction, so the
  first attempt follows the ack within milliseconds; `sweepInterval` (default 30 s) is the
  guarantee for everything else.
- Cancelling the scope cancels in-flight calls; their rows stay PENDING and are retried after
  restart. That is the correct shutdown (Sec 11.2).
- A hot `SharedFlow` is not used to carry rows: it broadcasts to every subscriber, drops
  values when no subscriber is collecting, and never completes. Each of those is wrong for a
  work queue where one worker must handle each row and none may be lost (D7). A `SharedFlow`
  would be an acceptable implementation of `wake`, where dropping a nudge is harmless.

### 7.4 The relay's in-flight set

A row selected at time T sits in the buffer or inside an HTTP call for seconds while the
ledger still says PENDING, so the next select must not hand it to a second worker (I4). The
relay keeps a concurrent set of the delivery ids it has selected and not yet recorded. An id
is added when selected and removed in `finally` after the outcome is recorded, on every exit
path including cancellation, so the set is bounded by `batchSize + parallelism` and empty when
the relay is idle (I5). It holds ids only, never data, and it must not survive a restart: after
a restart every PENDING row is fair game again, which is the redo at-least-once wants. The
alternative, a claim written to the ledger with a stale-claim reset, is the seam for a second
replica (Sec 14.2) and is not built while one process runs (D7).

### 7.5 HTTP channel

Built by the DSL over the JDK `HttpClient`, framework-free (D8):

```kotlin
channels += http("downstream") {
    method = POST
    url = "https://downstream.internal/api/files"
    header("X-Source", "sftp-ingest")
    auth = bearer(env("DOWNSTREAM_TOKEN"))            // bearer | basic | header | none
    timeout = 10.seconds
    body { e -> obj {                                 // Jackson tree builder over DeliveryEvent
        "fileId"   to e.transferId
        "fileName" to e.fileName
        "bucket"   to e.bucket
        "key"      to e.key
        "size"     to e.fileSize
        "sha256"   to e.digest
        "mtime"    to e.fileMtime
    } }
    response {
        success = 200..299
        retry = setOf(408, 429) + (500..599)          // everything else is Reject
        reference = "/requestId"                      // JSON pointer, optional
    }
    policy { maxAttempts = 50; giveUpAfter = 24.hours; backoff = exponential(5.seconds, max = 15.minutes) }
}
```

- The body is a function from the fixed `DeliveryEvent` vocabulary to a Jackson tree.
  String templates are excluded because they cannot escape JSON safely.
- Connection failures and timeouts are Retry. A status in `success` is Delivered, in `retry`
  is Retry, anything else is Reject. A `reference` pointer that does not resolve yields
  Delivered with a null reference and a WARN, never a failure.
- Every attempt logs `transferId`, channel, attempt, status and reference at INFO. The
  reference of the attempt recorded as delivered is stored on the row; earlier references live
  only in logs, and a repeat call always yields a different one.

### 7.6 Downstream obligations

Delivery is at-least-once. Downstream must treat a repeated call for the same `fileId` as the
same event. The request id it returns is per call and is never a dedup key. Both are recorded
in Sec 16 as facts to confirm with the downstream team.

---

## 8. Data Quality

```kotlin
interface QualityCheck {
    suspend fun check(file: LocalFile, meta: FileIdentity): Quality      // Pass | Fail(reason)
    companion object { val NONE: QualityCheck }                          // always Pass
}
```

Scope: the complete staged local file after the connector's size check and before the store. A
file still being written on the server is the readiness check's problem, upstream of this
seam; a failed store is the pipeline's. `Fail` moves the transfer to REJECTED, tells the
connector `nack(reason, redeliver = false)`, deletes the staged file, counts a metric, and
leaves the file in the inbox for an operator (D11). Nothing is uploaded and nothing is
notified. Default is `NONE`.

---

## 9. Source Consumption

- One `watch(dir, every = interval)` per route, `interval` default 1 hour, overlap `SKIP`
  (D12). Manual `poll` beside a live `watch` is not requested of the connector.
- Readiness is the connector's default: size stable across two stats 10 s apart plus a
  minimum age of one minute. The uploader's write convention is an open item inherited from the
  connector (Sec 16).
- `onAck = move("temp/", overwrite = true)` relative to the watched directory, `onNack =
  noop()`, `autoCreate` and `startupProbe` on. The connector excludes the temp folder from
  listing. Top-level regular files only; no recursion.
- Manual ack and nack, not the connector's `consume` helper, because ack happens in the
  middle of the pipeline and REJECTED and FAILED need `redeliver = false`.
- `maxInFlight` at the connector (default 16) bounds how far the lister runs ahead;
  `parallelism` here (default 4) bounds how many pipelines run. Staging disk needed is
  `parallelism × max file size`, hundreds of megabytes at most.
- `PollFailed` is logged at WARN and counted; the watch continues. `PollSkipped(Overlap)` is
  counted and means a poll took longer than the interval, which is an alert. A fatal
  connector error terminates the watch; the route is marked down, readiness drops (Sec 11.1)
  and a restart is the recovery, which is the connector's contract.
- Sizing statement: 450 files an hour is minutes of work; at 5,000 files an hour and 10 MB
  each the ceiling is the five-session cap on the server, roughly four parallel transfers, and
  the acceptance plan carries a load scenario at that rate (S13).

---

## 10. Failure Model

| Failure | Where | Effect |
|---|---|---|
| Connector recoverable error after its retry budget | download, ack | `failedAttempt`; `nack(redeliver = true)`; next poll retries; FAILED at `maxAttempts` |
| Connector fatal error | anywhere | watch terminates; route down; readiness false |
| Target client or 5xx error | store, verify | SDK retries first; then as connector recoverable above |
| Target 4xx (access denied, no such bucket) | store | same path, logged at ERROR with the reason: ops fixes configuration, the file waits |
| Ledger unavailable | any transition | as recoverable; the poll continues with other files; a run with the ledger down completes nothing |
| Quality Fail | quality | REJECTED, terminal until re-drive |
| Channel Retry | deliver | backoff per policy |
| Channel Reject, or policy exhausted | deliver | delivery FAILED; transfer stays ACKED; metric |
| Poll longer than the interval | watch | `PollSkipped(Overlap)`; alert |
| Process killed | anywhere | Sec 4.4 |

Every pipeline error is caught at the file boundary: it is recorded, the staged file is
deleted, the connector is told, and the coroutine ends. Nothing propagates to the route
collector except cancellation and the connector's fatal termination.

Stuck detection: a gauge of transfers in SEEN, DOWNLOADED or UPLOADED older than
`stuckAfter` (default three intervals), refreshed at every `PollCompleted`, and a gauge of the
oldest PENDING delivery's age. Both are alert inputs; neither takes action.

---

## 11. Startup and Shutdown

### 11.1 Startup

1. Build and validate the DSL configuration. Invalid configuration ends startup.
2. Ledger: one round trip on each table. A missing table ends startup with the DDL name in the
   message.
3. Target: `probe()`. For S3 that is a HEAD of the bucket; absent or forbidden ends startup,
   and the bucket is never created.
4. Staging directory: every file in it is deleted (D17). Nothing in it can be trusted after a
   restart, and the ledger will redo whatever was in flight.
5. Channels: no call by default. A downstream call may have side effects, so a startup probe
   is opt-in per channel and is a GET to a configured health URL when enabled.
6. Connector start, which runs its own probe including the marker rename into `temp/`.
7. Start the relay, then the watch for each route.

Readiness is true when steps 1 to 6 have passed and every route's watch is alive. A ledger or
bucket outage after startup shows in metrics and logs, not in readiness, because a restart
would not help. A terminated watch drops readiness, because a restart is exactly what helps.

### 11.2 Shutdown

Bounded by `drainTimeout` (default 30 s), from the Quarkus shutdown event, in this order:

1. Readiness false.
2. Cancel the route collectors. The connector's `close()` runs its own drain: in-flight
   downloads are cancelled and their partial files deleted, unacked files are treated as nacks.
   Pipelines past the download are cancelled at their next suspension point; a pipeline inside
   the S3 client returns when its API call timeout fires, which is why that timeout sits below
   `drainTimeout`.
3. Cancel the relay. In-flight deliveries are cancelled; their rows stay PENDING.
4. Close the S3 client, then the JDBI datasource.

Every stage's timeout, the connector's `drainTimeout + cancelGrace`, and the delivery timeout
are validated at build time to fit inside `drainTimeout`, and the pod's termination grace period
must exceed `drainTimeout` by a margin, which is deployment arithmetic written beside the
manifest, as `etl-host` does for its compose file.

---

## 12. Configuration and Operations

### 12.1 DSL

Immutable configuration from a `@DslMarker` builder validated at build time; the Quarkus
package maps `application.properties` onto it, so the same DSL serves tests and the host.

```kotlin
sftpIngest {
    ledger { /* datasource name */ }
    s3("landing-minio") {                         // a named target client; a route picks one
        endpoint = "https://minio.internal"; region = "us-east-1"; pathStyle = true
        credentials = fromEnvironment("S3_ACCESS_KEY", "S3_SECRET_KEY")
        connectTimeout = 5.seconds; socketTimeout = 30.seconds; apiCallTimeout = 60.seconds
    }
    staging { dir = Path("/var/ingest/stage") }
    relay { batchSize = 50; parallelism = 4; sweepInterval = 30.seconds }
    drainTimeout = 30.seconds

    route("vendor-drop") {
        source = sftp(connector = "vendor", directory = "/inbox") {   // built by the connector's own DSL
            every = 1.hours
            onDone = move("temp/")                // the ack action is a source concept
        }
        target = s3(client = "landing-minio", bucket = "landing") {
            key = { f -> "vendor/${f.name}" }     // the key is a target concept
        }
        parallelism = 4
        maxAttempts = 5
        stuckAfter = 3.hours
        quality = QualityCheck.NONE
        channels += http("downstream") { /* Sec 7.5 */ }
    }
}
```

Validation rules: `apiCallTimeout < drainTimeout`; every channel `timeout < drainTimeout`;
`parallelism <= connector.maxConcurrentTransfers`; staging directory exists and is writable
local disk; at least one channel per route; channel names unique per route; the key function
yields a non-empty key without `..` for a sample identity; `maxAttempts >= 1`.

### 12.2 Properties

Every DSL knob has a property under `sftp-ingest.*`; routes are a list. Secrets arrive only
through environment variables named in the properties, never as values in them (D14).

### 12.3 Admin

A JAX-RS resource under the host's admin role, mirroring `etl-host`:

| Endpoint | Does |
|---|---|
| `GET /admin/ingest/routes` | route state, last poll, counts by transfer state |
| `GET /admin/ingest/transfers?route=&state=&limit=` | ledger rows |
| `GET /admin/ingest/transfers/{id}/deliveries` | every delivery of one transfer: channel, state, attempts, last status, reference, delivered time. This is the "which request id did we send downstream for this file" query |
| `POST /admin/ingest/transfers/{id}/redrive` | REJECTED or FAILED to SEEN; the next poll picks it up |
| `POST /admin/ingest/deliveries/{id}/redrive` | FAILED delivery to PENDING; wakes the relay |

No endpoint triggers a poll; the connector's `watch` is the only lister (D12).

---

## 13. Metrics

Micrometer through the host's registry. Tags: `route` or `channel`; never a file name, id or
key.

| Metric | Type | Tags / notes |
|---|---|---|
| `sftp_ingest_files_total` | counter | `route`, `outcome`: done, rejected, failed, reacked |
| `sftp_ingest_stage_seconds` | timer | `route`, `stage`: download, quality, store, ack; `result`: ok, error |
| `sftp_ingest_inflight` | gauge | `route`: pipelines running |
| `sftp_ingest_stuck_files` | gauge | `route`: transfers older than `stuckAfter` before ACKED |
| `sftp_ingest_reconciled_total` | counter | `route` |
| `sftp_ingest_reconcile_skipped_total` | counter | `route`: truncated listing |
| `sftp_ingest_versions_pruned_total` | counter | `route`; emitted by the S3 target |
| `sftp_ingest_poll_total` | counter | `route`, `result`: completed, failed, skipped |
| `sftp_ingest_delivery_total` | counter | `channel`, `outcome`: delivered, retry, rejected, gave_up |
| `sftp_ingest_delivery_seconds` | timer | `channel` |
| `sftp_ingest_outbox_pending` | gauge | `channel` |
| `sftp_ingest_outbox_oldest_seconds` | gauge | `channel`: age of the oldest PENDING row |
| `sftp_ingest_relay_inflight` | gauge | size of the relay's in-flight set |
| `sftp_ingest_route_up` | gauge | `route`: 1 while the watch is alive |

Alert inputs, not alert rules: `route_up == 0`, `stuck_files > 0`, `outbox_oldest_seconds`
above the give-up window, `poll_total{result=skipped}` increasing, any `gave_up`.

---

## 14. Known Limitations and Future Extensions

### 14.1 Cron alignment and manual polls

The connector's ticker fires every `interval` from process start, not at the top of the hour,
and no manual poll exists. Both are appeals to the connector spec if wanted: an initial delay
or cron on `watch`, and a defined `poll` beside a live `watch`.

### 14.2 Second replica

Two processes on one directory need a claim before download, which the connector's spec
reserves as its own extension, and a claim on deliveries, which is an IN_FLIGHT delivery
state with a claim timestamp and a stale-claim reset in the ledger, replacing the relay's
in-flight set. Neither is built for one replica (D7, D13).

### 14.3 Retention of ledger rows

DONE and FAILED rows accumulate. A retention sweep is a later ticket with its own decision on
how long an operator wants to answer "did file X get through".

### 14.4 Content-derived notification fields

If a channel ever needs a field parsed from the file, an `extract` step lands between quality
and upload and the vocabulary stops being fixed. Not built until asked (Sec 16, item 3).

---

## 15. Decision Log

| ID | Decision | Rationale |
|---|---|---|
| D1 | The application owns one durable ledger in Oracle; the connector's `SeenRepository` is not used | The connector filters before emitting; a transfer recorded as uploaded but not yet moved would be filtered out and never acked. Two ledgers are two sources of truth, as the connector's D14 says |
| D2 | File identity is name, size and mtime | Matches the connector's in-flight key; a re-drop with a new mtime is a new file, a byte-identical re-copy with the same mtime is the same one |
| D3 | Two tables, no attempt table; attempts are traced by log | A transfer has many deliveries, each with its own state; an attempt history table would be a third table nobody has asked to query |
| D4 | AWS SDK v2, synchronous, Apache client, checksums when-required, path-style, placeholder region | Sequential per-file work gains nothing from async; SDK checksums can fail against older MinIO; the connector's digest is the integrity value |
| D5 | Key is a pure function of identity; the S3 target prunes all other versions inside every `store` | Versioning is a bucket-wide policy; a deterministic key makes retry an overwrite; the prune after the retry also cleans crash-gap versions |
| D6 | Order: store, ack, notify; deliveries are created in the ACKED transaction; reconciliation repairs move-then-crash | The source-side move is the commit, as in Camel, Spring Integration and NiFi; the outbox makes the notification reliable, not the ordering. Cost: a crash between the move and the ledger write delays that file's notification to the next poll |
| D7 | Relay is a cold flow with a buffer and a wake signal; rows never ride a `SharedFlow`; an in-memory in-flight set guards double selection | Cold gives backpressure by suspension and loses nothing; `SharedFlow` broadcasts, drops without subscribers and never completes. The set is bounded by construction and must not survive a restart; a ledger claim is the two-replica seam |
| D8 | Channel seam is one suspend function returning Delivered, Retry or Reject; the HTTP channel is declarative over a Jackson tree builder | The relay must not know HTTP; a body builder over a fixed vocabulary escapes correctly where string templates cannot; the reference lands on the delivery row |
| D9 | Per-channel policy; a transfer is DONE only when every channel delivered; a FAILED delivery never fails the transfer | The file is safe once ACKED; webhook practice retries each endpoint independently and dead-letters with a redeliver action |
| D10 | Poison files: FAILED after `maxAttempts`, left in the inbox, re-drivable, never deleted | Deleting is the one irreversible action and an operator with the file in hand can always decide |
| D11 | Quality runs on the complete staged file, before upload; `NONE` is the default | Completeness on the server belongs to readiness, integrity of the copy to the download; quality is about content |
| D12 | The connector's `watch` ticker polls; the relay is a coroutine loop; no Quarkus scheduler is used | The maintainer's choice; it leaves one scheduling model in the process. Cost: no top-of-hour alignment and no manual poll (Sec 14.1) |
| D13 | One replica per route | The connector's in-flight set is per process and the relay's guard is in memory; both seams for a second replica are named, neither built |
| D14 | Credentials from environment variables populated by Vault; rotation is a rollout | The SDK reads them without configuration; a live rotation would need a credentials provider that re-reads, which nobody has asked for |
| D15 | The DBA applies the DDL; the bucket is never created | The sibling archive layer's rule: an ambient side effect at boot is what provisioning exists to avoid |
| D16 | The S3 store is verified by a HEAD content length plus digest metadata | With SDK checksums off, the size match is the only cheap post-condition; the digest is what an auditor compares |
| D17 | The staging directory is emptied at startup; downloads are redone | A staged file from a dead process has no ledger row that vouches for it; files are small |
| D18 | One module; adapters are packages; ArchUnit sentences | The seams have real second implementations in tests; a second module would buy nothing until a second host exists |
| D19 | The body is rendered at send time from the transfer row; no payload column | A stored payload freezes a body shape across deployments and duplicates every field already on the row |
| D20 | The pipeline consumes the source through `IngestEvent` and a `Downloader` function that it owns; only the `sftp` package imports the connector | The connector is unimplemented while this application is built, so every phase but the binding must compile and test without it; the mapping is a dozen lines and the fake source in the test kit is its second implementation |
| D21 | Source and target are DSL vocabulary; the target seam is `store`, `verify`, `probe` and nothing about versions; no `Source` interface exists | The pipeline should not know what kind of place either end is. The source seam is already `IngestEvent` plus `Downloader` with the scripted source as its second implementation, so a `Source` interface would be a name without a capability. The old target seam leaked S3 versioning into the pipeline and the crash matrix; narrowing it moves that accident into the one adapter that has it. No registry, no plugin discovery, no source-times-target matrix: a second source or target is one adapter class and one DSL function, as a second channel is |

---

## 16. Open Items Before Implementation

1. **MinIO server version.** Decides whether SDK checksums could be turned back on. The
   design assumes they stay off.
2. **Downstream tolerates repeated calls for one `fileId` and returns a per-call reference.**
   Assumed. If repeats are not tolerated, the request must carry `fileId` as an idempotency
   key and the spec gains a rule it cannot fully keep.
3. **Every body field derives from file metadata and the upload.** Assumed. A field parsed
   from content adds an extract step (Sec 14.4).
4. **Uploader's write convention** on the server: temp-name-and-rename, marker file, or
   direct write. Inherited from the connector's open item 1; the default readiness heuristic
   stands until answered.
5. **Temp folder ownership.** `autoCreate` makes it; if the account cannot `mkdir`, the upstream
   creates it and the connector's probe verifies it.
6. **Lifecycle rule for non-current versions** on the bucket, requested from its owner as a
   safety net.
7. **Top-of-hour alignment.** If required, raise it against the connector's `watch` (Sec 14.1).
8. **Oracle schema and sequence names**, and the datasource the ledger shares with.
9. **Pod termination grace period** versus `drainTimeout`, written beside the manifest.

---

## 17. Acceptance Plan

Three tiers, the connector's shape:

1. **Fakes, no I/O.** In-memory ledger, in-memory target, recording channel, the
   connector's fake transport or a scripted `FileSeen` source. Pipeline state machine, entry
   points, crash matrix through hook points, reconciliation, relay invariants, shutdown phases.
   Deterministic through an injected `Clock` and `runTest`; no `Thread.sleep`.
2. **Real adapters, one at a time.** Ledger against Oracle in Testcontainers, tagged `oracle`;
   S3 target against MinIO in Testcontainers, versioning enabled, tagged `minio`; HTTP
   channel against a JDK `HttpServer` on loopback scripted per scenario; the connector's
   embedded SSHD from its testkit.
3. **End to end.** One route through the embedded SSHD, MinIO, Oracle and the loopback server,
   with the crash matrix replayed by killing and restarting the pipeline at each hook point.

### 17.1 Invariants

Tests are named `I<n>_<description>`.

| ID | Invariant |
|---|---|
| I1 | A transfer reaches DONE only if `verify` of its recorded target reference is true |
| I2 | The application never deletes a file from the source; the only source write is the connector's move |
| I3 | A delivery row is DELIVERED only after a channel returned Delivered for it |
| I4 | A delivery id is never inside two workers at once |
| I5 | The relay's in-flight set never exceeds `batchSize + parallelism` and is empty whenever the relay is idle |
| I6 | After `store` returns, exactly one copy exists at the key, including after a crash inside a previous `store` on the same key (S3: between PUT and prune) |
| I7 | A REJECTED or FAILED transfer is neither uploaded nor delivered until re-driven |
| I8 | Restart at any hook point converges to DONE with at most one extra upload and at most one extra delivery per channel |
| I9 | The staging directory holds no file that is not inside a running pipeline |
| I10 | `ack()` is called only when the ledger state is UPLOADED or later and the target reference was verified |
| I11 | The ACKED transition and its PENDING rows are one transaction; the DELIVERED transition and a DONE flip are one transaction |
| I12 | Shutdown returns within `drainTimeout` and leaves every PENDING row PENDING |
| I13 | Two channels on one route are delivered independently: one channel's Retry never delays the other |
| I14 | A configuration whose `apiCallTimeout` or any channel `timeout` is not below `drainTimeout` is rejected at build time |

### 17.2 Scenario table

| ID | Scenario | Expected |
|---|---|---|
| S1 | Happy path, one file, one channel | DONE; object with metadata; file in temp; one delivery with reference |
| S2 | Crash after store, before ledger UPLOADED (and, in the S3 adapter tier, inside store between PUT and prune) | Next poll: store again, one copy at the key, DONE |
| S3 | Crash after ledger UPLOADED, before move | Next poll: verify, ack, no second store |
| S4 | Crash after move, before ledger ACKED | Next poll: reconciliation marks ACKED, deliveries created, DONE |
| S5 | Crash after delivery sent, before ledger | Relay delivers again; downstream sees two calls with one `fileId`; row DELIVERED once |
| S6 | Copy missing at UPLOADED (deleted from the target) | Full pipeline again on the same row; DONE |
| S7 | Downstream 503 twice then 200 | Two Retry with backoff, then DELIVERED; attempts = 3 |
| S8 | Downstream 400 | Reject; delivery FAILED; transfer stays ACKED; `gave_up` not incremented, `rejected` is |
| S9 | Downstream down past `giveUpAfter` | Delivery FAILED with `gave_up`; re-drive returns it to PENDING and it delivers |
| S10 | Quality Fail | REJECTED; nothing uploaded; file stays in inbox; re-drive re-runs from download |
| S11 | Download fails five polls in a row | FAILED after `maxAttempts`; `nack(redeliver = false)`; after restart the ledger still answers FAILED |
| S12 | Same identity re-dropped after DONE | Verify, ack again, counted as `reacked`, no upload, no delivery |
| S13 | 5,000 files of 10 MB in one poll | All DONE; in-flight never above `parallelism`; staging never above `parallelism × 10 MB`; no `PollSkipped` at the next tick |
| S14 | Listing truncated at `maxFilesPerPoll` | Reconciliation skipped and counted; nothing marked ACKED by absence |
| S15 | Shutdown during store and during delivery | The store's row stays DOWNLOADED, delivery's row stays PENDING, close within bound, staging empty at next start |
| S16 | Ledger unavailable for one poll | Every file nacked with redelivery; nothing uploaded; next poll with the ledger back completes all |
| S17 | Two channels, one always 503 | The other channel delivers; transfer stays ACKED; `outbox_pending{channel}` shows one |
| S18 | Wrong SFTP password | Watch terminates; `route_up` 0; readiness false; process alive |
