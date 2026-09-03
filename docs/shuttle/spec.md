# Shuttle - Design Spec

Version: v0.4 (v0.3 amended for three operational watch-outs from a staff review, Sec 21; not yet implemented; supersedes v0.1 "SFTP Ingest")
Scope: one process, one replica, many routes; each route moves source objects from one place
to another, processes them on the way, and tells other systems
Status: ready for a phase plan
Depends on: `docs/sftpconnector/spec.md` v0.1 (the SFTP connector), itself unimplemented

---

## 1. Background and Goals

### 1.1 Problem

Files arrive somewhere and must end up somewhere else, with other systems told about it, and
nothing lost on the way. Two concrete cases exist today:

- **Vendor drop.** An upstream party drops files into one directory on an SFTP server. Every
  hour, every complete file must be copied into a MinIO bucket, the original moved into a temp
  folder on the same server that downstream purges, and a downstream API told where the copy
  landed. About 450 files an hour today, under 10 MB each; the design must not need rewriting
  at ten times that.
- **Image sets.** A NATS message names a metadata file in MinIO. The metadata lists images,
  also in MinIO. Every image must be uploaded to a partner's SFTP server, after which the
  message is acknowledged and downstream told once. An in-memory cache deduplicates this today,
  which is what a restart loses.

A third shape, moving files from A to B and telling nobody, must also be expressible. The
common structure is one **route**: a trigger that says a source object exists, a fetch of its
bytes, an optional processing chain, a store into a target, the acknowledgement of the trigger,
and notifications at chosen transfer states. Everything below the file event on the SFTP side is the
connector's business; this spec owns everything above it.

### 1.2 Goals

- **At-least-once, converging to exactly-once in effect.** Every source object reaches its
  target and every configured channel hears about it. A crash at any point is redone from a
  durable state store with at most one extra store and one extra notification, never a lost one.
- **Source and target as vocabulary, not as a framework.** A route reads as "from this, do
  that, to there, tell them". What kind of place either end is stays inside its adapter.
- **Processing as one seam.** Quality checks, renames, archives, attribute extraction, fan-out
  and custom code are all the same kind of thing: a step from one payload to another.
- **Configuration as data.** YAML is the primary form, validated at boot by numbered rules,
  with a typed Kotlin DSL underneath. Operations edit a mapping table, not code.
- **Bounded, observable, safe under shutdown.** Every stage has a timeout below the drain,
  every queue is bounded, every route is supervised, and a pod restart at any moment is
  survivable.
- **Framework-free core.** The pipeline, the seams and the notifier know nothing of Quarkus,
  JDBI, the AWS SDK, HTTP or NATS; each is an adapter package.

### 1.3 Non-goals

- No exactly-once notification. Downstream deduplicates on the transfer id (Sec 9.7).
- No multi-replica coordination. One process; the seam is named in Sec 15.1.
- No streaming or resume of transfers. Files stage on local disk.
- No expression language in the mapping table. Conditionals are named Kotlin providers, with a
  written trigger for revisiting (D30).
- No per-attempt delivery history table. Attempts are traced by log (D3).
- No creation of buckets, tables or directories on the far side (D15).
- No registry, plugin discovery or source-times-target matrix (D21).

---

## 2. Terminology

| Term | Definition |
|---|---|
| Route | One trigger, one fetch, a processing chain, one target, an ack, and notifications. The unit of configuration and of supervision. |
| Source object | The unit everything is counted in: one file for a polled directory, one message for a subscription. One transfer row, one ack, one `acked` notification. |
| Trigger | What says a source object exists: a `poll` of an object store directory, or a `subscribe` to a channel. |
| Object store | A place files live: SFTP server, S3 bucket. Declared once, usable as a route's source, its target, or its fetch location. |
| Channel | A place messages go or come from: HTTP endpoint, NATS subject, later mail or SMS. A route notifies through one; a route may be triggered by one. |
| Transfer | The state store's record of one source object, one row in `file_transfer`. |
| Child transfer | A row produced by a processing chain that yields several objects; carries `parent_id`. Never notified on its own. |
| Staged object | One local file with its name, size, mtime, digest and content type. What processors and targets see. |
| Payload | The list of staged objects a processing step receives and returns. One for a plain file. |
| Processor | One step of the chain: payload in, payload or rejection out, attributes as a side channel. |
| Attribute | A string named by a processor or the source event, carried on the transfer row and the stored object, readable by the mapping table. |
| Provider | A named Kotlin bean that computes a JSON node from a transfer row for the mapping table. |
| Ack | The commit action on the source side, whose vocabulary depends on the trigger kind. |
| Delivery | One notification announcing one transfer state on one channel. One row in `delivery_outbox`. |
| Notifier | The process-wide loop that turns pending deliveries into channel calls. |
| Shuttle state store | The Oracle tables that make a crash survivable. The only durable memory the process has. |
| Reconciliation | The end-of-poll repair, polling sources only, for a transfer moved but never recorded as acked. |
| Re-drive | An operator action returning a REJECTED or FAILED transfer, or a FAILED delivery, to play. |

---

## 3. Overall Model

### 3.1 Layers

```
 triggers    poll(objectStore, directory)  via the SFTP connector's watch, or an S3 listing (later)
             subscribe(channel, subject)   via a NATS subscription (milestone 2)
   │  RouteEvent: Seen(object, ack, nack) | PollCompleted(listed, truncated) | PollFailed | PollSkipped | RouteDown
   ▼
 route       one supervised collector per route; decides from the state store what each object needs;
             bounded parallel pipelines; reconciliation at PollCompleted for polling sources
   ▼
 pipeline    fetch -> process chain -> store (each object) -> STORED -> ack -> ACKED + deliveries
   ▼
 state store file_transfer, delivery_outbox                                    (Oracle through JDBI)
   │
 notifier    cold Flow over PENDING deliveries -> channel.deliver -> DELIVERED / retry / FAILED
   │
 channels    HttpChannel (JDK HttpClient); NatsChannel (milestone 2); fakes in tests
```

Three pieces of state exist, each with one owner: the connector's in-flight set of files, the
state store, and the notifier's in-flight set of delivery ids (Sec 9.5). Nothing else remembers
anything, and the two in-memory sets must not survive a restart.

### 3.2 Module and packages

One Maven module, `shuttle`, a Quarkus application modelled on `etl-host`. Package boundaries
are dependency sentences enforced by ArchUnit (D18):

| Package | Holds | May import |
|---|---|---|
| `infra.shuttle.core` | value types, `RouteEvent`, `Fetcher`, the five seams (Sec 3.4), `RouteRunner`, `TransferPipeline`, `Notifier`, built-in processors, the mapping renderer, `ShuttleConfig` + Kotlin DSL + validation rules, metric names | kotlin-stdlib, coroutines, micrometer-core, jboss-logging, Jackson databind |
| `infra.shuttle.yaml` | YAML loading onto the DSL | `core`, Jackson YAML |
| `infra.shuttle.sftp` | the SFTP object store: poll trigger and fetch over the connector, SFTP target (milestone 2) | `core`, the connector core |
| `infra.shuttle.s3` | the S3 object store: target, fetch, poll trigger (later) | `core`, AWS SDK v2 |
| `infra.shuttle.http` | the HTTP channel | `core`, `java.net.http` |
| `infra.shuttle.nats` | the NATS channel: subscribe trigger and publish (milestone 2) | `core`, jnats |
| `infra.shuttle.jdbi` | the Oracle state store | `core`, JDBI |
| `infra.shuttle.quarkus` | producers, host lifecycle, readiness, admin resource, named-bean lookup, validate and try modes | everything above, Quarkus |

Rules: nothing in `core` imports an adapter or a technology; each adapter imports only `core`
and its own technology; only `quarkus` imports Quarkus; only `sftp` and `quarkus` import the
connector. Every seam in `core` has a second implementation in the test kit. Logging is
`org.jboss.logging.Logger` directly in every package (no facade, no logger in any context
object); the pipeline puts `transferId` and `route` into the logging MDC around every stage so
every line carries them. Time is an injected `java.time.Clock`.

### 3.3 Thread model

Everything in `core` is `suspend`. Blocking calls, which are JDBI, the synchronous S3 client,
`HttpClient.send` and archive writing, run on one bounded view of `Dispatchers.IO` owned by the
module and sized to the sum of route parallelism; the connector owns its own for JSch.
Per-object pipelines run under one `SupervisorJob` scope per route, the notifier under one
scope per process, and no lock is held across I/O anywhere.

### 3.4 The five seams

| Seam | Role | Second implementation |
|---|---|---|
| `StateStore` | every state transition, Sec 8.2 | in-memory, test kit |
| `ObjectStoreTarget` | store, verify, probe, Sec 7.1 | in-memory, test kit |
| `DeliveryChannel` | deliver one event, Sec 9.2 | recording, test kit |
| `Processor` | one processing step, Sec 6.2 | the built-ins are each other's second implementation |
| `Hook` | named interleaving points, Sec 4.4 | no-op and the test driver |

`RouteEvent` is a sealed class and `Fetcher` a function type; the test kit's scripted source
produces them directly (D20). `Provider` is a one-method interface for named beans (Sec 9.6);
it is not a seam in the pipeline's sense because the pipeline never calls it, the renderer does.

---

## 4. Route Execution

### 4.1 Stages

One coroutine per source object, at most `parallelism` per route.

| # | Stage | Does | State after |
|---|---|---|---|
| 0 | Decide | Look up the transfer by identity; choose the entry point (Sec 4.3) | SEEN |
| 1 | Fetch | Bring the object's bytes to staging through the source's `Fetcher`; the digest is computed as the bytes stream; the staged object carries name, size, mtime, digest, content type | FETCHED |
| 2 | Process | Run the chain (Sec 6); attributes freeze at the end; every mapping table of the route's channels is checked against them (Sec 9.6) | PROCESSED, or REJECTED |
| 3 | Store | For each object in the final payload: `target.store(key, file, metadata)`; a payload of one is the transfer itself, a payload of N becomes N child rows (Sec 4.5) | STORED |
| 4 | Ack | The trigger's ack action (Sec 5.3); for a parent, only once every child is STORED. A polled file is moved first and written ACKED after, because the move is visible to the next listing; a subscribed message is written ACKED first and acked at the broker after, because an acked message is never redelivered (Sec 4.4) | ACKED, plus one PENDING delivery per `on: acked` channel, in one transaction |
| 5 | Notify | Owned by the notifier, not this coroutine (Sec 9) | DONE when every delivery is DELIVERED; immediately when the route notifies nobody |

Notifications other than `acked` (Sec 9.1) are created in the transaction of the
transition that defines them: `fetched` with FETCHED, `stored` with STORED.

Staging is deleted after stage 3 succeeds and on every failure path, including every file a
processor created; the local copy never outlives the pipeline that made it. The source object
is a different matter: what happens to it is the ack action, which every polled route states
explicitly (rule 12), because a silent `delete` would be the one irreversible default and a
silent `none` would re-list the same file for ever.

Staging is also bounded in bytes, not only in pipelines (D41). Before stage 1 the pipeline reads
the usable space of the store's staging volume; below the store's `staging.minFree` (default
1 GiB) the object is deferred: `nack(redeliver = true)`, no attempt counted, one WARN, the next
trigger retries. `shuttle_staging_free_bytes` is refreshed at every such check and
`shuttle_staging_deferred_total` counts the deferrals. A chain that fills the volume anyway,
an archive larger than its listing suggested, fails as a stage error like any other I/O failure.

### 4.2 Transfer states

```
 (none) → SEEN → FETCHED → PROCESSED → STORED → ACKED → DONE
                    │           │                             ▲
                    │           └── Reject ──→ REJECTED ── re-drive ─┘ (back to SEEN)
                    │
   any stage error, attempts < max ─→ state unchanged, nack(redeliver = true), next trigger retries
   any stage error, attempts = max ─→ FAILED, nack(redeliver = false); re-drive returns to SEEN
```

Child rows use the same states from FETCHED onward and have no ack of their own: their ACKED
is written by the parent's ack transaction. `maxAttempts` default 5.

### 4.3 Entry points, decided from the state store

A polling trigger emits the same object every poll until it is acked; a subscription redelivers
an unacknowledged message. The state store decides how much work is left:

| State | Action |
|---|---|
| none, SEEN, FETCHED, PROCESSED | Full run from stage 1. A staged file from an earlier process is never trusted (D17). For a parent that already has child rows, the chain runs again and yields the same children; each child whose row is STORED with a true `verify` skips the store, the rest are stored (S28). |
| STORED | `target.verify(ref)` for the transfer, or for every child. All true: skip to stage 4. Any false: full run on the same row. |
| ACKED, DONE | The object is back although it was acked. A polled file is fetched and digested first. The row's own digest means the same file came back: verify, ack again, counted as `reacked`, logged at WARN. A different digest means new content under an old identity: a new transfer is created with the next `revision`, pointing at the row it supersedes, and runs from stage 1; the old row and its target version are never touched (S12). A finished polled identity is fetched for this check at most once per the route's `recheckFinished` (default 24 h; `0s` means every poll), measured from the row's `updated_at`; listed again inside that window it is skipped with no fetch and no state write, so a directory whose ack is `none` costs no download per poll for files that stay (D40). A redelivered message is verified and acked again, `reacked`, without a fetch. |
| REJECTED, FAILED | `nack(redeliver = false)`, no work. |

### 4.4 Crash matrix

Hook points: `afterFetch`, `afterProcess`, `afterStore`, `afterLedgerStored`, `afterAck`,
`afterLedgerAcked`, `afterDeliverySent`. A crash inside `store` is the target adapter's
contract (Sec 7.2, I6).

| Crash after | Source | Target | State store | Next trigger does | Extra effects |
|---|---|---|---|---|---|
| fetch | object still there | nothing | SEEN or FETCHED | full run | none |
| process | still there | nothing | PROCESSED | full run | none |
| store, before ledger | still there | 1 copy | PROCESSED | full run: store again | one extra store |
| ledger STORED | still there | 1 copy | STORED | verify, ack | none |
| poll: move, before ledger | moved | 1 copy | STORED | reconciliation writes ACKED (Sec 4.6) | delayed notification |
| subscribe: ledger ACKED, before broker ack | not acked, redelivered | 1 copy | ACKED, PENDING | verify, ack the broker again, `reacked`; no new deliveries | none |
| ledger ACKED | acked | 1 copy | ACKED, PENDING | notifier delivers | none |
| delivery sent, before ledger | acked | 1 copy | ACKED, PENDING | notifier delivers again | one duplicate notification, deduplicated downstream |

Invariant proved by the table: at any crash point, at most one extra store and at most one
extra delivery per channel per event, and never a lost object (I8).

### 4.5 Parents and children

A final payload of N objects creates N child rows under the transfer in one transaction, each
FETCHED with its own staged object, digest and key. Children are stored under the route's
parallelism like any object. The parent's STORED is written when the last child is STORED;
the parent's ack is the only ack; the parent's `acked` notification is the only notification; a
child that reaches `maxAttempts` fails the parent. A child's STORED transition is one statement
on the child's row followed by a conditional update of the parent's row that fires only when no
sibling is left unstored; no lock on the parent is held per child, so N children storing
concurrently do not serialise on one row (D42). A re-drive of a parent re-runs the chain and
replaces its children. Two children of one parent that resolve to the same key are a cardinality
error: the transfer is rejected with both source paths in the reason, because storing both would
make one silently overwrite its sibling (S33).

### 4.6 Reconciliation, polling sources only

At `PollCompleted` with a complete listing, meaning it ended before the connector's
`maxFilesPerPoll`: every transfer of that route in STORED whose `updated_at` is older than the
poll's start and whose identity was not listed transitions to ACKED with its deliveries created,
through the same function stage 4 uses. A truncated listing skips reconciliation and counts it.
Subscription sources have no listing and need no reconciliation: their ledger write precedes the
broker ack (Sec 4.4), so the gap that reconciliation repairs for polled files cannot leave a
subscribed row behind.

---

## 5. Sources

### 5.1 Triggers

| Trigger | Declared on | Emits | Fetcher |
|---|---|---|---|
| `poll` | an object store, with `directory`, `every`, readiness checks | one `Seen` per complete file per poll, plus `PollCompleted` | the same store |
| `subscribe` | a channel, with `subject` | one `Seen` per message | the route's `fetch.store`, using a path read from the message at `fetch.path` |

`poll` on SFTP is the connector's `watch(dir, every)` with overlap SKIP, readiness defaulting to
size stable twice 10 s apart plus minimum age one minute, and the connector's in-flight set
bounding how far the lister runs ahead. `poll` on S3 is a later adapter. `subscribe` on NATS is
milestone 2. While a subscribed transfer runs, the trigger tells the broker every
`inProgressEvery` (default 10 s) that the message is still being worked on, so a run longer than
the consumer's ack wait is not redelivered under our feet; the operator keeps `inProgressEvery`
below the consumer's ack wait, which the process cannot read.

### 5.2 Identity

A polled file's identity is store, directory, name, size and mtime, plus a revision that starts
at 1 and increases only when the same name, size and mtime come back with a different digest
after the earlier transfer finished (D2, Sec 4.3). Size and mtime are the cheap prefilter and
the digest is the authority; the check costs one download only in the collision case, because a
finished file normally leaves the source directory, and when it stays, under `none`, at most one
download per `recheckFinished` (Sec 4.3). A message's identity
is channel, subject and the message id, or a configured pointer into the body when the
broker's id is not stable across redeliveries.

### 5.3 Ack vocabulary

The ack is the commit action on the source side. Its vocabulary belongs to the trigger kind and
is validated at boot (rule 12):

| Trigger | `onAck` | `onNack` |
|---|---|---|
| poll on SFTP | `move: <folder>`, `delete`, `none` | `none` (the file stays; redelivery is the next poll) |
| poll on S3 | `delete`, `move: <prefix>`, `tag: <key=value>`, `none` | `none` |
| subscribe on NATS | `ack`, `term` | `nak` |
| any | `callback: <channel>` | as above |

`callback` is for an upstream that must be told before it considers the object released: the
call is synchronous, retried with the stage, and the transfer is not ACKED until it succeeds.
Rule for choosing: if a wrong answer from the call must stop the pipeline, it is an ack action;
if upstream only wants to know, it is a notification on a transfer state (Sec 9.1).

---

## 6. Processing

### 6.1 Staged object and payload

```kotlin
data class StagedObject(
    val name: String,          // the name the target will see; rename changes it
    val path: Path,            // local staged file, read-only to processors
    val size: Long,
    val mtime: Instant,        // the source's modification time; a new file gets the clock's
    val digest: Digest,        // algorithm + hex, computed as the bytes streamed
    val contentType: String?,
)
data class Payload(val objects: List<StagedObject>)
```

### 6.2 The `Processor` seam

```kotlin
interface Processor {
    val produces: Set<String>                                   // attribute names it may set; checked at boot
    suspend fun process(payload: Payload, ctx: ProcessContext): Outcome
}
sealed interface Outcome {
    data class Continue(val payload: Payload) : Outcome         // same, changed, longer or shorter list
    data class Reject(val reason: String) : Outcome             // the transfer becomes REJECTED
}
interface ProcessContext {
    val transfer: TransferView            // id, route, source identity, source path, first seen, parent id
    val source: SourceView                // poll: the listing entry; subscribe: message body and headers
    val attributes: Map<String, String>
    fun setAttribute(name: String, value: String)
    fun newStagedFile(name: String): Path // a file in staging the pipeline owns and deletes
    suspend fun fetch(store: String, path: String): StagedObject   // pull another object; used by expand
    val clock: Clock
}
```

Four rules make a re-run harmless, since a crash re-fetches the original and runs the chain
again (I18):

- Inputs are immutable; a new or changed file is created through `newStagedFile`.
- No side effects outside staging: no network, no database. Telling anyone is an ack action or
  a notification.
- Digests are the pipeline's job: every object in the final payload has its digest computed if
  its file is new. `DIGEST` is the stored object's; `SOURCE_DIGEST` is what was received.
- Cardinality decides rows: one object is the transfer, N objects are N children (Sec 4.5).

### 6.3 Built-in processors

| Name | Reads | Returns | Sets |
|---|---|---|---|
| `quality` | the file | unchanged, or Reject | nothing |
| `rename` | name, attributes, dates, through a pattern such as `{yyyyMMdd}-{name}` | one object with a new name, same file | nothing |
| `zip` | every object | one archive created through `newStagedFile` | nothing |
| `unzip` | one archive, up to `maxEntries` (default 10,000) entries and `maxBytes` (default 10 GiB) uncompressed | one object per entry, or Reject naming the limit and the count or size that broke it (D41) | nothing |
| `extract` | `from: fileName` (the current name), `from: sourcePath` (the listing path or object key), `from: content` (the bytes), or `from: message` (the subscription message) | unchanged, or Reject when the regex does not match | the regex's named groups become attributes of the same names, or positional groups named by `into: [..]`; for JSON, the map key is the attribute name and the pointer its source |
| `expand` | a metadata file or the message | one child per listed path, fetched through `ctx.fetch` | nothing |
| `custom` | anything | anything | what it declares in `produces` |

A custom processor is a Kotlin class implementing the seam, registered as a named CDI bean,
referenced by name with an optional `config` map handed to its constructor. Unknown names fail
boot (rule 15).

### 6.4 Attributes

A bounded map of string to string on the transfer row (rule 22: at most 32 entries, 1 KB
total), stored as JSON in one column and copied onto the stored object as user metadata, one
`x-amz-meta-attr-<name>` each, which is why they are bounded. Attributes freeze when the chain
ends; every mapping table of the route's channels is checked against them then, and a missing
required attribute fails the transfer before the store, naming the row and the attribute.

### 6.5 Digests

`digest` is a process-wide default with a per-route override: `md5`, `sha256` or `sha1`. The
algorithm is handed to whichever component fetches the bytes. A mapping row may ask for a
specific algorithm, in which case a second digest is computed during the same stream. With
`md5`, the S3 target sends `Content-MD5` on every PUT and compares the returned ETag on a
single-part object (Sec 7.2). Comparing against an upstream-supplied expected value is the
`verifyDigest` processor, reading the expected value from an attribute (D22 of the connector:
the transport computes, the application compares).

---

## 7. Targets

### 7.1 The `ObjectStoreTarget` seam

```kotlin
interface ObjectStoreTarget {
    suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef
    //  contract: afterwards the current object at key is the one just written; nothing is deleted
    suspend fun verify(ref: TargetRef): Boolean
    suspend fun probe()
}
data class TargetRef(val kind: String, val location: String, val key: String, val ref: String?, val size: Long)
```

The key is a pure function of the staged object's name and the route's `key` pattern, so a
retry overwrites instead of creating a sibling, and the target never deletes: what an overwrite
leaves behind is the bucket's business, not the pipeline's (D5). Metadata carries digest, digest algorithm,
source mtime, source name, transfer id and the attributes.

### 7.2 S3 target

AWS SDK v2, synchronous client over the Apache HTTP client, endpoint override, path-style
access, placeholder region, environment credentials, request and response checksum calculation
when-required (D4), timeouts with the API-call timeout below the drain (rule 3). `store` is PUT
with `Content-MD5` when the digest is MD5, then a HEAD comparing content length and, on a
single-part object without server-side encryption, the ETag against the MD5. Nothing is deleted:
versioning is bucket-wide, a PUT makes the new object the current version, a GET by key returns
only that one, and older versions are expired by the bucket's lifecycle rule for non-current
versions, a deployment prerequisite (D5). The credential the process runs with carries no delete
permission. The multipart threshold is pinned above the largest expected file so the ETag rule
holds; if the bucket ever encrypts, the adapter falls back to size plus metadata with a WARN at
startup. `verify` is a HEAD of key and version id. `probe` is a HEAD of the bucket and a read of
its lifecycle configuration: the bucket is never created, and a missing non-current-version
expiry is a WARN naming the bucket rather than a failure, because the process still works and
only the bucket grows. A crash between PUT and HEAD leaves one non-current version and is
repaired by the next `store`; that is the adapter's own contract test (I6).

### 7.3 SFTP target (milestone 2)

`store` uploads to `<name>.part` in the target directory and renames over `<name>` with the
connector's overwrite policy, so exactly one copy exists at the key; `verify` is a stat
comparing size; `probe` is the connector's startup probe on the directory. The connector's
`upload` and `rename` operations are the whole adapter.

---

## 8. Shuttle State Store

### 8.1 Tables

DDL applied by the DBA (D15); the code carries the reference text.

```sql
CREATE TABLE file_transfer (
  id                NUMBER(19)     NOT NULL,
  route             VARCHAR2(64)   NOT NULL,
  parent_id         NUMBER(19),                        -- set on child rows
  kind              VARCHAR2(16)   NOT NULL,           -- OBJECT | MESSAGE | CHILD
  source_kind       VARCHAR2(16)   NOT NULL,           -- SFTP | S3 | NATS
  source_ref        VARCHAR2(1024) NOT NULL,           -- store + directory, or channel + subject
  source_name       VARCHAR2(512)  NOT NULL,           -- file name, or message id
  source_size       NUMBER(19),
  source_mtime      TIMESTAMP,
  revision          NUMBER(5)      DEFAULT 1 NOT NULL,  -- next value when the same identity returns with different content
  supersedes_id     NUMBER(19),                        -- the finished row this revision replaces
  source_digest     VARCHAR2(128),
  digest            VARCHAR2(128),
  digest_algo       VARCHAR2(16),
  stored_name       VARCHAR2(512),
  stored_mtime      TIMESTAMP,
  state             VARCHAR2(16)   NOT NULL,
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  last_error        VARCHAR2(2000),
  attributes        CLOB,                              -- JSON map, bounded (rule 22)
  target_kind       VARCHAR2(16),
  target_location   VARCHAR2(255),                     -- bucket, or host + directory
  target_key        VARCHAR2(1024),
  target_ref        VARCHAR2(512),                     -- adapter-defined; S3: the version id
  target_size       NUMBER(19),
  first_seen_at     TIMESTAMP      NOT NULL,
  updated_at        TIMESTAMP      NOT NULL,
  acked_at          TIMESTAMP,
  completed_at      TIMESTAMP,
  CONSTRAINT pk_file_transfer PRIMARY KEY (id),
  CONSTRAINT fk_file_transfer_parent FOREIGN KEY (parent_id) REFERENCES file_transfer (id),
  CONSTRAINT fk_file_transfer_supersedes FOREIGN KEY (supersedes_id) REFERENCES file_transfer (id),
  CONSTRAINT uq_file_transfer_identity UNIQUE (route, source_ref, source_name, source_size, source_mtime, revision)
);
CREATE INDEX ix_file_transfer_state  ON file_transfer (route, state, updated_at);
CREATE INDEX ix_file_transfer_parent ON file_transfer (parent_id);

CREATE TABLE delivery_outbox (
  id                NUMBER(19)     NOT NULL,
  file_transfer_id  NUMBER(19)     NOT NULL,
  on_state          VARCHAR2(16)   NOT NULL,           -- the moment this notification announces, fixed for ever: FETCHED | STORED | ACKED
  channel           VARCHAR2(64)   NOT NULL,
  notification_state VARCHAR2(16)  NOT NULL,           -- the notification's own progress: PENDING | DELIVERED | FAILED
  attempts          NUMBER(5)      DEFAULT 0 NOT NULL,
  next_attempt_at   TIMESTAMP      NOT NULL,
  last_status       VARCHAR2(64),
  last_error        VARCHAR2(2000),
  reference         VARCHAR2(255),                     -- the id the channel returned for the delivered call
  created_at        TIMESTAMP      NOT NULL,
  delivered_at      TIMESTAMP,
  CONSTRAINT pk_delivery_outbox PRIMARY KEY (id),
  CONSTRAINT fk_delivery_transfer FOREIGN KEY (file_transfer_id) REFERENCES file_transfer (id),
  CONSTRAINT uq_delivery_on_state_channel UNIQUE (file_transfer_id, on_state, channel)
);
CREATE INDEX ix_delivery_due ON delivery_outbox (notification_state, next_attempt_at);
```

Two tables because a transfer has many deliveries, each with its own attempts, retry time and
reference (D3). No payload column: the body is rendered at send time from the row (D19). No
attempt table: every attempt logs transfer id, event, channel, attempt, status and reference.

### 8.2 The `StateStore` seam

```kotlin
interface StateStore {
    suspend fun find(identity: SourceIdentity): Transfer?
    suspend fun seen(identity: SourceIdentity, kind: TransferKind): Transfer
    suspend fun supersede(finished: TransferId, kind: TransferKind): Transfer                  // a new row, revision + 1, the old row untouched, 1 txn
    suspend fun fetched(id: TransferId, staged: StagedSummary, events: List<DeliveryRequest>)   // + FETCHED rows, 1 txn
    suspend fun processed(id: TransferId, attributes: Map<String, String>)
    suspend fun children(id: TransferId, staged: List<StagedSummary>): List<Transfer>          // 1 txn
    suspend fun stored(id: TransferId, target: TargetRef, events: List<DeliveryRequest>)     // + STORED rows; parent when last child
    suspend fun acked(id: TransferId, events: List<DeliveryRequest>)                           // ACKED (+ children) + DONE rows; DONE when none
    suspend fun rejected(id: TransferId, reason: String)
    suspend fun failedAttempt(id: TransferId, error: String, maxAttempts: Int): Transfer
    suspend fun unlisted(route: RouteName, olderThan: Instant, listed: Set<SourceIdentity>): List<TransferId>
    suspend fun due(now: Instant, excluding: Set<DeliveryId>, limit: Int): List<Delivery>
    suspend fun delivered(id: DeliveryId, reference: String?)                                 // DONE flip when last
    suspend fun retryLater(id: DeliveryId, at: Instant, status: String?, error: String)
    suspend fun deliveryFailed(id: DeliveryId, status: String?, error: String)
    suspend fun redrive(id: TransferId)
    suspend fun redriveDelivery(id: DeliveryId)
    suspend fun stuck(route: RouteName, olderThan: Instant): Int
}
```

Every method is one transaction; `fetched`, `stored`, `acked` and `delivered` are the ones
that must be atomic across both tables (I11, I20).

---

## 9. Notifications

### 9.1 Notification moments

A route attaches channel deliveries to transfer states: `on: fetched`, `on: stored`,
`on: acked`, each naming the state whose transition creates the row. Each attachment is one
outbox row created in the transaction that defines that state (I20), delivered asynchronously
and at-least-once by the notifier. On the row, `on_state` says which moment the notification
announces, written from the route's `on:` key, and never changes even as the transfer moves on
to DONE; it is not a copy of the transfer's `state` but the reason the row exists, and it is
what tells two notifications to one channel apart. `notification_state` says how far the
sending itself has got, PENDING, DELIVERED or FAILED, and is what the notifier advances. The notifier needs no
route: the row names its channel, and the body is rendered from the transfer row it points to.
DONE is not a notification moment: it is reached only once every notification is delivered, so
a route with no attachments goes ACKED to DONE in the same transaction (I17).

### 9.2 The `DeliveryChannel` seam

```kotlin
interface DeliveryChannel {
    val name: ChannelName
    val policy: DeliveryPolicy
    suspend fun deliver(event: DeliveryEvent): DeliveryOutcome    // Delivered(reference) | Retry(status, reason) | Reject(status, reason)
}
```

`CancellationException` is never caught or converted.

### 9.3 Policy

Per channel: `maxAttempts` 50, `giveUpAfter` 24 h, exponential backoff from 5 s, factor 2, cap
15 min, full jitter, `timeout` 10 s below the drain (rule 3). A FAILED delivery never changes
the transfer's state: the object is safe; the transfer stays ACKED with a metric and a log line
naming the delivery (D9).

### 9.4 Notifier

One coroutine per process, a cold flow: select due PENDING rows excluding the in-flight ids,
bounded by `batch`; `buffer(batch)`; `flatMapMerge(workers)`; record each outcome in `finally`.
`emit` suspends when the buffer is full, so the next select runs after the previous batch
drained (I5). A conflated wake signal follows every transaction that creates rows; a sweep every
`sweepEvery` (30 s) is the guarantee. Cancelling the scope leaves rows PENDING (I12). Rows never
ride a `SharedFlow` (D7).

### 9.5 The notifier's in-flight set

A concurrent set of delivery ids selected and not yet recorded, added at select and removed in
`finally` on every exit path, bounded by `batch + workers`, empty when idle (I4, I5). It holds
ids only and must not survive a restart. A state-store claim is the second-replica seam (Sec
15.1).

### 9.6 Body: the mapping table

A channel's body is a list of rows, each producing one JSON path (dotted paths nest):

| Row key | Meaning |
|---|---|
| `field: <NAME>` | a column of the transfer row, from the fixed vocabulary below |
| `attribute: <name>` | an attribute set during processing or from the source event |
| `provider: <bean>` | a named Kotlin bean returning a JSON node, mounted whole at the path; `select: <pointer>` picks a piece; memoized per rendering |
| `value: <literal>` | a constant |
| `type: string \| number \| boolean` | coercion, default string |
| `format: <spec>` | timestamps (`ISO_INSTANT`, a pattern) and numbers |
| `default: <literal>`, `trim`, `upper`, `lower` | the only transformations a row can apply; none compose |
| `required: true` (default) | a missing value fails the transfer at attribute freeze; `false` omits the path |
| `digest: <algo>` | with `field: DIGEST`, asks for a specific algorithm |

Vocabulary: `TRANSFER_ID`, `PARENT_ID`, `ROUTE`, `KIND`, `SOURCE_KIND`, `SOURCE_REF`,
`SOURCE_NAME`, `SOURCE_PATH`, `SOURCE_SIZE`, `SOURCE_MTIME`, `SOURCE_DIGEST`, `STORED_NAME`,
`STORED_MTIME`, `DIGEST`, `DIGEST_ALGO`, `TARGET_KIND`, `TARGET_LOCATION`, `TARGET_KEY`,
`TARGET_REF`, `TARGET_SIZE`, `FIRST_SEEN_AT`, `ACKED_AT`, `EVENT`, `ATTEMPT`.

Boot checks: every `field` is in the vocabulary; every `provider` resolves to a bean; every
`attribute` is declared by a processor in that route; every
`select` is a valid pointer; every `format` parses (rules 16 to 19). A code-built configuration
may also give a Kotlin lambda producing a Jackson tree, the only escape hatch (D8).

### 9.7 Downstream obligations

Delivery is at-least-once. A receiver must treat a repeated call for the same transfer id and
event as the same event. The reference it returns is per call and never a dedup key.

---

## 10. Routes at Runtime

- **Independence.** Each route is one collector under its own supervisor with its own
  parallelism. Routes share the state store, the object stores' pools and the notifier.
- **Supervision.** A route whose trigger terminates, the connector's fatal error for example,
  is restarted by the process with exponential backoff from `initial` to `max` (30 s to 15
  min), each restart logged and counted. The route's gauge is 0 while down.
- **Readiness.** `all-routes-down`: the pod is unready only when every route is down. A
  partially healthy pod keeps serving. A configuration error or a failed probe of a store or
  channel at startup ends startup instead.
- **Pools.** One object store declaration is one connector and one pool, shared by every route
  on it. The session cap is per account, so the operator rule is one declaration per account
  with `pool.maxSize` at the account's cap (20 today). Rule 9: per store, the sum of
  `parallelism` over every route that polls it, fetches from it or targets it, plus one lister
  per polled directory, must be at most `maxSize`, because a session is consumed by listing,
  fetching and uploading alike; a route that uses one store as both source and target is counted
  twice on purpose. A route that states no `parallelism` runs one pipeline. `maxConcurrentTransfers`
  is the shared bulkhead. Two declarations for one account cannot be detected and would defeat
  the cap.

---

## 11. Failure Model

| Failure | Where | Effect |
|---|---|---|
| Connector recoverable error after its retry budget | fetch, ack | `failedAttempt`; `nack(redeliver = true)`; next trigger retries; FAILED at `maxAttempts` |
| Trigger terminates | route | route down; supervised restart with backoff; readiness per Sec 10 |
| Target client or 5xx | store, verify | SDK retries first; then as recoverable above |
| Target 4xx | store | same path, logged at ERROR: configuration for ops to fix, the object waits |
| State store unavailable | any transition | as recoverable; a run with the store down completes nothing (S16) |
| Staging volume below `staging.minFree` | before fetch | deferred: `nack(redeliver = true)`, no attempt counted, `shuttle_staging_deferred_total`; the next trigger retries |
| `unzip` beyond `maxEntries` or `maxBytes` | process | Reject, REJECTED until re-drive; the limit and the value are in the reason |
| Processor Reject | process | REJECTED, terminal until re-drive |
| Processor throws | process | as recoverable; five throws is FAILED |
| Missing required mapping input | attribute freeze | FAILED with the row named; no retry until re-drive after a fix |
| Channel Retry | deliver | backoff per policy |
| Channel Reject or policy exhausted | deliver | delivery FAILED; transfer stays ACKED |
| Callback ack fails | ack | as recoverable; the transfer is not ACKED until it succeeds |
| Poll longer than the interval | poll | `PollSkipped`; alert |
| Process killed | anywhere | Sec 4.4 |

Every pipeline error is caught at the object boundary: recorded, staging deleted, the trigger
told, the coroutine ends. Nothing propagates to the collector except cancellation and the
trigger's termination.

Stuck detection: transfers before ACKED older than `stuckAfter` (three intervals), refreshed
at every `PollCompleted` and every `sweepEvery` for subscriptions, and the oldest PENDING
delivery's age. Alert inputs; no action.

---

## 12. Startup, Validate Mode, Shutdown

### 12.1 Startup

1. Load and validate configuration (Sec 13.3). Any violation ends startup listing every rule
   broken, not the first.
2. State store: one round trip per table. A missing table ends startup naming the DDL.
3. Object stores and channels: `probe()` each declared one; nothing is created.
4. Staging directories emptied (D17).
5. Named beans resolved: every `custom` processor and `provider`.
6. Connectors started, with their own probes (marker rename into every ack target).
7. Notifier started, then every route.

### 12.2 Validate and try modes

`shuttle validate <files>` runs steps 1 and 5 only, connects to nothing, prints every violation
with its rule number, exits non-zero on any. This is what lets operations edit YAML without a
build and without a deployment.

`shuttle try --route <name> --file-name <name> [--source-path <path>] [--content <file>]
[--message <file>]` goes one step further for one route: it validates, then runs the route's
processing chain over the sample inputs with a fake context in a temp directory, and prints the
attributes each step set, the key the target would use, and the rendered body for every channel
the route notifies. It connects to nothing and stores nothing. This is where a user finds that a
regex group is named `orderNo` while the mapping says `orderNumber`, the way Logstash's grok
debugger or Benthos's mapping evaluator would show it, before any deployment (D35). Custom
processors run too, so one that reads a network would fail here first, which is the rule
anyway.

### 12.3 Shutdown

Bounded by `drainTimeout` (60 s), from the Quarkus shutdown event: readiness false; cancel the
route collectors, which drains each connector under its own bound; cancel the notifier, leaving
rows PENDING; close the S3 clients, then the datasource. Every stage timeout, channel timeout
and the connectors' drain plus cancel grace fit inside `drainTimeout` (rule 3), and the pod's
termination grace period exceeds it. The chain, each value below the next because a blocking PUT
cannot be interrupted: the largest expected PUT (about 20 s for 10 MB), `apiCall` 45 s,
`drainTimeout` 60 s, termination grace period 90 s in the manifest (D39).

---

## 13. Configuration

### 13.1 YAML, the primary form

```yaml
shuttle:
  shuttleStateStore:
    oracle: { datasource: shuttle }            # every transfer's progress, durable, two tables
  notifier:   { workers: 4, batch: 50, sweepEvery: 30s }
  supervision:
    restartBackoff: { initial: 30s, max: 15m }
    readiness: all-routes-down
  digest: md5                                  # process default; a route may override
  drainTimeout: 60s                            # below the pod's 90 s termination grace period

  objectStores:                                # where files live; source, target or fetch location
    vendor:
      sftp:
        host: sftp.example
        port: 22
        auth: { user: ${SFTP_USER}, password: ${SFTP_PASSWORD} }
        hostKey: acceptAll                     # warns at startup
        idleCutoff: 5m                         # the proxy's idle limit still applies even without a proxy block
        pool: { maxSize: 20, maxConcurrentTransfers: 16 }
        staging: { dir: /var/shuttle/stage/vendor, minFree: 1g }   # below minFree a fetch is deferred (Sec 4.1)
    partner:
      sftp:
        host: partner.example
        auth: { user: ${PARTNER_USER}, password: ${PARTNER_PASSWORD} }
        pool: { maxSize: 4 }                     # rule 9: mirror (1, the default) + image-sets (2) both target it
        staging: { dir: /var/shuttle/stage/partner }               # minFree defaults to 1g
    minio:
      s3:
        endpoint: https://minio.internal
        region: us-east-1
        pathStyle: true
        credentials: { accessKey: ${S3_ACCESS_KEY}, secretKey: ${S3_SECRET_KEY} }
        timeouts: { connect: 5s, socket: 30s, apiCall: 45s }   # apiCall below drainTimeout (rule 3)

  channels:                                    # where messages go, or come from
    downstream:
      http:
        method: POST
        url: https://downstream.internal/api/files
        auth: { bearer: ${DOWNSTREAM_TOKEN} }
        timeout: 10s
        response: { success: [200-299], retry: [408, 429, 500-599], reference: /requestId }
        policy: { maxAttempts: 50, giveUpAfter: 24h, backoff: { initial: 5s, max: 15m } }
        body:
          - { path: fileId,          field: TRANSFER_ID }
          - { path: file.name,       field: STORED_NAME }
          - { path: file.size,       field: TARGET_SIZE }
          - { path: file.md5,        field: DIGEST }
          - { path: location.bucket, field: TARGET_LOCATION }
          - { path: location.key,    field: TARGET_KEY }
          - { path: receivedAt,      field: SOURCE_MTIME, format: ISO_INSTANT }
          - { path: orderNumber,     attribute: orderNumber }
          - { path: order,           provider: orderDetails }
          - { path: event,           field: EVENT }               # fetched | stored | acked, so the receiver can route on it
          - { path: source,          value: vendor-drop }
    upstream-receipt:
      http:
        method: POST
        url: https://upstream.internal/api/received
        auth: { header: { name: X-Api-Key, value: ${UPSTREAM_KEY} } }
        timeout: 5s
        response: { success: [200-299], retry: [500-599] }
        body:
          - { path: object, field: SOURCE_PATH }
          - { path: md5,    field: SOURCE_DIGEST }
    events:
      nats: { url: nats://events.internal:4222, credentials: ${NATS_CREDS} }

  routes:
    vendor-drop:                               # milestone 1
      source:
        poll:
          store: vendor
          directory: /inbox
          every: 1h
          readiness: [ { sizeStable: { checks: 2, interval: 10s } }, { minAge: 1m } ]
          onAck: { move: temp/ }
      process:
        - { extract: { from: fileName, regex: "(?<orderNumber>\\d+)-.*\\.csv" } }
        - { rename: { pattern: "{yyyyMMdd}-{name}" } }
        - { zip: {} }
      target: { store: minio, bucket: landing, key: "vendor/{name}" }
      notify:
        - { on: acked, channel: downstream }
      parallelism: 4
      maxAttempts: 5
      stuckAfter: 3h
      recheckFinished: 24h                     # a finished file still listed is digested again at most this often (Sec 4.3)

    mirror:                                    # move A to B, tell nobody
      source: { poll: { store: vendor, directory: /outbound, every: 15m, onAck: delete } }
      target: { store: partner, directory: /incoming }

    # an unzip step reads `- { unzip: { maxEntries: 10000, maxBytes: 10g } }`; both limits default to those values

    image-sets:                                # milestone 2
      source:
        subscribe: { channel: events, subject: images.ready, onAck: ack, inProgressEvery: 10s }
      fetch: { store: minio, path: /metadata.path }
      process:
        - { extract: { from: message, json: { batchId: /batchId } } }
        - { expand: { format: json, files: "/images[*].path", from: minio } }   # quoted: `[` may not start inside a flow mapping's plain scalar
        - { custom: imageResizer, config: { maxWidth: 2048 } }
      target: { store: partner, directory: /incoming }
      notify:
        - { on: fetched, channel: upstream-receipt }
        - { on: acked,    channel: downstream }
      parallelism: 2
```

### 13.2 Kotlin DSL

The same model, for tests and code-built routes:

```kotlin
shuttle {
    shuttleStateStore { oracle(datasource = "shuttle") }
    notifier { workers = 4; batch = 50; sweepEvery = 30.seconds }
    supervision { restartBackoff(30.seconds, max = 15.minutes); readiness = AllRoutesDown }
    digest = Digest.MD5
    drainTimeout = 60.seconds

    objectStores {
        sftp("vendor") { endpoint { host = "sftp.example" }; auth { password(env("SFTP_USER"), env("SFTP_PASSWORD")) }
                         pool { maxSize = 20; maxConcurrentTransfers = 16 }
                         staging { dir = Path("/var/shuttle/stage/vendor"); minFree = 1.gib } }
        s3("minio") { endpoint = "https://minio.internal"; pathStyle = true
                      credentials = fromEnvironment("S3_ACCESS_KEY", "S3_SECRET_KEY") }
    }
    channels {
        http("downstream") {
            method = POST; url = "https://downstream.internal/api/files"; auth = bearer(env("DOWNSTREAM_TOKEN"))
            response { success = 200..299; retry = setOf(408, 429) + (500..599); reference = "/requestId" }
            body = mapping {
                "fileId"      from TRANSFER_ID
                "file.md5"    from DIGEST
                "orderNumber" fromAttribute "orderNumber"
                "order"       by provider("orderDetails")
                "source"      value "vendor-drop"
            }
        }
    }
    route("vendor-drop") {
        source = poll(objectStore("vendor"), directory = "/inbox") { every = 1.hours; onAck = move("temp/") }
        process = extract(from = FileName, regex = "(?<orderNumber>\\d+)-.*\\.csv") then rename("{yyyyMMdd}-{name}") then zip()
        target = objectStore("minio").bucket("landing") { key = "vendor/{name}" }
        notify(on = Acked, channel("downstream"))
        parallelism = 4
        recheckFinished = 24.hours
    }
}
```

### 13.3 Validation rules

Each is public numbering, reported by number in validate mode and at startup.

| Rule | Statement |
|---|---|
| 1 | Every name a route references exists in `objectStores` or `channels` |
| 2 | The referenced declaration offers the role used: poll or target on an object store, subscribe or notify on a channel, and the adapter implements that role |
| 3 | Every S3 `apiCall` timeout, channel `timeout`, and each connector's drain plus cancel grace is below `drainTimeout` |
| 4 | Route names, store names and channel names are unique; a store and a channel may not share a name |
| 5 | A route has exactly one `source` and exactly one `target` |
| 6 | A `subscribe` source has a `fetch` with a store and a path; a `poll` source has none |
| 7 | `parallelism >= 1` (1 when omitted), `maxAttempts >= 1`, `stuckAfter > 0`, `inProgressEvery > 0`, `recheckFinished >= 0` (24 h when omitted), every store's `staging.minFree >= 0` (1 GiB when omitted) |
| 8 | Every `notify.on` is one of `fetched`, `stored`, `acked`; a pair of state and channel appears once per route |
| 9 | Per object store, the sum of `parallelism` over every route that polls it, fetches from it or targets it, plus one lister per polled directory, is at most `pool.maxSize`, and `maxConcurrentTransfers <= maxSize` |
| 10 | Every SFTP store's `keepAlive` and `idleTimeout` are below its `idleCutoff` |
| 11 | Every staging directory exists, is writable, and is local disk; two stores do not share one |
| 12 | `onAck` is stated explicitly, no default, and it and `onNack` belong to the trigger kind's vocabulary; a `callback` names a channel offering the notify role |
| 13 | A `key` or `directory` pattern uses only `{name}` (the staged object's name at store time, after the chain), `{sourceName}` (the source object's original name), `{yyyyMMdd}` and attribute names declared in the route, and yields no `..` |
| 14 | Every built-in processor's configuration parses: patterns compile, pointers are valid, `extract.from` is one of `fileName`, `sourcePath`, `content`, `message` with `message` only on a subscribed route, a regex has named groups or an `into` list whose length equals its group count, `expand.from` names a store, `unzip.maxEntries >= 1` and `unzip.maxBytes > 0` |
| 15 | Every `custom` processor and every `provider` resolves to a named bean |
| 16 | Every mapping `field` is in the vocabulary |
| 17 | Every mapping `attribute` is declared by a processor in that route |
| 18 | Every mapping `select` is a valid JSON pointer and every `format` parses |
| 19 | A mapping row has exactly one of `field`, `attribute`, `provider`, `value` |
| 20 | A channel's `response.success` and `response.retry` are disjoint status sets |
| 21 | `digest` is `md5`, `sha256` or `sha1`; a mapping `digest` request is one of them |
| 22 | Attribute limits: a route's declared attribute names number at most 32 and each name is at most 64 characters |
| 23 | A `move` ack target is not the polled directory itself; the connector excludes it from listing |
| 24 | `readiness` is `all-routes-down` or `any-route-down`; `restartBackoff.initial <= max` |
| 25 | A secret appears only as a `${VAR}` reference, never as a literal |

---

## 14. Admin and Metrics

### 14.1 Admin

| Endpoint | Does |
|---|---|
| `GET /admin/shuttle/routes` | per route: up or down, last trigger, restart count, counts by state |
| `GET /admin/shuttle/transfers?route=&state=&limit=` | transfer rows, children folded under parents |
| `GET /admin/shuttle/transfers/{id}/deliveries` | event, channel, state, attempts, last status, reference, delivered time |
| `POST /admin/shuttle/transfers/{id}/redrive` | REJECTED or FAILED to SEEN |
| `POST /admin/shuttle/transfers/{id}/ack` | STORED to ACKED by hand: an operator override for a source the process can no longer reach, never a recovery path the design relies on |
| `POST /admin/shuttle/deliveries/{id}/redrive` | FAILED to PENDING; wakes the notifier |
| `POST /admin/shuttle/routes/{name}/restart` | restart a route now, resetting its backoff |

All under the host's admin role. No endpoint triggers a poll.

### 14.2 Metrics

Micrometer through the host's registry. Tags `route`, `channel`, `store`; never a name, id or key.

| Metric | Type | Tags / notes |
|---|---|---|
| `shuttle_transfers_total` | counter | `route`, `outcome`: done, rejected, failed, reacked |
| `shuttle_stage_seconds` | timer | `route`, `stage`: fetch, process, store, ack; `result`: ok, error |
| `shuttle_inflight` | gauge | `route` |
| `shuttle_children_total` | counter | `route` |
| `shuttle_stuck_transfers` | gauge | `route` |
| `shuttle_reconciled_total`, `shuttle_reconcile_skipped_total` | counter | `route` |
| `shuttle_poll_total` | counter | `route`, `result`: completed, failed, skipped |
| `shuttle_route_up` | gauge | `route` |
| `shuttle_route_restarts_total` | counter | `route` |
| `shuttle_delivery_total` | counter | `channel`, `event`, `outcome`: delivered, retry, rejected, gave_up |
| `shuttle_delivery_seconds` | timer | `channel` |
| `shuttle_outbox_pending`, `shuttle_outbox_oldest_seconds` | gauge | `channel` |
| `shuttle_notifier_inflight` | gauge | |
| `shuttle_supersedes_total` | counter | `route`; a finished identity came back with different content and got a new revision |
| `shuttle_staging_free_bytes` | gauge | `store`; usable space of the staging volume, refreshed before every fetch |
| `shuttle_staging_deferred_total` | counter | `route`; fetches deferred because the volume was below `staging.minFree` |

---

## 15. Known Limitations and Extensions

### 15.1 Second replica

Needs a claim before fetch on the source side, which the connector reserves as its own
extension, and an IN_FLIGHT delivery state with a claim timestamp and stale-claim reset in the
state store, replacing the notifier's in-flight set. Neither is built (D13).

### 15.2 Retention

DONE and FAILED rows accumulate; a retention sweep is a later ticket with its own decision on
how long "did object X get through" must stay answerable.

### 15.3 Expression language

Not built (D30). Revisit trigger: when the provider catalogue passes roughly ten beans that each
do one obvious thing, reopen the decision with that evidence.

### 15.4 Multiple targets per route

A second target needs a `target_copy` table beside the outbox and a crash row per target,
because the ack may happen only when every target holds the object. Not built; a route whose
target is another route's polled directory is two routes and works today.

### 15.5 Cron alignment and manual polls

The connector's ticker fires every `every` from route start; alignment to the hour and a manual
poll are appeals to the connector's spec.

---

## 16. Decision Log

| ID | Decision | Rationale |
|---|---|---|
| D1 | The application owns one durable state store in Oracle; the connector's `SeenRepository` is unused | The connector filters before emitting; a transfer recorded as uploaded but not yet acked would be filtered out and never acked. Two ledgers are two truths |
| D2 | A polled file's identity is store, directory, name, size, mtime and a revision; a message's is channel, subject, message id | Matches the connector's in-flight key; a re-drop with a new mtime is a new object; a re-drop with the same mtime and different content is a new revision, found by digest after one download, because re-acking it would move corrected content aside unread (v0.3) |
| D3 | Two tables, no attempt table, no payload column; attempts trace by log | One transfer has many deliveries; an attempt history nobody asked to query is a third table |
| D4 | AWS SDK v2 synchronous, Apache client, checksums when-required, path style | Sequential per-object work gains nothing from async; SDK checksums can fail against older MinIO |
| D5 | Key is a pure function of the object's name; the S3 target overwrites and never deletes; the bucket's lifecycle rule expires non-current versions | Versioning is bucket-wide; a deterministic key makes retry an overwrite; a GET by key already returns only the current version, so the v0.2 prune bought nothing except the power to delete a copy an earlier transfer had delivered and announced (v0.3) |
| D6 | Order: store, then ack and ledger ACKED in the order the source needs, then notify; a polled file is moved first and written ACKED after, reconciliation repairing the gap; a subscribed message is written ACKED first and acked at the broker after, redelivery repairing the gap; `acked` deliveries are created in the ACKED transaction | The source-side ack is the commit; a move is visible to the next listing and a broker ack is not, so each order puts the repairable step last (v0.3) |
| D7 | The notifier is a cold flow with a buffer and a wake; rows never ride a `SharedFlow`; an in-memory in-flight set guards double selection | Cold gives backpressure by suspension and loses nothing; `SharedFlow` broadcasts, drops without subscribers and never completes |
| D8 | Channel seam is one suspend function; the body is a mapping table rendered at send time; a Kotlin lambda is the code-only escape hatch | The notifier must not know HTTP; a table is reviewable by non-Kotlin readers and loadable from YAML |
| D9 | Per-channel policy; a transfer is DONE when every delivery is DELIVERED; a FAILED delivery never fails the transfer | The object is safe once ACKED; webhook practice retries per endpoint and dead-letters |
| D10 | Poison objects: FAILED after `maxAttempts`, left in place, re-drivable, never deleted | Deleting is the one irreversible action |
| D11 | Quality is a processor; `Reject` is its Fail | One seam for every step; completeness on the source belongs to readiness, integrity to the fetch |
| D12 | The connector's `watch` polls; the notifier is a coroutine loop; no Quarkus scheduler | One scheduling model in the process; cost: no top-of-hour alignment |
| D13 | One replica | Both in-memory sets are per process; both second-replica seams are named, neither built |
| D14 | Credentials from environment variables, populated by Vault; rotation is a rollout | The SDKs read them without configuration |
| D15 | The DBA applies DDL; buckets, directories and subjects are never created | An ambient side effect at boot is what provisioning exists to avoid |
| D16 | The S3 store is verified by content length, and by ETag against MD5 on single-part unencrypted objects, plus digest metadata; `Content-MD5` accompanies every MD5 PUT | Cheap post-conditions the server enforces; the multipart threshold is pinned above the largest file so the ETag rule holds |
| D17 | Staging is emptied at startup; fetches are redone | A staged file from a dead process has no row that vouches for it |
| D18 | One module; adapters are packages; ArchUnit sentences | Every seam has a real second implementation |
| D19 | Bodies are rendered at send time from the row; no payload column | A stored payload freezes a shape across deployments |
| D20 | The pipeline consumes sources through `RouteEvent` and a `Fetcher` it owns; only `sftp`, `nats` and `quarkus` import their technologies | Every phase but the bindings builds before the connector exists; the scripted source is the second implementation |
| D21 | Object stores and channels are declared once and given a role at the route; no `Source` interface, registry or plugin discovery | One SFTP server may be source for one route and target for another without duplicating secrets; a technology is not a role; the route names the direction at the use site |
| D22 | The target seam is `store`, `verify`, `probe` with the promise "exactly one copy at the key"; nothing about versions | The pipeline should not know what kind of place the target is; S3 versioning was leaking into the crash matrix |
| D23 | One `Processor` seam with `Continue` and `Reject`; quality, rename, zip, extract, expand and custom code are all processors; cardinality of the result decides child rows | Three special cases collapsed into one contract with four re-run rules |
| D24 | Processors are pure over staging: immutable inputs, no network, digests computed by the pipeline | A crash re-runs the chain; only a side-effect-free chain makes that harmless |
| D25 | Attributes are a bounded string map on the row and on the object; producers declare them; the mapping is checked against them at attribute freeze, before the store | A notification is never created that cannot be rendered, and the check happens while fixing it costs nothing |
| D26 | Notifications on transfer states `fetched`, `stored`, `acked`, each created in the transaction defining that state; the ack-versus-notification rule | Reliability comes from the outbox; a callback that must gate progress is an ack action |
| D27 | Ack vocabulary per trigger kind, plus `callback` | The no-data-loss argument depends on knowing what the commit does to the source |
| D28 | The unit of tracking, ack and `acked` notification is the source object; children never notify | One row, one ack, one notification, whatever the fan-out |
| D29 | YAML is the primary configuration, Kotlin DSL the model, numbered validation rules, a `validate` mode connecting to nothing | Operations edit data; the repository's SimpleEtl precedent; Benthos-style lint |
| D30 | No expression language; a table row has `format`, `default`, `trim`, `upper`, `lower` and nothing that composes; conditionals are providers | Smaller surface, reviewable by data; revisit trigger in Sec 15.3 |
| D31 | Routes are supervised with capped backoff; readiness `all-routes-down` by default | With per-route health nobody restarts the pod for one route |
| D32 | One object store declaration per account; pool arithmetic validated per declaration | The cap is per account and two declarations cannot be reconciled by the process |
| D33 | Digest algorithm is a process default with route override; MD5 is the first deployment's | Downstream expects MD5; MD5 buys `Content-MD5` and the ETag check for free |
| D34 | No logger in any context object; correlation through MDC | The repository's rule is direct JBoss logging; MDC gives correlation to every logger in the call |
| D35 | A `try` mode renders one route's chain and bodies over sample inputs offline | Validation proves shape, not outcome; every comparable tool gives users a way to see what a sample produces before deploying, and ours costs nothing because the chain and the renderer are already pure |
| D36 | Rule 9 counts every role a route gives a store, and a missing `parallelism` is 1 | A session is consumed by listing, fetching and uploading alike; 1 is the only default that cannot break a cap by omission |
| D37 | Two children of one parent on one key reject the transfer | The alternative is one child silently overwriting its sibling |
| D38 | The NATS trigger sends in-progress signals every `inProgressEvery` while a transfer runs | A run longer than the consumer's ack wait would otherwise be redelivered mid-flight |
| D39 | The timeout chain is PUT, `apiCall`, `drainTimeout`, termination grace, each below the next; reference values 20 s, 45 s, 60 s, 90 s | A blocking PUT cannot be interrupted, so every layer must outlast the one inside it; the reference configuration must pass its own validator |
| D40 | A finished polled identity is re-digested at most once per `recheckFinished`, measured from the row's `updated_at`; inside the window it is skipped without a fetch or a write | Identity carries mtime, so a file that stays under `none` is the same identity on every poll and D2's digest check would download the whole directory every interval; the row's own timestamp throttles it with no new column and no in-memory memory (v0.4) |
| D41 | Staging is bounded in bytes: `staging.minFree` defers a fetch below the watermark without counting an attempt; `unzip` rejects beyond `maxEntries` or `maxBytes` | Parallelism bounds pipelines, not bytes; an archive that expands past the volume evicts the pod, and a deferral is disk pressure, not the object's fault, so it must not walk the object to FAILED (v0.4) |
| D42 | A child's STORED is one statement on its own row plus a conditional parent update; no parent lock per child | N children of one parent store under the route's parallelism, and a parent lock taken by each would serialise the fan-out on one Oracle row (v0.4) |

---

## 17. Open Items Before Implementation

1. MinIO server version, and whether server-side encryption is on for the bucket (decides the
   ETag rule and whether SDK checksums could return).
2. Downstream tolerates repeated calls per transfer id and event, and returns a per-call reference.
3. The uploader's write convention on the vendor SFTP server (the connector's open item 1).
4. Temp folder ownership on the vendor server.
5. The lifecycle rule expiring non-current versions on the bucket, retention one year, in place
   before the first deployment; `probe` warns when it is missing (D5).
6. Top-of-hour alignment, if required, against the connector's `watch`.
7. Oracle schema and sequence names; the datasource the state store shares with.
8. The pod's termination grace period set to 90 s in the manifest (D39).
9. NATS JetStream stream and consumer configuration for `images.ready`, whether the message
   id is stable across redeliveries (Sec 5.2), and the consumer's ack wait, which
   `inProgressEvery` must stay below (D38).
10. The partner SFTP server's session cap and rename semantics (POSIX rename extension or not).
11. The connector's D21 records a five-session cap; infra now says 20 per account. An appeal to
    record in the connector's progress log.

---

## 18. Acceptance Plan

Three tiers, the connector's shape. **Milestone 1** is the vendor-drop and mirror routes:
poll on SFTP, S3 and SFTP targets are not both needed (S3 only), HTTP channel, rename, zip,
extract, providers. **Milestone 2** is the image-sets route: NATS subscribe, fetch from S3,
expand with children, the SFTP target, `fetched` notifications, callback acks.

1. **Fakes, no I/O.** In-memory state store, in-memory target, recording channel, scripted
   source, scripted fetcher, hook driver, clock fixture. Everything in Sec 4, 6, 9, 10 proven
   here under `runTest`; no `Thread.sleep`.
2. **Real adapters, one at a time.** State store on Testcontainers Oracle, tagged `oracle`; S3
   target on Testcontainers MinIO with versioning on, tagged `minio`; HTTP channel against a
   loopback JDK `HttpServer`; SFTP through the connector's embedded SSHD; NATS on Testcontainers,
   tagged `nats`, milestone 2.
3. **End to end.** Each milestone's routes through every real adapter, with the crash matrix
   replayed by killing and restarting at every hook point.

### 18.1 Invariants

| ID | Invariant |
|---|---|
| I1 | A transfer reaches DONE only if `verify` of its target reference, or of every child's, is true |
| I2 | The application never deletes a source object except through a configured `delete` ack; the only source writes are ack actions |
| I3 | A delivery row is DELIVERED only after a channel returned Delivered for it |
| I4 | A delivery id is never inside two workers at once |
| I5 | The notifier's in-flight set never exceeds `batch + workers` and is empty when idle |
| I6 | After `store` returns, the current object at the key is the one just written, including after a crash inside a previous `store` on the same key; `store` never deletes |
| I7 | A REJECTED or FAILED transfer is neither stored nor delivered until re-driven |
| I8 | Restart at any hook point converges to DONE with at most one extra store and one extra delivery per channel per event |
| I9 | Staging holds no file that is not inside a running pipeline |
| I10 | The ack action runs only when the transfer, and every child, is STORED and verified |
| I11 | Each transition that creates delivery rows is one transaction with the row change; DELIVERED and the DONE flip are one transaction |
| I12 | Shutdown returns within `drainTimeout` and leaves every PENDING row PENDING |
| I13 | Two channels on one event are delivered independently |
| I14 | Every validation rule of Sec 13.3 rejects a configuration that violates it, by number |
| I15 | Attributes never change after the chain ends, and a mapping table is checked against them before the store |
| I16 | A parent is acked only when every child is STORED, and a failed child fails the parent |
| I17 | A route with no notifications goes ACKED to DONE in one transaction and creates no outbox row |
| I18 | A processor never modifies an input file, and every file it creates is deleted with the staging area |
| I19 | Per object store, sessions in use by pipelines in any role plus listers never exceed the pool's `maxSize` |
| I20 | A notification row exists if and only if the transition to its transfer state was committed |
| I21 | A dead route is restarted with backoff between `initial` and `max`, and readiness follows the configured rule |
| I22 | A provider is invoked at most once per rendering however many rows select from it |
| I23 | A subscribed transfer whose message is redelivered after ledger ACKED ends acked with exactly one set of outbox rows |
| I24 | A finished identity that returns with a different digest becomes a new revision; the finished row and its target version are never modified |

### 18.2 Scenario table

| ID | Scenario | Expected |
|---|---|---|
| S1 | Vendor-drop happy path, one file, one channel | DONE; object with metadata and attributes; file in temp; one delivery with reference |
| S2 | Crash after store, before ledger (and, in the S3 tier, inside store between PUT and HEAD) | Next poll: store again; one current copy, one non-current version left for the lifecycle rule; DONE |
| S3 | Crash after ledger STORED, before ack | Next poll: verify, ack, no second store |
| S4 | Crash after the move, before ledger ACKED | Reconciliation marks ACKED and creates `acked` deliveries; DONE |
| S5 | Crash after delivery sent, before ledger | Delivered again; two calls with one transfer id; row DELIVERED once |
| S6 | Copy missing at STORED | Full run on the same row; DONE |
| S7 | Downstream 503 twice then 200 | Two Retry with backoff, then DELIVERED; attempts 3 |
| S8 | Downstream 400 | Reject; delivery FAILED; transfer ACKED; `rejected` counted |
| S9 | Downstream down past `giveUpAfter` | FAILED with `gave_up`; re-drive returns it to PENDING and it delivers |
| S10 | Processor Reject | REJECTED; nothing stored; object stays; re-drive re-runs from fetch |
| S11 | Fetch fails five polls in a row | FAILED; `nack(redeliver = false)`; after restart the store still answers FAILED |
| S12 | Same identity re-dropped after DONE | Fetched and digested. Same digest: verify, ack again, `reacked`, no store, no delivery. Different digest: revision 2 created, stored, acked, notified; revision 1 and its target version untouched |
| S13 | 5,000 files of 10 MB in one poll | All DONE; in-flight never above `parallelism`; staging bounded; no skipped poll next tick |
| S14 | Listing truncated at `maxFilesPerPoll` | Reconciliation skipped and counted |
| S15 | Shutdown during store and during delivery | Rows stay PROCESSED and PENDING; close within bound; staging empty at next start |
| S16 | State store unavailable for one poll | Every object nacked with redelivery; nothing stored; next poll completes all |
| S17 | Two channels on `acked`, one always 503 | The other delivers; transfer ACKED; pending gauge shows one |
| S18 | Wrong SFTP password | Route down; supervised restarts with backoff; readiness per rule; process alive |
| S19 | Mirror route, no notifications | ACKED to DONE in one transaction; no outbox row |
| S20 | Rename then zip | One archive stored under the renamed key; `STORED_NAME` differs from `SOURCE_NAME`; `SOURCE_DIGEST` and `DIGEST` differ |
| S21 | An attribute extracted from the file name used by the mapping | Body carries it; a route whose mapping names an undeclared attribute fails validation by rule 17 |
| S22 | One provider selected by three rows | One invocation; three paths filled |
| S23 | Two routes, one dead | The other keeps completing; readiness true under `all-routes-down` |
| S24 | Pool arithmetic exceeded | Rejected by rule 9 in validate mode and at startup |
| S25 | Validate mode on a file with five violations | Five rule numbers listed; exit non-zero; no connection opened |
| S26 | Missing required attribute at freeze | FAILED before the store, reason names the row; nothing stored |
| S27 | Image-sets happy path (M2) | Parent FETCHED; N children stored on the SFTP target; message acked once; `fetched` and `acked` delivered once each |
| S28 | Crash with half the children stored (M2) | Next redelivery: children verified, the rest stored, parent acked; message acked once |
| S29 | One child fails five times (M2) | Parent FAILED; message not acked; re-drive re-runs the chain |
| S30 | `callback` ack returns 500 then 200 (M2) | Transfer stays STORED through the failure; ACKED after the 200; one `acked` delivery |
| S31 | `shuttle try` on the vendor-drop route with a sample name | Prints the attributes per step, the key, and one body per notified channel; opens no connection; a mapping naming an attribute the regex does not produce is reported by rule 17 |
| S32 | Subscribed transfer: crash after ledger ACKED, before the broker ack (M2) | Message redelivered; verify, broker acked, `reacked`; outbox rows unchanged; no operator involved |
| S33 | `unzip` yields `a/x.csv` and `b/x.csv` under one key pattern | REJECTED with both paths in the reason; nothing stored |

---

## 19. Changes from v0.1

- Name: SFTP Ingest becomes Shuttle; module `shuttle`, package root `infra.shuttle`, metric
  prefix `shuttle_`, docs under `docs/shuttle/`.
- Vocabulary: object stores and channels declared once with roles at the route; triggers `poll`
  and `subscribe`; the ack vocabulary per trigger kind; `callback` acks.
- Ledger becomes the shuttle state store; `file_transfer` gains `parent_id`, `kind`,
  `source_*`, `stored_*`, `attributes`; `delivery_outbox` gains `on_state` and its own `state` becomes `notification_state`.
- Quality check becomes one of several processors under a single `Processor` seam; attributes
  and providers added; the mapping table replaces the Kotlin body builder as the primary form.
- Notifications on transfer states `fetched`, `stored`, `acked`; a route may notify nobody.
- YAML as the primary configuration with numbered rules, a validate mode and a try mode; Kotlin DSL kept as
  the model.
- Route supervision with backoff; readiness `all-routes-down`; pool arithmetic per store.
- Digest algorithm configurable, MD5 first, with `Content-MD5` and ETag checks on S3.
- Two milestones; the acceptance plan grows from 14 to 22 invariants and 18 to 31 scenarios.

---

## 20. Changes from v0.2

Three defects found by an independent review of v0.2, each settled by grilling:

- The S3 target no longer deletes. v0.2 pruned every other version at the key inside `store`,
  which erased the copy an earlier transfer had delivered and announced whenever a corrected
  file arrived under the same name, and made two same-named children of one parent re-store
  each other for ever. A GET by key returns only the current version with no deletion at all,
  so the bucket's lifecycle rule owns expiry and the process holds no delete permission (D5,
  I6, S2). Same-named children reject the transfer instead (D37, S33).
- Identity gains a revision, and a finished identity that comes back is digested before it is
  re-acked. v0.2 would have moved a corrected file with the same name, size and mtime aside
  without uploading it (D2, I24, S12, `revision` and `supersedes_id` in 8.1, `supersede` in 8.2).
- The ack order depends on the trigger kind. A subscribed message is written ACKED before the
  broker ack, so the crash between them is repaired by redelivery instead of by an operator
  (D6, 4.4, 4.6, I23, S32). Polled files keep move-then-ledger with reconciliation.
- The reference configuration now passes its own rules: rule 3 with `apiCall` 45 s under
  `drainTimeout` 60 s under a 90 s grace period (D39), rule 9 counting every role a route gives
  a store with `parallelism` defaulting to 1 (D36), and `partner` sized for both routes that
  target it.
- The NATS trigger sends in-progress signals while a transfer runs (D38, `inProgressEvery`).
- The acceptance plan grows to 24 invariants and 33 scenarios.

---

## 21. Changes from v0.3

Three watch-outs raised by a staff engineer's review, each settled against the v0.3 text:

- A finished file that stays in its directory is no longer downloaded on every poll. v0.3 said
  the digest check "costs one download only in the collision case, because a finished file
  normally leaves"; under `onAck: none`, or while a `move` keeps being refused, nothing leaves,
  and every interval re-downloaded the directory. `recheckFinished` throttles the check from
  the row's `updated_at` (D40, Sec 4.3, rule 7).
- Staging is bounded in bytes. `staging.minFree` defers a fetch below the watermark without
  counting an attempt, and `unzip` has `maxEntries` and `maxBytes` that reject a bomb (D41,
  Sec 4.1, 6.3, 11, rules 7 and 14, two meters in 14.2). `staging` is now an object with `dir`.
- Child completion takes no lock on the parent. The v0.3 design already batched child creation,
  bounded child stores by parallelism and let only the parent notify, so the review's other
  concern was met; what remained was the trap of a per-child parent lock, now ruled out (D42,
  Sec 4.5).
