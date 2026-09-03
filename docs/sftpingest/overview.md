# SFTP Ingest - The Picture

Companion to `spec.md`. The spec is the authority; this page exists so a reader gets the shape
in five minutes and knows which five decisions carry the requirements. Section numbers below
point into the spec.

---

## Part 1 - For everyone

### The job, as a mailroom

Every hour a courier checks an inbox on the SFTP server. For each envelope in it, the courier:

1. **Copies** it into the vault (the MinIO bucket) and checks the copy is complete.
2. **Stamps** the original and moves it to the "done" tray on the same server (the temp folder,
   which downstream empties on its own schedule).
3. **Tells** everyone who cares (the downstream systems) that the copy is in the vault.

The whole time, the courier writes every step for every envelope in a **logbook** (the ledger,
two tables in Oracle). If the courier collapses halfway, the next courier opens the logbook,
sees exactly where each envelope stopped, and continues from there. Nothing is done twice
unless the logbook could not be written in time, and nothing is ever skipped.

```mermaid
flowchart LR
    A[(SFTP inbox)] -->|1 copy| B[(MinIO vault)]
    A -->|2 move original| T[(temp tray)]
    B -->|3 tell| D[Downstream systems]
    L[[Logbook<br/>Oracle]] -. records every step .- A
    L -. records every step .- B
    L -. records every step .- D
```

### The three promises

| Promise | How it is kept |
|---|---|
| **Nothing is lost.** | A file leaves the inbox only after its copy is verified in the vault. The move is the last thing done to the original. A crash before the move leaves the file where it was, to be redone. |
| **Nobody is told too early.** | The notification is written to the logbook in the same stroke as "moved", and a separate loop sends it. It can only ever be sent after the copy exists and the original is moved. |
| **One bad envelope never stops the rest.** | Each file is handled on its own. A file that fails five times is marked FAILED, left in the inbox, and an operator decides. A file that fails quality is marked REJECTED the same way. Neither is ever deleted. |

### What can go wrong, and why it is fine

| If the courier collapses... | ...the next courier | Cost |
|---|---|---|
| while copying | copies again | one extra copy, overwritten |
| after copying, before writing it down | copies again | one extra copy, overwritten |
| after writing "copied", before moving | checks the vault, then moves | none |
| after moving, before writing "moved" | notices the file is gone from the inbox, writes "moved" | the notification waits one hour |
| after telling downstream, before writing it down | tells them again | downstream sees the same file id twice and ignores the repeat |

That table is the whole safety argument. Every row is a test (spec 17.2, S2 to S5).

---

## Part 2 - For engineers

### The whole system in one picture

One process, three components, four things outside it. The connector owns the server side of
the conversation and hands each ready file to the consumer as an event with an ack and a nack.
The pipelines do the work for one file and write every step to the ledger. The relay is a
second, independent loop that turns ledger rows into downstream calls. The pipelines and the
relay never talk to each other except through the ledger and one wake signal, which is what
lets either be restarted without the other noticing.

```mermaid
flowchart LR
    subgraph sftp["SFTP server"]
        IN["inbox/"]
        TMP["temp/ (purged by downstream)"]
    end

    subgraph proc["sftp-ingest process · one replica · Kotlin + Quarkus"]
        subgraph conn["SFTP connector (its own spec)"]
            TK["watch ticker · every 1 h"] --> LS["lister + readiness"]
            POOL["session pool · 5 max"]
            IFF["in-flight files · memory"]
        end
        subgraph pipe["Consumer + per-file pipelines · ×4 in parallel"]
            direction TB
            P1["1 decide entry point from the ledger"] --> P2["2 download through the connector"]
            P2 --> P3["3 quality check (NONE today)"] --> P4["4 PUT · HEAD · prune other versions"]
            P4 --> P5["5 ledger → UPLOADED"] --> P6["6 ack() → move to temp/ · commit point"]
            P6 --> P7["7 ledger → ACKED + PENDING per channel · 1 txn"]
        end
        subgraph relay["Relay · one coroutine, cold Flow"]
            direction LR
            R1["select due"] --> R2["buffer"] --> R3["workers ×4"] --> R4["record outcome"]
            RIF["in-flight delivery ids · memory"]
        end
        STG[("staging · local disk")]
    end

    subgraph ora["Oracle ledger · durable · the only truth"]
        FT[("file_transfer")]
        DO[("delivery_outbox")]
    end
    MINIO[("MinIO bucket · versioning on")]
    DS["Downstream · HTTP · dedupes on fileId"]

    IN -- "list · download (JSch)" --> POOL
    POOL -- "rename → temp/" --> TMP
    LS -- "FileSeen(file, ack, nack)" --> P1
    P2 -- "download()" --> POOL
    POOL -- ".part → file" --> STG
    P6 == "ack()" ==> POOL
    P4 -- "PUT · HEAD · prune (S3 SDK v2)" --> MINIO
    P5 == "JDBI" ==> FT
    P7 == "JDBI · ACKED + PENDING" ==> DO
    P7 -. "wake" .-> R1
    R1 == "select due (JDBI)" ==> DO
    R4 == "DELIVERED / retry / FAILED" ==> DO
    R3 -- "POST body (JDK HttpClient)" --> DS
    DS -. "2xx + request id" .-> R3
```

Thick edges are the durable commit path; dotted edges are signals or responses, never data;
the two memory boxes are the only state that must not survive a restart.

### Three pieces of state, three owners

Everything else is stateless and can be restarted at any moment.

```mermaid
flowchart TB
    subgraph connector["SFTP connector (its own spec)"]
        IF["in-flight set of files<br/>(in memory; which files are emitted and not yet acked)"]
    end
    subgraph ledger["Ledger (Oracle, durable)"]
        FT["file_transfer<br/>one row per file identity<br/>SEEN → DOWNLOADED → UPLOADED → ACKED → DONE"]
        DO["delivery_outbox<br/>one row per file × channel<br/>PENDING → DELIVERED | FAILED"]
    end
    subgraph relay["Relay (in memory)"]
        RS["in-flight set of delivery ids<br/>(bounded; empty when idle)"]
    end
    FT --- DO
```

- The **connector** remembers only which files it has emitted and not yet heard back about.
  It never persists anything (connector D14).
- The **ledger** is the single source of truth for what happened to a file. File identity is
  name + size + mtime (D2).
- The **relay's set** is not a cache and not a store: it is a "these rows are taken" guard so
  a delivery in flight is not selected twice (spec 7.4).

### One file's journey

```mermaid
sequenceDiagram
    autonumber
    participant C as Connector
    participant P as Pipeline (one coroutine per file)
    participant S as MinIO (S3 SDK)
    participant L as Ledger (Oracle)
    participant R as Relay
    participant D as Downstream

    C->>P: FileSeen(file, ack, nack)
    P->>L: find(identity) → decide entry point
    P->>C: download → staged file + SHA-256
    P->>P: quality check (NONE today)
    P->>S: PUT key (digest as metadata)
    P->>S: HEAD → size matches
    P->>S: list versions, delete all but the new one
    P->>L: UPLOADED (key, version id)
    P->>C: ack() → move to temp/
    P->>L: ACKED + one PENDING delivery per channel (one transaction)
    P-)R: wake
    R->>L: select due PENDING rows
    R->>D: POST body built from the ledger row
    D-->>R: 2xx + request id
    R->>L: DELIVERED (reference) → DONE when every channel delivered
```

The ack (step 9) is the **commit point** for the source: it is the last write to the original,
and it happens only after the copy is verified (I10). Everything after it is made reliable by
the outbox, not by ordering (D6).

### The state machine, with the crash points

```mermaid
stateDiagram-v2
    [*] --> SEEN: FileSeen, no row
    SEEN --> DOWNLOADED: staged + digest
    DOWNLOADED --> REJECTED: quality Fail
    DOWNLOADED --> UPLOADED: PUT + HEAD + prune
    UPLOADED --> ACKED: move to temp/ (+ PENDING deliveries)
    ACKED --> DONE: every delivery DELIVERED
    SEEN --> FAILED: attempts = max
    DOWNLOADED --> FAILED: attempts = max
    UPLOADED --> FAILED: attempts = max
    REJECTED --> SEEN: re-drive
    FAILED --> SEEN: re-drive
    DONE --> [*]

    note right of UPLOADED
        Crash before this row: redo from download.
        Crash after it: HEAD the object, then ack only.
    end note
    note right of ACKED
        Crash between the move and this row:
        reconciliation at the next poll writes it,
        because the file is gone from the listing
        and the object is proven to exist.
    end note
```

Entry points are decided from the ledger on every `FileSeen` (spec 4.3): anything below
UPLOADED restarts from download; UPLOADED and above verify the object and ack; REJECTED and
FAILED are nacked without work.

### The second loop: the relay

```mermaid
flowchart LR
    Q["select PENDING<br/>next_attempt_at ≤ now<br/>id ∉ in-flight set<br/>limit batchSize"] --> B["buffer(batchSize)<br/>emit suspends when full"]
    B --> W1[worker] & W2[worker] & W3[worker]
    W1 & W2 & W3 --> O{outcome}
    O -->|Delivered| DEL["DELIVERED + reference<br/>DONE if all channels"]
    O -->|Retry| RT["next_attempt_at = now + backoff<br/>attempts + 1"]
    O -->|Reject or policy exhausted| FL["delivery FAILED<br/>transfer stays ACKED"]
    DEL & RT & FL -->|finally: remove id| Q
    ACK["pipeline: acked()"] -. wake .-> Q
    T["sweep every 30 s"] -. wake .-> Q
```

A cold `Flow` with `buffer` and `flatMapMerge`, not a `SharedFlow`: a shared flow broadcasts
to every subscriber, drops values when nobody is collecting, and never completes, each of
which is wrong for a work queue (D7). The buffer bounds memory to `batchSize + parallelism`
rows; cancellation leaves rows PENDING, which is the correct shutdown.

### The five decisions that carry the requirements

Ranked. If you read nothing else in the spec, read these entries in its decision log.

| # | Decision | Requirement it carries | Spec |
|---|---|---|---|
| 1 | **The ledger is the only truth; the connector persists nothing.** | No data loss across a crash; at-least-once with a known resume point for every file. | D1, 4.3, 4.4 |
| 2 | **Ack is the commit; deliveries are created in the ACKED transaction; reconciliation repairs move-then-crash.** | Nobody is told before the file is safe; the notification is durable the instant the source is committed. | D6, 4.5, I10, I11 |
| 3 | **Deterministic key, prune every other version after every PUT.** | "Delete the old version if we upload twice" on a bucket whose versioning cannot be turned off; a retry is an overwrite, never a sibling. | D5, 6.3, I6 |
| 4 | **Cold-flow relay with a bounded buffer and an in-memory in-flight guard; per-channel policy; a failed delivery never fails the file.** | High throughput without unbounded memory; one slow channel never blocks another; a dead downstream is a dead-letter, not a stuck pipeline. | D7, D9, 7.3, 7.4, I4, I5, I13 |
| 5 | **Every timeout below the drain; bounded parallelism everywhere; one replica.** | Stability: a pod restart at any moment converges (I8); shutdown is bounded (I12); the five-session cap on the server is respected. | 3.3, 11.2, D13, I14 |

### Where each requirement lands

| Requirement from the brief | Where |
|---|---|
| Run hourly, all files under one folder | connector `watch(dir, every = 1h)`, overlap SKIP (spec 9, D12) |
| Move to temp after processing | connector `onAck = move("temp/")`, called only after UPLOADED (4.1, I10) |
| Quality check seam, no-op today | `QualityCheck.NONE` on the complete staged file (8, D11) |
| Download, upload, delete old version, notify | stages 1 to 5, prune in stage 3 (4.1, 6.3) |
| Multiple notification channels | `DeliveryChannel` seam, one outbox row per channel, HTTP first (7) |
| No data lost | crash matrix and I8 (4.4, 17.1) |
| High performance | bounded parallel pipelines, relay batch and buffer, S13 load scenario (9, 7.3) |
| High stability | failure model, startup checks, ordered shutdown (10, 11) |

### What is deliberately not here

No second replica, no streaming, no attempt-history table, no content parsing for the body,
no bucket or table creation, no Quarkus scheduler. Each has a named seam or a recorded reason
in spec 14 and 15.
