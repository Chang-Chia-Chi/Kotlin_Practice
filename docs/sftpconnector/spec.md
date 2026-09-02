# SFTP Connector - Design Spec

Version: v0.1 (agreed in design review, not yet implemented)
Scope: one connector per SFTP endpoint, single consumer per watched directory, in-process callers
Status: ready for a phase plan

---

## 1. Background and Goals

### 1.1 Problem

The ETL service must pull files from a remote SFTP server on a schedule, hand them to a
pipeline (record, upload to MinIO, mark done on the server) and do the same for uploads and
other file operations later. The network between the service and the server passes through an
HTTP CONNECT proxy with a five minute idle cutoff, and the server is an ordinary OpenSSH host
that this team does not operate. The only JVM SFTP clients are blocking, and the host service is
Kotlin with coroutines. Nothing in the JVM ecosystem gives a pooled, cancellable, Flow-shaped
SFTP source with resilience built in, and Camel would replace the service's programming model
rather than fit into it.

### 1.2 Goals

- A **connection pool** over JSch sessions with the semantics of a mature pool: bounded size,
  suspending acquire with a timeout, max lifetime with jitter, idle timeout, keepalive,
  validation on borrow, leak detection, and eviction of poisoned sessions.
- A **client** offering the common operations (list, stat, download, upload, rename, delete,
  mkdir, exists) as suspend functions, with reconnect transparent to the caller.
- A **source** that exposes a directory as a cold `Flow` of events with per-file ack and nack,
  readiness checks, bounded in-flight work and an overlap policy for the poll ticker.
- **Resilience** through Resilience4j's coroutine module: retry, circuit breaker, bulkhead,
  placed so that a failing server or proxy is backed off rather than hammered.
- A **graceful shutdown** that stops new work, drains leases within a bound, then forces the rest.
- A **Kotlin DSL** for configuration that validates at build time.
- A core module free of any application framework; Quarkus appears only in an adapter.

### 1.3 Non-goals

- No push or notify. SFTP has none; the source polls.
- No exactly-once delivery. The connector is at-least-once; the application deduplicates.
- No multi-instance coordination. One consumer per directory (Sec 14.2 keeps the seam).
- No streaming transfer in v1. Files are at most tens of megabytes; downloads stage on local
  disk (Sec 14.1 keeps the seam).
- No resume of partial transfers in v1 (Sec 14.1).

---

## 2. Terminology

| Term | Definition |
|---|---|
| Endpoint | One SFTP server address plus one credential. One connector serves one endpoint. |
| Session | One JSch `Session` with exactly one `ChannelSftp` open. The unit the pool manages. |
| Entry | The pool's record of one session: the session, its state, timestamps and counters. |
| Lease | The caller-facing handle to a borrowed entry. Released exactly once. |
| Poison | A flag set on an entry when an error proves the session unusable. A poisoned entry is closed on release, never reused. |
| Tick | One execution of the poll cycle for a watched directory. |
| Poll | One listing of a directory plus readiness checks, producing events. |
| In-flight | A file that has been emitted and neither acked nor nacked. |
| Ack | The consumer's statement that a file is done. Triggers the ack action (Sec 8). |
| Nack | The consumer's statement that a file failed. Triggers the nack action and, by default, redelivery on a later tick. |
| Readiness | A check that a listed file is complete on the server and safe to read. |
| Recoverable | An error that waiting or retrying may cure. |
| Fatal | An error that no amount of waiting cures: bad credentials, rejected host key, invalid configuration. |

---

## 3. Overall Model

### 3.1 Layers

```
 caller (pipeline, scheduler)
   │ Flow<SftpEvent>, suspend ops
   ▼
 source        watch / poll, readiness, in-flight set, ack + nack, overlap policy
   │
 client        list, stat, download, upload, rename, delete, mkdir, exists, withSession
   │           per-operation retry semantics, error mapping
 resilience    Retry( CircuitBreaker( Bulkhead( TimeLimiter( op ) ) ) )   (Resilience4j)
   │
 pool          registry under one Mutex, idle deque, Semaphore capacity, housekeeper, leases
   │
 transport     SftpTransport interface, one adapter: JSch (mwiede fork), blocking calls on a
   │           bounded IO dispatcher, three-tier cancellation
   ▼
 SSH via HTTP CONNECT proxy -> OpenSSH
```

Every layer above `transport` is free of JSch types. The pool manages `Transport.Connection`
objects it cannot inspect, the client calls transport operations, and the source calls the
client. This is the seam that lets sshj or Apache MINA SSHD replace JSch later without touching
the pool or the flow layer (D2).

### 3.2 Module boundaries

| Module | Depends on | Contains |
|---|---|---|
| `sftp-core` | kotlin-stdlib, kotlinx-coroutines, JSch (mwiede), resilience4j-kotlin, micrometer-core, slf4j-api | everything in Sec 3.1 |
| `sftp-quarkus` | `sftp-core`, Quarkus arc, config, micrometer | CDI producer, config mapping to the DSL, shutdown hook, registry binding |
| `sftp-testkit` | `sftp-core`, Apache MINA SSHD | embedded server, fault hooks, fake transport |

ArchUnit enforces: `sftp-core` never imports Quarkus; only the `transport.jsch` package imports
`com.jcraft`. Logging in core is `org.slf4j`, which Quarkus routes into its log manager without
configuration (D3).

### 3.3 Thread model

JSch blocks. Every JSch call runs on `Dispatchers.IO.limitedParallelism(n)` where `n` defaults
to `pool.maxSize`. This dispatcher is the first bulkhead: a stalled server can pin at most `n`
threads, and the rest of `Dispatchers.IO` stays free for the host. JSch additionally owns one
reader thread per open session. Coroutines pay off only in orchestration: acquire, waiting on
state, cancellation and the flow layer. The host runs JDK 17, so virtual threads are not an
option (D4).

---

## 4. Connection Pool

### 4.1 Structure

Copied from HikariCP and Commons Pool 2 rather than invented: a flat registry of all entries,
idle and in use, with per-entry state. Nobody drains a queue to evict an expired entry; expiry
is a state transition.

- **Registry**: every entry the pool has created and not yet closed. Guarded by one `Mutex`.
- **Idle deque**: entries in state `Idle`, LIFO. Warm sockets are reused first and cold ones
  age toward eviction.
- **Capacity**: a `Semaphore(maxSize)` bounding `idle + inUse + connecting`. Acquire takes a
  permit with a timeout before touching the registry.
- **Entry state**: a `StateFlow<EntryState>`: `Connecting`, `Idle`, `InUse`, `Validating`,
  `Evicting`, `Closed`. Waiters observe the state rather than register callbacks.

Rule: **no I/O inside the mutex.** Storage decisions are made under the lock and executed
outside it using the transitional states. Connect, validate and close all happen while the entry
is in `Connecting`, `Validating` or `Evicting` and the lock is not held.

### 4.2 Acquire

1. Take a permit, waiting at most `acquireTimeout`. Timeout throws `PoolExhaustedException`
   carrying pool statistics (active, idle, pending) so the log line explains itself.
2. Under the mutex, pop the most recently used idle entry. If none, register a new entry in
   `Connecting` and release the mutex.
3. Outside the mutex: connect a new entry, or validate a popped one if it has been idle longer
   than `validationBypass`. Validation is one round trip (`realpath "."`). A failed validation
   closes the entry and loops back to step 2 with the permit still held.
4. Transition to `InUse`, return a `Lease`.

Creating a session costs a TCP handshake through the proxy, key exchange, authentication, a
channel open and a forked `sshd` process on the server, which also drops new connections once
more than ten are unauthenticated. One validation round trip is cheaper than any of that, so
validation replaces creation, not the other way round (D5).

### 4.3 Release

Under the mutex: if the entry is poisoned or past its lifetime, transition to `Evicting`;
otherwise push to the idle deque and record `lastUsed`. Outside the mutex: close evicted entries.
Release the permit last, in both paths, so a waiter can never see capacity that does not exist.

### 4.4 Lease

The lease is the only handle callers see. It carries a release-once guard, the poison flag the
error mapper sets, and the acquisition timestamp and stack trace when leak detection is on.
`use { }` is the normal path and releases in `finally`; bare `acquire()` is advanced usage.
A second release is logged as a bug and ignored. Leak detection logs the acquire stack trace of
any lease held longer than `leakDetectionThreshold`; it never forces release, because a live
JSch call cannot be safely interrupted from outside (D6, Sec 5.3).

### 4.5 Housekeeper

One coroutine per pool running every `housekeepingInterval` (default 30 s):

- Under the mutex, collect idle entries past `maxLifetime` (with the entry's own jitter) or past
  `idleTimeout` while `idle > minIdle`, and transition them to `Evicting`. Close them outside.
- Entries in use past `maxLifetime` are flagged and evicted on release.
- Top up to `minIdle` by registering `Connecting` entries and connecting outside the lock.
- Keepalive is delegated to JSch's server-alive interval on each session, so a dead session is
  usually detected by the session itself before validation on borrow finds it.

Lifetime jitter is per entry, uniformly `[0, maxLifetimeJitter × maxLifetime]`, so a pool that
filled at startup does not expire all at once (HikariCP's variance rule).

### 4.6 Proxy and server idle limits

The configuration validator requires `keepAlive < idleCutoff` and `idleTimeout < idleCutoff`,
where `idleCutoff` is the smallest idle limit in the path: the proxy's five minutes here.
Defaults: keepalive 30 s, idle timeout 4 min, max lifetime 30 min.

---

## 5. Transport

### 5.1 Interface

`SftpTransport` opens a `Connection`; a `Connection` offers `list(path, selector)`, `stat`,
`openRead`, `openWrite`, `rename`, `delete`, `mkdir`, `realpath`, `abort()` and `close()`. The
JSch adapter is the only implementation in production; the testkit provides a scripted fake for
pool and source tests that never open a socket.

### 5.2 JSch adapter

- mwiede JSch. jcraft 0.1.55 lacks rsa-sha2 signatures, which modern OpenSSH requires (D7).
- One `Session`, one `ChannelSftp`. OpenSSH caps channels per session at ten and channel state
  such as the working directory is per channel; sharing a session across operations serializes
  them anyway.
- `Session.setTimeout(socketTimeout)`, `setServerAliveInterval(keepAlive)`, `ProxyHTTP` for
  the CONNECT proxy.
- Host key policy is an explicit enum: `Strict(knownHosts)`, `Fingerprint(sha256)`,
  `AcceptAll`. `AcceptAll` is never the DSL default and logs a warning at startup (D8).
- Rename uses the `posix-rename@openssh.com` extension when the server advertises it; the
  overwrite policy (Sec 8.2) accounts for servers that do not.

### 5.3 Cancellation: three tiers

JSch calls do not observe coroutine cancellation and blocking socket reads do not observe
`Thread.interrupt`. The adapter therefore cancels in three tiers, and only the last one
destroys the session (D9).

| Tier | Mechanism | Effect on the session |
|---|---|---|
| Cooperative | Transfers run with a `SftpProgressMonitor` whose `count` returns `false` once the coroutine is cancelled; listings run with an `LsEntrySelector` that returns `BREAK`. JSch closes the remote handle cleanly. | Usable, returned to the pool after validation |
| Socket timeout | `socketTimeout` on the session unblocks a stalled read with an exception. | Poisoned, evicted |
| Forced | If neither tier has unblocked the call within `cancelGrace` (default 5 s), the cancellation handler calls `abort()`, which disconnects the session from another thread. | Poisoned, evicted |

Resilience4j's `TimeLimiter` on a suspend function is `withTimeout`, which enters this ladder at
the cooperative tier. The socket timeout is what actually bounds a hung server.

### 5.4 Error mapping

All JSch errors are mapped in one class into the hierarchy of Sec 10. `SftpException` carries an
SSH_FX status code and maps by code. `JSchException` carries only a message and maps by a
maintained table of message prefixes (`Auth fail`, `timeout`, `session is down`,
`ProxyHTTP`, `UnknownHostKey`, `channel is not opened`). Unmapped messages classify as
recoverable and poisoned, never as fatal, so a new message wording degrades to a retry rather
than to a dead connector.

---

## 6. Client Operations

### 6.1 Operations

| Operation | Signature (abridged) | Retry semantics on a fresh session |
|---|---|---|
| `list` | `(dir, filter, maxEntries): Flow<RemoteFile>` | Blind retry |
| `stat` | `(path): RemoteFile?` | Blind retry |
| `download` | `(remote, localTarget): LocalFile` | Restart from zero into a fresh `.part` file |
| `upload` | `(local, remote, overwrite)` | Restart; remote partial is overwritten |
| `rename` | `(from, to, overwrite)` | On `NoSuchFile` after a retry, stat `to`; existing with the expected size counts as success |
| `delete` | `(path)` | `NoSuchFile` after a retry counts as success |
| `mkdir` | `(path, parents)` | `AlreadyExists` counts as success |
| `exists` | `(path): Boolean` | Blind retry |
| `withSession` | `(block: suspend Connection.() -> T): T` | No retry; the caller owns semantics |

"Transparent reconnect" means: an operation that fails with a poisoned session is retried on a
new lease within the retry budget of Sec 9, using the row above, so a session that dies mid-call
produces neither a caller-visible error nor a phantom failure (D10). It never means swapping the
session underneath a live call.

### 6.2 Session affinity

Sessions are fungible and the pool assigns them. Listing and downloading always run on different
leases, since a JSch channel serializes operations and pinning the lister for a download batch
would starve the tick. `withSession` exists for callers who need several operations on one
lease, such as working-directory affinity; it is not needed by the source.

### 6.3 Download staging

Download writes `<stagingDir>/<name>.part`, verifies the byte count against the listed size,
then renames atomically to `<name>`. An abort deletes the `.part` file, so no partial file
survives a run. The staging directory must be local disk; on NFS the rename and delete semantics
your migration notes describe would apply (D11).

---

## 7. Source

### 7.1 Shape

```
poll(dir, options): Flow<SftpEvent>          one listing, terminates when the listing is consumed
watch(dir, every, options): Flow<SftpEvent>  repeats poll on a ticker, never terminates on
                                             recoverable errors
```

Both are **cold** flows. Collection starts the work, cancellation stops it, and backpressure is
suspension: the lister suspends when the consumer is busy. A hot `SharedFlow` would either
suspend the producer, which defeats being hot, or drop file events, which is never acceptable
(D12). A caller wanting fan-out applies `shareIn` and owns the buffer policy.

Events carry metadata, never bytes:

```
sealed interface SftpEvent
  PollStarted(tick, dir)
  FileSeen(file: RemoteFile, ack: suspend () -> Unit, nack: suspend (reason, redeliver) -> Unit)
  FileGone(file)                 listed, then absent at download time
  PollSkipped(tick, cause)       overlap policy or breaker open
  PollFailed(tick, error)        recoverable error; watch continues
  PollCompleted(tick, seen, emitted, notReady)
```

### 7.2 Ack and nack

The ack model gives backpressure, post-processing and redelivery in one mechanism (D13).

- `ack()` runs the ack action (Sec 8.1) and releases the in-flight slot.
- `nack(reason, redeliver = true)` runs the nack action, releases the slot and, when
  `redeliver` is true, allows the file to be emitted on a later tick. `redeliver = false`
  excludes it until restart.
- Cancellation of the collector with unacked files is treated as nack with redelivery.
- Ack and nack are valid in any state of the event, including before any download. A
  consumer whose ledger already knows the file calls `ack()` and never downloads. This follows
  the messaging model (Kafka offset commit, NATS ack and term) rather than Camel or Spring
  Integration, which filter before emitting and therefore never move an already-seen file.
- Each of ack and nack is accepted once; the second call is logged and ignored.
- `ackWait` (optional) makes an unacked file eligible again after the duration, like NATS.
  Off by default: with a single consumer, a stuck file is a consumer bug to surface, not to
  hide.

`consume(dir, every) { file -> ... }` wraps `watch`: it acks when the block returns and nacks
when it throws. It is the documented normal path; manual ack is for pipelines that commit late.

### 7.3 In-flight set and backpressure

The source keeps an in-memory set of in-flight files keyed by path, size and mtime. A file in the
set is not emitted again by an overlapping or later tick. `maxInFlight` bounds the set; when it
is full the lister suspends until an ack or nack arrives. This is the backpressure knob that
protects the downstream, and it is the only state the connector holds about processed files.
Persistent idempotency belongs to the application (Sec 8.3).

### 7.4 Listing

`SSH_FXP_READDIR` returns entries in batches and JSch's selector sees each entry as it
arrives, so listing is a `channelFlow` with a bounded buffer and never materializes a
directory. `maxFilesPerPoll` stops the listing early. `sortBy` requires materialization and is
honored only together with `maxFilesPerPoll`, as Camel does. Directories are skipped by default;
`recursive` descends but always excludes the ack and nack target folders (Sec 8.2).

### 7.5 Readiness

`interface ReadinessCheck { suspend fun check(file, ctx): Readiness }` where `ctx` offers `stat`
and the clock, and `Readiness` is `Ready`, `NotReady(reason)` or `Skip`. Built-ins:

| Check | Meaning | Caveat |
|---|---|---|
| `SizeStable(checks, interval)` | Size unchanged across `checks` stats `interval` apart, inside one poll | A stalled uploader passes |
| `MinAge(duration)` | mtime older than `duration` | A slow appender fails until it stops |
| `MarkerFile(suffix)` | `<name><suffix>` exists | Requires producer cooperation; the only deterministic check |
| `RenameClaim` | Rename to a claim name succeeds | Proves nothing on Linux: rename succeeds while a writer holds the file open |
| `AllOf(vararg)` | Composite | |

Default: `SizeStable(2, 10.seconds) + MinAge(1.minutes)`. A file that is not ready is counted in
`PollCompleted.notReady` and reconsidered next tick.

### 7.6 Ticker and overlap

`watch` owns the ticker. `overlap` mirrors the Quarkus scheduler: `SKIP` (default) emits
`PollSkipped` when the previous tick is still running, `PROCEED` starts a new tick alongside. A
second `watch` on the same directory of the same connector is rejected at call time, since one
consumer per directory is an assumption of Sec 7.3.

---

## 8. Post-processing and Idempotency

### 8.1 Actions

`onAck` and `onNack` are each one of `Move(target, overwrite)`, `Delete` or `Noop`. Default is
`Noop` for both; the pipeline in Sec 1.1 configures `onAck = Move("temp/", overwrite = true)`.

### 8.2 Move rules

- The target may be inside the watched directory. Consequences and handling: the lister
  skips directories by default, and with `recursive` on it excludes the action targets
  automatically, so moved files are never re-listed. Camel's default move target is a hidden
  folder inside the watched directory, and the foot-gun there is users forgetting to exclude it
  under recursion; excluding automatically removes the foot-gun.
- Rename across filesystems fails with the generic `SSH_FX_FAILURE`. The startup probe
  (Sec 11.1) performs a rename into the target and fails fast on this.
- SFTP version 3 rename fails when the target exists on servers without the POSIX rename
  extension. `overwrite = true` is implemented as rename, and on failure delete the target then
  rename again.
- A file moved between listing and download yields `FileGone`, not an error.

### 8.3 Idempotency

The application ledger is the single source of truth about processed files. The connector
does not persist anything. This matches Camel and Spring Integration, both of which ship an
in-memory default and leave persistence to a plugged repository, and both of which document
move-or-delete after processing as the usual substitute (D14). A `SeenRepository` SPI with an
in-memory LRU default is provided for callers that cannot move files and want the connector to
filter; it is not used by the Sec 1.1 pipeline.

---

## 9. Resilience

Resilience4j's Kotlin module wraps suspend functions; the semaphore bulkhead is the coroutine
compatible variant. Order, outermost first (D15):

```
Retry( CircuitBreaker( Bulkhead( TimeLimiter( transport op ) ) ) )
```

| Component | Scope | Configuration | Counts as failure |
|---|---|---|---|
| Retry | Per operation, per call | `maxAttempts`, exponential backoff with jitter, cap | Recoverable errors only |
| Circuit breaker | Per endpoint | Failure rate over a sliding window, wait in open, one probe in half-open | Recoverable errors only; fatal errors are surfaced, not counted |
| Bulkhead | Per endpoint | `maxConcurrentTransfers`, plus the pool size and the dispatcher | |
| Time limiter | Per operation | Per-operation timeout | Timeout is recoverable |

When the breaker is open, acquire fails fast with `CircuitOpenException` and `watch` emits
`PollSkipped(cause = BreakerOpen)` for that tick. A fatal error short-circuits everything: no
retry, no breaker count, and `watch` terminates (Sec 10.2).

---

## 10. Failure Model

### 10.1 Hierarchy

```
SftpException (sealed)
  Recoverable            poisons: Boolean
    ConnectFailed        proxy refused, TCP timeout, DNS
    SessionLost          socket timeout, "session is down", connection lost
    OperationTimeout
    ServerFailure        SSH_FX_FAILURE and other generic codes
    PermissionDenied     poisons = false; no fast retry
    NoSuchFile           poisons = false; per-operation meaning (Sec 6.1)
  Fatal
    AuthenticationFailed
    HostKeyRejected
    ConfigurationError
  PoolExhausted
  CircuitOpen
```

`CancellationException` is never caught or wrapped. Every exception carries endpoint, operation,
path and attempt number in its message.

### 10.2 Behavior by class

| Class | Retry | Breaker | Lease | `watch` |
|---|---|---|---|---|
| Recoverable, poisons | Yes | Counted | Evicted | Emits `PollFailed`, continues |
| Recoverable, no poison | Yes, except `PermissionDenied` which waits a full tick | Counted | Returned | Emits `PollFailed`, continues |
| Fatal | No | Not counted | Evicted | Terminates with the error |
| PoolExhausted | No | Not counted | n/a | Emits `PollFailed`, continues |
| CircuitOpen | No | n/a | n/a | Emits `PollSkipped`, continues |

---

## 11. Startup and Shutdown

### 11.1 Startup

1. Build and validate configuration (Sec 12). Invalid configuration is `ConfigurationError`
   and the connector does not start.
2. Open one session and run the probe: `realpath` of each watched directory, `mkdir` of each
   action target when `autoCreate` is on, and a rename of a zero-byte marker into each action
   target and back. A failed probe is fatal at startup. `startupProbe = false` disables the
   marker rename for servers where writing a marker is unwelcome.
3. Fill to `minIdle` in the background; readiness does not wait for it.

### 11.2 Shutdown

`close()` is a suspend function with phases, bounded by `drainTimeout` (default 30 s):

1. Connector state becomes `Closing`. Acquire fails fast with `PoolExhausted(closing = true)`.
2. Watchers are cancelled. No new listing starts; unacked files are treated as nacks.
3. Drain: wait for leased entries to be released. In-flight downloads are cancelled through the
   Sec 5.3 ladder and their `.part` files deleted; at the file sizes in scope, finishing them is
   not worth an unbounded shutdown (D16).
4. Force: remaining leases are aborted, which unblocks their threads.
5. Housekeeper stops, dispatcher closes, every entry ends in `Closed`.

The connector owns a `CoroutineScope` with a `SupervisorJob`; the Quarkus adapter calls
`close()` from the shutdown event with `runBlocking` under the same timeout.

---

## 12. Configuration

Immutable configuration produced by a builder DSL with `@DslMarker`, validated at build time.
Units are `kotlin.time.Duration`.

```kotlin
sftpConnector("vendor-drop") {
    endpoint { host = "sftp.example"; port = 22; proxy { httpConnect("proxy.internal", 3128) } }
    auth { password(user, secret) }
    hostKey = HostKeyPolicy.AcceptAll                 // explicit, warns at startup
    pool {
        maxSize = 5; minIdle = 0
        acquireTimeout = 30.seconds
        maxLifetime = 30.minutes; maxLifetimeJitter = 0.1
        idleTimeout = 4.minutes; keepAlive = 30.seconds; idleCutoff = 5.minutes
        validationBypass = 500.milliseconds
        connectTimeout = 10.seconds; socketTimeout = 60.seconds; cancelGrace = 5.seconds
        leakDetectionThreshold = 10.minutes
    }
    resilience {
        retry { maxAttempts = 3; backoff = exponential(1.seconds, max = 30.seconds, jitter = true) }
        circuitBreaker { failureRateThreshold = 50; slidingWindow = 20; waitInOpen = 1.minutes }
        bulkhead { maxConcurrentTransfers = 4 }
    }
    polling {
        overlap = OverlapPolicy.SKIP
        maxFilesPerPoll = 1000; maxInFlight = 16
        readiness = sizeStable(checks = 2, interval = 10.seconds) + minAge(1.minutes)
        staging { dir = Path("/var/etl/stage") }
        onAck = move("temp/", overwrite = true); onNack = noop()
        autoCreate = true; startupProbe = true
    }
}
```

Validation rules: `keepAlive < idleCutoff`, `idleTimeout < idleCutoff`, `minIdle <= maxSize`,
`maxConcurrentTransfers <= maxSize`, staging directory exists and is writable, action targets
are not equal to the watched directory.

---

## 13. Metrics

Micrometer, bound to whatever registry the host supplies; `SimpleMeterRegistry` when none.
Tag `endpoint` on everything; never tag by file name or tick number.

| Metric | Type | Notes |
|---|---|---|
| `sftp_pool_active`, `sftp_pool_idle`, `sftp_pool_pending` | gauge | |
| `sftp_pool_acquire_seconds` | timer | |
| `sftp_pool_acquire_timeout_total` | counter | |
| `sftp_pool_created_total`, `sftp_pool_evicted_total{reason}` | counter | reason: lifetime, idle, poisoned, validation, shutdown |
| `sftp_pool_leak_total` | counter | |
| `sftp_op_seconds{op, result}` | timer | result: ok, recoverable, fatal, cancelled |
| `sftp_retry_total{op}` | counter | |
| `sftp_breaker_state` | gauge | 0 closed, 1 half-open, 2 open |
| `sftp_poll_seconds{result}` | timer | |
| `sftp_poll_files{state}` | counter | state: seen, emitted, notReady, gone |
| `sftp_inflight` | gauge | |
| `sftp_ack_total{outcome}` | counter | outcome: ack, nack, cancelled |

---

## 14. Known Limitations and Future Extensions

### 14.1 Streaming and resume

`Connection.openRead` exists on the transport so a streaming download that pins a lease for the
consumer's read can be added without changing the pool. Resume is the JSch `RESUME` mode plus
the local `.part` length and a stored remote size and mtime; it is deferred until file sizes
justify the extra edge cases.

### 14.2 Multiple consumers

A second instance polling the same directory needs a claim before download, since the in-flight
set is per process. The `RenameClaim` readiness check is the hook; it would need to become a
claim step rather than a readiness check.

### 14.3 Ack wait and in-progress

`ackWait` is specified but off; an `inProgress()` extension like NATS is not specified.

### 14.4 Other transports

The transport interface has one adapter, which by the project's own rule makes it a hypothetical
seam. It is kept because the cancellation ladder of Sec 5.3 is JSch-specific and the fake
transport in the testkit is a genuine second implementation.

---

## 15. Decision Log

| ID | Decision | Rationale |
|---|---|---|
| D1 | Own pool, modelled on HikariCP, not Commons Pool 2 and not a `SharedFlow` or `Channel` container | A `SharedFlow` broadcasts and cannot hand out exclusive items; a `Channel` cannot remove a specific expired entry; Commons Pool 2 blocks a thread on borrow with no cancellation. Mature pools all use a flat registry with per-entry state |
| D2 | Transport interface with a single JSch adapter | The cancellation ladder and error-message table are JSch-specific and volatile; the testkit fake is a real second implementation |
| D3 | Framework-free core, Quarkus adapter, slf4j in core | The connector must outlive one host framework; Quarkus routes slf4j without configuration |
| D4 | Blocking JSch calls on `Dispatchers.IO.limitedParallelism(maxSize)` | JDK 17 has no virtual threads; a bounded dispatcher is both the bulkhead and the protection for the host's IO pool |
| D5 | Validate on borrow after the bypass window, recreate only on failure | Session creation is several round trips, key-exchange CPU and a server fork, with OpenSSH dropping connections above ten unauthenticated; validation is one round trip |
| D6 | Leak detection is diagnostic, never enforcement | A JSch call cannot be safely interrupted from outside; the safe tier is the socket timeout |
| D7 | mwiede JSch fork | jcraft 0.1.55 is unmaintained and lacks rsa-sha2, which current OpenSSH requires |
| D8 | Host key policy is an explicit enum; `AcceptAll` is allowed, never default, and warns | Ops chose password auth without host key checking through a proxy; the risk is theirs to accept, and the connector makes it visible |
| D9 | Three-tier cancellation; only forced disconnect destroys the session | JSch's monitor and selector callbacks give a cooperative abort that keeps the session usable; forced disconnect is the last resort |
| D10 | Transparent reconnect means retry on a fresh lease with per-operation semantics | Retrying a rename or delete blindly after a lost reply produces phantom `NoSuchFile` failures on exactly the flaky network in scope |
| D11 | Local staging with `.part` then atomic rename; abort deletes the partial | Files are small; a partial file on NFS is where rename and delete semantics get surprising |
| D12 | Cold `Flow`, not `SharedFlow`, for the source | Cold gives backpressure by suspension and one loop per collector; hot must either suspend the producer or drop file events |
| D13 | Per-file ack and nack with `maxInFlight` | One mechanism gives backpressure, post-processing and redelivery; SFTP has no batch request, so per-poll ack would save nothing |
| D14 | Idempotency is the application's; the connector holds only an in-memory in-flight set | Two ledgers are two sources of truth; Camel and Spring Integration ship in-memory defaults and defer persistence to a plug-in |
| D15 | Retry outside breaker outside bulkhead outside time limiter | Resilience4j's recommended order; a retry must observe the breaker, and the breaker must count timeouts |
| D16 | Shutdown aborts in-flight downloads after the drain timeout | At-least-once makes a redo safe and the files are small; finishing would make shutdown unbounded |
| D17 | Events carry metadata; download is a separate call | The consumer chooses download concurrency and can ack a known file without downloading |
| D18 | Action targets may live inside the watched directory; the lister excludes them | Matches Camel's default layout; automatic exclusion removes the recursion foot-gun |
| D19 | Startup probe with marker rename, disableable | Cross-filesystem rename fails with a generic code; finding that at the first ack an hour later is worse than at startup |
| D20 | `PermissionDenied` is recoverable but waits a full tick | Ops can fix it without a restart; fast retries would only trip the breaker |
| D21 | `maxSize` follows the infra team's five; `maxConcurrentTransfers` defaults to four | Leaves one session for the lister and a human |

---

## 16. Open Items Before Implementation

1. **Proxy connection limit** - unknown; connect failures through the proxy classify as
   `ConnectFailed` and the breaker handles them, but a known cap would set `maxSize` directly.
2. **Producer-side completeness convention** - ask the upstream for a marker file or
   temp-name-then-rename; until then the default readiness check is heuristic.
3. **Temp folder ownership** - `autoCreate` creates it; if the account cannot `mkdir`, ask the
   upstream to create it and the probe will verify it.
4. **Error-message table for JSch** - to be assembled against the mwiede version pinned in the
   build, with a test that fails when a mapped message disappears from the library.

---

## 17. Acceptance Plan (Framework)

Three layers, same as snapshotcache:

1. **Fake transport, no socket.** Pool invariants, lease bookkeeping, housekeeper, source
   events, ack and nack, readiness, overlap and shutdown phases. Deterministic through injected
   `Clock` and declared hook points; no `Thread.sleep`.
2. **Embedded Apache MINA SSHD.** Real SFTP over loopback with a temp-directory filesystem and
   fault hooks: kill a session server-side mid-transfer, delay responses past the socket
   timeout, reject auth, remove a file between list and download, deny rename. Proves the JSch
   adapter, the error table and the cancellation ladder.
3. **Toxiproxy (optional).** Half-open connections and proxy stalls through Testcontainers.

### 17.1 Invariants

Tests are named `I<n>_<description>`.

| ID | Invariant |
|---|---|
| I1 | `idle + inUse + connecting <= maxSize` at every observable point |
| I2 | An entry is handed to at most one lease at a time |
| I3 | A poisoned entry is never returned to the idle deque |
| I4 | Every acquired permit is released exactly once, on every exit path including cancellation during connect |
| I5 | No transport call executes while the registry mutex is held |
| I6 | An entry past `maxLifetime` is closed on release, never reused |
| I7 | A file in the in-flight set is not emitted by any tick |
| I8 | Cancelling a collector with unacked files releases every in-flight slot |
| I9 | `close()` returns within `drainTimeout + cancelGrace` and leaves every entry `Closed` |
| I10 | A fatal error terminates `watch`; a recoverable error never does |
| I11 | A rename retried after a lost reply reports success when the target exists with the expected size |
| I12 | Ack and nack are each accepted once per event |
| I13 | After abort, no `.part` file exists in the staging directory |
| I14 | `keepAlive < idleCutoff` and `idleTimeout < idleCutoff` are rejected at build time when violated |

### 17.2 Scenario table

| ID | Scenario | Expected |
|---|---|---|
| S1 | Server kills the session during download | Download retried on a new lease, old entry evicted, consumer sees one `FileSeen` and one successful download |
| S2 | Server stalls past `socketTimeout` | `SessionLost`, poisoned, retried; breaker counts one failure |
| S3 | Breaker opens | `PollSkipped(BreakerOpen)` each tick until half-open probe succeeds |
| S4 | Pool exhausted with slow consumer | Acquire waits `acquireTimeout` then `PoolExhausted`; `watch` continues |
| S5 | File removed between list and download | `FileGone`, no error, no retry |
| S6 | Move target on another filesystem | Startup probe fails with `ConfigurationError` |
| S7 | Ack before download | Ack action runs, no transfer occurs |
| S8 | Previous tick still running under `SKIP` | `PollSkipped(Overlap)`, no second listing |
| S9 | Shutdown during download | `.part` deleted, lease released, `close()` within bound |
| S10 | Wrong password | `AuthenticationFailed`, no retry, breaker untouched, `watch` terminates |
| S11 | 100k entries with `maxFilesPerPoll = 1000` | Listing stops after 1000 entries, memory flat |
| S12 | Same file listed while in flight on a `PROCEED` overlap | Emitted once |
