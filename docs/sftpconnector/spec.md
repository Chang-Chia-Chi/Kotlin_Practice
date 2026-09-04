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
| `sftpconnector-core` | kotlin-stdlib, kotlinx-coroutines, JSch (mwiede), resilience4j-kotlin, micrometer-core, slf4j-api | everything in Sec 3.1 |
| `sftpconnector-testkit` | `sftpconnector-core`, Apache MINA SSHD | embedded server, fault hooks, fake transport |

There is no Quarkus adapter module. Ticket 14 built one and ticket 24 deleted it: it spelled every
configuration knob a third time and had no consumer, because shuttle - the only Quarkus host -
builds its configuration through the core DSL in its own words. A Quarkus host writes those few
lines itself until a second one makes a shared mapping worth its weight. D3 is unchanged by this:
Quarkus stays out of the core either way.

ArchUnit enforces: `sftpconnector-core` never imports Quarkus; only the `transport.jsch` package imports
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

1. Take a permit, waiting at most `acquireTimeout`. Timeout throws `PoolExhausted`
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
`readTo(path, sink)`, `writeFrom(path, source)`, `rename`, `delete`, `mkdir`, `realpath`,
`abort()` and `close()`. The JSch adapter is the only implementation in production; the testkit
provides a scripted fake for pool and source tests that never open a socket.

The transfer operations are `readTo(path, sink)` and `writeFrom(path, source)` rather than the
`openRead` and `openWrite` earlier drafts named. Returning a stream for the caller to pump would
put every blocking socket read or write on whatever thread the caller happened to be on, and
Sec 3.3 requires them all on the bounded dispatcher; handing the transport a sink or a source
keeps the whole transfer inside one call on that dispatcher, which is also where the progress
monitor of Sec 5.3 hangs. `openRead` is not `readTo` renamed - it is the streaming download of
Sec 14.1, deferred out of v1 by Sec 1.3, and whichever release builds it adds it alongside.

`SftpConnection` is `SftpSession` plus `close()`. See Sec 6.1 on `withSession` for why the
split exists. `SftpSession.renameReplaces` reports whether this server's rename replaces an
occupied target (the POSIX rename extension, read from the handshake); the overwrite policy in
Sec 8.2 branches on it, and the testkit fake answers `false`.

Operations join this interface as the ticket needing them arrives, absent rather than stubbed,
so nothing above the seam can call a method that does not yet work.

### 5.2 JSch adapter

- mwiede JSch. jcraft 0.1.55 lacks rsa-sha2 signatures, which modern OpenSSH requires (D7).
- One `Session`, one `ChannelSftp`. OpenSSH caps channels per session at ten and channel state
  such as the working directory is per channel; sharing a session across operations serializes
  them anyway.
- `setServerAliveInterval(keepAlive)`, `setServerAliveCountMax(1)`, `ProxyHTTP` for
  the CONNECT proxy.
- Host key policy is an explicit enum: `Strict(knownHosts)`, `Fingerprint(sha256)`,
  `AcceptAll`. `AcceptAll` is never the DSL default and logs a warning at startup (D8).
- Rename uses the `posix-rename@openssh.com` extension when the server advertises it; the
  overwrite policy (Sec 8.2) accounts for servers that do not. **This is not something the
  adapter arranges** (D29, measured): JSch sends the extension by itself whenever the server
  advertised it, and such a server replaces an occupied target and reports success. Refusing an
  overwrite is therefore the connector's own decision, taken before the request goes out, and
  cannot be delegated to the server. Any code that sends a bare rename and reads the answer is
  trusting a behaviour half the servers in scope do not have.
- **JSch reads `*` and `?` in the last path component as a glob** (D37, verified in the 2.28.7
  sources) for `rename`, `rm`, `put`, `get`, `stat` and `ls`. A rename onto `l*.csv` landed on
  and replaced a neighbour; `delete("/drop/*.csv")` removed every match. The adapter escapes
  `\`, `*` and `?` at the one place a path is handed to the library, so a path names one thing.
  `mkdir` and `realpath` are sent unquoted because JSch does not expand those.

### 5.3 Cancellation: three tiers

JSch calls do not observe coroutine cancellation and blocking socket reads do not observe
`Thread.interrupt`. The adapter therefore cancels in three tiers, and only the last one
destroys the session (D9).

| Tier | Mechanism | Effect on the session |
|---|---|---|
| Cooperative | Transfers run with a `SftpProgressMonitor` whose `count` returns `false` once the coroutine is cancelled; listings run with an `LsEntrySelector` that returns `BREAK`. JSch closes the remote handle cleanly. | Usable, returned to the pool after validation |
| Keepalive ladder | The server-alive probes fail one after another and unblock the stalled read with an exception, after `keepAlive x (serverAliveCountMax + 1)`. | Poisoned, evicted |
| Forced | If neither tier has unblocked the call within `cancelGrace` (default 5 s), the cancellation handler calls `abort()`, which disconnects the session from another thread. | Poisoned, evicted |

Resilience4j's `TimeLimiter` on a suspend function is `withTimeout`, which enters this ladder at
the cooperative tier. The keepalive ladder is what actually bounds a hung server.

**The middle tier is `keepAlive`, not `socketTimeout`** (D26, measured against mwiede JSch
2.28.7). JSch implements `serverAliveInterval` *by* setting the socket read timeout, so it
overwrites `session.timeout` whenever it is set, and the DSL requires `keepAlive` to be
positive, so it is always set. With `socketTimeout = 500 ms` and the default
`keepAlive = 30 s`, a stalled tunnel took 60 s to fail; with `socketTimeout = 5 s` and
`keepAlive = 300 ms` the same stall failed in 1.2 s.

**`socketTimeout` was therefore removed from the DSL** (D31). An earlier draft of this section
kept it on the grounds that it is what a reader reaches for, and the way to honour that would
have been to spend it as `serverAliveCountMax`, deriving the probe count from
`socketTimeout / keepAlive`. That was built and reverted. The library gives up after a whole
number of unanswered probes, so any bound is a multiple of `keepAlive`, and a duration knob
silently rounded to a multiple of a different knob is not one knob but half of two. Worse, the
name is a lie in this library - `serverAliveInterval` *is* the socket read timeout - so keeping
the name while giving it another job preserves exactly the misreading D26 exists to end.

What a reader reaches for is `keepAlive`. The adapter pins `serverAliveCountMax = 1` rather than
inheriting the library's default, so the bound on a hung server is twice `keepAlive`, and that
is the number to size against an SLA. A deployment that ever needs that bound tuned
independently of how often a session speaks should get a count of probes spelled as a count.

The same value bounds the key exchange, which is a trap for any test that shortens it: a
`keepAlive` below the handshake time fails `connect()` with `timeout in waiting for rekeying
process.` rather than failing the read.

### 5.4 Error mapping

All JSch errors are mapped in one class into the hierarchy of Sec 10. `SftpException` carries an
SSH_FX status code and maps by code. `JSchException` carries only a message and maps by a
maintained table of message prefixes (`Auth fail`, `session is down`, `failed to send channel
request` / `channel is not opened`, `connection is closed by foreign host`) plus the `java.net.`
marker JSch leaves in a stringified socket failure. The host key and proxy failures have
exception types of their own in this fork and are matched **by type**, so a rewording cannot
reclassify them. The measured table in the T2 progress entry is the authority for every row.
Unmapped messages become
`Unknown(rawMessage, cause)`, a recoverable and poisoned error, never fatal, so a new message
wording degrades to a retry rather than to a dead connector. `Unknown` keeps the original JSch
message and exception verbatim, logs at WARN with the raw text so the wording can be added to
the table, and increments `sftp_error_unmapped_total`. An unmapped error is therefore visible
in production the first time it occurs, not silently absorbed.

---

## 6. Client Operations

### 6.1 Operations

| Operation | Signature (abridged) | Retry semantics on a fresh session |
|---|---|---|
| `list` | `(dir, filter, maxEntries): Flow<RemoteFile>` | Blind retry |
| `stat` | `(path): RemoteFile?` | Blind retry |
| `download` | `(remote, localTarget): LocalFile` | Restart from zero into a fresh `.part` file |
| `upload` | `(local, remote, overwrite)` | Restart; remote partial is overwritten |
| `rename` | `(from, to, overwrite)` | On `NoSuchFile` **naming the source** after a retry, stat `to`: existing with the expected size counts as success, absent means the file is gone. `NoSuchFile` **naming the target** means nothing landed and the source is untouched (D38). A cancellation or timeout is a lost reply and carries no information |
| `delete` | `(path)` | `NoSuchFile` after a retry counts as success |
| `mkdir` | `(path, parents)` | `AlreadyExists` counts as success |
| `exists` | `(path): Boolean` | Blind retry |
| `withSession` | `(block: suspend SftpSession.() -> T): T` | No retry; the caller owns semantics |

`withSession` hands the block an `SftpSession`, not an `SftpConnection`. The difference is
`close()`: the pool lends the same session out again after the block ends, so a caller that hung
up on it would break the *next* caller's work rather than its own. The block is therefore handed
something with no hang-up on it, and the loan ends when the block has returned **and the last
call made on it has finished** (D39) - a reference stashed past the block fails loudly instead
of quietly using a session the pool has re-lent, and a call still on the wire when the block
returns is waited for rather than raced.
`abort()` is likewise the pool's, never a borrower's, because it destroys the session.

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

Download writes the partial file beside the target, which by default is `<stagingDir>/<name>`,
verifies the byte count against the listed size, then renames atomically onto the target. Beside
the target rather than always in the staging directory, so a caller that names its own target
keeps the rename on one filesystem. An abort deletes the `.part` file, so no partial file
survives a run. While writing, the client computes a digest of the bytes (algorithm from
`staging.digest`, default SHA-256, MD5 selectable) and returns it on `LocalFile.digest`. The
digest costs nothing extra because the bytes are already streaming through, and it is the
connector's whole contribution to integrity. Comparing it against an expected value is the
application's job, because only the application knows where the expected value comes from
(a sidecar file, a manifest, a database row). Completeness (Sec 7.5) asks whether the uploader
has finished writing; integrity asks whether the bytes arrived intact. They are answered at
different times, before and after the download, and by different parties (D22). The staging directory must be local disk; on NFS the rename and delete semantics
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
- Cancellation of the collector with unacked files makes each of them eligible again on a later
  tick, as a nack with redelivery would - but **the nack action does not run**: nobody said those
  files failed, and a configured `move("failed/")` would file every unprocessed message as a
  failure on every shutdown, inside a cancelled coroutine. They are counted
  `sftp_ack_total{outcome=cancelled}`, which is what that label is for (T10 deviation 3).
- Ack is always the consumer's call, after its own work on the file is complete. The
  connector never acks on its own; the `consume` helper below acks only when the consumer's
  block returns normally. The connector also does not require a download to precede an ack:
  a consumer whose ledger shows the file was fully processed in an earlier run, and only the
  server-side move is missing, may call `ack()` directly. That is a permission for crash
  recovery, not a behavior; a pipeline that always downloads first loses nothing. This follows
  the messaging model (Kafka offset commit, NATS ack) rather than Camel or Spring Integration,
  which filter before emitting and therefore never move an already-seen file.
- Each of ack and nack is accepted once; the second call is logged and ignored.
- `ackWait` (optional) makes an unacked file eligible again after the duration, like NATS.
  Off by default: with a single consumer, a stuck file is a consumer bug to surface, not to
  hide.

`consume(dir, every) { file -> ... }` wraps `watch`: it acks when the block returns and nacks
when it throws. It is the documented normal path; manual ack is for pipelines that commit late.

### 7.3 In-flight set and backpressure

The source keeps an in-memory set of in-flight files keyed by path, size and mtime. A file in the
set is not emitted again by an overlapping or later tick. `maxInFlight` bounds the set; when it
is full the lister suspends until an ack or nack arrives. A file already in the set is turned
away before it waits for room, and looked for again once room is taken - the second look is what
keeps the promise when a tick running alongside admitted it in between. This is the backpressure knob that
protects the downstream, and it is the only state the connector holds about processed files.
Persistent idempotency belongs to the application (Sec 8.3).

### 7.4 Listing

`SSH_FXP_READDIR` returns entries in batches and JSch's selector sees each entry as it
arrives, so listing is a `channelFlow` with a bounded buffer and never materializes a
directory. `maxFilesPerPoll` stops the listing early. `sortBy` is **not built**: it requires materialization
and would be honored only together with `maxFilesPerPoll`, as Camel does, and nothing in scope
asks for it (T10 deviation 5). Directories are skipped by default;
`recursive` descends but always excludes the ack and nack target folders (Sec 8.2).

### 7.5 Readiness

`interface ReadinessCheck { suspend fun check(file, ctx): Readiness }` where `ctx` offers `stat`
and the clock, and `Readiness` is `Ready`, `NotReady(reason)` or `Skip`. Built-ins:

| Check | Meaning | Caveat |
|---|---|---|
| `SizeStable(checks, interval)` | Size unchanged across `checks` stats `interval` apart, inside one poll, **batched**: every candidate is stated, one `interval` elapses, every candidate is stated again (D36) | A stalled uploader passes |
| `MinAge(duration)` | mtime older than `duration` | A slow appender fails until it stops |
| `MarkerFile(suffix)` | `<name><suffix>` exists | Requires producer cooperation; the only deterministic check |
| `RenameClaim` | Rename to a claim name succeeds | **Not built; see 14.2.** Proves nothing on Linux: rename succeeds while a writer holds the file open. Its use is the multi-consumer claim step, not readiness |
| `AllOf(vararg)` | Composite | |

Default: `SizeStable(2, 10.seconds) + MinAge(1.minutes)`. A file that is not ready is counted in
`PollCompleted.notReady` and reconsidered next tick.

Readiness runs as a phase between the listing and the emitting, over the poll's candidates as a
batch (bounded by `maxFilesPerPoll`, which is what that cap is for). So a poll costs
`(checks - 1) x interval` of readiness latency in total, not per file, and the listing's session
is released before the wait begins. Remembering sizes *across* polls was built first and
rejected (D36): on the hourly pipeline it made every file wait for the second poll, an hour of
latency where the default reads as ten seconds.

### 7.6 Ticker and overlap

`watch` owns the ticker. `overlap` mirrors the Quarkus scheduler: `SKIP` (default) emits
`PollSkipped` when the previous tick is still running, `PROCEED` starts a new tick alongside. A
second `watch` on the same directory of the same connector is rejected **when collected** - a
cold flow has done nothing at the call, and a claim taken there would leak on a flow nobody
collects - since one consumer per directory is an assumption of Sec 7.3. The claim is released
when the collector leaves, however it leaves.

---

## 8. Post-processing and Idempotency

### 8.1 Actions

`onAck` and `onNack` are each one of `Move(target, overwrite)`, `Delete` or `Noop`. Default is
`Noop` for both; the pipeline in Sec 1.1 configures `onAck = Move("temp/", Overwrite.REPLACE)`.

`overwrite` is the `Overwrite` enum (`REFUSE`, `REPLACE`), not a boolean, wherever it appears
(D33). `REPLACE` is not a bit on a request: SFTP version 3 has no way to say "put this here and
replace whatever is there", so on a server without the POSIX rename extension it is a short
sequence of requests with a gap in the middle, and the gap is what a caller has to know about.
A boolean cannot carry that, and a reader of `rename(from, to, true)` learns nothing from it.
A relative `target` is resolved under the watched directory it belongs to, in one place, so the
validator, the probe and the ack executor cannot disagree about which folder `temp/` is.

### 8.2 Move rules

- The target may be inside the watched directory. Consequences and handling: the lister
  skips directories by default, and with `recursive` on it excludes the action targets
  automatically, so moved files are never re-listed. Camel's default move target is a hidden
  folder inside the watched directory, and the foot-gun there is users forgetting to exclude it
  under recursion; excluding automatically removes the foot-gun.
- Rename across filesystems fails with the generic `SSH_FX_FAILURE`. The startup probe
  (Sec 11.1) performs a rename into the target and fails fast on this.
- SFTP version 3 rename fails when the target exists on servers without the POSIX rename
  extension. `Overwrite.REPLACE` is implemented as rename, and on failure - **only on a server
  without the extension, and only when a file (not a directory) is at the target** - delete the
  target then rename again (D40). On a server with the extension a refusal can never be about an
  occupied target, so it is passed on as given and nothing is cleared; S6's cross-filesystem case
  was deleting a healthy target before this rule. When the second rename is refused after the
  target was cleared, the failure says so: the target is now empty, and the caller must not read
  "source untouched" as "target untouched".
- A file moved between listing and download yields `FileGone`, not an error.

### 8.3 Idempotency

The application ledger is the single source of truth about processed files. The connector
does not persist anything. This matches Camel and Spring Integration, both of which ship an
in-memory default and leave persistence to a plugged repository, and both of which document
move-or-delete after processing as the usual substitute (D14). A `SeenRepository` SPI for callers
that cannot move files and want the connector to filter was specified here and is **not built**;
Sec 14.5 says why and what such a caller does instead.

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

When the breaker is open, acquire fails fast with `CircuitOpen` and `watch` emits
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
    ServerFailure        SSH_FX_FAILURE and other generic codes; poisons = false (D27)
    IncompleteTransfer   the connector's own check failed: bytes received != size listed; poisons (D28)
    Unknown              unmapped JSch message, raw text preserved; poisons
    PermissionDenied     poisons = false; no fast retry
    NoSuchFile           poisons = false; per-operation meaning (Sec 6.1)
  Fatal
    AuthenticationFailed
    HostKeyRejected
    ConfigurationError
  PoolExhausted
  CircuitOpen
  OverwriteRefused     the target is occupied and the policy said not to replace (D30)
  UnsafeFileName       a listed name that cannot be a local file name under the staging
                       directory; nothing was sent and no session was borrowed
```

`PoolExhausted`, `CircuitOpen`, `OverwriteRefused` and `UnsafeFileName` share a disposition of
their own, `ACCEPT_THE_REFUSAL` (C8): no retry inside the call, nothing counted against the
breaker, and the lease - where one was ever taken - returned rather than evicted. Each is the
connector's own decision, so sending the request again cannot change the answer and the server
has done nothing to be charged for.

`CancellationException` is never caught or wrapped. Every exception carries endpoint, operation,
path and attempt number in its message.

### 10.2 Behavior by class

| Class | Retry | Breaker | Lease | `watch` |
|---|---|---|---|---|
| Recoverable, poisons | Yes | Counted | Evicted | Emits `PollFailed`, continues |
| Recoverable, no poison, **the wire failed** (`IncompleteTransfer`) | Yes, fresh lease | Counted | Evicted | Emits `PollFailed`, continues |
| Recoverable, no poison, **the server answered** (`NoSuchFile`, `PermissionDenied`, `ServerFailure`) | No, not inside the call; the next tick is the retry (D41) | Not counted | Returned | Emits `PollFailed`, continues |
| Fatal | No | Not counted | Evicted | Terminates with the error |
| PoolExhausted | No | Not counted | n/a | Emits `PollFailed`, continues |
| CircuitOpen | No | n/a | n/a | Emits `PollSkipped`, continues |
| OverwriteRefused | No | Not counted | Returned | Emits `PollFailed`, continues |
| UnsafeFileName | No | Not counted | n/a | Raised to whoever called `download`; the poll is untouched |

The split in the second and third rows is the one the retry ladder actually needs (D41). A
failure the *wire* produced - a lost session, a timed-out call, a short read - says nothing
about the request, and a fresh lease may well succeed. A failure the *server answered* proves
the request arrived and was understood: the file is not there, the account may not, the
operation is refused. Sending it again inside the same call cannot change the answer, and
counting it against the breaker charges the connector for a healthy server doing its job - on a
server without the POSIX rename extension every refused overwrite would count. The per-operation
meaning in Sec 6.1 (`NoSuchFile` after a retry is success for `delete`, is `FileGone` for a
download) is what a *later* try reads; it is not a reason to send one now.

---

## 11. Startup and Shutdown

### 11.1 Startup

1. Build and validate configuration (Sec 12). Invalid configuration is `ConfigurationError`
   and the connector does not start.
2. Open one session and run the probe: `realpath` then `stat` of each watched directory,
   insisting on a directory; `mkdir` of each action target when `createActionTargets` is on;
   and a rename of a zero-byte marker into each action target and back. A failed probe is fatal
   at startup and names the directory, the check, and the remedy. `startupProbe = false`
   disables the marker rename for servers where writing a marker is unwelcome.

   `realpath` alone proves nothing about a path existing (D32, measured): on MINA SSHD and on
   OpenSSH, resolving a path that leads nowhere succeeds and returns the canonical name, and so
   does resolving one that leads to a file. It is a string operation. The `stat` is the check;
   `realpath` only fixes the spelling the rest of the probe uses.
3. Fill to `minIdle` in the background on the housekeeper's **first round**, one
   `housekeepingInterval` after start-up - thirty seconds with the shipped defaults. Readiness
   waits for neither. Topping up is one of the things the housekeeper does every round (Sec 4.5)
   and its loop waits before it sweeps, so there is no fill before the first round; a pool that
   is cold for one interval costs one handshake, which was not worth changing the housekeeper's
   timing for (T9 deviation 3). `minIdle` defaults to 0, so nothing waits until a deployment
   sets it.

### 11.2 Shutdown

`close()` is a suspend function with phases, bounded by `drainTimeout + cancelGrace`
(defaults 30 s and 5 s) and uncancellable end to end, because it is bounded by construction
(D43):

1. The connector's scope is cancelled: watchers and the housekeeper together. No new listing
   starts, a new `watch` claim is refused, and every unacked file is withdrawn as `cancelled`
   for redelivery. The housekeeper goes here rather than last because a closing pool refuses it
   rounds anyway, and nothing needs it to outlive the drain.
2. The pool stops lending. `acquire` fails fast at the door with `PoolExhausted(closing = true)`,
   with no wait; a caller already queued is refused when room frees. The closing state lives in
   the registry and every decision that reads it is taken under the registry's lock.
3. Drain: wait up to `drainTimeout` for leased entries to come back. A handback during the drain
   retires the session as `shutdown` whatever the caller said.
4. Force: every lease still held is cut at once through the Sec 5.3 ladder - one `cancelGrace`
   for all of them, not one each - which unblocks their threads and deletes their `.part` files;
   at the file sizes in scope, finishing them is not worth an unbounded shutdown (D16).
5. Every remaining session is hung up on in parallel; every entry ends `Closed` and is counted
   `sftp_pool_evicted_total{reason=shutdown}`.

There is no "dispatcher closes" step: `Dispatchers.IO.limitedParallelism` is a view over the
shared IO pool and owns no threads. `start` calls `close()` before rethrowing a refused probe,
so a start-up that fails leaves nothing open. A second `close()` is harmless.

The connector owns a `CoroutineScope` with a `SupervisorJob`. The Quarkus adapter calls
`close()` from the shutdown event with `runBlocking`, under a timeout **no shorter than**
`drainTimeout + cancelGrace` - the call is bounded and uncancellable, so a shorter timeout only
returns early while the close carries on - and from a worker thread, because `abort()` runs on
the closing thread and an event loop must not block on a socket close.

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
        connectTimeout = 10.seconds; cancelGrace = 5.seconds
        leakDetectionThreshold = 10.minutes
    }
    resilience {
        retry { maxAttempts = 3; backoff = exponential(1.seconds, max = 30.seconds, jitter = true) }
        circuitBreaker { failureRateThreshold = 50; slidingWindow = 20; waitInOpen = 1.minutes }
        bulkhead { maxConcurrentTransfers = 4 }
    }
    polling {
        directories("inbound/", "inbound-priority/")
        overlap = OverlapPolicy.SKIP
        maxFilesPerPoll = 1000; maxInFlight = 16
        readiness = sizeStable(checks = 2, interval = 10.seconds) + minAge(1.minutes)
        staging { dir = Path("/var/etl/stage"); digest = Digest.SHA256 }
        onAck = move("temp/", Overwrite.REPLACE); onNack = noop()
        createActionTargets = true; startupProbe = true
    }
}
```

Validation rules: `keepAlive < idleCutoff`, `idleTimeout < idleCutoff`, `minIdle <= maxSize`,
`maxConcurrentTransfers <= maxSize`, staging directory exists and is writable, action targets
are not equal to the watched directory; and `drainTimeout > cancelGrace`, `operationTimeout` and
`transferTimeout` longer than `acquireTimeout` - so a caller queued for a session is never
reported as the server timing out and counted against the breaker - `maxLifetimeJitter` in
`0.0..1.0`, a non-negative `validationBypass`, and every other duration positive.
`ConnectorDsl.build()` is the authority and reports every fault at once.

The configuration types are produced only by the DSL and cannot be constructed or copied outside
the connector, so a configuration that exists at all is one `build()` checked; a host that needs to
size a pool from its own numbers passes those numbers into the `pool` and `bulkhead` blocks rather
than rewriting a built configuration afterwards.

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
| `sftp_error_unmapped_total` | counter | any non-zero value is a table entry to add |
| `sftp_breaker_state` | gauge | 0 closed, 1 half-open, 2 open |
| `sftp_poll_seconds{result}` | timer | |
| `sftp_poll_files{state}` | counter | state: seen, emitted, notReady, gone |
| `sftp_inflight` | gauge | |
| `sftp_ack_total{outcome}` | counter | outcome: ack, nack, cancelled |

**Absent is not zero.** Some of these are registered on the first use rather than at start-up, so
the series does not exist at all until the thing it measures happens once - and an alert written
as `> 0` never fires on a series that is absent, which is exactly the case for the meters whose
whole value is that they should read zero. Registered lazily, per label value:
`sftp_pool_evicted_total{reason}`, `sftp_pool_leak_total`, `sftp_retry_total{op}`,
`sftp_error_unmapped_total`, `sftp_op_seconds{op,result}`, `sftp_poll_seconds{result}`. Registered
eagerly for every label value, and therefore readable as zero from the first scrape:
`sftp_poll_files{state}`, `sftp_ack_total{outcome}`, `sftp_inflight`, `sftp_breaker_state`,
`sftp_pool_active`/`idle`/`pending`, `sftp_pool_acquire_seconds`,
`sftp_pool_acquire_timeout_total`, `sftp_pool_created_total`. Alert on a lazy one with
`absent(x) or x > 0`, or with a rate or `increase`, rather than with a bare comparison.

---

## 14. Known Limitations and Future Extensions

### 14.1 Streaming and resume

`openRead` is the streaming download: the transport interface has room for it beside `readTo`
(Sec 5.1), so one that pins a lease for the consumer's read can be added without changing the
pool. It is **not built**. Resume is the JSch `RESUME` mode plus
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

### 14.5 `SeenRepository`

A `SeenRepository` SPI for callers that cannot move or delete files is **not built**. The
in-flight set of Sec 7.3 is the only state the connector holds about a file, it is in memory and
per process, and nothing in it survives a restart - which is precisely what a filtering caller
would need. An interface with an in-memory LRU default would be a second ledger inside the
connector against D14, with one implementation, no consumer in the Sec 1.1 pipeline, and no
answer for the restart that is the whole case for it.

A caller that cannot move files filters above the source: it collects `watch`, asks its own
ledger about `RemoteFile.path`, size and mtime, and acks the files it has already processed -
which is exactly the ack-without-a-download that Sec 7.2 permits for crash recovery. Whoever
needs the connector to do that filtering owns designing the persistence with it; the open-seams
table in `progress.md` carries the row.

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
| D22 | The connector computes a digest during staging; the application compares it | The digest is free while bytes stream through; the expected value's origin is application knowledge. Checksum is integrity, not completeness, and cannot replace the readiness check because it needs the download first |
| D23 | Unmapped JSch errors are `Unknown`, recoverable, poisoned, logged raw and counted | JSch error text is not an API; a wording change must surface as a metric and a log line, never as a silent misclassification or a dead connector |
| D26 | The middle cancellation tier is the keepalive ladder, not `socketTimeout` | Measured against mwiede 2.28.7 (Sec 5.3): JSch implements `serverAliveInterval` by setting the socket read timeout, so a positive `keepAlive` always overwrites `session.timeout`. A hung server is bounded by `keepAlive x (serverAliveCountMax + 1)` and not at all by `socketTimeout` |
| D44 | A partition is noticed at `2 x keepAlive` plus tens of milliseconds, never sooner | Two intervals is JSch's give-up from the last byte received; the disconnect, the ladder and the eviction follow it. Ten runs at `keepAlive = 500 ms` landed at 1023-1064 ms. Sec 17.3's bound is asserted with one keepalive of grace (Sec 17.3) |
| D43 | `close()` is bounded by `drainTimeout + cancelGrace`, cuts all held leases at once, and is uncancellable | The grace is per blocked call, so sequential cuts would cost one grace each; cutting in parallel makes the bound a sum of two knobs, not a product. A close that can be cancelled is a close whose bound is a suggestion. Measured under S9 with a download held mid-stream: about 1 s against `drainTimeout = 1 s`, `cancelGrace = 300 ms` (Sec 11.2) |
| D41 | Server-answered failures are neither retried inside the call nor counted by the breaker; only wire failures are | An answered request proves the server is reachable and understood it, which is what the breaker measures; and repeating an answered request inside the same call cannot change the answer, while it can move a *new* upstream file over a landed one (T11 found this race under `REPLACE`). `NoSuchFile` from a download would otherwise cost S5 three attempts and let a directory another system writes into open the breaker (Sec 10.2) |
| D42 | The transfer bulkhead is a kotlinx `Semaphore`, not resilience4j's `Bulkhead` | resilience4j's suspend bulkhead takes its permit inside `withContext(Dispatchers.IO)` and only then enters its try, so a caller cancelled at the switch back leaks the permit for the life of the process - the exact shape R1 found in the transport. Its no-wait variant turns the fifth transfer away instead of queuing it. A semaphore taken before the dispatcher switch has neither problem (Sec 9) |
| D37 | The adapter escapes `\`, `*` and `?` in every path it hands JSch for rename, rm, put, get, stat, ls | JSch expands those as a glob in the last component (Sec 5.2); a listed name containing them would otherwise act on its neighbours. Found by R2 against the embedded server: a replace onto `l*.csv` destroyed `ledger-old.csv` |
| D38 | A `NoSuchFile` from rename names the path that is missing | The server answers NO_SUCH_FILE for a missing target directory as well as a missing source, and the adapter used to report both against the source. I11's retry would then stat the target, find nothing, and report the source gone while it was still there. The client now looks at the source on that answer and names whichever is missing (Sec 6.1) |
| D39 | A borrowed session's loan ends when the last call on it finishes, not when the block returns | The revocation was a flag read at call start; a call launched from the block and still on the wire when the block returned kept using a session the pool had re-lent - I2 broken from outside the pool. Calls and the revocation now share a lock, and ending the loan waits under `NonCancellable`, bounded by the ladder (Sec 6.1) |
| D40 | `REPLACE` clears the target only where clearing could help: a non-extension server, a file at the target | On a server with the extension a refusal is never about the target, so clearing deletes a healthy file for nothing - S6's case did exactly that. A directory at the target is never cleared. A refusal after a clear names the now-empty target (Sec 8.2) |
| D36 | `SizeStable` observes inside one poll, batched: one wait per poll, not per file and not across polls | Across polls, the shipped default is an hour of latency per file on the hourly pipeline - the second tick is the first chance to see a stable size. Serially per file inside one poll, a hundred new files cost a quarter of an hour holding the listing's session. Batched, the poll pays `(checks - 1) x interval` once, with the listing's session already released, and `maxFilesPerPoll` bounds what is held (Sec 7.5) |
| D34 | Three pressure layers after the scenario table: seeded randomized adversary, Lincheck, soak | The JVM has no `labrpc`; the fake transport's one hook already is one. A scenario table proves the failures someone imagined; a seeded adversary checking every invariant after every op finds the interleaving nobody did, and is replayable by seed. Lincheck is a cheap guard on the two lock-guarded structures. The soak is the only place thread and heap flatness can be measured (Sec 17) |
| D35 | Performance is measured as degradation and recorded, never asserted; no JMH | Throughput is bounded by one JSch channel per session and the server's session cap, so there is no hot path to benchmark. A latency assertion loose enough not to flake is too loose to catch what it is for; the numbers go in the progress log as observations, the way T6 recorded S11's heap |
| D32 | The startup probe checks each watched directory with `stat`, not `realpath` alone | Measured (Sec 11.1): `realpath` of a path that leads nowhere, and of one that leads to a file, both succeed and return a canonical name on MINA SSHD and on OpenSSH. It is a string operation and proves nothing about existence. The same is why validation-on-borrow uses `realpath "."` - it proves the session answers, which is all that check wants |
| D33 | `overwrite` is an enum wherever it appears, never a boolean | `REPLACE` is a sequence of requests with a gap on a server without the POSIX rename extension, and a boolean cannot say so (Sec 8.1). `Move` takes the same type as `rename`, so a relative target resolves in one place and the validator, probe and executor agree about which folder it names |
| D31 | `socketTimeout` is removed rather than repurposed; `serverAliveCountMax` is pinned to 1 | Spending it as a probe count would round a duration to a multiple of `keepAlive`, making it half of two knobs; and keeping the name while changing the job preserves the misreading D26 exists to end. Pinning the count makes twice `keepAlive` a promise this connector makes rather than one inherited from a dependency's next release (Sec 5.3) |
| D29 | Refusing an overwrite is the connector's decision, never the server's | Measured (Sec 5.2): JSch sends `posix-rename@openssh.com` on its own whenever the server advertised it, so on such a server a rename onto an occupied target destroys the old file and reports success. `Overwrite.REFUSE` is unenforceable at the server and must be a look-then-request in the connector. A writer arriving between the two still wins; on a server without the extension the request itself is refused as well, which closes the race there and only there |
| D30 | A refused overwrite is `OverwriteRefused`, its own class beside `PoolExhausted` and `CircuitOpen` | It is a deterministic policy decision, so retrying it can never succeed and counting it against the breaker charges the connector for doing what it was told. `ServerFailure` is right about the session and the message but wrong about both of those, and from Sec 9 onward would cost three attempts and a breaker failure per call. The session is untouched - under `REFUSE` nothing was even sent - so the lease is returned and the watch continues |
| D28 | A byte count that disagrees with the listed size is `IncompleteTransfer`, not `SessionLost` | Every other `Recoverable` class describes a fault the wire reported; this one is the connector's own integrity check failing, and it had no class. Reporting it as `SessionLost` sends an operator to look at the network when the actual evidence is that a file changed size under them - which is precisely the signal open item 1 is waiting on, and the one a stalled uploader produces. It poisons, because a short read and a half-dead session are indistinguishable from here and the safe reading costs one handshake on a rare event |
| D45 | The shipped readiness default is a heuristic with a stated blind spot, and open item 1 stays open until the upstream team answers | An uploader paused for longer than `minAge` mid-file passes `SizeStable + MinAge`, and no code the connector can write closes that: only the producer's convention can. `markerFile(suffix)` (Sec 7.5) is where the answer lands the day it arrives - it is the one deterministic check - and until then the blind spot is documented in Sec 7.5 and the T15 entry rather than designed around. `IncompleteTransfer` (D28) is the signal a stalled uploader actually produces |
| D27 | `ServerFailure` does not poison the session | A well-formed `SSH_FX_FAILURE` proves the channel parsed the request and answered, so the session is healthy and a per-request refusal is no reason to throw it away. Sec 8.2 expects exactly that status from a server without `posix-rename`, which would otherwise evict a session on every overwrite rename. Real transport breakage arrives with an `IOException` cause and is classified `SessionLost` before this rule is reached |

D24 and D25 were withdrawn during the design review and are not reused; a citation to either is
a citation to nothing.

---

## 16. Open Items Before Implementation

1. **Producer-side completeness convention** - the uploader decides whether a listed file
   can be trusted as complete. Two conventions make it certain: upload under a temporary name
   and rename when finished (rename is atomic, so a half-written file never carries the final
   name), or write a marker file next to the finished file. Ask the upstream team how they
   upload. Until they answer, the default readiness check (Sec 7.5) is a heuristic that a
   stalled uploader can fool. **Still open, and no code can close it** (D45): T15 analysed what
   the shipped default protects against and what it does not - an uploader paused for longer
   than `minAge` mid-file, a burst writer, a server clock behind ours - and `markerFile(suffix)`
   ships as the deterministic answer for the day the upstream team replies.
2. ~~**Temp folder ownership**~~ - **closed by T9.** The startup probe handles both ownership
   models and says which one the deployment is in: `createActionTargets = true` runs `mkdir -p`
   and, when the account cannot create the folder, refuses with the remedy "set it false and ask
   the upstream to create it"; `createActionTargets = false` insists the folder is already there
   and refuses naming that setting. `StartupAgainstServerTest` covers all three cases.
3. ~~**JSch error wording**~~ - **closed by T2.** The table was assembled by staging each real
   condition against mwiede 2.28.7 and reading what came out; every row has an
   embedded-server test, so a wording change fails a test instead of misclassifying an error.
   Two findings worth carrying: the host key and proxy failures have exception types of their
   own in this fork and are matched by type rather than wording, and both transport breakages
   arrive as `SftpException` with the generic `SSH_FX_FAILURE` code and an `IOException`
   cause - the same type and code the server uses for its own refusals - so the mapper must
   check the cause before the status code. The measured table is in the T2 progress entry; T15
   added a tenth wording (`connection is closed by foreign host`) and Sec 5.4 now records which
   failures are matched by type rather than by wording.

Resolved during review: the proxy imposes no connection cap, so `maxSize` is bounded only by
the infra team's five sessions (D21).

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
3. **Toxiproxy.** Real network partitions through Testcontainers, between the CONNECT proxy and
   the SSH server - the production failure is "the tunnel is up but the far side is gone", which
   the network gives no signal for. A CI gate wherever Docker is present; skips with a clear
   message where it is not. The partition matrix is Sec 17.3.

Three more layers run after the scenario table, in the pressure ticket, the way a Raft
implementation is tested rather than the way a library usually is (D34):

4. **Seeded randomized adversary over the fake.** `FakeSftpTransport`'s single hook is a
   controllable network. A seeded random drives it for thousands of sequences - succeed, delay,
   throw one of the failure classes, kill the session mid-call - and every invariant in Sec 17.1
   is checked after every operation. Deterministic per seed, shrunk to the shortest failing
   prefix, runs on every build in seconds under virtual time. This is the layer that finds the
   interleaving nobody wrote a scenario for.
5. **Lincheck model checking** on the two lock-guarded structures, `InFlightSet` and
   `SessionRegistry`, exploring thread interleavings exhaustively and printing the one that
   breaks. A cheap regression guard, not an investment: one Mutex with nothing suspending inside
   it has few interleavings worth exploring.
6. **Soak (opt-in).** `watch` for hours behind a random-fault proxy, sampling threads, post-GC
   heap and the `sftp_*` meters every minute; asserts flatness by slope, recovery time by bound,
   and exactly-once delivery to the temp folder by count. Recovery is measured heal-to-next
   `PollCompleted` and bounded by `2 x keepAlive + max backoff + interval + waitInOpen`: a watch
   recovers on its next tick, and a breaker that opened lets nothing through until its wait in
   open has run (T16).

Performance is measured as *degradation*, not throughput: JSch serialises on one channel and the
server caps sessions, so throughput is bounded by design and a microbenchmark answers nothing.
What is recorded is acquire p50/p99 as concurrency passes `maxSize`, listing memory under
concurrent 100k listings, and op latency by failure class under each partition. Numbers are
observations in the progress log, not assertions in a test (D35).

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
| I15 | At-least-once with no phantom failure: every file that was acked is at the ack target; every file listed and not acked is in flight, not ready, or redelivered; no file is silently gone and no landed move is reported as failed. The phantom-failure clause is bounded by the retry budget and the breaker: a lost reply on the **last permitted** try is reported as a failure while the file sits at the target, and the consumer's WARN line is the only record (T16) |

### 17.2 Scenario table

| ID | Scenario | Expected |
|---|---|---|
| S1 | Server kills the session during download | Download retried on a new lease, old entry evicted, consumer sees one `FileSeen` and one successful download |
| S2 | Server stalls past the keepalive ladder | `SessionLost`, poisoned, retried; breaker counts one failure |
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

### 17.3 Partition matrix (Toxiproxy)

Topology: `client -> LoopbackConnectProxy (in-process) -> Toxiproxy (container) ->
EmbeddedSftpServer`. One test per row, named by its fault, asserting the disposition, the
counter that moved and the recovery - never the toxic itself.

| ID | Fault | While | Expected |
|---|---|---|---|
| P1 | half-open: `timeout` toxic with `timeout=0` (data drops, no FIN), both directions | mid-download | `SessionLost` within `2 x keepAlive` plus one keepalive of grace (D44); poisoned; retried on a fresh lease; one `FileSeen`, one file in the temp folder |
| P2 | `reset_peer` | mid-download | as P1, and faster than the keepalive bound |
| P3 | `reset_peer` | after a `rename` request is on the wire, before its reply | I11: retry stats the target; success reported once; no phantom `NoSuchFile`; `sftp_retry_total{op=rename}` is 1 |
| P4 | `proxy.disable()` | pool at 0 idle, a poll starting | `ConnectFailed`; breaker counts; `watch` emits `PollFailed` and continues; first poll after `enable()` is `PollCompleted` |
| P5 | flapping: `disable`/`enable` every 3 s for 60 s | `watch` running | breaker cycles closed, open, half-open, closed; sessions never exceed `maxSize`; every tick is `PollCompleted`, `PollFailed` or `PollSkipped`; the flow never terminates |
| P6 | `timeout=0` | during `close()` with a download in flight | I9 holds under a real partition; `.part` gone; `reason=shutdown` counted |

`2 x keepAlive` is JSch's give-up point measured from the last byte received; the connector's
write-off - disconnect, ladder, eviction - lands a few tens of milliseconds after it and can
never land before it. Measured under P1 and the stall at `keepAlive = 500 ms`: 1023-1064 ms
across ten runs (D44). A partition test asserts the bound plus one keepalive of grace and
records the number.

Docker is present on the development machine after all (T15); T1's finding was stale. The
skip path has not been exercised anywhere yet: Testcontainers falls back through every client
strategy and the named pipe is always reachable here, so it must be run once on a machine
without Docker and the message read.

Not in the matrix on purpose: cooperative cancel under `bandwidth` (proved with the loopback
proxy's `holdAfter`), `latency` p99 (the soak's job), `slicer` (SSH's own framing makes a
framing bug implausible).
