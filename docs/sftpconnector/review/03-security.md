# T17 lens 3: security

Reviewer: a fresh Opus 5 subagent with one lens, tracing every server-supplied string to every
sink and every secret to every renderer. Read-only; nothing was built or run, so every finding is
reproduced by reading and each names the test that would demonstrate it.

Scope read in full: spec 5.2, 5.4, 6.1, 6.3, 7.1, 7.4, 8, 10, 12, 14 and the decision log (D8,
D11, D37); progress.md's open-seams table, T1, T2, T6, T7, T9, T14; every main source file under
`sftpconnector/core`, `sftpconnector/quarkus` and `sftpconnector/testkit`.

Line numbers below are the real ones in the files as they stand. (Lens 4's report uses a different
numbering; where the two describe the same line it is called out.)

## What is sound, so the findings are read in proportion

- **No shell, no process, no format string.** `ProcessBuilder`, `Runtime.exec`, `String.format`
  and `.format(` appear nowhere in `core` or `quarkus`. Every one of the 30-odd log calls uses a
  compile-time constant format with `{}` placeholders and passes variable text as an argument, so
  a server-supplied string can never be parsed as a format.
- **The local staging join is genuinely well built.** `SftpClient.stagingTargetFor`
  (`SftpClient.kt:169-182`) reads the join backwards rather than blacklisting characters: after
  `resolve` and `normalize` the result must still start with the staging directory *and* its last
  element must equal the listed name exactly, plus an outright refusal of any name holding `\` and
  a `catch (InvalidPathException)` for names the filesystem cannot spell. I tried to defeat it and
  could not: `..`, `.`, the empty name, `/etc/passwd`, `sub/../evil`, `C:evil` and a NUL-bearing
  name are all refused, on both platforms. `UnsafeFileName` is `ACCEPT_THE_REFUSAL`, so a hostile
  name costs no retry and does not touch the breaker. This is the one place a server name reaches
  the local filesystem and it is checked there.
- **Host-key pinning is enforced on every reconnect, not just the first connect.**
  `JschTransport.openSession` (`:119-137`) constructs a fresh `JSch` and calls `setKnownHosts`
  per session, so every pool refill, every retry's fresh session and every post-`maxLifetime`
  replacement re-reads the file and re-verifies. `HostKeyRejected` is `Fatal` →
  `STOP_THE_CONNECTOR` (`SftpException.kt:189-190`, `Disposition`), so a key that changes at
  reconnect ends the watch instead of being retried past. This is the correct answer to "what
  could a MITM do at reconnect": under `Strict`, nothing but stop the connector.
- **`Strict` fails closed on a missing or empty known-hosts file.** JSch's
  `KnownHosts.setKnownHosts(String)` swallows `FileNotFoundException` and leaves an empty
  repository; with `StrictHostKeyChecking=yes` every key is then unknown and every handshake is
  rejected. The empty-path substitution the Quarkus adapter deliberately makes
  (`SftpConnectorProperties.kt:263-271`) therefore refuses rather than silently standing in
  `~/.ssh/known_hosts`, exactly as its comment claims.
- **No credential reaches any renderer.** `AuthMethod.Password` is deliberately not a data class
  and masks its own `toString` (`SftpConnectorConfig.kt:40-48`), so the generated `toString` of
  the enclosing `SftpConnectorConfig` data class prints `secret=***`. The password is handed to
  JSch as bytes at `JschTransport.kt:127` and nowhere else. `sftpconnector/quarkus` has exactly
  one log call in main (`SftpConnectorLifecycle.kt:83`, about a slow close) and `testkit` main has
  none, so neither the adapter nor the test kit can print a configuration.
- **`Attempt` cannot carry credentials.** It holds `endpoint` (`host:port`, no user), `operation`
  (a fixed vocabulary), `path` and `number` (`SftpException.kt:18-35`). No construction site
  anywhere passes a user name or a secret into any of the four. JSch's own
  `Auth fail for methods '...'` text names the server's offered methods, never the credential, so
  `AuthenticationFailed`'s message (`JschErrorMapper.kt:89-90`) carries nothing secret either.
- **No host key material reaches a log.** `HostKeyRejected`'s detail is JSch's message, which on
  the pinned 2.28.7 is `reject HostKey: <host>` or `HostKey has been changed: <host>` — no
  fingerprint. Nothing leaks; see M6 for the cost of that.
- **Meter tags are closed vocabularies.** Every tag value in `ClientMeters`, `SourceMeters`,
  `PoolMeters` and `JschErrorMapper` is the endpoint or a constant from a fixed set; no file name,
  no server text, no tick number becomes a series. There is no metric-cardinality injection.
- **JSch glob escaping is applied at the seam.** `String.literally()` (`JschTransport.kt:306`)
  escapes `\`, `*` and `?` before `ls`, `stat`, `get`, `put`, `rename` and `rm`, which closes
  D37's finding that `delete("/drop/*.csv")` removed every match. `mkdir` and `realpath` are sent
  raw, which is correct because JSch does not expand those; see L3 on the missing regression pin.
- **The pool and transport layers never touch a remote path.** The only path either sends is the
  constant `realpath(".")` used to validate a parked session (`SftpPool.kt:209`), so no
  server-supplied string reaches any of the pool's log lines or exception messages.

## Findings, by severity

### HIGH

**H1. A listed name is never required to be a single path component, and `RemoteFile.path` reaches
`rename` and `delete` unchecked.**
`JschTransport.kt:176-184` builds every entry's path as `dir + "/" + entry.filename` (via
`asDirectoryOf`, `:315`); `RemoteFile.name` is `path.substringAfterLast('/')`
(`RemoteFile.kt:22`); `SftpSource.FileHandling.perform` (`SftpSource.kt:406-413`) sends that path
as the *source* of a rename and as the argument of a delete.

Class of weakness: CWE-22, path traversal, on the remote side. The local side is guarded
(`stagingTargetFor`); the remote side has no equivalent, and the asymmetry is the defect.

Triggering condition: a hostile or non-conforming server answers `SSH_FXP_READDIR` for `inbound/`
with the filename `../../../home/etl/.ssh/authorized_keys`. `.` and `..` alone are filtered
(`JschTransport.kt:178`); nothing filters a filename containing `/`. The entry becomes
`RemoteFile(path = "inbound/../../../home/etl/.ssh/authorized_keys", name = "authorized_keys")`.
On ack with `onAck = move("done")`, `perform` sends
`rename("inbound/../../../home/etl/.ssh/authorized_keys", "inbound/done/authorized_keys")` — the
account's own key file is moved into a folder the attacker can read, and removed from where SSH
looks for it. With `onAck = delete()` the same entry deletes any path the account can unlink. The
move *target* is safe, because `substringAfterLast('/')` strips the traversal from `name`; the
source is not.

Why High and not Critical: an ordinary vendor with write access to the drop directory cannot
produce this, because `/` is not a legal character in a POSIX filename. It needs a compromised or
deliberately non-conforming server. But the module's own documentation adopts that threat model in
so many words — `UnsafeFileName`'s KDoc (`SftpException.kt:282-290`) says "a listed name is the
server's word, and whoever can write to the server can call a file `..`" — and the connector's
account is routinely more privileged than the party dropping files.

The fix is one line at the one place entries are made: refuse an entry whose `filename` contains
`/` at `JschTransport.kt:177-179`, the way `.` and `..` already are. A conforming server never
sends one.

Failing test (`sftpconnector/core`, JUnit 5 + Mockito; the fake cannot stage this — see the note
under H1's shape below):

```kotlin
@Test
fun `a listed name that is not a single path component is never sent back to the server`() = runTest {
    val connection = mock<SftpConnection>()
    whenever(connection.list(eq("/drop"), any())).thenAnswer { invocation ->
        val onEntry = invocation.getArgument<(RemoteFile) -> Listing>(1)
        onEntry(RemoteFile("/drop/../../etc/shadow", 12, EPOCH, isDirectory = false))
        Unit
    }
    // drive source.poll("/drop"), ack the one file it hands over
    verify(connection, never()).rename(argThat { contains("..") }, any())
    verify(connection, never()).delete(argThat { contains("..") })
}
```

Note for whoever writes it: `FakeSftpTransport.list` (`FakeSftpTransport.kt:126`) skips any entry
whose remainder past the directory prefix contains `/`, so the fake is structurally incapable of
reproducing this and no existing test could have caught it. The test needs a Mockito
`SftpConnection` or a widened fake. The three existing traversal tests
(`SftpClientTest.kt:216-249`) all cover the *local* join and all use names with no forward slash.

**H2. The shipped default staging directory is the shared JVM temp directory, and the partial file
is created under a server-chosen name without `NOFOLLOW_LINKS`.**
`ConnectorDsl.kt:490` defaults `staging.dir` to `Path.of(System.getProperty("java.io.tmpdir"))`.
`StagingArea.receive` (`StagingArea.kt:44-54`) opens
`<dir>/<listed name>.part` with `CREATE, TRUNCATE_EXISTING, WRITE` and no link option.
`ConnectorDsl.kt:218-223` checks the directory exists and is writable, and nothing else.

Class of weakness: CWE-377 insecure temporary file, plus CWE-59 link following.

Triggering condition: on Linux `java.io.tmpdir` is `/tmp`, mode 1777. The listed name is
attacker-chosen and therefore the partial file's name is predictable, so any other local account
or process in the same filesystem namespace can pre-create `/tmp/ledger.csv.part` as a symlink to
a file the connector's user may write. `Files.newOutputStream` with `CREATE` follows symlinks by
default, so the download is written through the link. The downloaded vendor data also lands at
umask permissions in a directory every local user can read and can `rename` within.

Escalation condition, stated plainly: in a single-user container with no other local account this
is not exploitable, and that is the likely deployment. It is reported at High because the connector
*ships* this default and its KDoc defends it, so a host that deploys on a shared box inherits it
silently. The fix is two cheap parts: add `LinkOption.NOFOLLOW_LINKS` to the `newOutputStream`
options at `StagingArea.kt:47-52` (it is an `OpenOption` and the default provider honours it), and
either default to a private `Files.createTempDirectory` or have the builder refuse a staging
directory that is group- or world-writable, beside the `isWritable` check already there.

Failing test (`sftpconnector/testkit`, POSIX-only, `@EnabledOnOs(LINUX)`):

```kotlin
@Test
fun `a download refuses to write through a symbolic link left at the partial file's name`() = runTest {
    Files.createSymbolicLink(stage.resolve("ledger.csv.part"), outsideTheStage)
    val client = clientOver(FakeSftpTransport().file("/drop/ledger.csv", CONTENT))
    val listed = client.list("/drop").toList().single()

    assertThatThrownBy { runBlocking { client.download(listed) } }
    assertThat(outsideTheStage).hasContent(ORIGINAL) // today it holds CONTENT
}
```

A second test, `I<n>_the default staging directory is private to this process`, would assert the
builder refuses a world-writable staging directory.

### MEDIUM

**M1. A server that lists `X.part` beside `X` can substitute the bytes delivered under `X`.**
`StagingArea.kt:44` names the partial `"${target.fileName}.part"`; `:67` finishes with
`Files.move(partial, target, ATOMIC_MOVE)`.

Class of weakness: CWE-367, a name collision between attacker-controlled input and the module's own
staging protocol, ending in an integrity failure.

Triggering condition: the directory holds `a.csv` and `a.csv.part`. Both are plain names and both
pass `stagingTargetFor`. A consumer that downloads concurrently — which `maxInFlight = 16` and
`maxConcurrentTransfers = 4` exist to permit, and which the open-seams table already names as a
supported shape — runs both. The download of `a.csv.part` writes `a.csv.part.part` and then
`ATOMIC_MOVE`s it onto `a.csv.part`, replacing the name under which `a.csv`'s transfer is still
being staged. `a.csv`'s own size check passes, because `Tally.count` counts its own stream, not
the file on disk; its final `Files.move(a.csv.part, a.csv)` then delivers the *other* file's bytes
under `a.csv`'s name, with a `LocalFile.digest` computed over bytes that are not the ones on disk.
Silent content substitution with a digest that does not match its own file.

Fix without disturbing D11: make the partial name unique per transfer, e.g.
`"${target.fileName}.${UUID.randomUUID()}.part"`. The `finally` at `:74` already removes it on
every path.

Failing test:

```kotlin
@Test
fun `two downloads whose names differ only by the partial suffix do not overwrite each other`() = runTest {
    val server = FakeSftpTransport().file("/drop/a.csv", BYTES_A).file("/drop/a.csv.part", BYTES_B)
    val listed = client.list("/drop").toList()
    val landed = listed.map { async { client.download(it) } }.awaitAll()

    assertThat(stage.resolve("a.csv")).hasBinaryContent(BYTES_A)
    landed.forEach { assertThat(it.digest).isEqualTo(digestOf(it.path.readBytes())) }
}
```

**M2. The one call a consumer naturally reaches for bypasses the name check, and nothing says so.**
`SftpEvent.FileSeen.download(localTarget)` (`SftpEvent.kt:41-44`) forwards to
`SftpClient.download` (`SftpClient.kt:147-153`), which applies `stagingTargetFor` **only** when
`localTarget` is null.

Class of weakness: CWE-1173, an unsafe default on the path of least resistance.

Triggering condition: a consumer writes the obvious thing —
`event.download(myDir.resolve(event.file.name))` — and reintroduces the exact defect the hotfix
before T11 closed. `event.file.name` is server-supplied; on Windows a listed
`..\..\evil.csv` resolves two directories up, which is the red run recorded in the seams table.
`SftpClient.download`'s KDoc does warn ("a caller that names its own target has taken over
deciding what is safe to write", `:142-143`), but `FileSeen.download`'s KDoc — the one a consumer
reads — does not, and says only "where the file lands".

Fix: apply the same check to the last element of an explicitly given target, or at minimum repeat
the warning at `SftpEvent.kt:41-44` and expose the checked name so a consumer has a safe way to
build one.

Failing test: `a consumer that names its own target under a listed name is still refused a name
that is not one`, asserting `UnsafeFileName` from `event.download(dir.resolve(event.file.name))`
for a file listed as `/drop/..\..\evil.csv`.

**M3. Server-supplied names and server error text reach log lines and exception messages with
control characters intact.**
`Attempt.describe` (`SftpException.kt:28-35`) appends `path` verbatim; `JschErrorMapper.unknown`
(`:151-167`) logs the server's raw message; `SftpSource.kt:381` and `:417` log `slot.file.path`;
`SftpClient.kt:180` puts the listed name in `UnsafeFileName`'s detail.

Class of weakness: CWE-117, log injection.

Triggering condition: a vendor creates a file whose name contains a newline — legal on every POSIX
filesystem and reachable without any server compromise, unlike H1. The name passes
`stagingTargetFor` (a newline is a valid file-name character on Linux), is handed over, and any
WARN or exception naming it forges a second log record. Against a plain-text appender an attacker
who can name files can write arbitrary lines into the connector's log, including lines that look
like the connector's own INFO. A JSON appender escapes it; the module does not choose the host's
appender.

Fix: strip or escape control characters once, in `Attempt.describe` and in `unknown`'s `raw`.
Lens 4 records the same observation as its L6; this is the same line and should be fixed once.

Failing test: `a listed name holding a newline is rendered on one line`, asserting
`UnsafeFileName(...).message` and the captured WARN contain no `\n` past the first.

**M4. A symbolic link is indistinguishable from a regular file, so the connector will read and act
on whatever it points at.**
`SftpATTRS.describe` (`JschTransport.kt:321-325`) keeps `size`, `mTime` and `isDir` and drops
`isLink`; `RemoteFile` (`RemoteFile.kt:14-23`) has no field for it. `SftpTransport.kt:39` records
that `realpath` follows links, and `channel.get`/`channel.stat` follow them server-side.

Class of weakness: CWE-59, link following, at a trust boundary the connector does not model.

Triggering condition: a vendor with write access to `inbound/` — an ordinary vendor, no server
compromise — creates the symlink `inbound/data.csv -> /etc/passwd`. `READDIR` reports it with
lstat attributes, so `isDirectory` is false and it looks like a small ordinary file. The download
follows the link server-side and delivers the target's bytes into the pipeline. The ack then moves
or deletes the *link*, leaving no trace at the target and no signal anywhere that a link was
involved. A second effect: `MinAge` judges on `modifiedAt`, which is the link's mtime from the
listing, while `SizeStable` calls `client.stat`, which follows the link and reads the target's
size — so one file is judged on two different objects.

This is a server-side control in the first instance (chroot, `follow_symlinks=no`). What the
connector owes is the ability to see it. Fix: carry `isLink` on `RemoteFile` and let a
configuration refuse linked entries.

Failing test: `a listed entry that is a symbolic link is reported as one` against the embedded
server with a link staged, asserting `RemoteFile.isLink`; today the field does not exist, so the
test does not compile, which is the honest red.

**M5. A recursive walk flattens every subdirectory into one action folder and one staging
directory, so two files of the same name collide.**
`SftpSource.walk` (`:335-341`) descends and emits full paths; `perform` (`:409`) builds the move
target from `file.name` only; `stagingTargetFor` (`SftpClient.kt:169-182`) does the same locally.

Class of weakness: CWE-706, name collision after a many-to-one mapping.

Triggering condition: `recursive = true`, and the vendor writes `inbound/a/report.csv` and
`inbound/b/report.csv`. Both move to `inbound/done/report.csv`. Under `Overwrite.REFUSE` the
second is refused for good and re-handed on every tick, which is the T12 seam. Under
`Overwrite.REPLACE` the second silently destroys the first, and both stage locally onto
`<staging>/report.csv`. `SftpClient.kt:141-143` documents this collision for two watched
directories; nothing documents it for subdirectories of one, which is the case `recursive` creates
by itself.

Failing test: `two files of one name in two subdirectories do not become one file`, asserting both
survive an ack under a recursive watch.

**M6. After a `HostKeyRejected` an operator cannot tell a key rotation from a man in the middle.**
`JschErrorMapper.kt:72-78` wraps JSch's message and adds nothing.

Class of weakness: insufficient logging of a security-relevant event (CWE-778), in the one place
where the response differs completely between two causes.

Triggering condition: the connector stops with *the server presented a host key the connector will
not accept: HostKey has been changed: sftp.example*. On the pinned JSch the fingerprint appears
only in its interactive prompt and never in the exception, so the message names no fingerprint, no
key type and no known-hosts path, and does not distinguish "changed" (possible MITM, page someone)
from "unrecognised" (rotation, update the file). Both are the same sentence.

Fix: read the offered key from `JSchHostKeyException`'s own fields where the fork exposes them,
and always name the `knownHosts` path the policy was configured with, which the connector knows
even when JSch does not say it. Lens 4 records the operability half of this as its M1; the two
should be fixed in one commit.

Failing test: `a rejected host key names the file that was consulted`, asserting the message
contains the configured known-hosts path.

### LOW

- **L1. The password is a `String` for the life of the process.** `SftpConnectorConfig.kt:45`,
  `ConnectorDsl.kt:333`, `SftpConnectorProperties.kt:68`. It cannot be zeroed, so it sits in the
  heap and in any heap dump for as long as the connector runs. JSch is handed a fresh byte array
  per connect (`JschTransport.kt:127`) and zeroes its own copy on disconnect, so the transport
  half is fine; the config half is not. A `CharArray` or a `() -> String` supplier would let a
  host clear it. Worth recording, not worth churning the DSL for on its own.
- **L2. `HostKeyPolicy.Strict` still does not check its own file at build time.** Already the T14
  seams row. It matters to this lens only because the failure mode is safe: the handshake rejects,
  it does not fall back. Confirmed by reading JSch's `KnownHosts.setKnownHosts(String)`. No
  action beyond closing the existing seam.
- **L3. Nothing pins the `mkdir`/`realpath` glob exception.** `JschTransport.kt:237` and `:166`
  send the path raw on the strength of D37's reading of the JSch sources. The escaping at `:306`
  has tests; the two deliberate exceptions have none, so a JSch version that started expanding
  `mkdir` would reintroduce D37's finding on the one call the startup probe makes with an
  operator-supplied path. One test against the embedded server, `mkdir names one directory even
  when the path holds a star`, would pin it.
- **L4. `sftp.connector.auth.password` is an ordinary SmallRye config property.** It carries no
  `SecretKeys` marking, so it is protected only by Quarkus's own name-based masking, which happens
  to match on `password`. If the property is ever renamed the masking silently stops applying.
  Noted so a rename does not lose it.

## Answers to the lens questions not already covered

- **Can `Attempt` messages carry credentials?** No. Four fields, none of which any call site ever
  populates with a user name or a secret, and no `describe` overload takes more.
- **Is `AcceptAll` reachable in production?** Yes, with one property:
  `sftp.connector.host-key.policy=ACCEPT_ALL` (`SftpConnectorProperties.kt:270`). That is spec D8,
  a recorded decision the maintainer took knowingly, and the connector makes it visible with a
  WARN at build time (`ConnectorDsl.kt:238-247`). It is not reported as a defect. Two consequences
  are worth the maintainer's eye: under `AcceptAll` no `HostKeyRejected` can ever be raised, so a
  server whose key changes mid-run produces no signal at all, at any level, ever; and there is no
  gate — no profile check, no separate acknowledgement flag — between a properties file and
  accepting any key, so the WARN is the whole of the defence.
- **What could a MITM do at reconnect?** Under `Strict`, nothing but stop the connector: the
  key is re-verified per session and the rejection is fatal. Under `AcceptAll`, everything —
  harvest the password on the first handshake, then serve any directory listing it likes, which is
  precisely the input H1 and M3 need.
- **`Fingerprint` pinning.** Not implemented; `HostKeyPolicy` (`SftpConnectorConfig.kt:54-63`) has
  two of spec 5.2's three cases. This is the T1 seams row and the ticket names it. Its security
  cost is not the missing feature but the pressure it creates: an operator who has the server's
  fingerprint but no known-hosts file has only `AcceptAll` available to them.

## Ranked list

| # | Severity | Finding | Owner |
|---|---|---|---|
| H1 | High | Listed names are not required to be one path component; the path reaches `rename` and `delete` | Ticket owner, this ticket, with the test |
| H2 | High | Default staging is the shared temp dir; the partial file follows symlinks | Ticket owner, this ticket, with the test |
| M1 | Medium | `X.part` beside `X` substitutes content under a concurrent download | Ticket owner; one-line unique partial name |
| M2 | Medium | `FileSeen.download(localTarget)` bypasses the name check unwarned | Whoever next touches the source's public surface |
| M3 | Medium | Control characters in names and server text forge log records | Fix with lens 4's L6, one commit |
| M4 | Medium | Symlinked entries are invisible to the connector | The maintainer; needs a policy decision, not just a field |
| M5 | Medium | A recursive walk flattens names into one folder | The maintainer; overlaps the T12 refusal seam |
| M6 | Medium | `HostKeyRejected` cannot distinguish rotation from MITM | Fix with lens 4's M1, one commit |
| L1 | Low | The password lives as a `String` | Recorded only |
| L2 | Low | `Strict` does not check its known-hosts file exists | Existing T14 seams row |
| L3 | Low | The `mkdir`/`realpath` glob exception is unpinned | Whoever next touches the adapter |
| L4 | Low | The password property is not marked as a secret key | Recorded only |

Two High, six Medium, four Low. No Critical.

## Verdict

The connector's security posture is asymmetric in one specific and correctable way: it treats the
server's word as untrusted where that word touches the *local* filesystem, and as trusted where it
touches the *remote* one. `stagingTargetFor` is the best-built guard in the module and I could not
get past it; twenty lines away, the same server-supplied string is sent back as the source of a
rename and the argument of a delete with no check at all (H1). The secret handling is clean — I
traced the password to every renderer and it reaches none of them, and `Attempt` is structurally
incapable of carrying one. Host-key pinning under `Strict` is genuinely enforced at every
reconnect, which is the property most implementations get wrong, and it fails closed on a missing
file.

What the maintainer must decide before production, in the order the decisions gate each other:

1. **`Fingerprint` pinning** (the ticket names it). While it is missing, an operator who has the
   server's fingerprint but no known-hosts file must choose `AcceptAll`, and `AcceptAll` is the
   configuration under which H1's input becomes available to any network attacker rather than
   requiring a compromised server. The two findings are linked: pinning is what keeps H1's
   preconditions off the table for a deployment that cannot manage a known-hosts file.
2. **Whether `AcceptAll` needs a gate beyond a WARN** — a separate acknowledgement property, or a
   refusal outside a named profile. D8 accepted the risk; what D8 did not decide is whether the
   risk should be one property away from a copy-pasted properties file. Deciding "no gate" is a
   fine answer, but it should be a decision and not an omission.
3. **Whether the connector models symbolic links at all** (M4). If the answer is "the server is
   chrooted and that is the infrastructure team's boundary", record it as a decision entry, because
   the module currently cannot see a link even to report one, and nothing in the spec says that is
   deliberate.
4. **The staging directory's default** (H2). Either the connector ships a private directory it
   creates itself, or the builder refuses a shared one, or the spec records that the default is
   for development and a deployment must name its own. The current position — a defended default
   pointing at `/tmp` — is the one that reads as a decision but is not.

H1, H2 and M1 are each a small diff with a test that fails today, and all three are inside this
ticket's remit. M4 and M5 need a ruling before code. Nothing here contradicts a spec section; H1
and M4 sit in the gap spec 14's known limitations do not currently name, and the spec would be
more honest for naming them.

---

## Adjudication (ticket owner, T17)

Written after the fixes, against the report above, which is left exactly as the reviewer wrote it.
Two Highs fixed in their own commits with the tests that found them; one Medium and one Low fixed
here; one Medium fixed by lens 4; the rest recorded as seams with an owner and a reason. Lens 4 was
fixing `StartupProbe.kt`, `source/SftpSource.kt` and `resilience/Resilience.kt` in parallel, so
nothing here touches those three files.

The report expects M3 and M6 to be fixed with lens 4's overlapping findings, one commit each. Lens
4 fixed its M1 (M6 here) and recorded its L6 (M3 here) as belonging to this lens, so **M3 is fixed
here** and M6 is not touched.

| # | Disposition | Where |
|---|---|---|
| H1 | **Fixed** | `ee2f891`, `JschListingNamesTest` |
| H2 | **Fixed** | `c42bda0`, `StagingSafetyTest` (four tests, one skipping off POSIX) |
| M1 | **Recorded as seam** - owner: the maintainer, needs a ruling against spec 6.3 and 14.1 | below |
| M2 | **Partly fixed** (the warning), rest a seam - owner: whoever next touches the source's public surface | `SftpEvent.kt` |
| M3 | **Fixed** (lens 4 recorded its L6 as this lens's) | `LogForgingTest`, and H1 closes the listing half at source |
| M4 | **Recorded as seam** - owner: the maintainer, needs a policy decision | below |
| M5 | **Recorded as seam** - owner: the maintainer, overlaps the T12 refusal seam | below |
| M6 | **Handled by lens 4** as its M1 | lens 4's commit |
| L1 | **Recorded as seam** - owner: the maintainer | below |
| L2 | **Already a seam** (T14's row); no action, and the report agrees | T14 seams table |
| L3 | **Fixed**, in the half a test can hold | `JschListingNamesTest`, fourth test |
| L4 | **Recorded as seam** - owner: whoever next renames a Quarkus property | below |

### H1 - fixed

`JschConnection.list` now builds an entry's path only when the server's filename is one path
segment: not empty, not `.` or `..`, and holding no separator and no control character. `.` and
`..` stay silent as they have since T6; anything else is skipped with one WARN naming the endpoint,
the directory and the raw name with its control characters escaped.

**Skip and warn, not fail the listing**, and the reasoning is spec 7.4 and 10.2. Spec 7.4 already
makes skipping the listing's way of not handing something over - directories go this way by
default, `.` and `..` always have - so a skip needs no new vocabulary and no new failure class.
Failing would need one: there is no class in 10.1 for "the server said something impossible", and
the nearest, `UnsafeFileName`, is about a name reaching the *local* filesystem. Worse, a failure
would cost the whole poll of that directory, on every tick, for as long as the entry sits there -
and the party who can name the entry is the party who would then be choosing when the connector
runs. Skipping keeps the rest of the drop moving and leaves the WARN as the record.

The same rule reaches `realpath`, the other server-supplied string that ends up in front of a path
join: its answer becomes the watched directory that every later listing, action target and log line
is built on, so an answer holding a control character is refused as a `ServerFailure`
(`SSH_FX_FAILURE`) - the server answered, the session is fine, the next tick is the retry, per
10.2's third row. `stat` needed nothing: it describes the path the connector sent and joins no
server-supplied name, so there is no join there to guard.

Tested with a Mockito stand-in for `ChannelSftp`, for the reason the report gives: a conforming
server cannot send such an entry, and `FakeSftpTransport.list` drops anything holding a separator
before it reports it. The fake was left as it is rather than widened - the report offers either -
because the stand-in sits at the seam the defect is at, and widening the fake would have put a
hostile-server switch into a class every other test in the suite builds on.

### H2 - fixed

Both halves, because either alone leaves the other's hole open.

The partial file is opened `CREATE_NEW + WRITE + NOFOLLOW_LINKS` - "make this file, and fail if
anything is already there". A `.part` found first is taken away only when reading it *without*
following links says it is a regular file, which is the only thing this connector can have written;
that keeps spec 6.3's stale-part rule working (14.1 defers resume, so a `.part` from a dead run is
a fragment nobody will finish) while refusing anything else with `UnsafeFileName`. That class is
the right one: ACCEPT_THE_REFUSAL, because the same thing will be there next time, no session was
borrowed, and the server did nothing wrong. What is refused is **left where it is** - the connector
did not put it there, and clearing it quietly would remove the only evidence that somebody did.

**The default staging directory is now `Files.createTempDirectory("sftp-connector-")` inside
`java.io.tmpdir`, made once per process**, rather than `<tmpdir>/sftp-connector-<pid>`. Both were
on the table; the random name is what decides it. A pid is predictable, and a directory created at
a predictable path under a mode-1777 parent can be created by somebody else first, at which point
its owner-only permissions are *their* owner's. `createTempDirectory` gets both properties at once:
the platform makes it `rwx------` where it has permissions to express that, and the name carries
random digits, which is the half that still holds where it has not - nothing can be planted at a
path nobody can guess. One directory per process rather than one per configuration built, and
registered for deletion at exit, which takes it away when the application has moved every file out
of it and leaves it alone when it has not.

**No T6 staging test needed its expectation changed.** `ConnectorDslTest.a connector nobody
configured for staging still has somewhere to put a download` asserts the default exists, is
writable, and digests with SHA-256; all three still hold. Nothing anywhere asserted the default
*was* `java.io.tmpdir`, which is the assertion this change would have had to break.

One honest gap: the symlink test needs a privilege Windows does not grant by default, so it carries
an assumption and skips on the maintainer's machine. Left alone that would be a guard nobody is
watching, so the same refusal is staged platform-independently by a directory at the partial file's
name - `anything at the partial file's name that this connector did not write is refused` - which
runs everywhere and was red before the fix.

### M2 - the warning, and the check as a seam

The KDoc on `SftpEvent.FileSeen.download` now says what `SftpClient.download`'s already said, in
the place a consumer actually reads it, and names the exact call that reintroduces the defect
(`myDir.resolve(event.file.name)`). That is the report's "at minimum".

Applying the check to a caller-given target is **not** done here, and is a seam. It is a behaviour
change and not a hardening: a caller naming a target outside the staging directory - which is the
documented reason the parameter exists, per `SftpClient.download`'s KDoc on two watched directories
holding one name - would start being refused. Deciding whether that parameter means "somewhere else
entirely" or "a different name in a safe place" is a public-surface decision.
**Owner: whoever next touches the source's public surface.**

### M3 - fixed

Guarded at the rendering rather than at the input, because not every string that reaches a message
is a name that could have been refused: the server's own error text goes the same way and there is
nothing to refuse it for. `String.onOneLine()` in `sftp.connector.error` escapes `\n`, `\r`, `\t`
and every other C0 control as a printable escape a reader can undo by eye, so the text stays a
copy of what the server said rather than a summary of it, and two places use it:

- **`Attempt.describe`**, which is the one place a failure's message is put together. Both strings
  in it that nobody in this connector wrote - the `path`, and a `detail` that may be quoting the
  server - go through it, so every `SftpException` in the hierarchy is covered by one change rather
  than by remembering at each construction site. That includes `UnsafeFileName`'s detail, which is
  where `SftpClient.kt:180` puts the listed name.
- **`JschErrorMapper.unknown`'s WARN**, the one log line in the connector that prints text nobody
  has read, and the one the connector has least say over.

`JschTransport` had grown its own copy of the same escaper for H1's WARN; it now uses this one.

Two notes on scope. First, **H1 closed the listing half of M3 at the source**: a listed name
holding a newline is no longer turned into a `RemoteFile` at all, so `slot.file.path` can no longer
carry one out of a listing. That matters because `SftpSource.kt:381` and `:417` log that path
directly and `SftpSource.kt` was lens 4's file this ticket - they were left untouched, and they are
now fed only names that have been checked. A path holding a control character can still reach them
from configuration, which is the operator's own text. Second, `Unknown.rawMessage` is left raw as a
property: it is data for a caller, and it is the *rendering* that was the defect.

### M6 - handled by lens 4

Lens 4 fixed this as its M1, in its own commit, as the report asks. Nothing here touches
`JschErrorMapper`'s host-key branch.

### M1 - seam, and why it is not a one-line fix

The report's fix - `"${target.fileName}.${UUID.randomUUID()}.part"` - closes the substitution and
opens something else. Spec 6.3 says no partial file survives a run, and today that holds two ways:
the `finally` removes it on every exit, and anything a killed process left is cleared by the next
download of the same file, which is the branch H2 has just made explicit. Under a random name there
is no next download of the same name, so a fragment from a crashed run is litter nothing will ever
collect - in a directory the connector now creates itself. Spec 14.1's resume design also reads
"the local `.part` length", which needs a partial file findable by name.

So M1 needs a ruling between three answers rather than a line: a random name plus a sweep of the
staging directory at startup; a deterministic unique name (the remote path digested, which stays
findable); or refusing to stage a listed name that collides with the connector's own `.part`
namespace, which is one line in `stagingTargetFor` and the same shape as its existing refusals but
denies a legitimately named `*.csv.part` file. **Owner: the maintainer**, against spec 6.3 and
14.1. The finding itself is accepted in full - the substitution is real and the report's reasoning
about `Tally.count` counting its own stream is correct.

### M4, M5, L1, L4 - seams

- **M4** (symlinked entries invisible). The report is right that this needs a policy decision and
  not just a field: `isLink` on `RemoteFile` is the cheap half, but what a connector *does* with a
  linked entry - refuse it, follow it, hand it over labelled - is a configuration surface, and the
  `MinAge`-versus-`SizeStable` split the report found (one judges the link's mtime, the other the
  target's size) is a readiness question sitting on top of that. **Owner: the maintainer.** Worth a
  decision entry either way, including "the server is chrooted and this is the infrastructure
  team's boundary", which the spec does not currently say.
- **M5** (a recursive walk flattens subdirectories into one action folder and one staging
  directory). Overlaps the T12 refusal seam and needs the same ruling. **Owner: the maintainer.**
- **L1** (the password lives as a `String`). Real, and cheap to state but not to fix: a `CharArray`
  or a `() -> String` changes the DSL, the Quarkus properties mapping and `AuthMethod.Password`'s
  masking together. The transport half is already right - JSch is handed a fresh byte array and
  zeroes its own copy. **Owner: the maintainer**, worth doing with the next change to the auth
  surface rather than on its own.
- **L4** (`sftp.connector.auth.password` is not marked a secret key). Recorded exactly as the
  report asks: the masking is Quarkus's name-based one and would stop silently on a rename.
  **Owner: whoever next renames a Quarkus property.**

### L3 - fixed in the half a test can hold

D37's two deliberate exceptions - `mkdir` and `realpath` are sent raw, everything else escaped -
now have a test under them: `mkdir and realpath are sent the path as written, and everything else
is sent it escaped`, against the same `ChannelSftp` stand-in.

It pins what the adapter *sends*, not what the library then does with it, and the difference is
worth stating. The report's suggested test - `mkdir names one directory even when the path holds a
star`, against the embedded server - cannot run on the maintainer's platform, because `*` is not a
legal character in a Windows file name, so it would have been a POSIX-only test for a claim that
matters everywhere. What is pinned instead is the thing a careless edit would actually break: an
escape added to `mkdir` or `realpath`, or one dropped from `rm` or `rename`. The
library-behaviour half stays where it was, proved against the embedded server by the escaping tests
D37 left.

### Nothing rejected

No finding in this report was judged wrong. H1 reproduced exactly as written - the first run of the
new test handed over `/drop/../../../home/etl/.ssh/authorized_keys`, `/drop/sub/nested.csv` and
`/drop//etc/shadow` - and H2's default was as described.
