# 22: Readiness checks take a stat function; the package cycles ArchUnit missed are frozen

**What to build:** `ReadinessContext` (`source/Readiness.kt:45-49`) takes a whole `SftpClient` and
exposes only `stat`, so a test of a readiness check builds client, pool and transport to get one
call. `config` imports `client.Overwrite` and `source.ReadinessCheck` while both import `config`,
and `error/SftpException.kt` imports `pool.PoolStats` while `pool` imports `error`. The
architecture test draws spec 3.1's layers top-down and catches none of these.

The obvious deepening - move readiness into `config` - recreates the cycle through
`transport.RemoteFile`, because `transport` imports `config` for its connection settings. So the
scope is narrower: give readiness its seam, remove the two edges that are free to remove, and
freeze the one that remains so it stays a decision and not an accident.

**Blocked by:** T17 lens 3 committed on `misc/ai_gen` (it touches `ConnectorDsl.kt`,
`SftpException.kt`)

**Model:** Opus 5 - mechanical moves plus one narrowed interface

**Status:** done

**Spec changes this ticket applies first:**

- 3.1: one sentence recording that `config` refers to the readiness and overwrite *types* the DSL
  configures, and to nothing else outside its layer; and that this is the only downward reference
  the layering permits.

- [x] `ReadinessContext` holds a stat function `suspend (String) -> RemoteFile?` and the clock, not
      an `SftpClient`; the source supplies the client's `stat` at the one place it builds the
      context. `SizeStable` and `MinAge` unchanged in behaviour
- [x] Test: `SizeStable` and `MinAge` exercised through a map-backed stat function and a
      `VirtualClock`, with no pool, client or transport constructed. Existing readiness tests
      unmodified
- [x] `client.Overwrite` moves to `sftp.connector.config` (the DSL is what configures it);
      `client` imports it from there
- [x] `PoolExhausted` carries the three counts as its own fields; `PoolStats` stays in `pool` and
      `error` no longer imports it
- [x] `ArchitectureTest`: a rule that `config` depends on nothing outside itself except
      `source.ReadinessCheck` and its built-ins, and that `error` depends on no other connector
      package; both rules red before the moves, green after
- [x] Shuttle's `readinessOf` in `SftpPollSource.kt` changes its imports only if the readiness
      types move; per the scope above they do not, so shuttle is untouched - verify by building it
- [x] Progress entry appended, recording why readiness did not move into `config`

Ground rules for every ticket: implement only this ticket; fixes are scoped to what a finding
names and carry the test that found it; no Thread.sleep; `@Test fun x() = runBlocking<Unit>`;
invariant tests named `I<n>_<description>`; never weaken an earlier ticket's test - a finding that
a test is wrong is reported, not silently corrected; comments and messages carry reasons, never
spec section numbers; append a progress entry describing what was done and every deviation. The
spec is docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation. Work in an isolated worktree branched from
`misc/ai_gen`; modify only `sftpconnector/` and `docs/sftpconnector/`.
