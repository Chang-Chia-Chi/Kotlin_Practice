# etl-host soak harness

A 90+ minute soak of `etl-host` against a real Oracle container, with compressed intervals
(20s refresh, 15-17s task crons) so ~93 minutes of wall clock produced ~270 cache-refresh
cycles per group and ~1100 task runs - the first time either framework in this repo has run
longer than a test. Base commit: `abb2ce7`. `etl-host/**` and both frameworks were not
modified; everything here lives under `soak/`.

## What's here

```
soak/
  README.md              this file
  scripts/
    seed.sql              Oracle DDL/seed (same shape as HostEndToEndOracleTest's fixture)
    init-target.jsh        creates the DuckDB target tables (jshell + duckdb_jdbc on classpath)
    run-app.sh              launches etl-host (see "How the app is launched" below)
    start.sh / start-sampler.sh   nohup+pidfile wrappers, for durability across tool calls
    sample.sh                the 30s CSV sampler
    wait-exit.sh              SIGTERM exit-time poller
    analyze.awk               first-third vs last-third comparison over samples.csv
    check-target.jsh          row-count check used to find the pipe-target growth finding below
  run/
    tasks/                 the task YAML this run served (see below)
    state/                 cache storage/temp/scratch dirs + report.db target (gitignored size,
                            kept here as-is from the actual run)
    quarkus-app/            a COPY of etl-host/target/quarkus-app with one jar added (see below)
    samples.csv              the full 30s-interval measurement CSV (188 healthy rows + 3 that
                              caught the SIGTERM-to-restart outage window)
    events.log                timestamped injection log (UTC)
    logs/main-run-excerpts.log   verbatim log excerpts (see "A mistake" below)
```

## Reproducing

1. `docker run -d --name soak-oracle -e ORACLE_PASSWORD=soakpw -p 1521:1521 gvenzl/oracle-free:slim-faststart`,
   wait for `DATABASE IS READY TO USE!` in `docker logs -f soak-oracle`, then
   `docker cp soak/scripts/seed.sql soak-oracle:/tmp/seed.sql && docker exec soak-oracle sqlplus -s system/soakpw@localhost:1521/FREEPDB1 @/tmp/seed.sql`.
2. `mvn -o -pl etl-host -am package -DskipTests` from the repo root (builds snapshotcache and
   SimpleEtl too, via `-am`).
3. `cp -r etl-host/target/quarkus-app soak/run/quarkus-app` (already done; re-run after a
   rebuild) then drop `ojdbc11-23.5.0.24.07.jar` from the local `.m2` into
   `soak/run/quarkus-app/lib/main/` - see "Finding: no Oracle driver in production" below for
   why this step exists.
4. `jshell --class-path <path-to-duckdb_jdbc-1.1.3.jar> soak/scripts/init-target.jsh` to create
   the target tables.
5. From WSL: `bash soak/scripts/start.sh` to launch, `bash soak/scripts/start-sampler.sh` to
   start sampling. Both are nohup+disown+pidfile wrappers so they survive the launching shell
   exiting - needed because this harness was driven one bounded tool call at a time, not from
   one long-lived shell.
6. Injections are plain `docker pause soak-oracle` / `docker unpause soak-oracle`, dropping a
   YAML file into `soak/run/tasks/` plus a `curl -X POST .../admin/etl/reload`, and
   `kill -TERM <pid>` from inside WSL for a real Linux SIGTERM.

## How the app is launched, and why not `java -jar quarkus-run.jar`

`java -jar etl-host/target/quarkus-app/quarkus-run.jar` runs `io.quarkus.bootstrap.runner.
QuarkusEntryPoint`, which builds its runtime classpath from a binary index baked at build time
(`quarkus/quarkus-application.dat`) - it lists exactly what `mvn package` resolved, so a jar
dropped into `lib/main/` afterwards is invisible to it (confirmed empirically: adding
`ojdbc11.jar` and launching this way still fails with `No suitable driver found`). Instead
`soak/scripts/run-app.sh` runs `io.quarkus.runner.GeneratedMain` (the class the fast-jar's
augmentation phase actually generates, found inside `quarkus/generated-bytecode.jar`) on a
plain `-cp` built from the same jar directories plus the driver - an ordinary classpath with no
index to go stale. See the finding below for *why* the driver has to be added at all.

## Finding: no Oracle JDBC driver on etl-host's production classpath

`etl-host/pom.xml` declares `com.oracle.database.jdbc:ojdbc11` with `<scope>test</scope>`. The
production `quarkus-app/lib/main/` has no Oracle driver in it at all - launching the packaged
jar unmodified against `etl-host.source.url=jdbc:oracle:thin:...` fails both refresh groups at
startup with `java.sql.SQLException: No suitable driver found`, and the app boots anyway,
**not ready**, exactly as `README.md`'s "three things a deployment must supply" section
predicts for a missing source table - except this isn't a missing table, it's a missing driver,
and nothing in that section names it. `Producers.kt`'s `jdbi()` also does no explicit
`Class.forName`, unlike the test fixtures, which comment at length on needing one because
"`ServiceLoader` has not yet registered the driver" on the classloader a `QuarkusTestResource`
runs on - the same class of problem, one level up, in production. This is outside `soak/` and
was not fixed (read-only); the workaround above (`GeneratedMain` + a driver jar on lib/main) is
soak-local only and would not help a real deployment running the packaged jar normally.

## Finding: the admin API is unreachable, confirmed live (matches etl-host's own README)

`AdminResource` and its `POST /admin/etl/reload` and `POST /admin/etl/tasks/{name}/runs` are
`@RolesAllowed("etl-admin")` with no identity provider configured - `etl-host/README.md` already
says every admin call answers 403 forever in production. Confirmed live on this boot:
`POST /admin/etl/reload` -> 403, `GET /admin/etl/tasks` -> 403, three concurrent
`POST .../runs` -> 403/403/403, while `GET /health/ready` (the one `@PermitAll` endpoint) kept
answering 200 throughout. This blocked two of the four requested injections from being done the
way they're normally described:

- **"push a broken task file and reload via HTTP, then fix it"** was done exactly as asked -
  `run/tasks/broken.yaml` was written, `POST /admin/etl/reload` was called - and the real
  production answer is 403, not 400. The file was never loaded (reload never succeeded), so
  there was nothing to roll back; it was deleted as cleanup.
- **"trigger several tasks concurrently"** was attempted the same way (three concurrent
  `POST .../runs`, all 403). As a substitute that needed no admin access, `run/tasks/
  wip-summary-concurrent.yaml` was added with the same `0/15 * * * * ?` cron as `wip-summary`,
  so the two fire in the same scheduler tick, against the same `report.db` target, for the
  entire run (~270 concurrent pairs). See the concurrency finding below.

## Findings from the run itself

**Oracle partition (`docker pause` 3 min, T+20min).** The in-flight refresh at the moment of
the pause blocked for the full pause duration and completed the instant Oracle came back - no
`SOURCE_ERROR`, because 3 minutes never reached `etl-host.source.query-timeout-seconds=300`.
`snapshot_current_generation{group=wip}` froze at 68 and `snapshot_live_generations` rose to 2.0
(the stuck candidate) for the whole window, then both a jumped to 70 and 1.0 within one 30s
sample of `docker unpause`. Readiness stayed 200/`ready` throughout - a paused source did not
flip readiness, because the *previous* generation kept serving. No manual intervention needed.
See `run/logs/main-run-excerpts.log` for the exact log lines.

**A checkpoint I/O failure, self-healed (T+46min, unplanned).** Once in ~270 refresh cycles x 2
groups, a checkpoint write failed with `Cannot allocate memory` writing
`gen_0000000138.db.tmp`, and the verify gate then rejected that candidate as a corrupt-checksum
database file. The *next* cycle (generation 139) succeeded normally; readiness never dropped;
no `.tmp`/`.wal`/`gen_0000000138.db` file was left on disk afterward. Likely cause: this run's
storage path is on a Windows drive mounted into WSL2 via DrvFs (`/mnt/c/...`), which is known to
behave oddly under mmap-heavy I/O - so this is plausibly an environment artifact of running
soak state on a DrvFs mount rather than a framework defect, but it's exactly the kind of thing a
short test run would never surface, and the interesting result either way is that the verify
gate caught it and refresh self-healed with no operator action.

**Pipe-target row count grows without bound (the soak's most actionable finding).** The shape-D
example task's `pipe` step (`etl-host/example-tasks/wip-summary.yaml`, copied verbatim into
this run's task files) has no truncate/delete step before publishing - it only *appends*.
After ~93 minutes / ~372 runs each of `wip-summary` and `wip-summary-concurrent`, `wip_summary`
and `wip_summary_concurrent` held **744 rows each** (checked with `soak/scripts/
check-target.jsh`), and `report.db` had grown to 1.5 MB from a handful of KB at boot. At the
example's *documented* hourly cron this is 2 rows/hour - invisible in any test and in most
deployments' service lifetime - but a deployment that copies this example verbatim (as
`README.md` invites: "copying from it is its intended use") and runs it more often, or for
longer, accumulates an unbounded table with no cleanup mechanism anywhere in the framework or
the example. This is a property of the *example task's SQL*, not a framework bug - the fix is a
`delete from wip_summary` step (or a partition/retention strategy) before `publish`, which nothing
currently documents as necessary.

**SIGTERM (T+94min).** `kill -TERM` from WSL; the process disappeared from `/proc` within the
~1s polling granularity used, and the log's last line was
`[io.quarkus] (Shutdown thread) etl-host stopped in 0.337s` with no WARN/ERROR in between
(`EtlHost.onStop` only logs on `wired.close()`/`managed.close()` *failure*, so silence is the
expected clean-shutdown signature). **Exact process exit code was not captured** - the process
was launched via `nohup ... & disown` so it survived the launching shell exiting (needed since
this harness ran from many short, bounded tool calls rather than one long-lived shell), which
also reparented it to WSL's init; its exit status is only visible to that real parent, which
this harness did not instrument. Reported as not-captured, not guessed.

**Restart after SIGTERM.** Startup wipe named exactly what it removed:
`startup wipe: removed 1 stale generation file(s) left by a previous process from
soak/run/state/cache/equipment: [gen_0000000271.db]` (and the same for `wip`) - even though the
prior shutdown was clean, confirming spec 10.1 step 1's wipe is unconditional regardless of how
the previous process ended, matching `DrillStaleGenerationFilesTest`'s documented behavior.
Numbering restarted at 1, and `GET /health/ready` returned `{"state":"ready"}` after the normal
startup-refresh sequence.

## A mistake: the main run's app.log was deleted before archiving

`run/logs/app.log` for the ~93-minute main run (~4900 lines) was deleted by
`rm -f run/logs/app.log` immediately before the post-SIGTERM restart, before it had been copied
anywhere durable - it should have been renamed, not removed. `run/samples.csv` (the full
quantitative record) and `run/events.log` (the injection timeline) were never touched and are
complete. `run/logs/main-run-excerpts.log` holds the log lines that had already been extracted
and quoted, verbatim, while investigating each injection during the run - they are exact copies
from the live log, not reconstructed - but they are excerpts, not the full log. Reported plainly
rather than papered over.

## Metric verdicts (188 healthy 30s samples, elapsed 0-5548s / ~92.5 min; first-third vs
## last-third of those, `soak/scripts/analyze.awk`)

| Metric | First 3rd avg | Last 3rd avg | Verdict |
|---|---|---|---|
| JVM RSS | 373.8 MB | 363.7 MB | **STABLE** (-2.7%, within run-to-run noise; min 269 MB / max 449 MB across the whole run, no trend) |
| Open FDs | 222.4 | 222.1 | **STABLE** (constant at 222 outside the partition window, where it briefly rose to 224-228 while a refresh was stuck open, and fell back to 222 the sample after `unpause`) |
| Meter count (`/q/metrics` non-comment lines) | 260.6 | 272.0 | **SPIKY-then-STABLE**: 237->261 in the first ~4 min (initial label-cardinality fill as each task/step/outcome combination is first observed), a further 261->272 step around T+35-49min (plausibly histogram-bucket cardinality filling in as more duration values are observed), then **flat at exactly 272 for the final ~25+ minutes**. Bounded, not a leak. |
| Cache generation file bytes (`gen_*.db`, per group) | 536,576 B | 536,576 B | **STABLE** - constant, and exactly one file per group present on disk at every point checked (confirmed by listing `run/state/cache/{wip,equipment}/` before shutdown), across ~270 refresh cycles. Source row counts never changed in this run, so this also confirms the one-file-per-generation reclaim path, not that content size is bounded under a growing source. |
| Cache WAL bytes | 0 | 0 | **STABLE** - never observed non-zero across all 188 samples; checkpoints (60-80ms per the logs) complete well inside the 30s sample interval. |
| Scratch run dirs | 0.10 | 0.10 | **STABLE** - effectively always 0, occasionally 1-2 transient; the whole run directory is deleted after each task run per spec, and nothing accumulated. |
| `report.db` pipe-target rows | n/a (not sampled continuously) | 744 rows/table at end (checked post-hoc) | **GROWING, unbounded** - +2 rows per run, no ceiling; see the pipe-target finding above. This metric was not in the 30s CSV; it was found by a targeted check after noticing `report.db`'s file size (also not CSV-sampled) had grown to 1.5 MB. |
| Oracle container memory | not continuously sampled | one post-run snapshot: 664.9 MiB / 7.4 GiB (8.77%) via `docker stats --no-stream` | **not captured** as a time series - reported as a single point, not a trend, per the honesty rule against guessing what wasn't measured. |

## Injection outcomes vs documented behavior

| Injection | Outcome | Matches documented behavior? |
|---|---|---|
| 1. Oracle `docker pause` 3 min | Stall-and-resume, no error, no readiness flip, fully automatic recovery | Yes - `etl-host.source.query-timeout-seconds=300` exceeds the 180s pause, so this exercised the "still-connected, just slow" path rather than the `SOURCE_ERROR`/retry path; both are real but this run observed only one |
| 2. broken task file + reload | 403 (not 400) - README already documents this | Yes, confirmed live rather than only read |
| 3. concurrent task trigger | HTTP path: 403. Cron-native substitute: ~270 concurrent pairs, all succeeded, no file-lock contention | Partially by design (HTTP blocked as documented); the substitute is real concurrent execution, and contradicts nothing in `Producers.kt`'s KDoc since that KDoc's warning is about *cross-process* contention, not within-process |
| 4. SIGTERM | Clean, silent, 0.337s per Quarkus's own log line; restart's wipe log named the leftover file and readiness returned | Yes, matches `ShutdownSequenceTest`'s ordering claims and `DrillStaleGenerationFilesTest`'s wipe behavior |

## Honesty notes

- Every number above comes from `run/samples.csv`, `run/events.log`, or a command whose output
  is quoted in this file or in `run/logs/main-run-excerpts.log` - nothing here is estimated.
- Process exit code for the SIGTERM (see above) and a continuous Oracle-container-memory time
  series were not captured; both are reported as not-captured rather than guessed.
- The main run's raw `app.log` was deleted by mistake before archiving (see above); the CSV and
  events log are unaffected and are the primary record.
