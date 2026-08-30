# etl-host staging

A stack that boots the reference host with **every prerequisite it deliberately does not create**:
an Oracle carrying the two source tables, the pipe target and the archive manifest, and a MinIO
carrying the archive bucket. Nothing here is hand-wired at run time - the point of the stack is that
`up` is the whole procedure.

It is a **staging** stack, not a production one. Every credential in it is a placeholder, the data
is generated, and the intervals are shortened so a person can watch the thing work in three minutes
instead of an hour.

## Boot it

Two commands, from the **repository root** for the first and either place for the second. The image
copies `etl-host/target/quarkus-app`, so the jar has to exist before the image is built.

```bash
mvn -pl etl-host -am package -DskipTests
docker compose -f etl-host/docker-compose.staging.yml up --build
```

`-am` is required: without it Maven resolves a stale SimpleEtl from the local repository, which
surfaces as `No parameter with name 'onTasksLoaded' found` rather than as anything about staleness.

Tear down with `docker compose -f etl-host/docker-compose.staging.yml down -v`. The `-v` matters -
Oracle's init script runs once per volume, so a re-`up` over a kept volume boots against tables that
already exist.

Roughly 60-90 seconds of that is Oracle starting. `etl-host` waits for it (`depends_on:
service_healthy`) rather than racing it.

## What to watch, in the order it happens

**1. The archive layer says what it is pointed at, before anything else.**

```
INFO [etl.ArchiveWiring] archive layer enabled: bucket snapshot-archive at http://minio:9000,
                         groups [equipment, wip], retention PT48H
```

Absent, `etl-host.archive.enabled` did not reach the process and nothing is being checkpointed. The
line is deliberately at boot rather than at the first tick, an hour later.

**2. Both groups refresh, and readiness flips.**

```
INFO [etl.JdbcGenerationSource] group wip generation 1: 500 row(s) read into wip, dataAsOf ...
INFO [etl.EtlHost] startup refresh of group wip: SUCCESS
INFO [etl.EtlHost] startup complete, readiness = ready
```

A `WARN ... SOURCE_ERROR ORA-00942` here means the init script did not land where the application
looks. A `WARN ... VERIFY_FAILED key_unique` means a group's SQL stopped projecting `id`.

**3. Readiness, at the path a manifest probes.** The compose healthcheck is this exact call, which
is why `docker compose ps` showing `(healthy)` is itself the assertion.

```bash
curl -s localhost:18080/q/health/ready
{"status":"UP","checks":[{"name":"snapshot-cache","status":"UP","data":{"state":"ready"}}]}
```

Port **18080** on the host, 8080 in the container. 8080 is the most contested port on a developer
machine and losing it is silent in the worst way: Docker still prints `0.0.0.0:8080->8080/tcp`, the
container is still healthy - its probe runs *inside* - and `curl localhost:8080` answers 404 from
somebody else's service. That happened while this stack was being written.

**4. All four authentication answers, which is the row a `@TestSecurity` suite cannot cover.**

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:18080/admin/etl/tasks                        # 401
curl -s -o /dev/null -w '%{http_code}\n' -u etl-admin:wrong localhost:18080/admin/etl/tasks     # 401
curl -s -o /dev/null -w '%{http_code}\n' -u etl-reader:staging-reader \
     localhost:18080/admin/etl/tasks                                                            # 403
curl -s -u etl-admin:staging-admin localhost:18080/admin/etl/tasks                              # 200
```

`etl-admin` and `etl-reader` are `application.properties` placeholders whose passwords this stack
overrides through `ETL_ADMIN_PASSWORD` and `ETL_READER_PASSWORD`. Those two variables are the entire
seam between the public defaults and a real secret store. A deployment either sets them or replaces
the `quarkus.security.users.embedded.*` block with OIDC or LDAP - **replace it, do not remove it**,
since deleting it restores a host that answers 403 to everyone forever.

**5. A run, crossing both frameworks.** The task file mounted from `example-tasks/` is spec 2.4's
shape D: `cacheCopy` out of a generation into scratch, `materialize` over it, `pipe` into Oracle.

```bash
curl -s -X POST -u etl-admin:staging-admin localhost:18080/admin/etl/tasks/wip-summary/runs
{"runId":"..."}
```

`triggeredBy` in the listing afterwards reads `etl-admin` - the identity the provider returned, not
a test annotation's - and the rows are in Oracle:

```
SITE      LOTS   TOTAL_QTY
F11        250       93750
F12        250       94125
```

**6. The archive tick, three minutes in.** Shortened from the hourly production default by
`ETL_HOST_ARCHIVE_INTERVAL`.

```
INFO [inf.sna.Archiver] archived group 'wip' as version 1 (1 objects, data_as_of ...)
```

The manifest row and the object are both real, and can be seen:

```bash
docker exec etl-host-oracle-1 bash -lc \
  "echo 'select group_id, version, status from snapshot_archive_manifest;' | \
   sqlplus -s etl/staging@//localhost:1521/FREEPDB1"
```

MinIO's console is at <http://localhost:9001> (`minioadmin` / `minioadmin`); the objects are under
`snapshot-archive/snapshots/wip/v1/`.

Expect one WARN alongside it, and it is correct:

```
WARN [inf.sna.ArchiveMaintenance] ALERT: group 'equipment' has no COMPLETE archive version at all
```

`equipment` is configured for archiving and its first version lands on the same tick, so the sweep
that runs immediately after sees the state that existed a moment earlier. It stops on the next pass.

## What this stack is not

- **Not an operating point.** Every interval, memory limit and pod size here is a value that boots,
  not a recommendation. `etl-host/README.md` says why at length: the operating point is a statement
  about a memory request, a page cache and a real schedule, and nothing in this repository measures
  it.
- **Not a security posture.** Plaintext placeholder passwords in a config file, `minioadmin`, and a
  database whose SYSTEM password is `staging`.
- **Not durable.** `down -v` deletes everything, which is intended.

## The grace period, and its arithmetic

`stop_grace_period: 75s`, and the sum is written out in `docker-compose.staging.yml` beside it
because getting it wrong has no symptom until the day it matters. In short: shutdown runs in a fixed
order inside one observer, the archiver's drain and the cache's lease drain are each bounded by
`etl-host.cache.lease-drain-timeout` (30s), the MinIO read timeout (20s) sits inside the first of
those rather than adding to it, and teardown is a few seconds - so 65s worst case, plus headroom.

Below it, Docker `SIGKILL`s mid-drain: leases are abandoned rather than released, and a PENDING
manifest row is left for the watchdog instead of never having existed. Kubernetes'
`terminationGracePeriodSeconds` is the same sum (snapshotcache spec 11.3). **Change
`lease-drain-timeout` or `archive.http-timeout` and this number moves with them.**

## The JVM flag with no property

The image sets `-Dkotlinx.coroutines.debug=on`, and it is the only place that setting is real.
Without it `LeaseInfo.owner` is the acquiring thread's name - `DefaultDispatcher-worker-1`, naming no
task - so "which job is stalling refresh" is unanswerable in exactly the incident the diagnostics
exist for. **No test can catch it being missed**, because kotlinx-coroutines' debug mode is `AUTO`
and surefire's `-ea` turns it on by itself. Verify it the only way there is:

```bash
docker exec etl-host-etl-host-1 cat /proc/1/cmdline | tr '\0' ' '
```
