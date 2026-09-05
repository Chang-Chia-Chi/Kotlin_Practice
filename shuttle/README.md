# Shuttle

Shuttle moves files from one place to another and tells other systems about it, losing nothing on the way.
One **route** is the whole vocabulary: a trigger that says a source object exists, a fetch of its bytes, an
optional processing chain, a store into a target, an acknowledgement of the trigger, and notifications at
chosen moments. What kind of place either end is - an SFTP directory, an S3 bucket, a NATS subject - stays
inside its adapter, so a route reads as "from this, do that, to there, tell them". Every step is recorded in a
durable state store, so a crash at any point is redone with at most one extra store and one extra
notification, never a lost one. Configuration is data: a YAML file validated at boot by numbered rules, which
operations can edit and check without a build and without a deployment.

---

## Quickstart: an SFTP drop to MinIO with an HTTP callback, on one laptop

Ten minutes, one repository and Docker Desktop. Verified end to end on Windows 11 with Docker 26.1.4 and
JDK 22; every command below was run, not reasoned about. Run them from the **repository root**.

### 1. Start the local stack

```powershell
docker compose -f shuttle/examples/docker-compose.yml up -d
docker compose -f shuttle/examples/docker-compose.yml ps -a
```

Five containers, all prefixed `shuttle-example-`: an SFTP server on port 2222 (user `vendor`, with `/drop`,
`/drop/temp` and `/outbound` already made), MinIO on 9000 with a versioned `landing` bucket, an `mc`
container that made the bucket and stays alive as the MinIO command line, Oracle Free on 1521 with spec
8.1's schema applied at first start, and an HTTP echo server on 8081 that prints every request body it
receives. Oracle takes about a minute to reach `(healthy)`; wait for it before step 4.

Every credential in that file is a **throwaway local value**. Nothing there is a secret.

### 2. Build the runner jar

```powershell
mvn -pl shuttle -am package -DskipTests
```

The `quarkus-maven-plugin` is already wired into the module, so this writes
`shuttle/target/quarkus-app/quarkus-run.jar`. (`-pl shuttle` alone is enough once the other reactor modules
are installed in your local repository. `mvn -pl shuttle quarkus:run` needs plugin artifacts that were not
in this machine's `~/.m2`, so the jar is what this README uses.)

### 3. Set the environment

Everything shuttle needs, in one block. **Use forward slashes** in `SHUTTLE_CONFIG` and `SHUTTLE_STAGE_DIR`:
these reach the process through MicroProfile Config, whose expression syntax eats a backslash, so
`shuttle\examples\vendor-drop.yaml` arrives as `shuttleexamplesvendor-drop.yaml`.

```powershell
$env:JAVA_HOME              = "C:\Users\<you>\.jdks\openjdk-22.0.1"   # any JDK 17 or later
$env:SHUTTLE_CONFIG         = "shuttle/examples/vendor-drop.yaml"     # `shuttle.config`, comma-separated
$env:SHUTTLE_STAGE_DIR      = "$PWD/.local/stage".Replace('\','/')    # where fetched bytes land (rule 11)
$env:SHUTTLE_ADMIN_PASSWORD = "admin-pw"        # throwaway; there is deliberately no default (see below)
$env:SHUTTLE_DB_URL         = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
$env:SHUTTLE_DB_USER        = "shuttle"
$env:SHUTTLE_DB_PASSWORD    = "shuttle-pw"      # throwaway, matches docker-compose.yml
$env:SFTP_USER              = "vendor"
$env:SFTP_PASSWORD          = "vendor-pw"       # throwaway, matches docker-compose.yml
$env:S3_ACCESS_KEY          = "minioadmin"      # throwaway, matches docker-compose.yml
$env:S3_SECRET_KEY          = "minioadmin"      # throwaway, matches docker-compose.yml
$env:DOWNSTREAM_TOKEN       = "local-token"     # the bearer token the echo server will print back

New-Item -ItemType Directory -Force -Path "$env:SHUTTLE_STAGE_DIR/vendor" | Out-Null
```

`SHUTTLE_ADMIN_PASSWORD` has no default anywhere in this module: unset, the process refuses to start
(`SRCFG00011`) rather than come up with a credential that is public in this repository.

The staging directory has to exist before anything runs - rule 11 checks it, in `validate` mode too.

### 4. Check the configuration before starting anything

```powershell
& "$env:JAVA_HOME\bin\java.exe" -jar shuttle/target/quarkus-app/quarkus-run.jar validate shuttle/examples/vendor-drop.yaml
```

```
ok: shuttle\examples\vendor-drop.yaml
```

Quarkus prints its banner and "Installed features" first; the report is the last line. Nothing was
connected to - `validate` holds no client of any kind.

### 5. Run the chain over a sample, still connecting to nothing

```powershell
& "$env:JAVA_HOME\bin\java.exe" -jar shuttle/target/quarkus-app/quarkus-run.jar `
    try shuttle/examples/vendor-drop.yaml --route vendor-drop `
    --file-name 123-order.csv --content shuttle/examples/sample/123-order.csv
```

```
step 1 extract: attributes {orderNumber=123}
step 2 rename: attributes {} objects [20260904-123-order.csv]
step 3 zip: attributes {} objects [20260904-123-order.csv.zip]
key: vendor/20260904-123-order.csv.zip
body downstream (acked):
{ "fileId" : "0", "file" : { "name" : "20260904-123-order.csv.zip", ... }, "orderNumber" : "123", ... }
```

This is where a regex group named `orderNumber` while the mapping says `orderNumber**s**` shows up - before
any deployment.

### 6. Serve

```powershell
& "$env:JAVA_HOME\bin\java.exe" -jar shuttle/target/quarkus-app/quarkus-run.jar
```

The last line of startup is `shuttle started: 2 routes, 1 channels`, then each route says what it watches.
Two warnings are expected and correct: the `landing` bucket has no lifecycle rule expiring non-current
versions (D5), and the connector accepts any host key because the example says `hostKey: acceptAll`.

Leave it running and open a second shell for the rest.

### 7. Drop a file the way the vendor would

```powershell
powershell -NoProfile -File shuttle/examples/seed.ps1
```

Within about 20 seconds (`every: 15s` plus the readiness checks), in the serve window:

```
delivery transfer=1 event=acked channel=downstream attempt=1 status=delivered
```

### 8. Look at what happened

The object, under the key the chain produced, with a version id because the bucket is versioned:

```powershell
docker exec shuttle-example-mc mc ls --recursive --versions local/landing
```
```
[2026-09-04 21:46:13 UTC]   196B STANDARD e27d8880-6387-4e8c-94c6-2444388ad884 v1 PUT vendor/20260904-123-order.csv.zip
```

The source file, moved into the done folder by `onAck: { move: temp/ }`:

```powershell
docker exec shuttle-example-sftp ls -lR /home/vendor/drop
```
```
/home/vendor/drop:
drwxr-xr-x    2 vendor   users         4096 Sep  4 21:46 temp
/home/vendor/drop/temp:
-rwxr-xr-x    1 vendor   root            44 Sep  4 21:35 123-order.csv
```

The callback the echo server received:

```powershell
docker logs shuttle-example-echo
```
```json
{"fileId":"1","file":{"name":"20260904-123-order.csv.zip","size":"196","md5":"<the archive's md5>"},
 "location":{"bucket":"landing","key":"vendor/20260904-123-order.csv.zip"},
 "receivedAt":"2026-09-04T21:35:27Z","orderNumber":"123","event":"acked","source":"vendor-drop"}
```

`orderNumber` came from the extract step, `location.key` from the target, `source` is a constant in the
mapping table. `file.name` and `file.md5` are the stored archive's own name and digest (the row is written
again at STORED with the object that went to the target; spec 8.1); the source's are the `SOURCE_NAME` and
`SOURCE_DIGEST` fields, and the same name and digest are on the object's S3 metadata.

The transfer, through the admin endpoint:

```powershell
curl.exe -s -u shuttle-admin:admin-pw "http://localhost:8080/admin/shuttle/transfers?route=vendor-drop"
```
```json
[{"id":1,"route":"vendor-drop","kind":"OBJECT","state":"DONE","sourceRef":"vendor:/drop",
  "sourceName":"123-order.csv","attributes":{"orderNumber":"123"},
  "target":{"kind":"s3","location":"landing","key":"vendor/20260904-123-order.csv.zip",
            "ref":"e27d8880-6387-4e8c-94c6-2444388ad884","size":196},
  "ackedAt":"2026-09-04T21:46:13.924094Z","completedAt":"2026-09-04T21:46:14.810549Z","children":[]}]
```

And one line of the scrape:

```powershell
curl.exe -s http://localhost:8080/q/metrics | Select-String shuttle_transfers_total
```
```
shuttle_transfers_total{outcome="done",route="vendor-drop"} 1.0
```

### 9. The second route

`mirror` is spec 13.1's "move A to B, tell nobody": it polls `/outbound` every 60 s, deletes the source on
ack and notifies no one. Feed it with the same script:

```powershell
powershell -NoProfile -File shuttle/examples/seed.ps1 -Directory outbound
```

A minute later `mc ls` shows `mirror/123-order.csv`, `/home/vendor/outbound` is empty, and
`/admin/shuttle/transfers?route=mirror` has a DONE row with no deliveries.

### 10. Stop

Ctrl-C the serve window - shutdown is bounded by `drainTimeout` (60 s in the example) - then:

```powershell
docker compose -f shuttle/examples/docker-compose.yml down -v
```

`-v` throws the Oracle volume away, so the next `up` applies the schema again.

---

## The three modes

`ShuttleMain` reads the first argument as the mode and puts it in `shuttle.mode` before Quarkus boots, so a
command mode never starts the host.

| Mode | Command | Does |
|---|---|---|
| `validate` | `java -jar ... validate <files>` | Startup steps 1 and 5 only: loads the YAML, judges all 26 rules, resolves every named bean. Prints `rule <n>: <message>` per violation, exits 1 on any. Connects to nothing. |
| `try` | `java -jar ... try <files> --route <name> --file-name <name> [--source-path <path>] [--content <file>] [--message <file>]` | Validates, then runs that route's own chain and context over the sample in a temp directory: the attributes each step set, the key the target would use, the body rendered for every channel the route notifies. `expand` reads the sample files sitting beside `--content`, one key and one body per child. Connects to nothing, stores nothing. |
| serve | `java -jar ...` (no argument) | Spec 12.1 in order: state store, store and channel probes, staging emptied, named beans, then the notifier and every route. Readiness at `/q/health/ready`, the scrape at `/q/metrics`, the admin endpoints below. |

## Admin endpoints (spec 14.1)

All seven are under the `shuttle-admin` role. `/q/health` and `/q/metrics` are deliberately open, because a
kubelet carries no credential. An anonymous admin call is 401, a caller without the role 403, a call before
the host is up 503, an unknown id 404, and an operation that does not apply to the row's state 409.

| Endpoint | Does |
|---|---|
| `GET /admin/shuttle/routes` | per route: up or down, last trigger, restart count, counts by state |
| `GET /admin/shuttle/transfers?route=&state=&limit=` | transfer rows, children folded under parents |
| `GET /admin/shuttle/transfers/{id}/deliveries` | event, channel, state, attempts, last status, reference, delivered time |
| `POST /admin/shuttle/transfers/{id}/redrive` | REJECTED or FAILED back to SEEN |
| `POST /admin/shuttle/transfers/{id}/ack` | STORED to ACKED by hand; an operator override, never a recovery path the design relies on |
| `POST /admin/shuttle/deliveries/{id}/redrive` | FAILED back to PENDING; wakes the notifier |
| `POST /admin/shuttle/routes/{name}/restart` | restart a route now, resetting its backoff |

```powershell
$a = "shuttle-admin:admin-pw"
curl.exe -s -u $a  http://localhost:8080/admin/shuttle/routes
curl.exe -s -u $a "http://localhost:8080/admin/shuttle/transfers?route=vendor-drop&state=done&limit=10"
curl.exe -s -u $a  http://localhost:8080/admin/shuttle/transfers/1/deliveries
curl.exe -s -u $a -X POST http://localhost:8080/admin/shuttle/transfers/1/redrive
curl.exe -s -u $a -X POST http://localhost:8080/admin/shuttle/transfers/1/ack
curl.exe -s -u $a -X POST http://localhost:8080/admin/shuttle/deliveries/1/redrive
curl.exe -s -u $a -X POST http://localhost:8080/admin/shuttle/routes/vendor-drop/restart
```

## Metrics (spec 14.2)

Micrometer through the host's registry, on `/q/metrics`. Tags are `route`, `channel` and `store`; never a
name, an id or a key.

| Metric | Type | Tags |
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
| `shuttle_supersedes_total` | counter | `route` |
| `shuttle_staging_free_bytes` | gauge | `store` |
| `shuttle_staging_deferred_total` | counter | `route` |

## The example files

| File | Is |
|---|---|
| `examples/vendor-drop.yaml` | spec 13.1's `vendor-drop` and `mirror` routes, trimmed to the local stack; every secret a `${VAR}` |
| `examples/docker-compose.yml` | the SFTP server, MinIO, Oracle and the echo endpoint |
| `examples/schema.sql` | spec 8.1's DDL, **verbatim**. `StateStoreSchemaTest` fails if it drifts from the spec or from `StateStoreSchema.DDL`; edit the spec, then copy it here |
| `examples/oracle-init.sh` | applies `schema.sql` as the application user at the database's first start |
| `examples/seed.ps1`, `examples/seed.sh` | put a sample CSV in the SFTP drop directory. Only the PowerShell one is verified here |
| `examples/sample/123-order.csv` | the sample; its name has to match the route's `(?<orderNumber>\d+)-.*\.csv` |

## Where the documents live

- **Spec**: `docs/shuttle/spec.md` (and `overview.md` / `overview.html` for the shorter read). The
  numbered validation rules are section 13.3, the admin table 14.1, the metric names 14.2, the DDL 8.1.
- **Plan**: `docs/shuttle/plan.md`.
- **Progress log**: `docs/shuttle/progress.md` - one entry per ticket, each recording what was built, the
  concepts it named, its acceptance evidence and every deviation from the spec.
- **Tickets**: `.scratch/shuttle/issues/`.

## Docker Desktop 4.5x and the container tiers

Docker Desktop with engine 29 or later refuses API versions below 1.40, and the Testcontainers 1.20 client used by
the `oracle`, `minio`, `nats`, `acceptance` and `load` tiers still asks for 1.32, so every container start fails
with `client version 1.32 is too old`. Until Testcontainers is upgraded, pin the client's API version once per
machine:

```
echo api.version=1.44 > %USERPROFILE%\.docker-java.properties
```

The default tier needs no Docker at all.
