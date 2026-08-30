# etl-host

**The reference host.** A real Quarkus application composing both frameworks through their two
public front doors - `openSnapshotCache` (snapshotcache spec 5.4) and `EtlWiring` (SimpleEtl spec
11.2) - and the executable form of SimpleEtl spec 8.6's host-obligation table.

`composed-host-example` composes the same two doors with hand-rolled stand-ins: a `ManualCron` a
test fires by hand, a `ReadinessProbe` nothing serves, no registry, no security. That is the right
shape for measuring cross-boundary behaviour, and it is why the example exists. What it cannot do
is discharge the obligations that are *about* a framework: a cron a scheduler really fires, a
`@RolesAllowed` a security layer really enforces, a `MeterRegistry` a scrape really reads. Each of
those stand-ins is replaced here by the real thing, which is the whole difference between the two
modules.

Nothing may depend on this module. It publishes no API; copying from it is its intended use.

## Scope

| In | Out |
|---|---|
| CDI producers, in the documented order - cache first | Anything either framework already owns |
| The Quarkus `Scheduler` binding for task crons | A second wait, retry or backoff mechanism |
| The refresh tick, and the lease/pinning poll spec 5.4 assigns to the host | Deployment manifests, dashboards, alert rules |
| `AdminResource`, readiness, the metric bindings | The operating point (below) |
| The startup and shutdown sequences (snapshotcache spec 10.1, 10.2) | |

## What stays deployment configuration

**The operating point is not a framework fact and no test here is evidence about it.** How many
tasks run concurrently, at what `scratchMemoryLimitMb`, against what `servingMemoryLimit`, in a pod
of what size, with what `terminationGracePeriodSeconds` - all of it is a statement about a memory
request, a page cache, a JVM heap and a real schedule. `composed-host-example`'s M2 measured only
that the two terms of the pod-budget formula are real and do not interfere; it deliberately did not
measure the point itself.

So `application.properties` here holds *a* set of values that boots and passes a suite. It is a
worked example of the shape, not a recommendation, and every number in it is meant to be replaced
by the deployment that owns the pod. Two of them are load-bearing wherever they appear, and are the
reason this file says so out loud:

- **`quarkus.scheduler.start-mode=forced`** - missed, no task ever fires and no error is raised.
- **`-Dkotlinx.coroutines.debug=on`** - missed, `LeaseInfo.owner` names a shared IO worker instead
  of the task, and *no test can catch it*, because surefire's `-ea` turns the flag on by itself.
  Verify by reading a running pod's command line.

## Run the application

Everything below this heading was measured on a cold `java -jar` boot, because until then this
section held only `mvn test` invocations - three ways to run the *suite*, and no way to run the
*host*. What a suite proves and what a deployment needs are different lists.

```bash
mvn -o -pl etl-host -am package -DskipTests
java -Dkotlinx.coroutines.debug=on -jar etl-host/target/quarkus-app/quarkus-run.jar
```

It boots in about 1.4 s and is immediately **not ready**, which is correct and is the first thing
to understand about it:

```
WARN  [etl.EtlHost] startup refresh of group wip: SOURCE_ERROR ... Table with name lot does not exist!
INFO  [etl.EtlHost] startup complete, readiness = awaiting-first-generation
$ curl localhost:8080/q/health/ready
{"status":"DOWN","checks":[{"name":"snapshot-cache","status":"DOWN",
                            "data":{"state":"awaiting-first-generation"}}]}   # HTTP 503
```

It stays that way until a generation publishes - and it recovers **on its own** when a later
refresh tick succeeds, without a restart. That was not true until the readiness flag became a
computed state: it used to be written once, at the end of startup, so a host whose first refresh
failed was not-ready forever while every subsequent tick published perfectly.

**Three things a deployment must supply that this module deliberately does not, and that nothing
fails loudly about if you forget:**

1. **The source tables.** `etl-host.cache.sql`'s keys are the group list and each statement must
   project an `id` column (see the property's comment). No table, no generation, no readiness -
   as WARN, per group, at startup.
2. **The task files.** `etl-host.etl.task-directory` is created if absent and an empty directory
   validates fine, so the host boots happily serving **zero tasks** and `GET /admin/etl/tasks`
   answers `[]`. The worked shape-D example lives in `HostFixture.writeTaskFiles`; copy from
   there, since nothing under `src/main/resources` ships one.
3. **Real credentials.** The host now ships an identity provider - `quarkus-elytron-security-properties-file`
   with two embedded users - so a cold boot no longer answers **403 forever** to every admin
   call. What it ships are **placeholders**: `etl-admin` and `etl-reader`, both with the
   password `placeholder-change-me`, both overridable by environment variable
   (`ETL_ADMIN_PASSWORD`, `ETL_READER_PASSWORD`). A deployment either sets those or replaces
   the whole `quarkus.security.users.embedded.*` block with its own mechanism. **Replace it,
   do not remove it** - deleting the block restores the 403-forever boot exactly.

```bash
curl -u etl-admin:placeholder-change-me localhost:8080/admin/etl/tasks     # 200
curl -u etl-admin:wrong               localhost:8080/admin/etl/tasks       # 401
curl -u etl-reader:placeholder-change-me localhost:8080/admin/etl/tasks    # 403
```

**Readiness answers at both `/q/health/ready` and `/health/ready`.** The first is SmallRye's
conventional path, served by a `@Readiness` bean, and is what a deployment manifest should probe;
the second is this module's own JAX-RS resource, kept because something may already be pointed at
it. Both render the same `EtlHost.readinessState`, so they cannot disagree. Metrics are at the
Quarkus default `/q/metrics`.

## Run the tests

```bash
# The module's own suite. `-am` is required: the reactor builds snapshotcache and SimpleEtl
# first, and without it Maven resolves a STALE SimpleEtl from the local repository - which
# surfaces as "No parameter with name 'onTasksLoaded' found", not as anything about staleness.
mvn -pl etl-host -am test

# The same, faster, by not also running the upstream modules' suites (~5 minutes of
# Testcontainers). Use it while iterating - but the run that counts is the one above, because a
# -Dtest filter does not scan the classes it excludes, and a test RESOURCE on an excluded class
# can still be global. That is exactly the bug this filter hid once.
mvn -pl etl-host -am test -Dtest='etlhost.*Test' -Dsurefire.failIfNoSpecifiedTests=false

# The end-to-end test against a real Oracle (Testcontainers, minutes).
mvn -pl etl-host -am test -Dtest=HostEndToEndOracleTest -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false
```

`-DexcludedGroups=none` rather than `-Dgroups=oracle`, and the difference is worth knowing before
you trust a green run: a literal `<excludedGroups>` in a plugin's `<configuration>` **beats** the
user property of the same name, so `-Dgroups=oracle` against such a pom runs zero tests and reports
BUILD SUCCESS. This module's `excludedGroups` is a `<properties>` entry for that reason, and
overriding it is what actually opts in.

A worked task file lives in `example-tasks/` - the same shape-D YAML the test fixture runs, outside test code so copying it needs no source dive.
