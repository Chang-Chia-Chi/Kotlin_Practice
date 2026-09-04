# 33: Every log line inside a transfer carries its transfer id and route

**What to build:** Spec 3.2 says `transferId` and `route` sit in the logging MDC around every stage.
Nothing in the module touches the MDC today, so an operator grepping a log for one transfer finds only the
lines that happen to interpolate the id. After this ticket every log line emitted while a pipeline runs a
transfer (fetch, process, store, ack, the callback, the connector's own lines under the pipeline's coroutine)
and every line the notifier emits while delivering a row carries `transferId` and `route` (and `channel`
for the notifier) in the MDC, across coroutine suspension and dispatcher hops, and the keys are gone again
outside those scopes. Review findings Spec 4 and Standards 1.

**Blocked by:** None (can start immediately)

**Nature:** cross-cutting; coroutine context propagation

**Status:** done

- [x] `TransferPipelineTest`: a test with a capturing SLF4J appender (or the JBoss log manager's equivalent already on the test classpath) asserts that a WARN emitted by a stage failure inside `run` carries `transferId` and `route`, and that a line logged after `run` returns carries neither; red before the fix
- [x] `NotifierTest`: the same for a delivery failure WARN with `transferId`, `route` and `channel`
- [x] Propagation uses `kotlinx-coroutines-slf4j`'s `MDCContext` (in `~/.m2`, pick the version the coroutines BOM in the reactor pins; the pom's `dependencyManagement` decides) wrapped once at the pipeline's entry and once around a delivery, not per log call; blocking calls on the bounded IO dispatcher inherit it
- [x] `ArchitectureTest` gains a rule or the existing "no logger in Context classes" rule is kept; `sftp-core` and the connector are untouched
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
