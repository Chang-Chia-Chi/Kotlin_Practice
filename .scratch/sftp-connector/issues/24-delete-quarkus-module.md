# 24: Delete `sftpconnector-quarkus`

**What to build:** Nothing. Remove. `sftpconnector/quarkus` (T14) spells every configuration knob
a third time - builder, `@ConfigMapping` interface, `toConnectorConfig` - and has no consumer:
shuttle, the only Quarkus host, builds its config through the core DSL in its own words and
imports nothing from the module (`shuttle/.../ShuttleLifecycle.kt:44` cites `SftpConnectorLifecycle`
in a comment only). Deleting it makes complexity vanish and nothing reappears elsewhere, which is
the deletion test passing. It also removes two standards findings (versions pinned in its pom,
and a KDoc claiming the endpoint tags every log line) without fixing them.

**Blocked by:** None (no overlap with the T17 lens 3 files in flight)

**Model:** Opus 5

**Status:** done

- [x] `sftpconnector/quarkus/` deleted, and its `<module>` line in `sftpconnector/pom.xml`
- [x] Parent `pom.xml` `dependencyManagement`: remove only entries that nothing else in the reactor
      uses (shuttle also uses Quarkus and Micrometer - check before removing anything; when in
      doubt leave it)
- [x] `docs/sftpconnector/implementer-brief.md` module table: the `sftpconnector/quarkus` row
      becomes one line saying the adapter was deleted by ticket 24 and why; spec 3.2 gets the same
      sentence; D3 stands unchanged (Quarkus stays out of core - there is simply no adapter module
      until a second host needs one)
- [x] `ShuttleLifecycle.kt:44`'s comment reworded so it no longer names a class that does not
      exist - the only shuttle edit, one line
- [x] Full reactor green
- [x] Because this ticket touches `shuttle/`: run shuttle's default tier in the worktree (`mvn -B -o -q -pl shuttle test`, about 90 s) and put the counts in the progress entry. `ShuttleQuarkusTest` may fail on port 8081 and `ShuttleHostTest`'s two readiness cases may flake under parallel builds until shuttle ticket 30 lands; rerun those alone and say so
- [x] Progress entry appended; T14's entry stays as history with one line at its top pointing here;
      an open-seams row "A second Quarkus host would want a properties mapping" with owner "that
      host" and consequence "it writes its own, as shuttle did"

Ground rules for every ticket: implement only this ticket; no Thread.sleep; never weaken an
earlier ticket's test; comments and messages carry reasons, never spec section numbers; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the progress
log records a deliberate deviation. Work in an isolated worktree branched from `misc/ai_gen`;
modify only `sftpconnector/`, `docs/sftpconnector/`, the root `pom.xml`, and the one shuttle line named.
