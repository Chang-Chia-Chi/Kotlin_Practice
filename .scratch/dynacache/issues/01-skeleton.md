# 01: Skeleton, frozen reply model, test tooling

**What to build:** The April scaffold becomes the plan's scaffold: AssertJ leaves every pom and
Mockito arrives next to JUnit 5; the engine gains the frozen `Reply` model (the five RESP2
types with byte-array equality), the binary-safe hash-tag-aware `Key`, `PartitionId`, the
`Command` sealed root with `Ping` only, and the `CommandEngine` interface (`submit` returning a
`CompletableFuture<Reply>`, and `atomically`) as stubs throwing NotImplementedError. Read
`DynaCache/CONTEXT.md` and `DynaCache/docs/adr/` first; use their words.

**Blocked by:** None (can start immediately)

**Nature:** scaffold and frozen surface (Opus)

**Status:** done (DynaCache cbe9df2)

- [x] `mvn package` green from `DynaCache/`; one passing test per module
- [x] No pom mentions AssertJ; Mockito core is a test dependency of every module
- [x] `Reply.Bulk(null)` and `Reply.Bulk(bytes)` compare by content; `Key` equality is by bytes
- [x] `Key("{user1}.a")` and `Key("{user1}.b")` hash alike; a key without braces hashes whole
- [x] `CommandEngine.submit` and `atomically` exist with the signatures of plan 2.3 and throw NotImplementedError
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200 to 600 lines including tests; JUnit 5 + Mockito only, no AssertJ or MockK;
no sleeps, time is an injected Clock; spec-named tests keep their names, constraint tests
`C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd` at the seams the
plan entry names, red before green, one slice at a time; append a progress entry to
docs/dynamiccache/progress.md describing what was done and every deviation. The spec is
docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this ticket's entry
is docs/dynamiccache/plans/p1-data-engine.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/. DynaCache/ is its own git repository; commit code
there and docs in the parent.
