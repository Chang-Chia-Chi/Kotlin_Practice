# 66: A versioned store beside the engine

**What to build:** One module answers "what is held under this key, with its version" and
"install this value with this version", and nobody else touches the version table or the
engine's view/install hooks. Today the (value, version) pair is installed in three places as
two unsynchronised steps, read in two places as two steps, and spec 5.3 is decided twice with
different outcomes (replicate applies the remote command over the local value; anti-entropy
merges values). After this ticket replication, read repair, anti-entropy and snapshot restore
all go through the versioned store; a reader never observes a value paired with another
install's version (the store installs and reads the pair inside one engine task); spec 5.3 is
decided in the store whether the incoming pair arrives as a command or as a value; the
cluster's recording engine test double is deleted because the store is the test surface.
The open read race found in the review (value read, then version read, with a write between)
gets its first deterministic seam and its regression test here.

**Blocked by:** 55 (A snapshot-set part is a persist adapter), 65 (A replicate carries codec bytes)

**Nature:** conflict resolution semantics, spec 2.5 and 5.3, concurrency (Fable)

**Status:** done (DynaCache c8d5674e, merged into misc/ai_gen; anti-entropy now carries tombstones, closing T28 dev 3 and T30 dev 1/6)

- [x] `read_never_pairs_a_value_with_another_installs_version`: a write interleaved between
      the store's value read and version read, driven deterministically through the store's
      seam, cannot produce a stale value with a newer version
- [x] `spec_5_3_decided_once`: the same concurrent pair arriving as a replicate and as an
      anti-entropy value produces the same stored outcome
- [x] Replication, anti-entropy and snapshot restore have no direct reference to the version
      table or the engine's view/install hooks; the recording engine double is deleted
- [x] Every existing read-repair, anti-entropy, convergence, snapshot and acceptance test
      passes unchanged; the convergence checker reads the pair from the store
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; this one may reach 800 lines
including tests because it moves three call sites, but no further; JUnit 5 + Mockito only,
no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named tests keep their names,
constraint tests `C<n>_<description>`, invariant tests `I<n>_<description>`; Matt Pocock `tdd`
at the seams the plan entry names, red before green, one slice at a time, `codebase-design`
vocabulary for any new interface, and a `code-review` self-pass before the commit; append a
progress entry to docs/dynamiccache/progress.md describing what was done and every deviation.
The spec is docs/dynamiccache/design-spec.md, the plan is docs/dynamiccache/plan.md and this
ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket
when they disagree, unless the progress log records a deliberate deviation. Modify only
DynaCache/ and, when a measurement forces it, docs/dynamiccache/.
