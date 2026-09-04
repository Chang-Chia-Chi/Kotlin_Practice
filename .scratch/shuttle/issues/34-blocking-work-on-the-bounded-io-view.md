# 34: Archive writing, digests and content reads run on the bounded IO view

**What to build:** Spec 3.3 and plan 2.5 say every blocking call, archive writing included, runs on the one
bounded view of `Dispatchers.IO` the module owns, sized to the sum of route parallelism. Today zip and unzip,
the digest computation and the content read for `extract` block on the pipeline's own dispatcher, which is
the host scope's `Dispatchers.Default`, so a large archive occupies a CPU worker and the bounded view's
budget (rule 9's arithmetic) is not what bounds the module's blocking work. Progress 14 claims the opposite;
the claim is corrected. Review finding Standards 2.

**Blocked by:** 33 (MDC around every stage), because both change how the pipeline enters a stage

**Nature:** dispatcher discipline

**Status:** done

- [x] A test proves that a processor's blocking work runs on the bounded view: a chain with a zip step run under a pipeline whose `io` is a one-thread named dispatcher asserts the thread name inside the step (through the `ProcessContext` or a test processor), red before the fix
- [x] `ProcessingChain`'s built-in processors (zip, unzip, digest, extract's content read, rename's file move if it blocks) and `Digest.of` run under `withContext(io)`; the dispatcher is passed once (to the chain or the context), not per step
- [x] Custom processors keep the contract spec 6.2 gives them (document in the KDoc whether a custom processor is called on the bounded view or must switch itself; choose the one that keeps rule 9 true and record it)
- [x] `TryCommand` (which runs the chain offline) still works with a plain dispatcher
- [x] Progress entry appended, correcting progress 14's sentence about archive writing

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
