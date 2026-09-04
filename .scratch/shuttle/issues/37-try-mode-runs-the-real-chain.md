# 37: `shuttle try` runs the same processing chain the pipeline runs

**What to build:** D35 promises try mode reuses the pure chain, so what an operator sees offline is what
the route will do. Today `TryCommand` re-implements the step loop, its own key expansion (`directory/key`
where the pipeline uses `key`), and a third `ProcessContext` whose `fetch` throws `NotImplementedError`
naming ticket 17; rule 22 and digest recomputation are skipped, and a route with `expand` cannot be tried at
all. After this ticket `TryCommand` calls `ProcessingChain` with an observer that reports each step's
attribute deltas and objects, through the pipeline's own context shape, with the fetcher map so `expand`
works against sample files, and the key an operator sees is the key the target would get. Review findings
Spec 7, Standards 4, Architecture C4.

**Blocked by:** 34 (blocking work on the bounded IO view), because both change `ProcessingChain`

**Nature:** deepening; one chain, one context, one key function

**Status:** ready-for-agent

- [ ] `TryCommandTest`: a route with `expand` over a sample metadata file and sample children prints one key and one body per child; red before the fix (today: `NotImplementedError`)
- [ ] `TryCommandTest`: the key printed equals the key `TransferPipeline` would store under for the same inputs (assert through the shared key function, not by duplicating its rule in the test)
- [ ] `ProcessingChain` exposes per-step observation (attributes set, objects after the step) as a parameter with a no-op default; `TransferPipeline` passes nothing and is otherwise untouched
- [ ] The third `ProcessContext` implementation is deleted; try mode uses the same one the pipeline uses, in a temp directory, with a fetcher map over the sample files
- [ ] Rule 22 and the digest are judged in try mode as in serve mode; S31's tests stay green with their ids
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
