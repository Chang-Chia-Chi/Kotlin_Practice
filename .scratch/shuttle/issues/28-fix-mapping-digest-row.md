# 28: Fix: a mapping row's `digest: <algo>` renders that digest or nothing

**What to build:** A mapping row that asks for `digest: sha256` renders the SHA-256 of the object
when the route computed one, and renders the row as missing (subject to `required`) when it did
not; today the renderer ignores the row's algorithm and emits whatever digest the transfer carries,
so an MD5 hex is delivered labelled sha256. Spec 9.6 asks for a second digest; ticket 04's deviation
4 describes "missing on mismatch", which the code never did. Review findings B7, Spec 3, Standards 6.

**Blocked by:** None (can start immediately)

**Nature:** renderer correctness, one row kind

**Status:** ready-for-agent

- [ ] `MappingRendererTest`: a `digest: sha256` row on a transfer digested with MD5 renders missing (and a `required: true` row fails the render naming the row); the same row on a transfer digested with SHA-256 renders the hex; red before the fix
- [ ] Decide, against spec 6.5 and 9.6, whether a route may carry two digests (then the pipeline computes the second when a mapping asks) or only one (then the row is validated by a rule at boot: a mapping asking for an algorithm the route does not compute is a violation, numbered, in YAML and DSL); record the decision and implement that one
- [ ] `ValidateCommandTest` or `RulesTest` covers the boot-time check if that is the choice
- [ ] Progress entry appended, correcting ticket 04's deviation 4

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
