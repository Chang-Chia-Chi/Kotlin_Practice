# 26: Fix: two parents may expand a child with the same name, size and mtime

**What to build:** Two parents (two manifests naming one shared image, or two archives whose generated
entries share a name, size and epoch mtime) each create their child rows and store them. Today Oracle
refuses the second child with `ORA-00001` on the identity index of spec 8.1, the parent walks to
FAILED and a re-drive fails the same way, while the in-memory store permits it, so no test on the
fakes sees it (the same adapter drift D44 found). Review finding B3.

**Blocked by:** None (can start immediately)

**Nature:** schema and adapter parity at the StateStore seam

**Status:** done

- [x] `StateStoreContract` gains the case (two parents, one identical child identity each): green on the in-memory store and on Oracle (`-DexcludedGroups=none -Dtest=JdbiStateStoreTest`); red on Oracle before the fix
- [x] The identity constraint distinguishes children by parent (include the parent id, or exclude CHILD rows from it): decide with spec 4.5's "a child's identity is its parent's plus its path" and spec 5.2, and record it as a decision entry
- [x] The in-memory store enforces exactly what Oracle enforces
- [x] Spec 8.1's DDL block is updated to the new constraint together with `StateStoreSchema.DDL`, keeping `StateStoreSchemaTest`'s verbatim comparison green (this is the one ticket allowed to touch the block, and only for this constraint)
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>`; every new configuration knob lands in the YAML
grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in spec Sec 14.2; append
a progress entry to docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement forces it,
docs/shuttle/. Run the Oracle class with `-DexcludedGroups=none`; a plain `-Dtest=` on a tagged class runs zero
tests and reports green.
