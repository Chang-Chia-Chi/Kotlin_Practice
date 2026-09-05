# 43: A custom processor receives its `config`, with `${VAR}` expanded

**What to build:** Spec 6.2 gives a `custom` processor step a `config` map that the named bean receives.
Today the host and try mode resolve the bean by name only and drop the map, so a step such as
`custom: imageResizer, config: { maxWidth: 2048 }` (spec 13.1's own example) runs the bean with nothing;
and `${VAR}` inside `custom.config` is left literal by the loader with no error for a missing variable.
After this ticket the bean is given its config through the seam spec 6.2 describes (or the smallest
faithful equivalent: a `Processor` factory taking the map, resolved once at boot), `${VAR}` inside it is
expanded like every other secret reference (rule 25 semantics), a missing variable is reported at validate
time with a rule number, and try mode does the same. Review findings Standards 6 (part) and the bug hunt's
last unconfirmed item.

**Blocked by:** None (can start immediately)

**Nature:** seam wiring for the one processor kind that takes configuration

**Status:** done

- [x] `ShuttleHostTest` (or `ShuttleHostM2WiringTest`): a `custom` step with a config map is built with that map and the bean sees it (a test bean recording what it was given, through `NamedBeans`); red before the fix
- [x] `YamlLoaderTest` and `RulesTest`: `${VAR}` inside `custom.config` expands from the environment; a missing variable is a numbered violation naming the step (extend rule 25 or the rule the loader already uses for secrets; do not invent a second mechanism)
- [x] `TryCommandTest`: try mode hands the same config to the bean
- [x] Spec 6.2 and 13.3 state the contract in one sentence each if they did not already
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200-600 lines including tests; no Thread.sleep;
invariant tests named `I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`,
regression tests for a review finding by `B<n>_<description>` or `SPEC<n>_<description>`; every new configuration
knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every new meter uses the names fixed in
spec Sec 14.2; append a progress entry to docs/shuttle/progress.md describing what was done and every deviation.
The spec is docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when they
disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and, when a measurement
forces it, docs/shuttle/. Never edit inside spec 8.1's DDL block: StateStoreSchemaTest compares it verbatim.
