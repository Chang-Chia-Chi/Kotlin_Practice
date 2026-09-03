# 04: Mapping renderer and providers

**What to build:** A channel's mapping table renders a JSON body from a transfer row and its attributes: fields
from the vocabulary, attributes, providers mounted whole or selected by pointer and invoked once
per rendering, literal values, coercion, formatting, defaults and the four row transformations.
The same component checks a table against a route's declared attributes before anything is stored.

**Blocked by:** 01 (Skeleton)

**Nature:** pure function work

**Status:** done

- [x] Every row key of spec Sec 9.6 has a test; dotted paths nest; a name containing quotes and backslashes is escaped
- [x] `I22`: one provider selected by three rows is invoked once
- [x] A missing required value reports the row; `required: false` omits the path; `default` applies before `required`
- [x] The check function rejects an undeclared attribute, an unknown field, an unregistered provider, an invalid pointer and an unparseable format, by rule number
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
