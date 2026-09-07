# 73: Narrow the command engine seam to submit

**What to build:** The seam both engines present is `submit` (and close); a batch is a
capability the connection handler asks of the AP engine it reaches, not a method every
adapter must implement. Today `atomically` has one real implementation and six others that
throw, refuse, pass through or are marked to-do, and close chains four deep through
dispatcher, node, router and replication. After this ticket the six dead implementations are
deleted, the wrapper chain forwards one method, MULTI/EXEC and EVAL behave exactly as before,
and plan 2.3's seam table is updated to say so.

**Blocked by:** 65 (A replicate carries codec bytes)

**Nature:** interface narrowing (Opus)

**Status:** done (DynaCache ebd6ca60, merged into misc/ai_gen; BatchEngine has two adapters, ApEngine and ClusterNode)

- [x] The command engine interface has no batch method; only the AP engine offers one, and
      the handler reaches it through a stated capability, not a cast
- [x] The CP engine, the forwarding CP engine, the router, replication, the dispatcher and
      the dispatcher test's recording engine have no batch code
- [x] Every MULTI/EXEC, EVAL, Lua, P1 and P5 acceptance test passes unchanged
- [x] Plan 2.3's seam table is updated in docs/dynamiccache/plan.md
- [x] Progress entry appended

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec.md,
the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
