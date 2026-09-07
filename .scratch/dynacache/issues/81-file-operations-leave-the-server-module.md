# 81: File operations leave the server module

**What to build:** Plan rule 2.2 holds for the server as it now does for the cluster. The rule
says `java.nio.file` appears only in the engine's persist package and the CP module; the server
breaks it in two files, and that is the standards review's one unresolved hard violation. The
composition root has a real need the rule did not anticipate: it reads a data directory from
the command line and hands it to things that take a path. That need is a path as a *value*, not
file work. After this ticket the server does no file *operations*: creating directories,
testing existence, listing, deleting all live behind the persist package's own interfaces, and
the rule is restated to say what is actually intended, that `Path` may be carried as
configuration by the composition root while every `Files` call lives in the engine's persist
package or the CP module.

Both halves are required: the code moves the operations down, and plan 2.2's sentence is
rewritten so the rule and the code agree. A rule the code cannot honour is worse than no rule,
which is why the review flagged it as "either amend it or the rule is dead".

**Blocked by:** None (can start immediately)

**Nature:** module graph and wiring, plan 2.2 (Opus)

**Status:** ready-for-agent

- [ ] No `Files`, `java.io.File` or `kotlin.io.path` operation remains in the server module's
      main sources; a `Path` carried as configuration is what remains, if anything
- [ ] Whatever the server did to prepare a data directory now happens behind a persist-package
      interface, with its own test at that seam
- [ ] Plan 2.2's `java.nio.file` sentence is rewritten to state the intent, and says which
      package owns file operations
- [ ] Every existing server, cluster and acceptance test passes; a node still creates its data
      directory on first start
- [ ] Progress entry appended

Size budget: 200 to 600 lines including tests; the diff may be net negative. If moving an
operation down would force the persist package to learn something about the server's command
line, stop and report rather than pushing configuration downward. If a fixed contract does not
survive contact with reality, stop, write what you found to the progress file, and report.

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The plan is docs/dynamiccache/plan.md and this
ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/ and, for
this ticket's rule amendment, docs/dynamiccache/plan.md.
