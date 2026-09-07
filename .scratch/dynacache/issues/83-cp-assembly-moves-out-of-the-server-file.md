# 83: The CP assembly moves out of the server file

**What to build:** One file stops changing for four unrelated reasons. `DynaCacheServer.kt` is
513 lines holding the Netty bootstrap, the frame decoder, the per-connection handler with its
batch and session state, `main`, and the whole assembly of the CP subsystem: a Raft change, a
Netty change, a handler change and a command-line change all land in the same place, which is
the review's Divergent Change finding. After this ticket the CP assembly lives beside the CP
code it wires, and the server file holds the socket, the handler and `main`. Nothing about how
a node starts, what it listens on, or what a client sees changes.

**Blocked by:** 81 (File operations leave the server module). Not a functional gate: both
tickets edit the same two files heavily, so they are sequenced to avoid a merge.

**Nature:** wiring, no behaviour change (Opus)

**Status:** done (DynaCache 95776a55, merged into misc/ai_gen; the args helper stays in the server by design)

- [x] The CP assembly is not in `DynaCacheServer.kt`; the file's remaining reasons to change
      are the socket, the handler and `main`
- [x] A node in single-node mode and a node in cluster mode both start, serve and shut down as
      before; the existing acceptance tests prove it unchanged
- [x] The module graph is unchanged: nothing new is imported into a module that did not already
      depend on it
- [x] Progress entry appended

Size budget: 200 to 600 lines; the diff is a move, so report how much is genuinely new. If the
CP assembly cannot move without the CP module learning about Netty or the command line, stop
and report: that would mean the seam is in the wrong place and the ticket needs rethinking
rather than forcing.

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The plan is docs/dynamiccache/plan.md and this
ticket's entry is docs/dynamiccache/plans/p6-review-fixes.md. Modify only DynaCache/.
