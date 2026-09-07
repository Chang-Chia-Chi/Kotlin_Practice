# 80: A failed snapshot cut abandons the snapshot, not the node

**What to build:** A node that cannot write its part of a distributed snapshot keeps serving.
Today the cut's file work can raise an ordinary `IOException` (a full disk, a permission the
process lost, a directory removed underneath it), and the node's inbound loop has no
per-envelope catch, so that exception ends the loop and the node stops answering anything: a
disk problem on one node becomes an availability problem for the third of the keyspace it
coordinates. After this ticket a cut that fails for an environmental reason abandons that
snapshot set on this node, records the failure where an operator can see it, and leaves the
node reading its next envelope. Found by T75; pre-existing since T36.

The decision this ticket owns is where the policy sits. The loop's absence of a catch is
deliberate (T68): a blanket `try` around every handler would swallow programming errors and
turn a broken build into a silently degraded node. So either the snapshot handler owns its own
failure policy, or the loop gains one that distinguishes an environmental failure from a bug
(by exception type, not by position). Choose one, state why in the progress entry, and make the
choice visible in the code rather than implied.

**Blocked by:** None (can start immediately)

**Nature:** failure semantics at a trust boundary, C10 and availability (Fable)

**Status:** done (DynaCache 2a6f1f83, merged into misc/ai_gen; policy in the snapshot handler, not the loop)

- [x] `a_cut_that_cannot_write_abandons_the_set_and_the_node_lives`: with the snapshot
      directory made unwritable, a marker arrives, the node records no part for that snapshot,
      and the next ordinary command on the same node still answers
- [x] `a_failed_cut_leaves_no_half_written_part`: nothing of the abandoned set remains that a
      later restore could read as complete
- [x] A programming error inside a handler still fails loudly rather than being swallowed; a
      test pins whichever boundary the chosen policy draws
- [x] Every existing Chandy-Lamport, inbound-loop and P4 acceptance test passes
- [x] Progress entry appended

Size budget: 200 to 600 lines including tests. Making the directory unwritable is a filesystem
fact, not a mock; if Windows makes that awkward in a test, inject a failing sink through the
part adapter's seam instead and say so. Do not change T49's cut-then-open order, T74's WAL
placement or T75's id validation. If a fixed contract does not survive contact with reality,
stop, write what you found to the progress file, and report.

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
