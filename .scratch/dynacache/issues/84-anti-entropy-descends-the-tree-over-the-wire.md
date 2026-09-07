# 84: Anti-entropy descends the tree over the wire

**What to build:** Two replicas that disagree about a range exchange only the subtrees that
disagree, so the cost of finding the divergence scales with how much diverged rather than with
how big the range is. That is the property a Merkle tree exists to buy, and today only half of
it is collected: the roots are compared over the wire, but on a mismatch the peer ships its
entire leaf list and the requester builds the peer's tree and descends locally. A range of a
million keys with one bad key costs a million leaves on the wire. After this ticket the descent
happens across the exchange: the requester asks for the children of the nodes that disagreed,
level by level, and only the leaves under a subtree that really differs are ever sent. A range
whose roots match still costs one comparison, as it does today.

The value here is the idea rather than a measured problem: at three nodes with modest ranges
the leaf list is cheap, and the current protocol is simpler. This is the classic Dynamo
behaviour and the reason the structure is a tree at all, and it was chosen deliberately over
recording the shortcut as a deviation.

The decision this ticket owns is the request budget. Anti-entropy's step promises one range,
at most two requests and a bounded wait, which is how plan 2.5's "every fan-out bounded" reads
for this process; a descent needs more than two. Either bound the descent (a maximum number of
rounds per step, the range resuming on a later tick) or restate the promise, in the class's own
documentation and in the progress entry. Do not leave a background process that can issue an
unbounded number of requests before its deadline.

**Blocked by:** None (can start immediately)

**Nature:** background convergence protocol, C6 (Fable)

**Status:** ready-for-agent

- [ ] `a_single_divergent_key_costs_a_descent_not_the_range`: with a range of many keys and one
      key differing, the exchange carries a number of hashes that grows with the tree's depth,
      and leaves only for the subtree that differs; the test asserts the count, not just the
      outcome
- [ ] `a_matching_range_still_costs_one_comparison`: unchanged from today
- [ ] The keys the exchange decides to sync are exactly those the local diff decides today, so
      `anti_entropy_heals_divergence`, `convergence_after_partition` and every existing
      anti-entropy and convergence test pass unchanged, tombstones included
- [ ] The request budget per step is bounded and stated, in the class's documentation and the
      progress entry
- [ ] Progress entry appended

Size budget: 200 to 600 lines including tests. C6 still binds: the tree is a pure function of
the range's triples and two nodes holding the same data produce the same root, so the descent
must not depend on scan order or on a node's own fan-out. Both sides must agree on fan-out
before descending; say how. Do not change the conflict rule, the versioned store or what a leaf
is. If a fixed contract does not survive contact with reality, stop, write what you found to
the progress file, and report.

Ground rules for every ticket: implement only this ticket; 200 to 600 lines including tests;
JUnit 5 + Mockito only, no AssertJ or MockK; no sleeps, time is an injected Clock; spec-named
tests keep their names, constraint tests `C<n>_<description>`, invariant tests
`I<n>_<description>`; Matt Pocock `tdd` at the seams the plan entry names, red before green,
one slice at a time, `codebase-design` vocabulary for any new interface, and a `code-review`
self-pass before the commit; append a progress entry to docs/dynamiccache/progress.md
describing what was done and every deviation. The spec is docs/dynamiccache/design-spec.md
(2.4, C6, 6.7), the plan is docs/dynamiccache/plan.md and this ticket's entry is
docs/dynamiccache/plans/p6-review-fixes.md; the spec wins over this ticket when they disagree,
unless the progress log records a deliberate deviation. Modify only DynaCache/ and, when a
measurement forces it, docs/dynamiccache/.
