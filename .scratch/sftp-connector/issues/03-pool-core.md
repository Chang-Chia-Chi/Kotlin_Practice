# 03: Pool core: registry, entry states, acquire and release

**What to build:** A caller can acquire a session from a bounded pool and release it back, with the pool
keeping every entry in one registry guarded by one mutex, idle entries in a LIFO deque, capacity
enforced by a semaphore, and each entry exposing its state as a StateFlow. No transport call
ever runs while the mutex is held; connect and close happen in the transitional states outside
the lock. Built and proven against the fake transport in the testkit, no socket.

**Blocked by:** 01 (Walking skeleton)

**Status:** ready-for-agent

- [ ] Entry states Connecting, Idle, InUse, Validating, Evicting, Closed as a StateFlow per entry
- [ ] Acquire pops the most recently used idle entry or registers a Connecting entry and connects outside the lock
- [ ] Release pushes to the idle deque and releases the permit last
- [ ] Fake transport in the testkit with scripted connect success, failure and delay via hook points
- [ ] I1: idle + inUse + connecting never exceeds maxSize; I2: an entry is handed to at most one lease at a time; I5: no transport call executes while the mutex is held (asserted through a hook that fails if invoked under the lock)
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`; every new configuration knob lands in the DSL block for its area with
build-time validation; every new meter uses the names fixed in spec Sec 13; append a
progress entry describing what was done and every deviation. The spec is
docs/sftpconnector/spec.md and it wins over this ticket when they disagree, unless the
progress log records a deliberate deviation.
