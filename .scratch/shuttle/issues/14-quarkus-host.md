# 14: Quarkus host, validate mode and admin

**What to build:** The application runs: producers in order, YAML loaded and validated at boot with every rule
number reported, named beans resolved for custom processors and providers, startup checks that
fail loudly on a missing table or bucket, staging wiped, readiness per the configured rule, the
seven admin endpoints under the admin role, a validate mode that connects to nothing, and an
ordered shutdown within the drain timeout.

**Blocked by:** 02 (YAML loader), 08 (Crash matrix), 09 (Notifier), 10 (Oracle state store), 11 (S3 target), 12 (HTTP channel), 13 (SFTP poll source)

**Nature:** shutdown ordering and timing work

**Status:** ready-for-agent

- [ ] `I12`: close returns within the drain timeout with a delivery parked in a stalled loopback server, and every PENDING row stays PENDING
- [ ] S15, S18, S24, S25 through the real host
- [ ] A boot with a missing table fails naming the DDL; a boot with a missing bucket fails naming the bucket; both readiness rules behave as spec Sec 10 says
- [ ] Every endpoint of spec Sec 14.1 answers under the admin role and changes what it says it changes, including the manual ack and the route restart
- [ ] Blocking calls run on the module's bounded IO dispatcher; metrics appear in the host registry
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>`, scenario tests by their `S<n>` id, validation tests by `rule<n>_`; every
new configuration knob lands in the YAML grammar and the Kotlin DSL with a numbered rule; every
new meter uses the names fixed in spec Sec 14.2; append a progress entry to
docs/shuttle/progress.md describing what was done and every deviation. The spec is
docs/shuttle/spec.md and the plan is docs/shuttle/plan.md; the spec wins over this ticket when
they disagree, unless the progress log records a deliberate deviation. Modify only shuttle/ and,
when a measurement forces it, docs/shuttle/.
