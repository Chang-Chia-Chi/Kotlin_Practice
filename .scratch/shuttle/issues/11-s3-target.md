# 11: S3 target and fetcher over the AWS SDK

**What to build:** Copies land in a versioned MinIO through AWS SDK v2 configured as agreed. Store is one call
that leaves exactly one copy at the key: PUT with Content-MD5 when the digest is MD5, HEAD of
the content length, the ETag compared with the MD5 on single-part unencrypted objects, then a
prune of every other version of exactly that key. Verify checks the version still exists; probe
fails startup on a missing bucket. A fetcher streams an object to staging with its digest.

**Blocked by:** 01 (Skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] The shared target contract test class passes against the in-memory target and the S3 target on Testcontainers MinIO with versioning enabled, tagged `minio`
- [ ] `I6` on MinIO: three stores of one key leave one version; a crash between PUT and prune, played through an adapter hook, is repaired by the next store
- [ ] A corrupted body is rejected by Content-MD5; the ETag check passes on a single-part object and is skipped with a WARN when the bucket reports encryption
- [ ] Verify of a deleted version is false; a key sharing a prefix with a neighbour is never pruned by the neighbour's store; the multipart threshold is pinned above the largest expected file
- [ ] The fetcher's digest matches the object's; the AWS SDK appears only in the s3 package
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
