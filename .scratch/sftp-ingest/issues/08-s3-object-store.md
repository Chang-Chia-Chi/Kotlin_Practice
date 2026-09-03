# 08: S3 object store adapter over the AWS SDK

**What to build:** Objects land in a versioned MinIO through AWS SDK v2 configured as agreed: synchronous client
over the Apache HTTP client, endpoint override, path-style access, placeholder region,
environment credentials, checksums only when required, and explicit timeouts. Put returns the
version id and stores the digest metadata, head answers null for an absent object, prune
deletes every other version of exactly that key, and probe fails startup on a missing bucket.

**Blocked by:** 01 (Walking skeleton)

**Nature:** adapter work

**Status:** ready-for-agent

- [ ] The shared object-store contract test class passes against both the in-memory store and the S3 store on Testcontainers MinIO with versioning enabled, tagged `minio`
- [ ] `I6` on MinIO: three puts of one key leave exactly one version after prune
- [ ] Head of a deleted version returns null; a key sharing a prefix with a neighbour is never pruned by the neighbour's prune
- [ ] Metadata on every object carries digest, digest algorithm, source mtime, source name and transfer id
- [ ] Probe fails with a message naming the bucket when it is absent or forbidden; the bucket is never created
- [ ] The AWS SDK appears only in the s3 package
- [ ] Progress entry appended

Ground rules for every ticket: implement only this ticket, stubs throwing NotImplementedError
for later seams; 200-600 lines including tests; no Thread.sleep; invariant tests named
`I<n>_<description>` and scenario tests named by their `S<n>` id; every new configuration knob
lands in the DSL with build-time validation; every new meter uses the names fixed in spec Sec 13;
append a progress entry to docs/sftpingest/progress.md describing what was done and every
deviation. The spec is docs/sftpingest/spec.md and the plan is docs/sftpingest/plan.md; the
spec wins over this ticket when they disagree, unless the progress log records a deliberate
deviation. Modify only sftp-ingest/ and, when a measurement forces it, docs/sftpingest/.
