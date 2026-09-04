# 42: A runnable example: SFTP drop to MinIO with an HTTP callback, on one laptop

**What to build:** A person with Docker Desktop and this repository can, by following one README, bring up an
SFTP server, MinIO and Oracle locally, start shuttle in serve mode on the spec's vendor-drop route, drop a file
into the SFTP directory, and watch it land in the MinIO bucket, the source file move to the done folder, and an
HTTP callback arrive at a local echo endpoint, then inspect the transfer through the admin endpoints and the
metrics scrape. The same README shows `validate` and `try` mode on the same YAML. Everything in the README is
verified by running it, not by reasoning.

**Blocked by:** None

**Nature:** documentation plus example assets; no production code

**Status:** done

- [x] `shuttle/README.md`: what shuttle is in five sentences, the quickstart, then the three modes, the admin
  endpoints (spec 14.1) with one curl each, the metrics names (spec 14.2), and where the spec, plan and
  progress log live
- [x] `shuttle/examples/vendor-drop.yaml`: the spec 13.1 vendor-drop and mirror routes trimmed to what the
  local stack needs (SFTP store on the local server, MinIO store, one `http` channel to the echo endpoint,
  Oracle state store), every secret a `${VAR}`, comments explaining each knob in one line; it passes
  `shuttle validate`
- [x] `shuttle/examples/docker-compose.yml`: an SFTP server (`atmoz/sftp` with a fixed user and the drop/done
  directories created), MinIO with the bucket created and versioning enabled (an `mc` init container), Oracle
  Free (`gvenzl/oracle-free:23-slim-faststart`, with the 8.1 DDL applied at first start from
  `StateStoreSchema.DDL`'s text, checked into `examples/schema.sql` and kept identical to spec 8.1's block by
  `StateStoreSchemaTest`), and an HTTP echo server that logs request bodies
- [x] `shuttle/examples/seed.ps1` (verified on Windows; `seed.sh` is the unverified twin) that copies a sample
  CSV into the SFTP drop directory
- [x] The README's serve command works from this repository (the runner jar from `mvn -pl shuttle package`;
  the environment variables it needs are listed in one block, including `SHUTTLE_ADMIN_PASSWORD`,
  `SHUTTLE_DB_*`, the SFTP and S3 credentials and `shuttle.config` pointing at the example YAML)
- [x] The whole walkthrough was run once end to end on this machine and the progress entry records what was
  observed (the object key in MinIO, the moved file, the callback body the echo server logged, the admin
  `transfers` answer, one metric line) and every place the README had to be corrected
- [x] Progress entry appended, checklist ticked, Status done

Ground rules for every ticket: implement only this ticket; no `Thread.sleep`; append a progress entry to
`docs/shuttle/progress.md` describing what was done and every deviation. The spec is `docs/shuttle/spec.md`
and the plan is `docs/shuttle/plan.md`; the spec wins over this ticket when they disagree, unless the progress
log records a deliberate deviation. Never edit inside spec 8.1's DDL block: `StateStoreSchemaTest` compares it
verbatim. This ticket adds no production code: nothing under `shuttle/src/main` changes.
