# SFTP NAS-Migration — Test Plan

Basis for test-script authoring. Each case has a stable ID (`T1-*` Tier-1 logic,
`T2-*` Tier-2 semantics, `C-*` canary) so scripts can reference it. Terms in
**bold** map to definitions in [CONTEXT.md](./CONTEXT.md).

## Tiers & environments

| Tier | Environment | Validates | Gate |
|------|-------------|-----------|------|
| **Tier 1 — logic** | sftp testcontainer / local temp dirs, `NAS1_ROOT`/`NAS2_ROOT` pointed at local dirs | Script correctness — pure logic, no NFS needed | Run in CI on every change |
| **Tier 2 — semantics** | VM (CentOS-8 ideal, Ubuntu acceptable) with **real NFS** mounts (Option A test exports, or Option B self-hosted NFS) matching prod `vers=`/options | NFS-specific safety (silly-rename, stale handles, reboot) | Must pass before canary |
| **Canary** | Production VM, one oldest low-traffic partition | End-to-end under real load/identities | Gates the wider backfill |

**Hard gate order:** Tier-1 green → Tier-2 green (esp. `T2-01`) → purge dry-run reviewed (`T1-30`) → canary → batched backfill.

## Fixtures / harness helpers

- `make_partition <date> <category> <retention-class> <size>` — create `NAS1_ROOT/<date>/<category>/...` with deterministic, checksummable content.
- `set_today <YYYY-MM-DD>` — inject a fixed UTC "now" (env var the script reads) so age math is deterministic; tests must not depend on the wall clock.
- `sentinel on|off` — create/remove `NAS2_ROOT/.nas2_sentinel` to simulate NAS2 available/unavailable (see **NAS2 Availability Guard**).
- `active_sessions <n>` — stub the live-load signal for backfill gating.
- Assert helpers: `assert_symlink_resolves`, `assert_sha256_matches`, `assert_exit_nonzero`, `assert_no_local_shadow_growth`.

---

## Tier 1 — logic (local, no NFS)

### Age computation (**Age Computation (UTC)**)
- `T1-01` Parse `YYYYMMDD` folder name as UTC; age = UTC-today − date in epoch-days.
- `T1-02` **Force UTC regardless of VM TZ:** set container TZ to a non-UTC zone, assert age is unchanged (uses `date -u`, not local).
- `T1-03` Boundary: a partition exactly at midnight UTC rolls over by one whole day, never a fraction.
- `T1-04` Leap day (`20240229`) and year boundary (`20251231`→`20260101`) parse correctly.
- `T1-05` Malformed/non-date folder name → skipped with a warning, never treated as age 0 or deleted.

### Eligibility (**Migration Eligibility Threshold**)
- `T1-06` Real dir on NAS1 with age > max-short-term → eligible.
- `T1-07` Already a **symlink** → NOT eligible (no re-processing of a migrated partition).
- `T1-08` Hot partition (age ≤ max-short-term) → NOT eligible.

### Fit check & watermark (**Fit Check**, **High/Low Watermark**, **NAS2 Reserve**)
- `T1-09` `size ≤ NAS2_free − reserve` → migrate.
- `T1-10` `size > NAS2_free − reserve` → do NOT migrate; emit fit-check alert; stop loop.
- `T1-11` Boundary `size == NAS2_free − reserve` → defined behavior (migrate), asserted explicitly.
- `T1-12` Loop starts only when NAS1 > HIGH; stops as soon as NAS1 < LOW.
- `T1-13` Ordering is oldest-eligible first.
- `T1-14` Running `NAS2_free` is decremented per migration so the loop respects the reserve without re-stat each time.

### Availability guard (**NAS2 Availability Guard**)
- `T1-15` Sentinel present → `check_nas2` passes.
- `T1-16` Sentinel absent → `check_nas2` exits non-zero **before any write**.
- `T1-17` After a failed guard, assert **nothing** was written under `NAS2_ROOT` (no local-shadow growth).

### Swap (**Safe-Move Sequence**)
- `T1-18` `mv D → .D.bak` then `ln -s` yields a **relative** symlink whose target resolves to the NAS2 dir.
- `T1-19` Post-swap, `D` is a symlink, `.D.bak` exists, NAS2 copy is intact.

### Reconciliation — idempotency at each crash point (**Crash-During-Copy Safety**, reconciliation rules)
- `T1-20` `.bak` present + `D` missing + NAS2 copy **complete/verified** → roll forward (create symlink, reap `.bak`).
- `T1-21` `.bak` present + `D` missing + NAS2 copy **incomplete** → roll back (`mv .bak → D`, drop partial NAS2).
- `T1-22` `.bak` present + `D` is symlink → finish interrupted cleanup (reap `.bak`).
- `T1-23` `.bak` present + `D` is real dir (anomaly) → alert, take no destructive action.
- `T1-24` Empty `.bak` dir → `rmdir`’d by sweep.
- `T1-25` Running reconciliation twice in a row is a no-op the second time (idempotent).

### Purge — the irreversible op (**Purge Job**, **Long-term Retention (varies)**, **Purge Cutoff Bias**)
- `T1-26` Per-category delete through symlink: catX (70d) at age 71 deleted on NAS2; catY (90d) survives.
- `T1-27` Date-level cleanup (remove symlink + empty NAS2 dir) ONLY once partition fully drained at max retention.
- `T1-28` Cutoff bias: age == retention → **kept**; age > retention → deleted (strict `>`).
- `T1-29` Non-migrated (real dir) partition purged directly on NAS1.
- `T1-30` **Dry-run mode logs "would delete X" and deletes NOTHING** (must pass before purge is armed).
- `T1-31` File-id purge glob (`rm -f root/*/<cat>/<cat><id>*`) resolves through the date symlink to the real file on NAS2.
- `T1-32` Short-term (4d) and file-id (1–2d) purges operate only on hot NAS1 real dirs — unchanged behavior, no symlink interaction.

### Concurrency (**Shared Migration/Purge Lock**)
- `T1-33` Second instance cannot acquire the local `flock` while first holds it.
- `T1-34` Migration and purge mutually exclude via the same lock.
- `T1-35` `flock -w <timeout>` gives up rather than blocking forever.

### Metrics (**observability**)
- `T1-36` `.prom` file written atomically (`.tmp` → `mv`); a concurrent reader never sees a partial file.
- `T1-37` `*_last_success_timestamp_seconds` advances only on a successful run.
- `T1-38` `sftp_migration_nas2_fit_check_failed` set to 1 exactly when `T1-10` fires.

### Backfill gating (**Load-Gated Backfill**)
- `T1-39` `active_sessions > threshold` → batch skipped (logged), retried next interval.
- `T1-40` `rsync --bwlimit` flag is passed through with the configured value.

---

## Tier 2 — semantics (real NFS)

> Requires real NFS mounts matching prod `vers=`/options. Local-fs/overlayfs is
> NOT acceptable here — it gives false confidence (**Test Tiers**).

- `T2-01` **HEADLINE — in-flight download survives swap + delete.** Start a large SFTP `GET` of a file in partition D; mid-stream, run the full swap + **immediate** `rm -rf .D.bak`. Assert the `GET` completes and SHA-256 matches the original. *(Validates inode-bound fd + **NFS silly-rename**; this is the central "no corruption" guarantee.)*
- `T2-02` Crash mid-`rsync` (kill -9) → NAS1/D intact, downloads unaffected; next run resumes rsync to completion (**Crash-During-Copy Safety**).
- `T2-03` Crash in the `mv`→`ln` gap → path briefly missing; fast reconciliation timer restores it within the interval; any in-flight `GET` still completes.
- `T2-04` Immediate delete with an open reader → `.nfsXXXX` appears, reader unaffected, dir reaped by reconciliation after the reader closes.
- `T2-05` **Permission parity (positive):** after migration, download as a real downstream user → succeeds (**Permission Parity**).
- `T2-06` **Permission parity (negative):** stage NAS2 with a wrong GID/idmap → verify gate fails / download denied → proves the gate + canary catch it.
- `T2-07` ENOSPC: fill NAS2 mid-copy → rsync fails, NAS1/D intact, partial NAS2 copy dropped, fit/alert raised.
- `T2-08` **Stale mount:** induce a stale NFS handle on `/mnt/nas2` → sentinel read fails (ESTALE) → guard trips, no writes (**NAS2 Availability Guard**).
- `T2-09` **Unmounted NAS2:** unmount `/mnt/nas2`, run script → guard exits; assert the local root filesystem did **not** grow (NFS-shadow footgun closed — **Mountpoint/Availability Guard**).
- `T2-10` **Reboot survival:** migrate a partition, reboot the VM, assert the symlink still resolves and a download succeeds (mount persistence is infra-owned, but this validates the outcome).

---

## Canary — production (one partition)

- `C-01` Select the oldest, lowest-traffic eligible partition; migrate it via the full pipeline.
- `C-02` Verify the symlink resolves and a download **as a real downstream user** checksums correctly.
- `C-03` Observe ~1 day: download-error metrics flat, NAS1 usage dropped, NAS2 usage rose by the partition size, no client reports.
- `C-04` Re-run the `T2-01`-style in-flight test against the canary partition on the CentOS-8 prod client (closes the Ubuntu-vs-CentOS kernel gap).
- `C-05` Only after `C-01..04` clean → enable nightly batched backfill with `--bwlimit` + load gate.

---

## Exit criteria

1. All Tier-1 green in CI.
2. `T2-01` (headline), `T2-05/06` (perms), `T2-08/09` (guard), `T2-10` (reboot) all green on real NFS.
3. Purge dry-run (`T1-30`) output reviewed by a human and matches expectation before the purge is armed in prod.
4. Canary `C-01..04` clean and observed for the agreed window.

> No-backup reminder: purge deletes are irreversible. `T1-26..32` + the `T1-30`
> dry-run are the only safety net before data is permanently removed — treat
> them as blocking, not advisory.
