# SFTP Migration — Glossary

## Terms

### SFTP Service
The server-side process that accepts SFTP connections from downstream clients. Most likely OpenSSH `sshd` with an `sftp` subsystem configured. Runs on a CentOS-8 VM hosted on VMware.

### NAS (Network Attached Storage)
A network filesystem mounted on the CentOS-8 VM. Currently one NAS mount is used as the root path for all files. A second NAS mount is being added due to capacity limits.

### Root Path / Mounting Path
The directory that serves as the top-level visible to downstream clients when they connect via SFTP. All date-partitioned folders live directly under this path. **Confirmed: the SFTP root = the NAS1 mount point** (client path `/<date>` maps to `NAS1/<date>`).

### Chroot/Symlink Design Fork (pending discovery)
NAS2 is a separate mount point, so symlink reachability depends on chroot:
- **No chroot** → NAS2 at `/mnt/nas2` (invisible to clients), **absolute** symlinks. Cleanest.
- **Chroot into NAS1 root** → NAS2 must be bind-mounted *under* the root (`/mnt/nas1/.nas2`), **relative** symlinks (`<date> → .nas2/<date>`). NAS2 subtree becomes visible (dot-hidden).
- **Universal option** (avoids blocking on discovery): NAS2-under-root + relative symlinks works in BOTH cases; cost is a dot-hidden `.nas2` entry in the tree.
Check: `grep -A20 -E 'Match|ChrootDirectory' /etc/ssh/sshd_config`.

### Date Partition
A subdirectory under the root path named by date (e.g., `20260527/`). Files for downstream download are organized into these folders. Within each date partition, files are further organized into **Category** subfolders.

### Category
A subfolder under a date partition (e.g., `root/20260527/<category>/file`). Category determines retention: some categories are **long-term** (70-day retention), others are **short-term** (4-day retention). The purge cronjob decides retention by category.

### Long-term Category
Files retained 70 days. Volume ≈ 200G/day.

### Short-term Category
Files retained 4 days. Volume ≈ 2T/day.

### Shrinking Partition (key property)
Because short-term categories are purged after 4 days, a date partition older than 4 days contains **only** long-term categories — roughly 200G instead of ~2.2T. Old partitions are therefore small and stable (no further writes/purges until day 70). They are the natural migration units.

### Migration Unit (decided)
Whole date partition. One symlink per date: `NAS1/<date>` becomes a symlink → `NAS2/<date>`. Chosen over per-category symlinks for simplicity — fewer links, simpler purge, no partition split across mounts mid-life.

### Unification Mechanism (decided — see ADR 0001)
Symlink-per-date, not a union mount. The microsecond ENOENT flip window is accepted because downstream retries. mergerfs (union mount) noted as the alternative that eliminates the window, at the cost of a FUSE layer + critical-path daemon.

### Safe-Move Sequence
The procedure that moves a partition without corrupting in-flight downloads: `rsync -a` NAS1→NAS2 → Verify Gate (checksum + permission parity) → atomic-ish swap (`mv` dir aside, then `ln -s`) → **immediate `rm -rf` of the set-aside copy** (NFS silly-rename protects in-flight same-host readers) → reconciliation sweep removes any `.bak` dir left non-empty by a lingering `.nfsXXXX` file. Relies on open fds being inode-bound and NFS silly-rename. **CIFS fallback:** if the NAS is CIFS (no silly-rename), revert to lsof-gated / defer-and-retry deletion instead of immediate delete.

### High / Low Watermark
Fixed percentage constants (NOT self-modifying) that bound NAS1 usage. Migration starts when NAS1 usage exceeds the High Watermark and stops once it drops below the Low Watermark. Adaptivity to volume growth comes from reading *live* usage each run, not from changing the thresholds.

### NAS2 Reserve
An absolute amount of free space always kept on NAS2 (e.g., ≥1T). The per-partition **Fit Check** (`partition_size > NAS2_free − NAS2_Reserve` ⇒ do not move, alert) uses it. Replaces a separate NAS2 percentage cap — one knob for NAS2 safety.

### Migration Run
A single, lock-protected (single-instance) execution of the migration job. The lock ensures the Fit Check's free-space reading is not invalidated by a concurrent run.

### Shared Migration/Purge Lock
Migration and the purge job must hold the **same** lock so they never operate on the filesystem concurrently. Prevents a purge from deleting a category out of a partition while migration is mid-copy of that partition. **Implementation: local-disk `flock` (e.g., `/run/sftp-migration.lock`), NOT a NAS-based lockfile** — `flock` over NFS is unreliable, and both jobs run on the same VM so a local lock gives perfect mutual exclusion. Use `flock -w <timeout>` + `rsync --timeout` so a hung job doesn't hold the lock forever (which would silently stop retention). A stuck-lock condition pages.

### Verify Gate
A mandatory `rsync -an --checksum` comparison between NAS1 and NAS2 that must show byte-identical before the symlink swap proceeds. Guarantees a crash-during-copy (which leaves a partial NAS2 copy) can never result in swapping in incomplete data — the source is deleted only after this gate passes.

### Crash-During-Copy Safety
Because NAS1/D is read-only source during rsync and is only deleted after the Verify Gate, a crash mid-copy leaves the complete NAS1 dir intact and serving all downloads. Recovery: reconciliation resumes rsync (or drops the partial NAS2 copy on ENOSPC). Safest place to crash.

### Test Tiers
Tier 1 (logic) = sftp testcontainer, fast/CI, validates script correctness. Tier 2 (semantics) = real NFS, validates in-flight-download-survives-delete. A container/VM on local fs (ext4/overlayfs) gives FALSE confidence for Tier 2 — must use real NFS.

### NFS Client-Side Safety (de-risks testing)
Silly-rename (open file surviving unlink) is performed by the **NFS client kernel**, not the NAS appliance. So Tier-2 fidelity comes from matching the *client*: CentOS-8 + same NFS version + same mount options. The NFS server can be a test export on the real NAS (Option A) or a self-hosted NFS server (Option B) — **never the prod VM**. Prod data is never used for testing.

### Canary Rollout (not a test)
First prod exposure = one oldest/low-traffic partition: migrate, verify symlink + checksummed download, observe ~1 day, then widen. Controlled rollout, distinct from Tier-1/Tier-2 testing.

### Rollback Reality (capacity-constrained)
NAS1 is full, so a large backfill **cannot be bulk-reversed** — there's no room to move it all back. The primary safety mechanism is therefore *limiting blast radius up front* (canary → small batches → verify between), not reversal. Single-partition reversal is possible; mass reversal is not. Most "bad migration" cases are a broken symlink or perms mismatch (fix the mount/symlink, don't move data).

### NAS2 Availability Dependency (new SPOF)
After migration, every migrated partition's download depends on NAS2 being mounted and healthy. Pre-migration only NAS1 mattered; now both are load-bearing. NAS2 outage → dangling symlinks → cold-data download failures (hot/recent data on NAS1 unaffected). Mitigation: NAS2 mount-health metric (sentinel-file read each scrape) wired to Alertmanager; NAS2 should carry NAS1-equivalent reliability.

### No Backup (confirmed) — raises purge-correctness bar
NAS data is **not** backed up/DR-replicated. Infra team handles *hardware* health and disk swaps (redundancy), and owns NAS2 mount-health monitoring. But hardware redundancy ≠ backup: it does NOT protect against the software deleting the wrong data. Migration's delete is safe (verified NAS2 copy kept until after swap). The **purge `rm -rf` is the one irreversible operation** — an over-deletion bug is unrecoverable. Therefore: (1) purge gets a **dry-run/log-only first phase** before being armed; (2) date-parsing gets exhaustive Tier-1 tests (boundary/malformed/timezone).

### NAS2 Availability Guard (sentinel-file, script safety)
Before any write/swap, the migration script must confirm NAS2 is available by **reading a sentinel file THROUGH the `.nas2` bind-mount path** (`head -c1 /mnt/nas1/.nas2/.nas2_sentinel || exit`). Reading via the bind-mount path — the same path symlinks resolve through — catches *three* failure modes in one syscall: (1) NAS2 itself unmounted, (2) `.nas2` bind mount dropped while `/mnt/nas2` stays mounted (every migrated symlink would silently resolve into an empty local dir on NAS1 — defeating the migration), (3) stale NFS handle (read fails with ESTALE). Reading `/mnt/nas2/.nas2_sentinel` *directly* would miss case (2) — a critical hole. The sentinel file is placed by infra on NAS2; the bind mount makes it reachable at both paths, but only the via-root path validates the integrity of the symlink resolution chain. Trivially testable — `touch`/remove the sentinel or remove the `.nas2` link to simulate each failure mode.

### Script Testability Structure
Migration logic is written as composable functions (`check_nas2`, `migrate_partition`, `verify`, `swap`, `purge_partition`, `reconcile`) with `NAS1_ROOT`/`NAS2_ROOT` parameterized via env/config. Tier-1 tests point the roots at local temp dirs and unit-test each function in isolation; only `check_nas2` touches the sentinel guard.

### Initial Backfill vs Steady State
Backfill = the one-time drain of the existing backlog (NAS1 at 80–84% → below low watermark); moves many TB. Steady state = hourly job moving ~1 aged-out partition/day (negligible load). Backfill needs extra controls steady state doesn't: bandwidth cap + pacing.

### Load-Gated Backfill
Instead of hard-coding an off-peak window (traffic pattern unknown), the backfill yields to live load: `rsync --bwlimit` (default ~50 MB/s, safe on 1GbE) + skip-batch-if-active-SFTP-sessions-above-threshold. Discovers quiet periods at runtime. Same "adapt to live readings" principle as the watermarks.

**Working assumptions (validate, non-blocking):** worst-case network = 1GbE (~110 MB/s); traffic = business-hours peak, overnight quiet.

### Purge Job
The existing cronjob that enforces retention: deletes short-term categories at 4 days and long-term categories at 70 days. **Currently uses `rm -rf` + `mtime`, which is incompatible with migration** (see Purge Incompatibility). Must be rewritten to select by folder-name date and to resolve-target-then-unlink for symlinked partitions.

### Purge Incompatibility (blocker)
The current mtime+`rm -rf` purge breaks under migration two ways: (1) a migrated partition's symlink carries a fresh mtime, so `find -mtime +70` won't purge it on time (it counts from migration, not the data's date); (2) `find -P` (default) does not descend into symlinked directories, so it never reaches the long-term files on NAS2 — and `rm -rf` on the symlink itself deletes only the link, orphaning the data. Net effect: NAS2 fills and never drains, defeating the migration. **Prerequisite:** purge must be rewritten and tested BEFORE migration is enabled in production.

### File-ID Purge
A purge rule that deletes individual files by id (files named `<category><id><filename>`). These files live only **1–2 days**, so they are always inside the hot window (real dir on NAS1) and are deleted long before migration eligibility (>4 days). **Never interacts with migration or symlinks — needs no change.**

### Purge / Migration Interaction (resolved)
Of all purge rules, **only the long-term 70-day category purge** ever meets a symlinked (migrated) partition. The file-id purge (1–2 day files) and short-term category purge (4-day) operate exclusively on hot NAS1 real dirs. Therefore the purge rewrite scope is **just the long-term purge**, not all purges.

### Age Computation (UTC, confirmed)
Partition folder dates are in **UTC**. Both migration eligibility and purge must compute "today" with `date -u` (epoch-day comparison), **explicitly UTC — never the VM's local TZ** (the CentOS VM may be set to local time; a naive `date +%Y%m%d` would reintroduce a midnight-boundary off-by-one against UTC folder names). Eliminates the TZ/DST bug class.

### Purge Cutoff Bias (no-backup safety)
Purge only when `age > retention` *strictly*, biasing toward keeping. Given no backup, keeping a partition an extra day is free; deleting one a day early is unrecoverable. A retention+1 safety margin is used during the dry-run/early phase.

### Migration Eligibility Threshold
A partition becomes migration-eligible only when it is a **real directory on NAS1** (not already a symlink), **not currently held by the producer/purge** (shared lock), and age > the *maximum short-term retention* (≥5 days) — guaranteeing it contains **only long-term data** when moved. The not-a-symlink check prevents re-processing an already-migrated partition.

### Long-term Retention (varies by category)
Long-term categories have **different** retentions (e.g., catX=70d, catY=90d), not a single 70-day value. Consequence: the long-term purge is two-phase — (1) delete each category at its own age through the symlink; (2) remove the date symlink + empty NAS2 dir only once the partition is fully drained at its max-retention age.

### ADR-0001 Revisit Trigger (resolved — does not apply)
Investigated: the multiple purge mechanisms do NOT all interact with migration (only the long-term purge does). The fragility concern that would have favored mergerfs is therefore minimal. Combined with uncertainty over whether mergerfs is installable on the corporate VM, decision A (symlink-per-date) stands.

### Downstream Client
Any system or user that connects to the SFTP service to download files. Clients navigate by date-partition paths. They are unaware of which physical NAS holds a given partition.

### Chroot Jail
An OS-level restriction that confines SFTP users to a specific directory. If configured, clients cannot navigate above the chroot root, and symlinks pointing outside the jail are broken.

### SFTP Access Model (confirmed)
**Per-user logins** (not a single service account). Multiple downstream users authenticate individually and read a shared date-partitioned tree. Consequence: a UID/GID/idmap mismatch on NAS2 breaks downloads for **all** users at once, not one account. Per-user SFTP commonly implies chroot → reinforces the universal design (NAS2-under-root + relative symlinks).

### Permission Parity (prerequisite)
NAS2 must grant the SFTP users the same access as NAS1: same UID/GID mapping, same NFSv4 idmap domain, same squash policy, and the same group-read GID for the common downstream group. Enforced by (1) a stat-based uid/gid/mode parity check in the Verify Gate, and (2) a canary download performed as a real downstream user. If NAS2 is a new volume on the *same* appliance, parity is inherited; if a different appliance, it must be actively aligned and verified.

### Database Handshake
The coordination mechanism between the upload side and downstream clients. Clients query a database using filename and upload time to discover the SFTP path where a file can be downloaded. This is the authoritative routing layer — clients do not scan directories.

### File Location Record
A database record that maps a filename + upload time to a downloadable location. The exact fields stored (full SFTP path vs. relative path vs. just filename) are TBD — see open question below.

## Confirmed Facts

- Symlinks work on this VM — already validated by a symlink pointing to an unrelated NAS mount for execution files.
- **Database handshake is pattern B**: clients construct the SFTP path themselves from filename + upload date (`/{date}/{category}/{filename}`). The DB does NOT store a physical path. → Both NAS mounts MUST appear as one unified tree under the SFTP root. No client change is acceptable.
- NAS1 is 90% full and must be relieved by physically moving files to NAS2.
- Two NAS mounts: NAS1 = 25T, NAS2 = 10T.

## Capacity Math (steady state)

| Class | Rate | Retention | Volume |
|-------|------|-----------|--------|
| Short-term | 2T/day | 4 days | 8T |
| Long-term | 200G/day | 70 days | 14T |
| **Total** | | | **22T** (≈90% of NAS1) |

**Constraint:** NAS2 (10T) cannot hold all 14T of long-term data. It holds at most ~50 days of long-term-only partitions (50 × 200G = 10T). The remaining ~20 days of long-term + the 4-day hot window stay on NAS1.

**Growth note:** 200G/day and 2T/day are *current peaks* and will rise. So the number of long-term days NAS2 can hold will shrink over time. This argues for a **free-space-adaptive** migration trigger rather than a fixed age threshold — the mechanism must rebalance as per-day volume grows.

## Open Questions

- Migration unit: whole date partition vs. per-category subfolder?
- Migration trigger: fixed age threshold vs. adaptive on free space?
- Is a chroot jail configured? (`grep -A20 Match /etc/ssh/sshd_config`) — affects whether symlinks cross into NAS2 safely.
