# sftp-migration

Migrates aged long-term SFTP date-partitions from NAS1 to NAS2 via per-date
symlinks, transparently to downstream. Rewrites the long-term purge to be
UTC- and symlink-aware. Bash + bats-core.

Design: `docs/sftp-migration/` (CONTEXT.md, adr/, discovery.md, test-plan.md).
Implementation plan: `docs/superpowers/plans/2026-05-28-sftp-migration-*.md`.
**Modifying the code?** Read `MAINTAINERS.md` first — file map + the rules
each came from a real bug we caught.

## Schedule (hand to infra)

- **Migration:** hourly — `bin/sftp-migrate`. Owns `check_nas2`, reconcile,
  and the watermark drain loop. Always emits metrics on every exit path.
- **Purge:** daily — `bin/sftp-purge`. **Keep `PURGE_DRY_RUN=1` until the
  dry-run output has been reviewed** (no backup; deletes are irreversible).
- **Reconciliation:** `bin/sftp-migrate` already calls reconcile first.
  Running the migration entrypoint on a 1–2 min standalone timer (while
  the hourly drain runs separately) bounds the dangerous mv→ln gap exposure
  to that interval — both invocations share `LOCK_FILE`, so they never race.

## Config (env, see lib/config.sh for defaults)

| Var | Default | Purpose |
|---|---|---|
| `NAS1_ROOT` | `/mnt/nas1` | SFTP root mount point |
| `NAS2_ROOT` | `/mnt/nas2` | Second NAS mount |
| `NAS2_SENTINEL` | `${NAS1_ROOT}/.nas2/.nas2_sentinel` | Read THROUGH the bind mount (catches bind-drop) |
| `SYMLINK_REL_PREFIX` | `.nas2/` | Relative target for date symlinks (chroot-safe) |
| `LOCK_FILE` | `/run/sftp-migration.lock` | Local-disk flock, shared by migrate+purge |
| `METRICS_FILE` | `/var/lib/node_exporter/textfile_collector/sftp_migration.prom` | Atomic textfile target |
| `HIGH_WATERMARK` / `LOW_WATERMARK` | 80 / 70 | NAS1 percent-used; drain when >HIGH, stop when <LOW |
| `NAS2_RESERVE_BYTES` | 1 TiB | Fit check refuses partitions that would push NAS2 below this |
| `MIN_MIGRATE_AGE_DAYS` | 5 | Age `>` this is eligible (strict; short-term purged) |
| `RSYNC_BWLIMIT` | 51200 (KB/s) | Bandwidth cap for backfill |
| `MAX_ACTIVE_SESSIONS` | 20 | Backfill yields when active SFTP sessions exceed this |
| `LONGTERM_RETENTIONS` | `catX:70 catY:90` | Space-separated `category:days` |
| `PURGE_DRY_RUN` | **1** | Set to `0` ONLY after reviewing dry-run output |

## Infra-owned prerequisites

- `/mnt/nas2` mount in `/etc/fstab` with `_netdev` and proper ordering.
- `/mnt/nas2 → /mnt/nas1/.nas2` **bind mount** in fstab with
  `x-systemd.requires-mount-for=/mnt/nas2`. Without this, every migrated
  partition's symlink would resolve into an empty local dir on NAS1.
- Sentinel file placed at `/mnt/nas2/.nas2_sentinel` (any non-empty content).
  Reachable at `/mnt/nas1/.nas2/.nas2_sentinel` via the bind mount.
- `node_exporter` textfile collector configured to read `METRICS_FILE`.
- Cron entries:
  ```
  0 * * * *   /opt/sftp-migration/bin/sftp-migrate
  10 2 * * *  PURGE_DRY_RUN=1 /opt/sftp-migration/bin/sftp-purge
  ```

## Testing

```bash
bats test/unit/                                    # Tier-1 logic, ~75 tests
RUN_NFS_TESTS=1 bats test/nfs/semantics.bats       # Tier-2 on a real NFS host
```

Bats 1.2.1+. The repo lives on a Windows host during development; tests run
in WSL Ubuntu. See `docs/superpowers/plans/2026-05-28-sftp-migration-index.md`
for the dev-environment notes.

## Operator workflow for arming the purge

1. Deploy with `PURGE_DRY_RUN=1` (the default).
2. Watch the first 2–3 daily purge runs in the log. Each line begins with
   `DRY-RUN would delete …` or `DRY-RUN would attempt to remove … (if empty)`.
3. Sanity-check: dates match what you expect to be at-or-past their retention.
4. Flip `PURGE_DRY_RUN=0` in the cron env. From the next run on, deletes are real.
