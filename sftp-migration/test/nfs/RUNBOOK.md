# Tier-2 NFS Runbook

Run on a CentOS-8 (or Ubuntu) VM with `NAS1_ROOT`/`NAS2_ROOT` on real NFS
matching prod `vers=`/options. T2-01 is automated in `semantics.bats`; the
rest are manual / semi-automated below. Validates the NFS-specific safety
properties that local/overlayfs Tier-1 cannot validate.

## Prerequisite

```bash
export NAS1_ROOT=/mnt/nas1
export NAS2_ROOT=/mnt/nas2
export NAS2_SENTINEL=$NAS1_ROOT/.nas2/.nas2_sentinel
export METRICS_FILE=/tmp/sftp_migration.prom
export LOCK_FILE=/tmp/sftp-migration.lock
RUN_NFS_TESTS=1 bats test/nfs/semantics.bats
```

## Manual cases

- **T2-02 crash mid-rsync:** start `bin/sftp-migrate`; `kill -9` it during
  copy. Assert `NAS1/<date>` intact (no `.bak` yet), downstream downloads
  unaffected; re-running resumes via `--partial` and completes.
- **T2-03 crash in mv→ln gap:** add a `sleep 5` between `mv` and `ln -s` in
  a test build of `swap_to_symlink`; kill in the gap; observe path missing;
  run `reconcile`; assert path restored (rolled forward if NAS2 verified,
  rolled back otherwise).
- **T2-04 immediate-delete with open reader:** as T2-01 but also verify a
  `.nfsXXXX` appears under `.bak` while the fd is held, and the next
  reconcile sweep removes the dir after the reader exits.
- **T2-05 perm parity (positive):** migrate; `sftp` as a real downstream
  user; download succeeds.
- **T2-06 perm parity (negative):** stage NAS2 with a wrong GID (`chown :other`);
  `verify_partition` must fail; the canary's downstream-user download must
  return permission-denied.
- **T2-07 ENOSPC:** fill NAS2 to within < partition size; run migrate;
  assert `rsync` fails, `NAS1/<date>` intact, partial `NAS2/<date>` dropped
  (by next reconcile rollback), fit-check metric==1.
- **T2-08 stale handle:** induce a stale NFS handle on `/mnt/nas2`; assert
  `check_nas2` trips (sentinel read returns ESTALE).
- **T2-09 unmounted NAS2:** `umount /mnt/nas2`; run `bin/sftp-migrate`;
  assert exit non-zero and the local OS root filesystem did NOT grow
  (`df` before/after) — proves the NFS-shadow footgun is closed.
- **T2-09b bind-mount dropped:** with `/mnt/nas2` still mounted, `umount
  /mnt/nas1/.nas2`; assert `check_nas2` trips (sentinel-via-bind-mount-path
  unreadable). Catches the failure mode the direct-path check would miss.
- **T2-10 reboot survival:** migrate one partition; `reboot` the VM; assert
  the symlink still resolves (mounts come up via fstab + ordering) and a
  download succeeds.

## Acceptance gate

All cases above must be green on a CentOS-8 client (matching prod kernel/
nfs-utils) before the canary partition migrates in prod.
