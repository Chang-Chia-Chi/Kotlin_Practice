#!/usr/bin/env bats
# Tier-2 NFS semantics tests. Skipped unless RUN_NFS_TESTS=1 AND NAS1_ROOT is
# on real NFS (validated via `stat -f`). Run on a CentOS-8 (or Ubuntu) VM with
# the actual NFS mounts wired up — these don't run in CI and can't run off
# /mnt/c (overlayfs gives false confidence per the test-plan.md "Test Tiers").
load ../helpers/setup

require_nfs() {
  [ "${RUN_NFS_TESTS:-0}" = "1" ] || skip "set RUN_NFS_TESTS=1 on an NFS-backed host"
  case "$(stat -f -c %T "$NAS1_ROOT" 2>/dev/null)" in
    nfs*) : ;;
    *) skip "NAS1_ROOT is not on NFS" ;;
  esac
}

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh capacity.sh move.sh reconcile.sh
  sentinel on
}
teardown() { teardown_roots; }

@test "T2-01: open reader survives swap + immediate delete (silly-rename)" {
  require_nfs
  make_partition 20260101 catX 1048576
  local f sum_before captured sum_after
  f="$NAS1_ROOT/20260101/catX/catX0001file"
  sum_before="$(sha256sum "$f" | awk '{print $1}')"
  exec 3< "$f"                       # hold the inode open BEFORE the swap
  migrate_partition 20260101         # rsync -> verify -> swap -> rm -rf .bak
  captured="$TEST_TMP/captured"
  cat <&3 > "$captured"              # finish reading via the original fd
  exec 3<&-
  sum_after="$(sha256sum "$captured" | awk '{print $1}')"
  [ "$sum_before" = "$sum_after" ]
}
