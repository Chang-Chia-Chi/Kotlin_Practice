#!/usr/bin/env bats
# Boolean predicates are called directly (without `run`) where exit code is
# all that's asserted; use `run` only when $output / $lines is needed.
load ../helpers/setup
load ../helpers/assertions

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh capacity.sh move.sh
  sentinel on
}
teardown() { teardown_roots; }

@test "verify gate passes for an identical copy" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  verify_partition 20260101
}

@test "verify gate fails when the NAS2 copy is tampered" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  echo tampered >> "$NAS2_ROOT/20260101/catX/catX0001file"
  ! verify_partition 20260101
}

@test "verify gate fails on permission mismatch" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  chmod 600 "$NAS2_ROOT/20260101/catX/catX0001file"
  ! verify_partition 20260101
}

@test "T1-18/19: swap replaces dir with a resolving relative symlink" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  swap_to_symlink 20260101
  [ -L "$NAS1_ROOT/20260101" ]
  [ -f "$NAS1_ROOT/.20260101.bak/catX/catX0001file" ]
  [ -f "$NAS1_ROOT/20260101/catX/catX0001file" ]
}

@test "migrate_partition happy path: resolving symlink, no .bak, content intact" {
  make_partition 20260101 catX 2048
  local before; before="$(sha256sum "$NAS1_ROOT/20260101/catX/catX0001file" | awk '{print $1}')"
  migrate_partition 20260101
  [ -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
  local after; after="$(sha256sum "$NAS1_ROOT/20260101/catX/catX0001file" | awk '{print $1}')"
  [ "$before" = "$after" ]
}

@test "T1-17: guard failure aborts migrate with no NAS2 write" {
  make_partition 20260101 catX 2048
  sentinel off
  ! migrate_partition 20260101
  assert_no_local_shadow_growth
}

@test "C1: verify_copy fails when rsync invocation itself fails" {
  make_partition 20260101 catX 64
  rsync_partition 20260101              # genuine copy first
  rsync() { return 99; }                # stub: rsync binary "fails" with non-zero
  ! verify_partition 20260101
}

@test "C2: migrate_partition aborts (no swap, no .bak) when rsync fails" {
  make_partition 20260101 catX 64
  rsync() { return 30; }                # ENOSPC-class failure
  ! migrate_partition 20260101
  [ ! -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
  [ -d "$NAS1_ROOT/20260101" ]          # source dir untouched
}

@test "I3: migrate_partition is a no-op when path is already a symlink" {
  make_partition 20260101 catX 64
  migrate_partition 20260101            # first migration
  [ -L "$NAS1_ROOT/20260101" ]
  migrate_partition 20260101            # re-run: no-op
  [ -L "$NAS1_ROOT/20260101" ]
}

@test "I3: migrate_partition refuses when .<date>.bak exists from prior crash" {
  make_partition 20260101 catX 64
  mkdir -p "$NAS1_ROOT/.20260101.bak"   # simulate crash leftover
  ! migrate_partition 20260101
  [ -d "$NAS1_ROOT/20260101" ]          # real dir still in place
}
