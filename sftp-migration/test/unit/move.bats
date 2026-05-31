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
