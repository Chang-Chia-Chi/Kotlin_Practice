#!/usr/bin/env bats
load ../helpers/setup

setup() {
  setup_roots
  load_lib dates.sh eligibility.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
}
teardown() { teardown_roots; }

@test "T1-06: real dir older than min age is eligible" {
  make_partition 20260101 catX 1024
  is_eligible 20260101
}

@test "T1-07: already-migrated symlink is NOT eligible" {
  mkdir -p "$NAS2_ROOT/20260101"
  ln -s "$NAS2_ROOT/20260101" "$NAS1_ROOT/20260101"
  ! is_eligible 20260101
}

@test "T1-08: hot partition within min age is NOT eligible" {
  make_partition 20260530 catX 1024
  ! is_eligible 20260530
}

@test "T1-13: eligible list is oldest-first" {
  make_partition 20260101 catX 1024
  make_partition 20260115 catX 1024
  make_partition 20260110 catX 1024
  run list_eligible_oldest_first
  [ "${lines[0]}" = "20260101" ]
  [ "${lines[1]}" = "20260110" ]
  [ "${lines[2]}" = "20260115" ]
}
