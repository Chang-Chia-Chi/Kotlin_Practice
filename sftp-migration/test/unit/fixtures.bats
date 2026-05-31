#!/usr/bin/env bats
load ../helpers/setup
load ../helpers/assertions

setup()    { setup_roots; }
teardown() { teardown_roots; }

@test "make_partition creates a checksummable file on NAS1" {
  make_partition 20260101 catX 2048
  [ -f "$NAS1_ROOT/20260101/catX/catX0001file" ]
  [ "$(stat -c%s "$NAS1_ROOT/20260101/catX/catX0001file")" -eq 2048 ]
}

@test "sentinel on/off toggles the NAS2 sentinel file" {
  sentinel on
  [ -f "$NAS2_SENTINEL" ]
  sentinel off
  [ ! -f "$NAS2_SENTINEL" ]
}

@test "assert_no_local_shadow_growth passes when NAS2 has no date dirs" {
  run assert_no_local_shadow_growth
  [ "$status" -eq 0 ]
}
