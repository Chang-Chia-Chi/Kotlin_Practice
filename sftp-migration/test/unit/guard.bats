#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh guard.sh; }
teardown() { teardown_roots; }

@test "T1-15: sentinel present -> check_nas2 passes" {
  sentinel on
  run check_nas2
  [ "$status" -eq 0 ]
}

@test "T1-16: sentinel absent -> check_nas2 fails non-zero" {
  sentinel off
  run check_nas2
  [ "$status" -ne 0 ]
}

@test "empty sentinel still passes — presence is the signal, not content" {
  : > "$NAS2_SENTINEL"
  run check_nas2
  [ "$status" -eq 0 ]
}
