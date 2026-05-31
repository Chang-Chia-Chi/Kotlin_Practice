#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh move.sh backfill.sh; }
teardown() { teardown_roots; }

@test "T1-39: backfill yields when sessions exceed threshold" {
  export MAX_ACTIVE_SESSIONS=5
  ACTIVE_SESSIONS_OVERRIDE=10 backfill_should_yield
  ! ACTIVE_SESSIONS_OVERRIDE=2 backfill_should_yield
}

@test "T1-40: rsync_partition passes --bwlimit when configured" {
  rsync() { printf '%s\n' "$*" > "$TEST_TMP/rsync_args"; }
  export RSYNC_BWLIMIT=12345
  make_partition 20260101 catX 64
  rsync_partition 20260101
  grep -q -- "--bwlimit=12345" "$TEST_TMP/rsync_args"
}
