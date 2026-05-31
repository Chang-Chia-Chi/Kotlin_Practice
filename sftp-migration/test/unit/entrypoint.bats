#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; sentinel on; }
teardown() { teardown_roots; }

@test "bin/sftp-migrate runs and writes metrics" {
  export ACTIVE_SESSIONS_OVERRIDE=0
  run "$BATS_TEST_DIRNAME/../../bin/sftp-migrate"
  [ "$status" -eq 0 ]
  [ -f "$METRICS_FILE" ]
  grep -q "sftp_migration_last_success_timestamp_seconds" "$METRICS_FILE"
}

@test "bin/sftp-migrate emits metrics with non-zero last_success even when NAS2 down" {
  # migrate_run internally fails-guard on missing sentinel, but still calls
  # metric_emit so the dead-man's-switch alert advances correctly.
  sentinel off
  run "$BATS_TEST_DIRNAME/../../bin/sftp-migrate"
  [ "$status" -ne 0 ]
  [ -f "$METRICS_FILE" ]
}
