#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh metrics.sh; }
teardown() { teardown_roots; }

@test "T1-36: metric_emit writes atomically with no leftover temp file" {
  metric_emit
  [ -f "$METRICS_FILE" ]
  run bash -c "ls ${METRICS_FILE}.tmp.* 2>/dev/null"
  [ "$status" -ne 0 ]
  grep -q "sftp_nas_free_bytes" "$METRICS_FILE"
  grep -q "sftp_migration_nas2_fit_check_failed 0" "$METRICS_FILE"
}

@test "T1-37: last-success timestamp is emitted when set" {
  _M_LAST_SUCCESS=1748736000 metric_emit
  grep -q "sftp_migration_last_success_timestamp_seconds 1748736000" "$METRICS_FILE"
}

@test "T1-38: fit-check metric reflects the failure flag" {
  _M_FIT_CHECK_FAILED=1 metric_emit
  grep -q "sftp_migration_nas2_fit_check_failed 1" "$METRICS_FILE"
}

@test "metric_emit emits mount_healthy=1 when df succeeds" {
  metric_emit
  grep -qE 'sftp_nas_mount_healthy\{mountpoint="[^"]*nas1"\} 1' "$METRICS_FILE"
  grep -qE 'sftp_nas_mount_healthy\{mountpoint="[^"]*nas2"\} 1' "$METRICS_FILE"
}

@test "metric_emit emits mount_healthy=0 and free=NaN when df fails" {
  # Stub the df-backed reader to fail for NAS1. NAS2 still succeeds.
  nas_free_bytes() {
    case "$1" in
      "$NAS1_ROOT") return 1 ;;
      *) command df -PB1 "$1" | awk 'NR==2 { print $4 }' ;;
    esac
  }
  metric_emit
  grep -qE 'sftp_nas_mount_healthy\{mountpoint="[^"]*nas1"\} 0' "$METRICS_FILE"
  grep -qE 'sftp_nas_free_bytes\{mountpoint="[^"]*nas1"\} NaN' "$METRICS_FILE"
  # NAS2 still reads as healthy.
  grep -qE 'sftp_nas_mount_healthy\{mountpoint="[^"]*nas2"\} 1' "$METRICS_FILE"
}
