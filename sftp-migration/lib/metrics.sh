# shellcheck shell=bash
# Prometheus textfile-collector emission. Written atomically (.tmp -> mv) so
# the scraper never reads a partial file. predict_linear() over
# sftp_nas_free_bytes in Prometheus drives the NAS2-full procurement forecast;
# the fit_check_failed gauge and the last_success_timestamp gauge drive
# the acute-alert path (NAS2 too full to fit, or job not running at all).
metric_emit() {
  local tmp free1 free2 healthy1 healthy2
  tmp="${METRICS_FILE}.tmp.$$"
  # On df failure, emit free_bytes=NaN (predict_linear ignores NaN data points,
  # so the procurement-forecast alert does NOT fire on a transient hang) AND
  # a separate sftp_nas_mount_healthy=0 gauge that operators alert on directly.
  # The previous "free=0 on failure" caused predict_linear to see an instant
  # cliff and page ops at 3am about "NAS full" when the mount was just laggy.
  if free1="$(nas_free_bytes "$NAS1_ROOT")"; then healthy1=1; else healthy1=0; free1=NaN; fi
  if free2="$(nas_free_bytes "$NAS2_ROOT")"; then healthy2=1; else healthy2=0; free2=NaN; fi
  {
    printf '# TYPE sftp_nas_free_bytes gauge\n'
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS1_ROOT" "$free1"
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS2_ROOT" "$free2"
    printf '# TYPE sftp_nas_mount_healthy gauge\n'
    printf 'sftp_nas_mount_healthy{mountpoint="%s"} %s\n' "$NAS1_ROOT" "$healthy1"
    printf 'sftp_nas_mount_healthy{mountpoint="%s"} %s\n' "$NAS2_ROOT" "$healthy2"
    printf '# TYPE sftp_migration_nas2_fit_check_failed gauge\n'
    printf 'sftp_migration_nas2_fit_check_failed %s\n' "${_M_FIT_CHECK_FAILED:-0}"
    printf '# TYPE sftp_migration_last_success_timestamp_seconds gauge\n'
    printf 'sftp_migration_last_success_timestamp_seconds %s\n' "${_M_LAST_SUCCESS:-0}"
  } > "$tmp"
  mv -f "$tmp" "$METRICS_FILE"
}
