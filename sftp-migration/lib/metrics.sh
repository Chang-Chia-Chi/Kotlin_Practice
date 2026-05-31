# shellcheck shell=bash
# Prometheus textfile-collector emission. Written atomically (.tmp -> mv) so
# the scraper never reads a partial file. predict_linear() over
# sftp_nas_free_bytes in Prometheus drives the NAS2-full procurement forecast;
# the fit_check_failed gauge and the last_success_timestamp gauge drive
# the acute-alert path (NAS2 too full to fit, or job not running at all).
metric_emit() {
  local tmp free1 free2
  tmp="${METRICS_FILE}.tmp.$$"
  # nas_free_bytes can fail (df error); default to 0 to keep the textfile
  # well-formed. Prometheus will see the drop and alert via predict_linear.
  free1="$(nas_free_bytes "$NAS1_ROOT")" || free1=0
  free2="$(nas_free_bytes "$NAS2_ROOT")" || free2=0
  {
    printf '# TYPE sftp_nas_free_bytes gauge\n'
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS1_ROOT" "$free1"
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS2_ROOT" "$free2"
    printf '# TYPE sftp_migration_nas2_fit_check_failed gauge\n'
    printf 'sftp_migration_nas2_fit_check_failed %s\n' "${_M_FIT_CHECK_FAILED:-0}"
    printf '# TYPE sftp_migration_last_success_timestamp_seconds gauge\n'
    printf 'sftp_migration_last_success_timestamp_seconds %s\n' "${_M_LAST_SUCCESS:-0}"
  } > "$tmp"
  mv -f "$tmp" "$METRICS_FILE"
}
