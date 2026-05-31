# shellcheck shell=bash
# with_lock <timeout_secs> <command...>: run the command holding an exclusive
# lock on LOCK_FILE (local disk — NFS flock is unreliable, and migration+purge
# both run on this VM so a local lock is authoritative). Returns non-zero if the
# lock can't be acquired within the timeout (so a hung peer doesn't block forever).
with_lock() {
  local timeout
  local lockfd
  local rc
  timeout="$1"
  shift
  if ! exec {lockfd}> "$LOCK_FILE"; then
    warn "could not open lock file $LOCK_FILE (path inaccessible?)"
    return 1
  fi
  if ! flock -w "$timeout" "$lockfd"; then
    warn "could not acquire lock $LOCK_FILE within ${timeout}s"
    exec {lockfd}>&-
    return 1
  fi
  "$@"
  rc=$?
  # Closing the fd releases the flock (kernel drops on last close); no
  # explicit `flock -u` needed.
  exec {lockfd}>&-
  return "$rc"
}
