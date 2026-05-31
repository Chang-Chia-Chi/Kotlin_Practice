# shellcheck shell=bash
# Live-load signal: count established connections to port 22. Overridable in
# tests via ACTIVE_SESSIONS_OVERRIDE. Robust to zero matches (|| true so
# grep -c's exit-1-on-no-match doesn't surface as a hard error).
active_sessions() {
  local n
  if [ -n "${ACTIVE_SESSIONS_OVERRIDE:-}" ]; then
    echo "$ACTIVE_SESSIONS_OVERRIDE"
    return
  fi
  n="$(ss -tn state established '( sport = :22 )' 2>/dev/null | grep -c ':22' || true)"
  echo "${n:-0}"
}

# backfill_should_yield: true (0) when load is too high to migrate right now.
# Phase 6's migrate_run breaks out of the drain loop on a true return.
backfill_should_yield() {
  [ "$(active_sessions)" -gt "$MAX_ACTIVE_SESSIONS" ]
}
