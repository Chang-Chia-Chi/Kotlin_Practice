# shellcheck shell=bash
# All date math is UTC. Tests set NOW_OVERRIDE (epoch seconds) for determinism;
# production leaves it unset and uses the real UTC clock.

now_epoch() { echo "${NOW_OVERRIDE:-$(date -u +%s)}"; }

# parse_partition_epoch_days <YYYYMMDD> -> epoch-day count (UTC), or return 1
# for a name that is not a valid calendar date.
parse_partition_epoch_days() {
  local name="$1" secs
  [[ "$name" =~ ^[0-9]{8}$ ]] || return 1
  secs="$(date -u -d "${name:0:4}-${name:4:2}-${name:6:2}" +%s 2>/dev/null)" || return 1
  echo $(( secs / 86400 ))
}

today_epoch_days() { echo $(( $(now_epoch) / 86400 )); }

# partition_age_days <YYYYMMDD> -> whole UTC days old, or return 1 if invalid.
partition_age_days() {
  local pday
  pday="$(parse_partition_epoch_days "$1")" || return 1
  echo $(( $(today_epoch_days) - pday ))
}
