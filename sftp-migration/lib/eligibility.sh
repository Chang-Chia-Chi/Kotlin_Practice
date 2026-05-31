# shellcheck shell=bash
# A partition is migration-eligible iff it is a REAL directory on NAS1 (not a
# symlink — i.e. not already migrated), has a valid UTC date name, and is older
# than MIN_MIGRATE_AGE_DAYS (so its short-term categories are already purged).
is_eligible() {
  local name path age
  name="$1"
  path="$NAS1_ROOT/$name"
  [ -d "$path" ] || return 1
  [ -L "$path" ] && return 1
  age="$(partition_age_days "$name")" || return 1
  [ "$age" -gt "$MIN_MIGRATE_AGE_DAYS" ]
}

# Print eligible partition names oldest-first. The glob skips dotfiles, so the
# .nas2 bind mount and .<date>.bak set-aside dirs are never considered.
list_eligible_oldest_first() {
  local entry name
  for entry in "$NAS1_ROOT"/*; do
    [ -e "$entry" ] || continue
    name="$(basename "$entry")"
    is_eligible "$name" && echo "$name"
  done | sort
}
