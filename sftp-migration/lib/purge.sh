# shellcheck shell=bash
# The IRREVERSIBLE side of the system. Deletes are unrecoverable under
# no-backup — every operation here is gated by:
#  - strict `age > retention` (age==retention is KEPT; bias toward keeping)
#  - unknown categories are NEVER purged (fail-safe)
#  - PURGE_DRY_RUN=1 logs would-delete and deletes nothing (first prod cycles
#    run in dry-run; only armed after the log is reviewed)
#
# All `local`s split (bash same-statement RHS-reads-outer quirk).

# category_retention <cat> -> retention days, or return 1 if unknown.
category_retention() {
  local cat pair
  cat="$1"
  for pair in $LONGTERM_RETENTIONS; do
    [ "${pair%%:*}" = "$cat" ] && { echo "${pair##*:}"; return 0; }
  done
  return 1
}

# resolve_partition_data_dir <date> -> the real data dir, following the symlink
# when migrated (so the irreversible rm actually reclaims NAS2 space).
resolve_partition_data_dir() {
  local date path
  date="$1"
  path="$NAS1_ROOT/$date"
  if [ -L "$path" ]; then readlink -f "$path"; else echo "$path"; fi
}

# purge_category <date> <cat>: delete <cat> once age > its retention (strict >,
# biasing toward keeping). Resolves through the date symlink so migrated data
# on NAS2 is actually reclaimed. Honors PURGE_DRY_RUN.
purge_category() {
  local date cat age ret target
  date="$1"
  cat="$2"
  age="$(partition_age_days "$date")" || return 0    # invalid name -> skip safely
  ret="$(category_retention "$cat")"  || return 0    # unknown cat -> NEVER purge
  [ "$age" -gt "$ret" ] || return 0                  # not old enough -> keep
  target="$(resolve_partition_data_dir "$date")/$cat"
  [ -e "$target" ] || return 0                       # already gone
  if [ "$PURGE_DRY_RUN" = "1" ]; then
    log "DRY-RUN would delete $target (age=$age > ret=$ret)"
  else
    rm -rf "$target"
    log "purged $target (age=$age > ret=$ret)"
  fi
}

# cleanup_partition <date>: once fully drained, remove the date symlink + empty
# NAS2 dir (migrated) or the empty NAS1 dir (non-migrated). Honors PURGE_DRY_RUN.
cleanup_partition() {
  local date path target
  date="$1"
  path="$NAS1_ROOT/$date"
  if [ -L "$path" ]; then
    target="$(readlink -f "$path")"
    [ -n "$(ls -A "$target" 2>/dev/null)" ] && return 0
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would remove symlink $path and empty $target"
    else
      rmdir "$target" 2>/dev/null
      rm -f "$path"
      log "cleaned up migrated partition $date"
    fi
  elif [ -d "$path" ]; then
    [ -n "$(ls -A "$path" 2>/dev/null)" ] && return 0
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would rmdir $path"
    else
      rmdir "$path"
      log "cleaned up partition $date"
    fi
  fi
}

# purge_run: long-term purge across all partitions — per-category at each
# category's own retention, then clean up drained partitions.
# Body in a subshell so `shopt nullglob` doesn't leak to caller.
purge_run() (
  local entry date pair cat
  shopt -s nullglob
  for entry in "$NAS1_ROOT"/*; do
    date="$(basename "$entry")"
    parse_partition_epoch_days "$date" >/dev/null || continue
    for pair in $LONGTERM_RETENTIONS; do
      cat="${pair%%:*}"
      purge_category "$date" "$cat"
    done
    cleanup_partition "$date"
  done
)
