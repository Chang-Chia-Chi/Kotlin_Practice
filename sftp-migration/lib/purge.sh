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
#
# CRITICAL SAFETY: refuses to follow a symlink whose target doesn't resolve
# under $NAS2_ROOT. Without this check, a compromised producer (or a sloppy
# admin) that can write into $NAS1_ROOT could create a symlink to any path
# (e.g., /etc), and the next purge cycle would run `rm -rf /etc/<cat>` —
# permanent damage outside the NAS roots under no-backup.
resolve_partition_data_dir() {
  local date path real
  date="$1"
  path="$NAS1_ROOT/$date"
  if [ -L "$path" ]; then
    real="$(readlink -f "$path")" || return 1
    case "$real/" in
      "$NAS2_ROOT"/*) echo "$real" ;;
      *) warn "resolve_partition_data_dir: $path resolves to $real (outside NAS2_ROOT); refusing"
         return 1 ;;
    esac
  else
    echo "$path"
  fi
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
  local resolved
  resolved="$(resolve_partition_data_dir "$date")" || return 0   # refused -> skip
  target="$resolved/$cat"
  [ -e "$target" ] || return 0                       # already gone
  if [ "$PURGE_DRY_RUN" = "1" ]; then
    log "DRY-RUN would delete $target (age=$age > ret=$ret)"
  elif rm -rf "$target"; then
    log "purged $target (age=$age > ret=$ret)"
  else
    warn "purge_category: rm -rf $target returned non-zero; will retry next cycle"
  fi
}

# cleanup_partition <date>: once fully drained, remove the date symlink + empty
# NAS2 dir (migrated) or the empty NAS1 dir (non-migrated). Honors PURGE_DRY_RUN.
#
# Uses rmdir's "fails iff non-empty" semantic as the atomic empty-check, then
# only removes the symlink on success. Otherwise: warn loudly and leave the
# symlink intact so the next purge cycle retries — never orphan the NAS2 dir
# (the previous "rmdir 2>/dev/null then unconditional rm -f symlink" pattern
# would have orphaned it on ENOTEMPTY from a lingering .nfsXXXX).
cleanup_partition() {
  local date path target
  date="$1"
  path="$NAS1_ROOT/$date"
  if [ -L "$path" ]; then
    target="$(resolve_partition_data_dir "$date")" || return 0   # refused -> skip
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would attempt to remove $target and symlink $path (if empty)"
    elif rmdir "$target" 2>/dev/null; then
      rm -f "$path"
      log "cleaned up migrated partition $date"
    else
      # Non-empty -> either categories still present (next cycle handles it)
      # or a .nfsXXXX from an in-flight reader. Either way: leave the symlink
      # intact so retry semantics are preserved; never orphan NAS2.
      warn "cleanup_partition: $target not empty (likely .nfsXXXX or still-purging categories); symlink retained for retry"
    fi
  elif [ -d "$path" ]; then
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would rmdir $path (if empty)"
    elif rmdir "$path" 2>/dev/null; then
      log "cleaned up partition $date"
    else
      warn "cleanup_partition: $path not empty; will retry next cycle"
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

# purge_file_id <date> <cat> <id>: delete files by id. The date symlink is an
# INTERMEDIATE path component, so the glob follows it to the real file on NAS2
# (works identically on a non-migrated real dir). This mirrors the existing
# file-id purge — proves it needs no change post-migration.
#
# Callee-side input validation: under no-backup, a destructive helper cannot
# trust its caller. An empty $id would expand the glob to `${cat}*` and wipe
# the whole category; an empty $cat would wipe everything under the date; a
# $date containing `..` would traverse out of NAS1_ROOT entirely. Each arg
# is therefore regex-gated before the rm is allowed to run, and `--` ends
# option-parsing so an id beginning with `-` can't be interpreted as a flag.
purge_file_id() {
  local date cat id resolved
  date="$1"
  cat="$2"
  id="$3"
  parse_partition_epoch_days "$date" >/dev/null \
    || { warn "purge_file_id: invalid date '$date'"; return 1; }
  [[ "$cat" =~ ^[A-Za-z0-9_-]+$ ]] \
    || { warn "purge_file_id: invalid category '$cat'"; return 1; }
  [[ "$id" =~ ^[A-Za-z0-9]+$ ]] \
    || { warn "purge_file_id: invalid id '$id'"; return 1; }
  # Route through resolve_partition_data_dir so a symlink pointing OUTSIDE
  # NAS2_ROOT is refused — same hardening as purge_category.
  resolved="$(resolve_partition_data_dir "$date")" \
    || { warn "purge_file_id: refused — symlink target outside NAS2_ROOT"; return 1; }
  rm -f -- "$resolved/$cat/${cat}${id}"*
}
