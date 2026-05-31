# shellcheck shell=bash
# Stateless crash recovery: infer interrupted migrations from the filesystem and
# repair them. Idempotent; safe to run at the start of every migration run and
# on a fast standalone timer. Repairs the only dangerous window (between
# mv-aside and ln -s) and finishes interrupted .bak cleanup.
#
# Concurrency: designed for at-most-one invocation at a time, but degrade-safe
# under accidental concurrent calls (each rm is idempotent and the failing
# loser just warns). Phase 6 entrypoints wrap reconcile in `with_lock` so this
# stays a non-issue. If you ever call reconcile() outside the shared lock,
# expect noisy warnings, not corruption.
#
# All `local`s split (bash same-statement RHS-reads-outer quirk).
# Body runs in a subshell so `shopt nullglob` is scoped — caller shell state
# is never mutated. (dotglob would be unnecessary because the literal leading
# `.` in the glob pattern already matches dotfiles regardless.)
reconcile() (
  local bak date path
  shopt -s nullglob
  for bak in "$NAS1_ROOT"/.*.bak; do
    [ -d "$bak" ] || continue
    date="$(basename "$bak")"
    date="${date#.}"
    date="${date%.bak}"
    # Validate that what we parsed is actually a calendar date before any
    # destructive op on derived paths. Without this, a stray `.x.bak` would
    # run `rm -rf $NAS2_ROOT/x` (under no-backup, permanent damage).
    parse_partition_epoch_days "$date" >/dev/null \
      || { warn "reconcile: ignoring non-partition .bak '$bak'"; continue; }
    path="$NAS1_ROOT/$date"
    if [ -L "$path" ]; then
      # Symlink already created -> interrupted cleanup; finish it.
      # (A lingering .nfsXXXX may keep .bak around; the next run removes
      # it once empty — make this rm non-fatal so the reconcile sweep
      # doesn't crash on an in-flight reader.)
      if rm -rf "$bak" 2>/dev/null; then
        log "reconcile: finished cleanup for $date"
      else
        warn "reconcile: $bak not fully removed (likely .nfsXXXX held open); will retry next run"
      fi
    elif [ -e "$path" ]; then
      warn "reconcile: anomaly for $date (real dir and .bak both present)"
    else
      # Path missing -> crashed mid-swap. Roll forward if NAS2 verified, else back.
      if verify_copy "$bak" "$NAS2_ROOT/$date"; then
        ln -s "${SYMLINK_REL_PREFIX}${date}" "$path"
        if rm -rf "$bak" 2>/dev/null; then
          log "reconcile: rolled forward $date"
        else
          warn "reconcile: $bak not fully removed (likely .nfsXXXX held open); will retry next run"
        fi
      else
        mv "$bak" "$path"
        rm -rf "${NAS2_ROOT:?}/$date"
        warn "reconcile: rolled back $date (NAS2 copy incomplete)"
      fi
    fi
  done
)
