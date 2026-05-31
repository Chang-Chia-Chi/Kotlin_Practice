# shellcheck shell=bash
# Stateless crash recovery: infer interrupted migrations from the filesystem and
# repair them. Idempotent; safe to run at the start of every migration run and
# on a fast standalone timer. Repairs the only dangerous window (between
# mv-aside and ln -s) and finishes interrupted .bak cleanup.
#
# All `local`s split (bash same-statement RHS-reads-outer quirk).
reconcile() {
  local bak date path
  shopt -s nullglob dotglob
  for bak in "$NAS1_ROOT"/.*.bak; do
    [ -d "$bak" ] || continue
    date="$(basename "$bak")"
    date="${date#.}"
    date="${date%.bak}"
    path="$NAS1_ROOT/$date"
    if [ -L "$path" ]; then
      # Symlink already created -> interrupted cleanup; finish it.
      # (A lingering .nfsXXXX may keep .bak around; the next run removes
      # it once empty — make this rm non-fatal so the reconcile sweep
      # doesn't crash on an in-flight reader.)
      rm -rf "$bak" 2>/dev/null \
        || warn "reconcile: $bak not fully removed (likely .nfsXXXX held open); will retry next run"
      log "reconcile: finished cleanup for $date"
    elif [ -e "$path" ]; then
      warn "reconcile: anomaly for $date (real dir and .bak both present)"
    else
      # Path missing -> crashed mid-swap. Roll forward if NAS2 verified, else back.
      if verify_copy "$bak" "$NAS2_ROOT/$date"; then
        ln -s "${SYMLINK_REL_PREFIX}${date}" "$path"
        rm -rf "$bak" 2>/dev/null \
          || warn "reconcile: $bak not fully removed (likely .nfsXXXX held open); will retry next run"
        log "reconcile: rolled forward $date"
      else
        mv "$bak" "$path"
        rm -rf "${NAS2_ROOT:?}/$date"
        warn "reconcile: rolled back $date (NAS2 copy incomplete)"
      fi
    fi
  done
  shopt -u nullglob dotglob
}
