# shellcheck shell=bash

# rsync_partition <date>: copy NAS1/<date> -> NAS2/<date>, preserving metadata,
# resumable (--partial), bandwidth-capped. Source is read-only; safe to re-run.
#
# `local` declarations are split so the RHS never reads outer-scope $date.
rsync_partition() {
  local date src dst opts
  date="$1"
  src="$NAS1_ROOT/$date"
  dst="$NAS2_ROOT/$date"
  opts=(-a --delete --partial)
  [ -n "$RSYNC_BWLIMIT" ] && opts+=(--bwlimit="$RSYNC_BWLIMIT")
  mkdir -p "$dst"
  rsync "${opts[@]}" "$src/" "$dst/"
}

# verify_copy <src_dir> <dst_dir>: dst must be byte-identical (checksum) AND
# match ownership/mode of src. Non-zero on any mismatch — INCLUDING a failure
# to actually run rsync (binary missing, OOM, NFS hang). Empty stdout from
# rsync only means "identical" when rc==0; otherwise it's silent failure.
verify_copy() {
  local src dst diff rc rel s d
  src="$1"
  dst="$2"
  # -i prints an itemized line per differing file; without it -an emits nothing
  # and a content mismatch silently slips through.
  diff="$(rsync -ani --checksum --delete "$src/" "$dst/" 2>/dev/null)"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    warn "verify: rsync invocation failed (rc=$rc) for $src vs $dst"
    return 1
  fi
  if [ -n "$diff" ]; then
    warn "verify: content mismatch ($src vs $dst)"
    return 1
  fi
  while IFS= read -r -d '' s; do
    rel="${s#"$src"/}"
    d="$dst/$rel"
    [ -e "$d" ] || { warn "verify: missing $rel"; return 1; }
    if [ "$(stat -c '%u:%g:%a' "$s")" != "$(stat -c '%u:%g:%a' "$d")" ]; then
      warn "verify: permission mismatch for $rel"
      return 1
    fi
  done < <(find "$src" -mindepth 1 -print0)
  return 0
}

# verify_partition <date>: verify the NAS1 source against the NAS2 copy.
verify_partition() { verify_copy "$NAS1_ROOT/$1" "$NAS2_ROOT/$1"; }

# swap_to_symlink <date>: set the real dir aside as .<date>.bak (so in-flight,
# inode-bound download fds keep reading), then create a RELATIVE symlink that
# resolves through .nas2/ to the NAS2 copy. Relative target works inside chroot.
# Defensive guards refuse to proceed on already-migrated paths or leftover .bak
# (reconciliation territory) so the swap never enters an undefined state.
swap_to_symlink() {
  local date path bak
  date="$1"
  path="$NAS1_ROOT/$date"
  bak="$NAS1_ROOT/.$date.bak"
  if [ -L "$path" ]; then
    warn "swap: $path is already a symlink; skipping (already migrated)"
    return 0
  fi
  if [ -e "$bak" ]; then
    warn "swap: $bak already exists; reconcile required"
    return 1
  fi
  mv "$path" "$bak"
  ln -s "${SYMLINK_REL_PREFIX}${date}" "$path"
}

# migrate_partition <date>: full safe move for one partition.
# guard -> rsync -> verify gate -> swap -> immediate delete of .bak.
# On NFS, immediate rm is safe (silly-rename preserves any open reader); the
# reconciliation sweep (Phase 4) cleans any .bak left non-empty by a .nfsXXXX.
#
# Idempotency: a no-op when the partition is already a symlink (re-run safe).
# A leftover .<date>.bak signals a prior crash and forces reconcile-first.
migrate_partition() {
  local date path bak
  date="$1"
  path="$NAS1_ROOT/$date"
  bak="$NAS1_ROOT/.$date.bak"
  if [ -L "$path" ]; then
    log "migrate: $date already migrated; skipping"
    return 0
  fi
  if [ -e "$bak" ]; then
    warn "migrate: $bak exists from prior crash; reconcile required"
    return 1
  fi
  check_nas2 || return 1
  rsync_partition "$date"  || { warn "rsync failed for $date";  return 1; }
  verify_partition "$date" || { warn "verify failed for $date; not swapping"; return 1; }
  swap_to_symlink "$date"
  # rm -rf may fail with ENOTEMPTY if NFS silly-renamed an in-flight file
  # into .bak (.nfsXXXX). That's the expected NFS-safe path: the migration
  # itself has succeeded (symlink in place, NAS2 copy verified); the reconcile
  # sweep (Phase 4) removes the lingering dir once the open fd closes.
  # Treat the rm failure as non-fatal and log loudly so it isn't misread as
  # a real problem.
  rm -rf "$bak" 2>/dev/null \
    || warn "migrate: $bak not fully removed (likely .nfsXXXX held open); reconcile will sweep"
  log "migrated $date"
}
