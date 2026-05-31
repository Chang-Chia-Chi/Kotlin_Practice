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
# match ownership/mode of src. Non-zero on any mismatch.
verify_copy() {
  local src dst diff rel s d
  src="$1"
  dst="$2"
  # -i prints an itemized line per differing file; without it -an emits nothing
  # and a content mismatch silently slips through.
  diff="$(rsync -ani --checksum --delete "$src/" "$dst/" 2>/dev/null)"
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
