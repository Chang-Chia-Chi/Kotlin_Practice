# shellcheck shell=bash
# Usage is measured with df PER MOUNT — never du over the tree, because the
# date symlinks and the .nas2 bind mount would make du miscount.
#
# Each helper validates that the awk pipeline produced a non-empty integer.
# A failing df/du (stale mount, ENOENT, permission denied) otherwise leaks an
# empty value into arithmetic — Phase 6 watermark/fit-check would silently
# compare against bogus numbers.

_is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

nas_used_pct() {
  local n
  n="$(df -P "$1" 2>/dev/null | awk 'NR==2 { gsub(/%/,"",$5); print $5 }')"
  _is_uint "$n" || { warn "nas_used_pct: invalid df output for $1"; return 1; }
  echo "$n"
}

nas_free_bytes() {
  local n
  n="$(df -PB1 "$1" 2>/dev/null | awk 'NR==2 { print $4 }')"
  _is_uint "$n" || { warn "nas_free_bytes: invalid df output for $1"; return 1; }
  echo "$n"
}

dir_size_bytes() {
  local n
  n="$(du -sb "$1" 2>/dev/null | awk '{ print $1 }')"
  _is_uint "$n" || { warn "dir_size_bytes: invalid du output for $1"; return 1; }
  echo "$n"
}

# fits_on_nas2 <size_bytes>: 0 if the partition fits while preserving the reserve.
# Propagates failure if the underlying free-bytes read couldn't be obtained.
fits_on_nas2() {
  local size free
  size="$1"
  free="$(nas_free_bytes "$NAS2_ROOT")" || return 1
  [ "$size" -le $(( free - NAS2_RESERVE_BYTES )) ]
}
