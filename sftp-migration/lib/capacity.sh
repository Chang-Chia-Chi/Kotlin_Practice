# shellcheck shell=bash
# Usage is measured with df PER MOUNT — never du over the tree, because the
# date symlinks and the .nas2 bind mount would make du miscount.

nas_used_pct()   { df -P  "$1" | awk 'NR==2 { gsub(/%/,"",$5); print $5 }'; }
nas_free_bytes() { df -PB1 "$1" | awk 'NR==2 { print $4 }'; }
dir_size_bytes() { du -sb "$1" | awk '{ print $1 }'; }

# fits_on_nas2 <size_bytes>: 0 if the partition fits while preserving the reserve.
fits_on_nas2() {
  local size free
  size="$1"
  free="$(nas_free_bytes "$NAS2_ROOT")"
  [ "$size" -le $(( free - NAS2_RESERVE_BYTES )) ]
}
