# shellcheck shell=bash
# check_nas2: confirm NAS2 is genuinely mounted and reachable by reading a
# sentinel file that only exists on the real NAS2. If NAS2 is unmounted, the
# sentinel is absent (writes would otherwise hit the local shadow dir); if the
# mount is stale, the read fails with ESTALE. Either way we refuse to proceed.
check_nas2() {
  if head -c1 "$NAS2_SENTINEL" >/dev/null 2>&1; then
    return 0
  fi
  warn "NAS2 not available (sentinel unreadable: $NAS2_SENTINEL)"
  return 1
}
