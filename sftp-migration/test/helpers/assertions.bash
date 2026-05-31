# shellcheck shell=bash
# No date partition dirs were created under NAS2 (used to prove the guard
# blocked writes when NAS2 is "unmounted").
assert_no_local_shadow_growth() {
  local found
  found="$(find "$NAS2_ROOT" -mindepth 1 -maxdepth 1 -type d ! -name '.*' | wc -l)"
  [ "$found" -eq 0 ]
}
