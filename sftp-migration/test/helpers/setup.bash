# shellcheck shell=bash
_LIB_DIR="${BATS_TEST_DIRNAME}/../../lib"

setup_roots() {
  TEST_TMP="$(mktemp -d)"
  export TEST_TMP
  export NAS1_ROOT="$TEST_TMP/nas1"
  export NAS2_ROOT="$TEST_TMP/nas2"
  export NAS2_SENTINEL="$NAS2_ROOT/.nas2_sentinel"
  export LOCK_FILE="$TEST_TMP/lock"
  export METRICS_FILE="$TEST_TMP/metrics.prom"
  mkdir -p "$NAS1_ROOT" "$NAS2_ROOT"
}

teardown_roots() { rm -rf "$TEST_TMP"; }

# Source config then any named libs (after env overrides are set).
load_lib() {
  # shellcheck source=/dev/null
  source "$_LIB_DIR/config.sh"
  local m
  for m in "$@"; do
    # shellcheck source=/dev/null
    source "$_LIB_DIR/$m"
  done
}
