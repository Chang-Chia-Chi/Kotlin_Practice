# shellcheck shell=bash
_LIB_DIR="${BATS_TEST_DIRNAME}/../../lib"

setup_roots() {
  TEST_TMP="$(mktemp -d)"
  export TEST_TMP
  export NAS1_ROOT="$TEST_TMP/nas1"
  export NAS2_ROOT="$TEST_TMP/nas2"
  # Sentinel is read THROUGH the .nas2 bind mount; this matches the prod
  # config.sh default and means removing the .nas2 link in a test simulates
  # a bind-mount drop and trips the guard.
  export NAS2_SENTINEL="$NAS1_ROOT/.nas2/.nas2_sentinel"
  export LOCK_FILE="$TEST_TMP/lock"
  export METRICS_FILE="$TEST_TMP/metrics.prom"
  mkdir -p "$NAS1_ROOT" "$NAS2_ROOT"
  # Emulate the prod bind mount so relative symlinks `.nas2/<date>` resolve.
  ln -s "$NAS2_ROOT" "$NAS1_ROOT/.nas2"
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

# sentinel on|off — simulate NAS2 available/unavailable.
sentinel() {
  case "$1" in
    on)  printf 'ok' > "$NAS2_SENTINEL" ;;
    off) rm -f "$NAS2_SENTINEL" ;;
  esac
}

# make_partition <date> <category> [bytes] [root]
# Deterministic content so checksums are stable across copies.
make_partition() {
  local date="$1" cat="$2" bytes="${3:-1024}" root="${4:-$NAS1_ROOT}"
  mkdir -p "$root/$date/$cat"
  head -c "$bytes" /dev/zero | tr '\0' 'x' > "$root/$date/$cat/${cat}0001file"
}
