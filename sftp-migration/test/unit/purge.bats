#!/usr/bin/env bats
# Boolean predicates called directly; `run` only when $output/$lines needed.
load ../helpers/setup

setup() {
  setup_roots
  load_lib log.sh dates.sh purge.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
  export PURGE_DRY_RUN=0
  export LONGTERM_RETENTIONS="catX:70 catY:90"
}
teardown() { teardown_roots; }

# Migrated partition: data on NAS2, symlink on NAS1.
migrate_fixture() {
  local date c
  date="$1"; shift
  mkdir -p "$NAS2_ROOT/$date"
  for c in "$@"; do
    mkdir -p "$NAS2_ROOT/$date/$c"
    echo data > "$NAS2_ROOT/$date/$c/${c}0001file"
  done
  ln -s ".nas2/$date" "$NAS1_ROOT/$date"
}

@test "T1-26: per-category purge through symlink at differing retentions" {
  migrate_fixture 20260321 catX catY     # age 72: catX(70) deleted, catY(90) kept
  purge_run
  [ ! -e "$NAS2_ROOT/20260321/catX" ]
  [ -e "$NAS2_ROOT/20260321/catY" ]
  [ -L "$NAS1_ROOT/20260321" ]
}

@test "T1-27: fully-drained partition cleaned up (symlink + NAS2 dir removed)" {
  migrate_fixture 20260301 catX catY     # age 92: both deleted -> empty
  purge_run
  [ ! -e "$NAS1_ROOT/20260301" ]
  [ ! -e "$NAS2_ROOT/20260301" ]
}

@test "T1-28: cutoff is strict (age==retention kept, age>retention deleted)" {
  migrate_fixture 20260323 catX          # age 70 == ret -> keep
  migrate_fixture 20260322 catX          # age 71 >  ret -> delete
  purge_run
  [ -e "$NAS2_ROOT/20260323/catX" ]
  [ ! -e "$NAS2_ROOT/20260322/catX" ]
}

@test "T1-29: non-migrated (real dir) partition purged on NAS1" {
  mkdir -p "$NAS1_ROOT/20260301/catX"
  echo d > "$NAS1_ROOT/20260301/catX/catX0001file"
  purge_run
  [ ! -e "$NAS1_ROOT/20260301/catX" ]
}

@test "T1-30: dry-run deletes nothing and logs intent" {
  export PURGE_DRY_RUN=1
  migrate_fixture 20260301 catX catY
  run purge_run
  [ "$status" -eq 0 ]
  [ -e "$NAS2_ROOT/20260301/catX" ]
  [ -e "$NAS2_ROOT/20260301/catY" ]
  echo "$output" | grep -q "DRY-RUN would delete"
}
