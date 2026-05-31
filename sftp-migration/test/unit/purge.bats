#!/usr/bin/env bats
# Boolean predicates called directly; `run` only when $output/$lines needed.
load ../helpers/setup

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh purge.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
  export PURGE_DRY_RUN=0
  export LONGTERM_RETENTIONS="catX:70 catY:90"
  sentinel on   # purge_run now re-checks per iteration; tests that need
                # NAS2-unavailable behavior explicitly call `sentinel off`.
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

@test "T1-31: file-id purge globs through the date symlink to NAS2" {
  migrate_fixture 20260401 catShort
  : > "$NAS2_ROOT/20260401/catShort/catShort0042report"
  purge_file_id 20260401 catShort 0042
  [ ! -e "$NAS2_ROOT/20260401/catShort/catShort0042report" ]
}

@test "T1-32: file-id purge works on a non-migrated real dir too" {
  mkdir -p "$NAS1_ROOT/20260530/catShort"
  : > "$NAS1_ROOT/20260530/catShort/catShort0042report"
  purge_file_id 20260530 catShort 0042
  [ ! -e "$NAS1_ROOT/20260530/catShort/catShort0042report" ]
}

@test "bin/sftp-purge runs (dry-run) without deleting" {
  export PURGE_DRY_RUN=1
  sentinel on
  migrate_fixture 20260301 catX catY
  run "$BATS_TEST_DIRNAME/../../bin/sftp-purge"
  [ "$status" -eq 0 ]
  [ -e "$NAS2_ROOT/20260301/catX" ]
  [ -e "$NAS2_ROOT/20260301/catY" ]
}

@test "C1: purge_file_id rejects empty id and deletes nothing" {
  migrate_fixture 20260401 catShort
  : > "$NAS2_ROOT/20260401/catShort/catShort0042report"
  ! purge_file_id 20260401 catShort ""
  [ -e "$NAS2_ROOT/20260401/catShort/catShort0042report" ]   # untouched
}

@test "C1: purge_file_id rejects empty category and deletes nothing" {
  migrate_fixture 20260401 catShort
  : > "$NAS2_ROOT/20260401/catShort/catShort0042report"
  ! purge_file_id 20260401 "" 0042
  [ -e "$NAS2_ROOT/20260401/catShort/catShort0042report" ]
}

@test "C1: purge_file_id rejects invalid date and deletes nothing" {
  mkdir -p "$NAS1_ROOT/20260401/catShort"
  : > "$NAS1_ROOT/20260401/catShort/catShort0042report"
  ! purge_file_id "../etc" catShort 0042
  [ -e "$NAS1_ROOT/20260401/catShort/catShort0042report" ]
}

@test "C1: purge_file_id rejects id with path-traversal characters" {
  mkdir -p "$NAS1_ROOT/20260401/catShort"
  : > "$NAS1_ROOT/20260401/catShort/catShort0042report"
  ! purge_file_id 20260401 catShort "../.."
  [ -e "$NAS1_ROOT/20260401/catShort/catShort0042report" ]
}

@test "I3: bin/sftp-purge refuses to run when NAS2 sentinel is absent" {
  export PURGE_DRY_RUN=1
  sentinel off
  migrate_fixture 20260301 catX catY
  run "$BATS_TEST_DIRNAME/../../bin/sftp-purge"
  [ "$status" -ne 0 ]
  [ -e "$NAS2_ROOT/20260301/catX" ]   # nothing touched
}

@test "I1: cleanup_partition refuses to remove symlink when NAS2 dir non-empty" {
  # Simulate the "partition not fully drained" / .nfsXXXX case: a leftover
  # file in NAS2/<date> means rmdir will fail. The symlink must stay so the
  # next cycle retries — never orphan the NAS2 dir.
  migrate_fixture 20260301 catX
  cleanup_partition 20260301
  [ -L "$NAS1_ROOT/20260301" ]                # symlink retained
  [ -e "$NAS2_ROOT/20260301/catX" ]           # NAS2 untouched
}

@test "SEC-C1: purge refuses to follow a symlink whose target is outside NAS2_ROOT" {
  # Producer-compromise scenario: a symlink in NAS1_ROOT pointing to an
  # attacker-chosen directory must NOT cause rm -rf against that directory.
  mkdir -p "$TEST_TMP/outside/catX"
  : > "$TEST_TMP/outside/catX/sentinel"
  ln -s "$TEST_TMP/outside" "$NAS1_ROOT/20260301"   # age 92, catX(70) eligible
  purge_run
  [ -L "$NAS1_ROOT/20260301" ]                       # symlink untouched
  [ -e "$TEST_TMP/outside/catX/sentinel" ]           # outside path NOT deleted
}

@test "SEC-H1: purge_file_id refuses when date symlink target is outside NAS2_ROOT" {
  mkdir -p "$TEST_TMP/outside/catShort"
  : > "$TEST_TMP/outside/catShort/catShort0042report"
  ln -s "$TEST_TMP/outside" "$NAS1_ROOT/20260401"
  ! purge_file_id 20260401 catShort 0042
  [ -e "$TEST_TMP/outside/catShort/catShort0042report" ]   # outside untouched
}

@test "CR-I5: purge_run is idempotent (second run is a no-op)" {
  migrate_fixture 20260301 catX catY
  purge_run                                  # first run drains and cleans up
  [ ! -e "$NAS1_ROOT/20260301" ]
  run purge_run                              # second run: nothing to do
  [ "$status" -eq 0 ]
  [ ! -e "$NAS1_ROOT/20260301" ]
}

@test "purge_run aborts mid-run when NAS2 becomes unavailable" {
  # Bind-mount-drop-mid-run scenario: per-iteration check_nas2 must catch
  # the drop and abort loudly rather than silently turning every remaining
  # migrated partition into a "refused" warning.
  migrate_fixture 20260301 catX catY
  migrate_fixture 20260302 catX catY
  _hits=0
  check_nas2() { _hits=$((_hits + 1)); [ $_hits -le 1 ]; }
  ! purge_run
  [ ! -e "$NAS2_ROOT/20260301/catX" ]        # first partition processed
  [ -e "$NAS2_ROOT/20260302/catX" ]          # second untouched — we aborted
}
