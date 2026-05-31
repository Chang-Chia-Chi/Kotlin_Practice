#!/usr/bin/env bats
load ../helpers/setup

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh capacity.sh move.sh reconcile.sh
  sentinel on
}
teardown() { teardown_roots; }

# Stage a "crash between mv-aside and ln -s": data in .bak, path missing.
stage_bak() {
  make_partition "$1" catX 1024
  mv "$NAS1_ROOT/$1" "$NAS1_ROOT/.$1.bak"
}

@test "T1-20: .bak + missing D + complete NAS2 -> roll forward" {
  stage_bak 20260101
  rsync -a "$NAS1_ROOT/.20260101.bak/" "$NAS2_ROOT/20260101/"
  reconcile
  [ -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
  [ -f "$NAS1_ROOT/20260101/catX/catX0001file" ]
}

@test "T1-21: .bak + missing D + incomplete NAS2 -> roll back" {
  stage_bak 20260101
  mkdir -p "$NAS2_ROOT/20260101/catX"
  echo partial > "$NAS2_ROOT/20260101/catX/catX0001file"
  reconcile
  [ -d "$NAS1_ROOT/20260101" ]
  [ ! -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS2_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
}

@test "T1-22: .bak + D is symlink -> finish cleanup" {
  stage_bak 20260101
  rsync -a "$NAS1_ROOT/.20260101.bak/" "$NAS2_ROOT/20260101/"
  ln -s ".nas2/20260101" "$NAS1_ROOT/20260101"
  reconcile
  [ -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
}

@test "T1-23: .bak + D is real dir -> anomaly, no destruction" {
  make_partition 20260101 catX 1024
  cp -a "$NAS1_ROOT/20260101" "$NAS1_ROOT/.20260101.bak"
  reconcile
  [ -d "$NAS1_ROOT/20260101" ]
  [ -e "$NAS1_ROOT/.20260101.bak" ]
}

@test "T1-24: empty .bak dir with symlink present is swept" {
  mkdir -p "$NAS1_ROOT/.20260101.bak"
  ln -s ".nas2/20260101" "$NAS1_ROOT/20260101"
  reconcile
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
}

@test "T1-25: reconcile is idempotent (second run is a no-op)" {
  stage_bak 20260101
  rsync -a "$NAS1_ROOT/.20260101.bak/" "$NAS2_ROOT/20260101/"
  reconcile
  run reconcile
  [ "$status" -eq 0 ]
  [ -L "$NAS1_ROOT/20260101" ]
}

@test "CR-C1: reconcile ignores a .bak file whose name isn't a valid date" {
  # A stray .x.bak (or .tmp.bak, .foo.bak) must NOT cause rm -rf "$NAS2_ROOT/x"
  # or mv operations against non-date paths. Defense in depth on the
  # destructive side under no-backup.
  mkdir -p "$NAS1_ROOT/.x.bak"
  : > "$NAS1_ROOT/.x.bak/sentinel"
  mkdir -p "$NAS2_ROOT/x"
  : > "$NAS2_ROOT/x/sentinel"
  reconcile
  [ -e "$NAS1_ROOT/.x.bak/sentinel" ]
  [ -e "$NAS2_ROOT/x/sentinel" ]
}

@test "I-6: reconcile rolls back when verify_copy itself fails (rsync rc!=0)" {
  # Stage a "mid-swap" state with a COMPLETE NAS2 copy, but force verify_copy
  # to fail (simulating the rsync-binary-missing / OOM / NFS-hang case). The
  # safe response is rollback, not roll forward on uncertainty.
  stage_bak 20260101
  rsync -a "$NAS1_ROOT/.20260101.bak/" "$NAS2_ROOT/20260101/"
  verify_copy() { return 2; }                     # simulate invocation failure
  reconcile
  [ -d "$NAS1_ROOT/20260101" ]                    # path restored from .bak
  [ ! -L "$NAS1_ROOT/20260101" ]                  # no symlink (didn't roll fwd)
  [ ! -e "$NAS2_ROOT/20260101" ]                  # partial NAS2 cleaned up
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]             # .bak moved back to path
}
