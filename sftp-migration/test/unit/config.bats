#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; }
teardown() { teardown_roots; }

@test "config provides sane defaults" {
  load_lib
  [ "$LOW_WATERMARK" -lt "$HIGH_WATERMARK" ]
  [ "$PURGE_DRY_RUN" -eq 1 ]
  [ "$MIN_MIGRATE_AGE_DAYS" -ge 5 ]
  [ -n "$NAS2_RESERVE_BYTES" ]
}

@test "env overrides win over defaults" {
  export HIGH_WATERMARK=85
  load_lib
  [ "$HIGH_WATERMARK" -eq 85 ]
}
