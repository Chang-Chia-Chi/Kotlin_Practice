#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib dates.sh; }
teardown() { teardown_roots; }

fixed_today() { export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s); }

@test "T1-01: age is whole UTC days" {
  fixed_today
  [ "$(partition_age_days 20260525)" -eq 7 ]
}

@test "T1-02: age unchanged regardless of VM local TZ" {
  fixed_today
  local utc tokyo
  utc="$(partition_age_days 20260525)"
  export TZ="Asia/Tokyo"
  tokyo="$(partition_age_days 20260525)"
  [ "$utc" -eq "$tokyo" ]
}

@test "T1-03: consecutive dates differ by exactly one day" {
  local d1 d2
  d1="$(parse_partition_epoch_days 20260101)"
  d2="$(parse_partition_epoch_days 20260102)"
  [ $(( d2 - d1 )) -eq 1 ]
}

@test "T1-04: leap day and year boundary parse" {
  run parse_partition_epoch_days 20240229
  [ "$status" -eq 0 ]
  run parse_partition_epoch_days 20251231
  [ "$status" -eq 0 ]
}

@test "T1-05: malformed or invalid names are rejected" {
  run parse_partition_epoch_days notadate
  [ "$status" -ne 0 ]
  run parse_partition_epoch_days 20260230
  [ "$status" -ne 0 ]
}
