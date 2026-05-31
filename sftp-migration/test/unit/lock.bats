#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh lock.sh; }
teardown() { teardown_roots; }

@test "lock is acquired and command runs when free" {
  run with_lock 1 true
  [ "$status" -eq 0 ]
}

@test "T1-33/34/35: held lock blocks a second acquirer; -w times out" {
  ( exec {fd}>"$LOCK_FILE"; flock "$fd"; sleep 2 ) &
  local holder=$!
  sleep 0.3
  run with_lock 1 true
  [ "$status" -ne 0 ]
  wait "$holder"
}
