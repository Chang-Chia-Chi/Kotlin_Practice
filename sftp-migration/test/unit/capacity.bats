#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh; }
teardown() { teardown_roots; }

@test "T1-09: partition that fits is accepted" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  fits_on_nas2 4000
}

@test "T1-10: partition larger than free-minus-reserve is rejected" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  ! fits_on_nas2 6000
}

@test "T1-11: boundary size == free-minus-reserve fits" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  fits_on_nas2 5000
}
