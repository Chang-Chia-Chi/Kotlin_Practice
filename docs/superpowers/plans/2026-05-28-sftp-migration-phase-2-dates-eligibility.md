# Phase 2 — UTC Age Computation & Eligibility Selection

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Pure, deterministic logic to compute a partition's age in UTC days and to decide which partitions are eligible to migrate.

**Architecture:** All date math forced to UTC (`date -u`) and overridable via `NOW_OVERRIDE` for deterministic tests. Eligibility excludes symlinks (already migrated), invalid names, and the hot window.

**Tech Stack:** Bash 4+, GNU coreutils `date`, bats-core. Depends on Phase 1 (`config.sh`, `log.sh`, test harness).

---

### Task 1: UTC age computation (`dates.sh`)

Implements **T1-01..T1-05** (see `test-plan.md`). Term: **Age Computation (UTC)**.

**Files:**
- Create: `sftp-migration/lib/dates.sh`
- Create: `sftp-migration/test/unit/dates.bats`

- [ ] **Step 1: Write the failing date tests**

Create `sftp-migration/test/unit/dates.bats`:

```bash
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/dates.bats`
Expected: FAIL — `source: .../lib/dates.sh: No such file or directory`.

- [ ] **Step 3: Implement dates.sh**

Create `sftp-migration/lib/dates.sh`:

```bash
# shellcheck shell=bash
# All date math is UTC. Tests set NOW_OVERRIDE (epoch seconds) for determinism;
# production leaves it unset and uses the real UTC clock.

now_epoch() { echo "${NOW_OVERRIDE:-$(date -u +%s)}"; }

# parse_partition_epoch_days <YYYYMMDD> -> epoch-day count (UTC), or return 1
# for a name that is not a valid calendar date.
parse_partition_epoch_days() {
  local name="$1" secs
  [[ "$name" =~ ^[0-9]{8}$ ]] || return 1
  secs="$(date -u -d "${name:0:4}-${name:4:2}-${name:6:2}" +%s 2>/dev/null)" || return 1
  echo $(( secs / 86400 ))
}

today_epoch_days() { echo $(( $(now_epoch) / 86400 )); }

# partition_age_days <YYYYMMDD> -> whole UTC days old, or return 1 if invalid.
partition_age_days() {
  local pday
  pday="$(parse_partition_epoch_days "$1")" || return 1
  echo $(( $(today_epoch_days) - pday ))
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/dates.bats`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/dates.sh sftp-migration/test/unit/dates.bats
git commit -m "feat(sftp-migration): UTC-forced partition age computation"
```

---

### Task 2: Eligibility selection (`eligibility.sh`)

Implements **T1-06, T1-07, T1-08, T1-13**. Term: **Migration Eligibility Threshold**.

**Files:**
- Create: `sftp-migration/lib/eligibility.sh`
- Create: `sftp-migration/test/unit/eligibility.bats`

- [ ] **Step 1: Write the failing eligibility tests**

Create `sftp-migration/test/unit/eligibility.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup() {
  setup_roots
  load_lib dates.sh eligibility.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
}
teardown() { teardown_roots; }

@test "T1-06: real dir older than min age is eligible" {
  make_partition 20260101 catX 1024
  run is_eligible 20260101
  [ "$status" -eq 0 ]
}

@test "T1-07: already-migrated symlink is NOT eligible" {
  mkdir -p "$NAS2_ROOT/20260101"
  ln -s "$NAS2_ROOT/20260101" "$NAS1_ROOT/20260101"
  run is_eligible 20260101
  [ "$status" -ne 0 ]
}

@test "T1-08: hot partition within min age is NOT eligible" {
  make_partition 20260530 catX 1024
  run is_eligible 20260530
  [ "$status" -ne 0 ]
}

@test "T1-13: eligible list is oldest-first" {
  make_partition 20260101 catX 1024
  make_partition 20260115 catX 1024
  make_partition 20260110 catX 1024
  run list_eligible_oldest_first
  [ "${lines[0]}" = "20260101" ]
  [ "${lines[1]}" = "20260110" ]
  [ "${lines[2]}" = "20260115" ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/eligibility.bats`
Expected: FAIL — `source: .../lib/eligibility.sh: No such file or directory`.

- [ ] **Step 3: Implement eligibility.sh**

Create `sftp-migration/lib/eligibility.sh`:

```bash
# shellcheck shell=bash
# A partition is migration-eligible iff it is a REAL directory on NAS1 (not a
# symlink — i.e. not already migrated), has a valid UTC date name, and is older
# than MIN_MIGRATE_AGE_DAYS (so its short-term categories are already purged).
is_eligible() {
  local name="$1" path="$NAS1_ROOT/$name" age
  [ -d "$path" ] || return 1
  [ -L "$path" ] && return 1
  age="$(partition_age_days "$name")" || return 1
  [ "$age" -gt "$MIN_MIGRATE_AGE_DAYS" ]
}

# Print eligible partition names oldest-first. The glob skips dotfiles, so the
# .nas2 bind mount and .<date>.bak set-aside dirs are never considered.
list_eligible_oldest_first() {
  local entry name
  for entry in "$NAS1_ROOT"/*; do
    [ -e "$entry" ] || continue
    name="$(basename "$entry")"
    is_eligible "$name" && echo "$name"
  done | sort
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/eligibility.bats`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/eligibility.sh sftp-migration/test/unit/eligibility.bats
git commit -m "feat(sftp-migration): symlink-aware oldest-first eligibility selection"
```
