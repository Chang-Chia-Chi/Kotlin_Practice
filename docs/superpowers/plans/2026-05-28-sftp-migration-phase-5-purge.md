# Phase 5 — Purge Rewrite (Dry-Run-First, Two-Phase, Symlink-Aware)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Rewrite ONLY the long-term purge to select by UTC folder-name date, delete each long-term category at its own retention through the date symlink, and clean up a partition once fully drained — defaulting to dry-run because deletes are irreversible (no backup).

**Architecture:** Two-phase per partition — per-category delete (strict `age > retention`, biasing toward keeping), then date-level cleanup when empty. Resolves through the symlink so migrated data on NAS2 is actually reclaimed. The 4-day and file-id purges keep working unchanged (they only ever touch hot NAS1 real dirs / glob through the symlink).

**Tech Stack:** Bash 4+, bats-core. Depends on Phases 1–4 (`config.sh`, `log.sh`, `dates.sh`, `lock.sh`).

---

### Task 1: Long-term purge core (`purge.sh`)

Implements **T1-26, T1-27, T1-28, T1-29, T1-30**. Terms: **Purge Job**, **Long-term Retention (varies)**, **Purge Cutoff Bias**, **No Backup**.

**Files:**
- Modify: `sftp-migration/lib/config.sh` (add retentions)
- Create: `sftp-migration/lib/purge.sh`
- Create: `sftp-migration/test/unit/purge.bats`

- [ ] **Step 1: Add long-term retentions to config.sh**

Append to `sftp-migration/lib/config.sh`:

```bash
# Long-term category retentions as space-separated "category:days" pairs.
# Unknown categories are NEVER purged (fail-safe). Tune per discovery.
: "${LONGTERM_RETENTIONS:=catX:70 catY:90}"
```

- [ ] **Step 2: Write the failing purge tests**

Create `sftp-migration/test/unit/purge.bats`:

```bash
#!/usr/bin/env bats
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
  local date="$1" c; shift
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/purge.bats`
Expected: FAIL — `source: .../lib/purge.sh: No such file or directory`.

- [ ] **Step 4: Implement purge.sh**

Create `sftp-migration/lib/purge.sh`:

```bash
# shellcheck shell=bash

# category_retention <cat> -> retention days, or return 1 if unknown (never purge).
category_retention() {
  local cat="$1" pair
  for pair in $LONGTERM_RETENTIONS; do
    [ "${pair%%:*}" = "$cat" ] && { echo "${pair##*:}"; return 0; }
  done
  return 1
}

# resolve_partition_data_dir <date> -> the real data dir, following the symlink.
resolve_partition_data_dir() {
  local date="$1" path="$NAS1_ROOT/$date"
  if [ -L "$path" ]; then readlink -f "$path"; else echo "$path"; fi
}

# purge_category <date> <cat>: delete <cat> once age > its retention (strict >,
# biasing toward keeping — deletes are irreversible). Resolves through the date
# symlink so migrated data on NAS2 is reclaimed. Honors PURGE_DRY_RUN.
purge_category() {
  local date="$1" cat="$2" age ret target
  age="$(partition_age_days "$date")" || return 0
  ret="$(category_retention "$cat")"  || return 0
  [ "$age" -gt "$ret" ] || return 0
  target="$(resolve_partition_data_dir "$date")/$cat"
  [ -e "$target" ] || return 0
  if [ "$PURGE_DRY_RUN" = "1" ]; then
    log "DRY-RUN would delete $target (age=$age > ret=$ret)"
  else
    rm -rf "$target"
    log "purged $target (age=$age > ret=$ret)"
  fi
}

# cleanup_partition <date>: once fully drained, remove the date symlink + empty
# NAS2 dir (migrated) or the empty NAS1 dir (non-migrated). Honors PURGE_DRY_RUN.
cleanup_partition() {
  local date="$1" path="$NAS1_ROOT/$date" target
  if [ -L "$path" ]; then
    target="$(readlink -f "$path")"
    [ -n "$(ls -A "$target" 2>/dev/null)" ] && return 0
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would remove symlink $path and empty $target"
    else
      rmdir "$target" 2>/dev/null
      rm -f "$path"
      log "cleaned up migrated partition $date"
    fi
  elif [ -d "$path" ]; then
    [ -n "$(ls -A "$path" 2>/dev/null)" ] && return 0
    if [ "$PURGE_DRY_RUN" = "1" ]; then
      log "DRY-RUN would rmdir $path"
    else
      rmdir "$path"
      log "cleaned up partition $date"
    fi
  fi
}

# purge_run: long-term purge across all partitions — per-category at each
# category's own retention, then clean up drained partitions.
purge_run() {
  local entry date pair cat
  shopt -s nullglob
  for entry in "$NAS1_ROOT"/*; do
    date="$(basename "$entry")"
    parse_partition_epoch_days "$date" >/dev/null || continue
    for pair in $LONGTERM_RETENTIONS; do
      cat="${pair%%:*}"
      purge_category "$date" "$cat"
    done
    cleanup_partition "$date"
  done
  shopt -u nullglob
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/purge.bats`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add sftp-migration/lib/config.sh sftp-migration/lib/purge.sh sftp-migration/test/unit/purge.bats
git commit -m "feat(sftp-migration): symlink-aware two-phase long-term purge with dry-run"
```

---

### Task 2: File-id purge (unchanged behavior, proven through symlink)

Implements **T1-31, T1-32**. Term: **File-ID Purge**.

**Files:**
- Modify: `sftp-migration/lib/purge.sh` (add `purge_file_id`)
- Modify: `sftp-migration/test/unit/purge.bats` (add cases)

- [ ] **Step 1: Add the failing file-id tests**

Append to `sftp-migration/test/unit/purge.bats`:

```bash
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/purge.bats`
Expected: FAIL — `purge_file_id: command not found`.

- [ ] **Step 3: Implement purge_file_id**

Append to `sftp-migration/lib/purge.sh`:

```bash
# purge_file_id <date> <cat> <id>: delete files by id. The date symlink is an
# INTERMEDIATE path component, so the glob follows it to the real file on NAS2
# (works identically on a non-migrated real dir). This mirrors the existing
# file-id purge, which therefore needs no change post-migration.
purge_file_id() {
  local date="$1" cat="$2" id="$3"
  rm -f "$NAS1_ROOT/$date/$cat/${cat}${id}"*
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/purge.bats`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/purge.sh sftp-migration/test/unit/purge.bats
git commit -m "feat(sftp-migration): file-id purge proven to resolve through date symlink"
```

---

### Task 3: Purge entrypoint (`bin/sftp-purge`)

**Files:**
- Create: `sftp-migration/bin/sftp-purge`
- Modify: `sftp-migration/test/unit/purge.bats` (smoke test)

- [ ] **Step 1: Add the failing entrypoint smoke test**

Append to `sftp-migration/test/unit/purge.bats`:

```bash
@test "bin/sftp-purge runs (dry-run default) without deleting" {
  export PURGE_DRY_RUN=1
  migrate_fixture 20260301 catX catY
  run "$BATS_TEST_DIRNAME/../../bin/sftp-purge"
  [ "$status" -eq 0 ]
  [ -e "$NAS2_ROOT/20260301/catX" ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/purge.bats`
Expected: FAIL — `no such file or directory: .../bin/sftp-purge`.

- [ ] **Step 3: Implement bin/sftp-purge**

Create `sftp-migration/bin/sftp-purge`:

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$HERE/lib/config.sh"
# shellcheck source=/dev/null
source "$HERE/lib/log.sh"
# shellcheck source=/dev/null
source "$HERE/lib/dates.sh"
# shellcheck source=/dev/null
source "$HERE/lib/lock.sh"
# shellcheck source=/dev/null
source "$HERE/lib/purge.sh"

# Share the migration lock so purge never runs concurrently with a migration.
with_lock 300 purge_run
```

- [ ] **Step 4: Make it executable and run the test**

Run: `cd sftp-migration && chmod +x bin/sftp-purge && bats test/unit/purge.bats`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/bin/sftp-purge sftp-migration/test/unit/purge.bats
git commit -m "feat(sftp-migration): purge entrypoint under shared lock"
```
