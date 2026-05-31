# Phase 1 — Scaffolding, Config, Logging, NAS2 Availability Guard

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stand up the project skeleton, configuration, logging, the bats test harness, and the sentinel-based NAS2 availability guard.

**Architecture:** Sourced bash libs with env-overridable config so tests run against local temp dirs. The guard reads a sentinel file on the real NAS2 to prevent the NFS-shadow footgun.

**Tech Stack:** Bash 4+, bats-core.

**Prerequisite:** `bats` installed (`dnf install bats` or from source). Verify: `bats --version`.

---

### Task 1: Config and logging libraries

**Files:**
- Create: `sftp-migration/lib/config.sh`
- Create: `sftp-migration/lib/log.sh`
- Create: `sftp-migration/test/helpers/setup.bash`
- Create: `sftp-migration/test/unit/config.bats`

- [ ] **Step 1: Write the test harness helper**

Create `sftp-migration/test/helpers/setup.bash`:

```bash
# shellcheck shell=bash
_LIB_DIR="${BATS_TEST_DIRNAME}/../../lib"

setup_roots() {
  TEST_TMP="$(mktemp -d)"
  export TEST_TMP
  export NAS1_ROOT="$TEST_TMP/nas1"
  export NAS2_ROOT="$TEST_TMP/nas2"
  # Sentinel via the bind-mount path; matches the prod config.sh default and
  # means removing the .nas2 link in a test simulates a bind-mount drop.
  export NAS2_SENTINEL="$NAS1_ROOT/.nas2/.nas2_sentinel"
  export LOCK_FILE="$TEST_TMP/lock"
  export METRICS_FILE="$TEST_TMP/metrics.prom"
  mkdir -p "$NAS1_ROOT" "$NAS2_ROOT"
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
```

- [ ] **Step 2: Write the failing config test**

Create `sftp-migration/test/unit/config.bats`:

```bash
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/config.bats`
Expected: FAIL — `source: .../lib/config.sh: No such file or directory`.

- [ ] **Step 4: Implement config.sh**

Create `sftp-migration/lib/config.sh`:

```bash
# shellcheck shell=bash
: "${NAS1_ROOT:=/mnt/nas1}"
: "${NAS2_ROOT:=/mnt/nas2}"
# Sentinel is read THROUGH the .nas2 bind mount (the same path symlinks resolve
# through) so a dropped bind mount — even while /mnt/nas2 itself stays mounted —
# trips the guard. Reading via NAS2_ROOT directly would NOT catch this case and
# every migrated partition's symlink would silently resolve into an empty local
# dir on NAS1, defeating the migration.
: "${NAS2_SENTINEL:=${NAS1_ROOT}/.nas2/.nas2_sentinel}"
: "${LOCK_FILE:=/run/sftp-migration.lock}"
: "${METRICS_FILE:=/var/lib/node_exporter/textfile_collector/sftp_migration.prom}"

# Watermarks: percent of NAS1 used.
: "${HIGH_WATERMARK:=80}"
: "${LOW_WATERMARK:=70}"

# Always keep at least this many bytes free on NAS2 (default 1 TiB).
: "${NAS2_RESERVE_BYTES:=1099511627776}"

# A partition is migration-eligible only when older than this (>= max short-term retention).
: "${MIN_MIGRATE_AGE_DAYS:=5}"

# rsync bandwidth cap in KB/s for backfill (empty = unlimited).
: "${RSYNC_BWLIMIT:=51200}"

# Backfill yields when active SFTP sessions exceed this.
: "${MAX_ACTIVE_SESSIONS:=20}"

# Purge safety: 1 = log would-delete, delete nothing.
: "${PURGE_DRY_RUN:=1}"
```

- [ ] **Step 5: Implement log.sh**

Create `sftp-migration/lib/log.sh`:

```bash
# shellcheck shell=bash
log()  { printf '%s [INFO] %s\n'  "$(date -u +%FT%TZ)" "$*"; }
warn() { printf '%s [WARN] %s\n'  "$(date -u +%FT%TZ)" "$*" >&2; }
die()  { printf '%s [ERROR] %s\n' "$(date -u +%FT%TZ)" "$*" >&2; exit 1; }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/config.bats`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
git add sftp-migration/lib/config.sh sftp-migration/lib/log.sh sftp-migration/test/helpers/setup.bash sftp-migration/test/unit/config.bats
git commit -m "feat(sftp-migration): config, logging, bats harness scaffolding"
```

---

### Task 2: Fixture & assertion helpers

**Files:**
- Modify: `sftp-migration/test/helpers/setup.bash` (append fixtures)
- Create: `sftp-migration/test/helpers/assertions.bash`
- Create: `sftp-migration/test/unit/fixtures.bats`

- [ ] **Step 1: Write the failing fixtures test**

Create `sftp-migration/test/unit/fixtures.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup
load ../helpers/assertions

setup()    { setup_roots; }
teardown() { teardown_roots; }

@test "make_partition creates a checksummable file on NAS1" {
  make_partition 20260101 catX 2048
  [ -f "$NAS1_ROOT/20260101/catX/catX0001file" ]
  [ "$(stat -c%s "$NAS1_ROOT/20260101/catX/catX0001file")" -eq 2048 ]
}

@test "sentinel on/off toggles the NAS2 sentinel file" {
  sentinel on
  [ -f "$NAS2_SENTINEL" ]
  sentinel off
  [ ! -f "$NAS2_SENTINEL" ]
}

@test "assert_no_local_shadow_growth passes when NAS2 has no date dirs" {
  run assert_no_local_shadow_growth
  [ "$status" -eq 0 ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/fixtures.bats`
Expected: FAIL — `make_partition: command not found`.

- [ ] **Step 3: Append fixtures to setup.bash**

Append to `sftp-migration/test/helpers/setup.bash`:

```bash
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
```

- [ ] **Step 4: Implement assertions.bash**

Create `sftp-migration/test/helpers/assertions.bash`:

```bash
# shellcheck shell=bash
# No date partition dirs were created under NAS2 (used to prove the guard
# blocked writes when NAS2 is "unmounted").
assert_no_local_shadow_growth() {
  local found
  found="$(find "$NAS2_ROOT" -mindepth 1 -maxdepth 1 -type d ! -name '.*' | wc -l)"
  [ "$found" -eq 0 ]
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/fixtures.bats`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add sftp-migration/test/helpers/setup.bash sftp-migration/test/helpers/assertions.bash sftp-migration/test/unit/fixtures.bats
git commit -m "test(sftp-migration): partition fixtures and shadow-growth assertion"
```

---

### Task 3: NAS2 availability guard (`check_nas2`)

Implements test cases **T1-15**, **T1-16** (see `test-plan.md`).

**Files:**
- Create: `sftp-migration/lib/guard.sh`
- Create: `sftp-migration/test/unit/guard.bats`

- [ ] **Step 1: Write the failing guard tests**

Create `sftp-migration/test/unit/guard.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh guard.sh; }
teardown() { teardown_roots; }

@test "T1-15: sentinel present -> check_nas2 passes" {
  sentinel on
  run check_nas2
  [ "$status" -eq 0 ]
}

@test "T1-16: sentinel absent -> check_nas2 fails non-zero" {
  sentinel off
  run check_nas2
  [ "$status" -ne 0 ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/guard.bats`
Expected: FAIL — `source: .../lib/guard.sh: No such file or directory`.

- [ ] **Step 3: Implement guard.sh**

Create `sftp-migration/lib/guard.sh`:

```bash
# shellcheck shell=bash
# check_nas2: confirm NAS2 is genuinely mounted and reachable by reading a
# sentinel file that only exists on the real NAS2. If NAS2 is unmounted, the
# sentinel is absent (writes would otherwise hit the local shadow dir); if the
# mount is stale, the read fails with ESTALE. Either way we refuse to proceed.
check_nas2() {
  if head -c1 "$NAS2_SENTINEL" >/dev/null 2>&1; then
    return 0
  fi
  warn "NAS2 not available (sentinel unreadable: $NAS2_SENTINEL)"
  return 1
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/guard.bats`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/guard.sh sftp-migration/test/unit/guard.bats
git commit -m "feat(sftp-migration): sentinel-based NAS2 availability guard"
```
