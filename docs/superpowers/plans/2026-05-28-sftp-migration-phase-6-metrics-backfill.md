# Phase 6 — Metrics, Backfill Orchestration, Tier-2 NFS Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Emit Prometheus metrics atomically, gate the backfill on live load, tie everything together in the watermark-driven `migrate_run` and the `bin/sftp-migrate` entrypoint, and provide the Tier-2 real-NFS semantics tests.

**Architecture:** Metrics via the node_exporter textfile collector (atomic `.tmp`→`mv`). Backfill yields when active SFTP sessions exceed a threshold. `migrate_run` reconciles, then drains NAS1 toward LOW, oldest-first, honoring the fit check and load gate. Tier-2 validates NFS silly-rename on a real mount.

**Tech Stack:** Bash 4+, `ss`, `df`, `flock`, bats-core. Depends on Phases 1–5.

---

### Task 1: Metrics emission (`metrics.sh`)

Implements **T1-36, T1-37, T1-38**. Term: observability / procurement forecast.

**Files:**
- Create: `sftp-migration/lib/metrics.sh`
- Create: `sftp-migration/test/unit/metrics.bats`

- [ ] **Step 1: Write the failing metrics tests**

Create `sftp-migration/test/unit/metrics.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh metrics.sh; }
teardown() { teardown_roots; }

@test "T1-36: metric_emit writes atomically with no leftover temp file" {
  metric_emit
  [ -f "$METRICS_FILE" ]
  run bash -c "ls ${METRICS_FILE}.tmp.* 2>/dev/null"
  [ "$status" -ne 0 ]
  grep -q "sftp_nas_free_bytes" "$METRICS_FILE"
  grep -q "sftp_migration_nas2_fit_check_failed 0" "$METRICS_FILE"
}

@test "T1-37: last-success timestamp is emitted when set" {
  _M_LAST_SUCCESS=1748736000 metric_emit
  grep -q "sftp_migration_last_success_timestamp_seconds 1748736000" "$METRICS_FILE"
}

@test "T1-38: fit-check metric reflects the failure flag" {
  _M_FIT_CHECK_FAILED=1 metric_emit
  grep -q "sftp_migration_nas2_fit_check_failed 1" "$METRICS_FILE"
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/metrics.bats`
Expected: FAIL — `source: .../lib/metrics.sh: No such file or directory`.

- [ ] **Step 3: Implement metrics.sh**

Create `sftp-migration/lib/metrics.sh`:

```bash
# shellcheck shell=bash
# Prometheus textfile-collector emission. Written atomically (.tmp -> mv) so the
# scraper never reads a partial file. predict_linear() over sftp_nas_free_bytes
# in Prometheus drives the NAS2-full procurement forecast.
metric_emit() {
  local tmp="${METRICS_FILE}.tmp.$$" free1 free2
  free1="$(nas_free_bytes "$NAS1_ROOT")"
  free2="$(nas_free_bytes "$NAS2_ROOT")"
  {
    printf '# TYPE sftp_nas_free_bytes gauge\n'
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS1_ROOT" "$free1"
    printf 'sftp_nas_free_bytes{mountpoint="%s"} %s\n' "$NAS2_ROOT" "$free2"
    printf '# TYPE sftp_migration_nas2_fit_check_failed gauge\n'
    printf 'sftp_migration_nas2_fit_check_failed %s\n' "${_M_FIT_CHECK_FAILED:-0}"
    printf '# TYPE sftp_migration_last_success_timestamp_seconds gauge\n'
    printf 'sftp_migration_last_success_timestamp_seconds %s\n' "${_M_LAST_SUCCESS:-0}"
  } > "$tmp"
  mv -f "$tmp" "$METRICS_FILE"
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/metrics.bats`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/metrics.sh sftp-migration/test/unit/metrics.bats
git commit -m "feat(sftp-migration): atomic prometheus textfile metrics"
```

---

### Task 2: Backfill load gate (`backfill.sh`)

Implements **T1-39, T1-40**. Term: **Load-Gated Backfill**.

**Files:**
- Create: `sftp-migration/lib/backfill.sh`
- Create: `sftp-migration/test/unit/backfill.bats`

- [ ] **Step 1: Write the failing backfill tests**

Create `sftp-migration/test/unit/backfill.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh move.sh backfill.sh; }
teardown() { teardown_roots; }

@test "T1-39: backfill yields when sessions exceed threshold" {
  export MAX_ACTIVE_SESSIONS=5
  ACTIVE_SESSIONS_OVERRIDE=10 run backfill_should_yield
  [ "$status" -eq 0 ]
  ACTIVE_SESSIONS_OVERRIDE=2 run backfill_should_yield
  [ "$status" -ne 0 ]
}

@test "T1-40: rsync_partition passes --bwlimit when configured" {
  rsync() { printf '%s\n' "$*" > "$TEST_TMP/rsync_args"; }
  export RSYNC_BWLIMIT=12345
  make_partition 20260101 catX 64
  rsync_partition 20260101
  grep -q -- "--bwlimit=12345" "$TEST_TMP/rsync_args"
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/backfill.bats`
Expected: FAIL — `source: .../lib/backfill.sh: No such file or directory`.

- [ ] **Step 3: Implement backfill.sh**

Create `sftp-migration/lib/backfill.sh`:

```bash
# shellcheck shell=bash
# Live-load signal: count established connections to port 22. Overridable in
# tests via ACTIVE_SESSIONS_OVERRIDE. Robust to zero matches (|| true).
active_sessions() {
  if [ -n "${ACTIVE_SESSIONS_OVERRIDE:-}" ]; then echo "$ACTIVE_SESSIONS_OVERRIDE"; return; fi
  local n
  n="$(ss -tn state established '( sport = :22 )' 2>/dev/null | grep -c ':22' || true)"
  echo "${n:-0}"
}

# backfill_should_yield: true (0) when load is too high to migrate right now.
backfill_should_yield() {
  [ "$(active_sessions)" -gt "$MAX_ACTIVE_SESSIONS" ]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/backfill.bats`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/backfill.sh sftp-migration/test/unit/backfill.bats
git commit -m "feat(sftp-migration): load-gated backfill signal and bwlimit passthrough"
```

---

### Task 3: Watermark-driven `migrate_run`

Implements **T1-12** and the drain/fit-check integration. Terms: **High/Low Watermark**, **Fit Check**, **Load-Gated Backfill**.

**Files:**
- Modify: `sftp-migration/lib/move.sh` (add `migrate_run`)
- Create: `sftp-migration/test/unit/migrate_run.bats`

- [ ] **Step 1: Write the failing migrate_run tests**

Create `sftp-migration/test/unit/migrate_run.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh eligibility.sh capacity.sh move.sh backfill.sh metrics.sh
  export NOW_OVERRIDE; NOW_OVERRIDE=$(date -u -d 2026-06-01 +%s)
  sentinel on
}
teardown() { teardown_roots; }

@test "T1-12: no migration when NAS1 at or below HIGH" {
  nas_used_pct() { echo 70; }
  make_partition 20260101 catX 1024
  run migrate_run
  [ "$status" -eq 0 ]
  [ -d "$NAS1_ROOT/20260101" ]
  [ ! -L "$NAS1_ROOT/20260101" ]
}

@test "migrate_run drains oldest-first and stops below LOW" {
  # File-backed counter so it survives subshells: first read HIGH, then LOW.
  nas_used_pct() {
    local n; n=$(( $(cat "$TEST_TMP/uc" 2>/dev/null || echo 0) + 1 ))
    echo "$n" > "$TEST_TMP/uc"
    if [ "$n" -le 1 ]; then echo 85; else echo 69; fi
  }
  fits_on_nas2()          { return 0; }
  backfill_should_yield() { return 1; }
  make_partition 20260101 catX 1024
  make_partition 20260102 catX 1024
  run migrate_run
  [ "$status" -eq 0 ]
  [ -L "$NAS1_ROOT/20260101" ]
  [ -d "$NAS1_ROOT/20260102" ]
}

@test "migrate_run sets fit-check metric when a partition does not fit" {
  nas_used_pct()          { echo 85; }
  fits_on_nas2()          { return 1; }
  backfill_should_yield() { return 1; }
  make_partition 20260101 catX 1024
  run migrate_run
  grep -q "sftp_migration_nas2_fit_check_failed 1" "$METRICS_FILE"
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/migrate_run.bats`
Expected: FAIL — `migrate_run: command not found`.

- [ ] **Step 3: Implement migrate_run in move.sh**

Append to `sftp-migration/lib/move.sh`:

```bash
# migrate_run: reconcile, then drain NAS1 toward LOW while above HIGH,
# oldest-eligible first, honoring the NAS2 fit check and yielding to live load.
# Emits metrics on every exit path.
migrate_run() {
  reconcile
  check_nas2 || { metric_emit; return 1; }
  _M_FIT_CHECK_FAILED=0
  local used name size
  used="$(nas_used_pct "$NAS1_ROOT")"
  if [ "$used" -le "$HIGH_WATERMARK" ]; then
    log "NAS1 at ${used}% <= HIGH; nothing to do"
    _M_LAST_SUCCESS="$(date -u +%s)"; metric_emit; return 0
  fi
  while IFS= read -r name; do
    [ -z "$name" ] && continue
    if backfill_should_yield; then log "yielding to live load"; break; fi
    size="$(dir_size_bytes "$NAS1_ROOT/$name")"
    if ! fits_on_nas2 "$size"; then
      warn "fit-check: $name ($size B) does not fit on NAS2; stopping"
      _M_FIT_CHECK_FAILED=1; break
    fi
    migrate_partition "$name" || { warn "migrate failed for $name; stopping"; break; }
    used="$(nas_used_pct "$NAS1_ROOT")"
    [ "$used" -lt "$LOW_WATERMARK" ] && { log "NAS1 at ${used}% < LOW; done"; break; }
  done < <(list_eligible_oldest_first)
  _M_LAST_SUCCESS="$(date -u +%s)"
  metric_emit
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/migrate_run.bats`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/move.sh sftp-migration/test/unit/migrate_run.bats
git commit -m "feat(sftp-migration): watermark-driven migrate_run with fit check and load gate"
```

---

### Task 4: Migration entrypoint (`bin/sftp-migrate`)

**Files:**
- Create: `sftp-migration/bin/sftp-migrate`
- Create: `sftp-migration/test/unit/entrypoint.bats`

- [ ] **Step 1: Write the failing entrypoint smoke test**

Create `sftp-migration/test/unit/entrypoint.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; sentinel on; ln -sf "$NAS2_ROOT" "$NAS1_ROOT/.nas2"; }
teardown() { teardown_roots; }

@test "bin/sftp-migrate runs and writes metrics" {
  export ACTIVE_SESSIONS_OVERRIDE=0
  run "$BATS_TEST_DIRNAME/../../bin/sftp-migrate"
  [ "$status" -eq 0 ]
  [ -f "$METRICS_FILE" ]
  grep -q "sftp_migration_last_success_timestamp_seconds" "$METRICS_FILE"
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/entrypoint.bats`
Expected: FAIL — `no such file or directory: .../bin/sftp-migrate`.

- [ ] **Step 3: Implement bin/sftp-migrate**

Create `sftp-migration/bin/sftp-migrate`:

```bash
#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for m in config log guard dates eligibility capacity move lock reconcile backfill metrics; do
  # shellcheck source=/dev/null
  source "$HERE/lib/$m.sh"
done

# Single-instance, shared with purge so the two never run concurrently.
with_lock 300 migrate_run
```

- [ ] **Step 4: Make it executable and run the test**

Run: `cd sftp-migration && chmod +x bin/sftp-migrate && bats test/unit/entrypoint.bats`
Expected: PASS (1 test).

- [ ] **Step 5: Run the full Tier-1 suite**

Run: `cd sftp-migration && bats test/unit/`
Expected: PASS (all unit tests across phases).

- [ ] **Step 6: Commit**

```bash
git add sftp-migration/bin/sftp-migrate sftp-migration/test/unit/entrypoint.bats
git commit -m "feat(sftp-migration): migration entrypoint under shared lock"
```

---

### Task 5: Cron/systemd wiring (docs only — infra installs)

**Files:**
- Create: `sftp-migration/README.md`

- [ ] **Step 1: Document the schedule and hand-off**

Create `sftp-migration/README.md`:

```markdown
# sftp-migration

Migrates aged long-term SFTP date-partitions from NAS1 to NAS2 via per-date
symlinks; rewrites the long-term purge to be UTC- and symlink-aware.

## Schedule (hand to infra)
- Migration: hourly — `bin/sftp-migrate`
- Reconciliation: every 1–2 min — `bin/sftp-migrate` is safe to run often
  (reconcile is a no-op when clean), or run `reconcile` standalone.
- Purge: daily — `bin/sftp-purge` (KEEP `PURGE_DRY_RUN=1` until the dry-run
  output has been reviewed).

## Config (env)
NAS1_ROOT, NAS2_ROOT, NAS2_SENTINEL, HIGH_WATERMARK, LOW_WATERMARK,
NAS2_RESERVE_BYTES, MIN_MIGRATE_AGE_DAYS, RSYNC_BWLIMIT, MAX_ACTIVE_SESSIONS,
LONGTERM_RETENTIONS, PURGE_DRY_RUN, METRICS_FILE.

## Infra-owned prerequisites
NAS2 mount + `.nas2` bind mount in fstab (`_netdev`, ordered); sentinel file
placed on NAS2; node_exporter textfile collector reads METRICS_FILE.
```

- [ ] **Step 2: Commit**

```bash
git add sftp-migration/README.md
git commit -m "docs(sftp-migration): schedule, config, infra prerequisites"
```

---

### Task 6: Tier-2 NFS semantics tests

Implements **T2-01** (automated) and a runbook for **T2-02..T2-10**. Run on an
NFS-backed VM (`RUN_NFS_TESTS=1`), NOT in CI. Term: **Test Tiers**, **NFS Client-Side Safety**.

**Files:**
- Create: `sftp-migration/test/nfs/semantics.bats`
- Create: `sftp-migration/test/nfs/RUNBOOK.md`

- [ ] **Step 1: Implement the automated silly-rename test**

Create `sftp-migration/test/nfs/semantics.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

require_nfs() {
  [ "${RUN_NFS_TESTS:-0}" = "1" ] || skip "set RUN_NFS_TESTS=1 on an NFS-backed host"
  case "$(stat -f -c %T "$NAS1_ROOT" 2>/dev/null)" in
    nfs*) : ;;
    *) skip "NAS1_ROOT is not on NFS" ;;
  esac
}

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh capacity.sh move.sh reconcile.sh
  sentinel on
}
teardown() { teardown_roots; }

@test "T2-01: open reader survives swap + immediate delete (silly-rename)" {
  require_nfs
  make_partition 20260101 catX 1048576
  local f="$NAS1_ROOT/20260101/catX/catX0001file"
  local before; before="$(sha256sum "$f" | awk '{print $1}')"
  exec 3< "$f"                       # hold the inode open BEFORE the swap
  migrate_partition 20260101         # rsync -> verify -> swap -> rm -rf .bak
  local captured="$TEST_TMP/captured"
  cat <&3 > "$captured"              # finish reading via the original fd
  exec 3<&-
  [ "$(sha256sum "$captured" | awk '{print $1}')" = "$before" ]
}
```

- [ ] **Step 2: Run on the NFS VM to verify it passes**

Run (on the NFS-backed host, with `NAS1_ROOT`/`NAS2_ROOT`/`.nas2` on real NFS):
`RUN_NFS_TESTS=1 bats test/nfs/semantics.bats`
Expected: PASS (skips in CI).

- [ ] **Step 3: Write the manual Tier-2 runbook**

Create `sftp-migration/test/nfs/RUNBOOK.md`:

```markdown
# Tier-2 NFS Runbook (manual / semi-automated)

Run on a CentOS-8 (or Ubuntu) VM with NAS1/NAS2 on real NFS matching prod
`vers=`/options. T2-01 is automated in semantics.bats; the rest below.

- **T2-02 crash mid-rsync:** start `bin/sftp-migrate`; `kill -9` it during copy.
  Assert NAS1/<date> intact, downloads unaffected; re-run resumes and completes.
- **T2-03 crash in mv->ln gap:** add a `sleep` between `mv` and `ln -s` in a test
  build of swap_to_symlink; kill in the gap; run reconcile; assert path restored.
- **T2-04 immediate-delete with open reader:** as T2-01 but confirm a `.nfsXXXX`
  appears under .bak and the next reconcile reaps the dir after the reader exits.
- **T2-05 perm parity (positive):** migrate; `sftp` as a real downstream user;
  download succeeds.
- **T2-06 perm parity (negative):** stage NAS2 with wrong GID; assert verify_gate
  fails / download denied.
- **T2-07 ENOSPC:** fill NAS2; run migrate; assert rsync fails, NAS1 intact,
  partial NAS2 dropped, fit-check metric=1.
- **T2-08 stale handle:** induce stale mount; assert check_nas2 trips.
- **T2-09 unmounted NAS2:** `umount /mnt/nas2`; run migrate; assert exit non-zero
  and local root did not grow (`df` before/after).
- **T2-10 reboot survival:** migrate; reboot; assert symlink resolves + download OK.
```

- [ ] **Step 4: Commit**

```bash
git add sftp-migration/test/nfs/semantics.bats sftp-migration/test/nfs/RUNBOOK.md
git commit -m "test(sftp-migration): Tier-2 NFS silly-rename test and runbook"
```
