# Phase 4 — Immediate Delete, Stateless Reconciliation, Shared Lock

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Crash-safety via stateless reconciliation that repairs any interrupted migration, and a local-disk `flock` shared by migration and purge.

**Architecture:** Reconciliation infers state from the filesystem (no journal) and is idempotent. Verify logic is refactored into a generic `verify_copy` so reconciliation can validate a `.bak` against NAS2 before rolling forward. The lock is on local disk (NFS `flock` is unreliable; both jobs run on the same VM).

**Tech Stack:** Bash 4+, `flock` (util-linux), bats-core. Depends on Phases 1–3.

---

### Task 1: Shared local-disk lock (`lock.sh`)

Implements **T1-33, T1-34, T1-35**. Term: **Shared Migration/Purge Lock**.

**Files:**
- Create: `sftp-migration/lib/lock.sh`
- Create: `sftp-migration/test/unit/lock.bats`

- [ ] **Step 1: Write the failing lock tests**

Create `sftp-migration/test/unit/lock.bats`:

```bash
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/lock.bats`
Expected: FAIL — `source: .../lib/lock.sh: No such file or directory`.

- [ ] **Step 3: Implement lock.sh**

Create `sftp-migration/lib/lock.sh`:

```bash
# shellcheck shell=bash
# with_lock <timeout_secs> <command...>: run the command holding an exclusive
# lock on LOCK_FILE (local disk — NFS flock is unreliable, and migration+purge
# both run on this VM so a local lock is authoritative). Returns non-zero if the
# lock can't be acquired within the timeout (so a hung peer doesn't block forever).
with_lock() {
  local timeout="$1"; shift
  local lockfd
  exec {lockfd}> "$LOCK_FILE" || return 1
  if ! flock -w "$timeout" "$lockfd"; then
    warn "could not acquire lock $LOCK_FILE within ${timeout}s"
    exec {lockfd}>&-
    return 1
  fi
  "$@"
  local rc=$?
  flock -u "$lockfd"
  exec {lockfd}>&-
  return "$rc"
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/lock.bats`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/lock.sh sftp-migration/test/unit/lock.bats
git commit -m "feat(sftp-migration): local-disk flock shared by migration and purge"
```

---

### Task 2: Refactor verify into `verify_copy`

Lets reconciliation validate a `.bak` against NAS2. Phase 3 tests must stay green.

**Files:**
- Modify: `sftp-migration/lib/move.sh`

- [ ] **Step 1: Replace `verify_partition` with `verify_copy` + thin wrapper**

In `sftp-migration/lib/move.sh`, replace the entire `verify_partition()` function with:

```bash
# verify_copy <src_dir> <dst_dir>: dst must be byte-identical (checksum) AND
# match ownership/mode of src. Non-zero on any mismatch.
verify_copy() {
  local src="$1" dst="$2" diff rel s d
  diff="$(rsync -an --checksum --delete "$src/" "$dst/" 2>/dev/null)"
  if [ -n "$diff" ]; then
    warn "verify: content mismatch ($src vs $dst)"
    return 1
  fi
  while IFS= read -r -d '' s; do
    rel="${s#"$src"/}"
    d="$dst/$rel"
    [ -e "$d" ] || { warn "verify: missing $rel"; return 1; }
    if [ "$(stat -c '%u:%g:%a' "$s")" != "$(stat -c '%u:%g:%a' "$d")" ]; then
      warn "verify: permission mismatch for $rel"
      return 1
    fi
  done < <(find "$src" -print0)
  return 0
}

# verify_partition <date>: verify the NAS1 source against the NAS2 copy.
verify_partition() { verify_copy "$NAS1_ROOT/$1" "$NAS2_ROOT/$1"; }
```

- [ ] **Step 2: Run Phase 3 tests to confirm the refactor is green**

Run: `cd sftp-migration && bats test/unit/move.bats`
Expected: PASS (6 tests) — behavior unchanged.

- [ ] **Step 3: Commit**

```bash
git add sftp-migration/lib/move.sh
git commit -m "refactor(sftp-migration): extract verify_copy for reuse in reconciliation"
```

---

### Task 3: Stateless reconciliation (`reconcile.sh`)

Implements **T1-20..T1-25**. Terms: **Crash-During-Copy Safety**, **Safe-Move Sequence**.

**Files:**
- Create: `sftp-migration/lib/reconcile.sh`
- Create: `sftp-migration/test/unit/reconcile.bats`

- [ ] **Step 1: Write the failing reconciliation tests**

Create `sftp-migration/test/unit/reconcile.bats`:

```bash
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/reconcile.bats`
Expected: FAIL — `source: .../lib/reconcile.sh: No such file or directory`.

- [ ] **Step 3: Implement reconcile.sh**

Create `sftp-migration/lib/reconcile.sh`:

```bash
# shellcheck shell=bash
# Stateless crash recovery: infer interrupted migrations from the filesystem and
# repair them. Idempotent; safe to run at the start of every migration run and
# on a fast standalone timer. Repairs the only dangerous window (between
# mv-aside and ln -s) and finishes interrupted .bak cleanup.
reconcile() {
  local bak date path
  shopt -s nullglob dotglob
  for bak in "$NAS1_ROOT"/.*.bak; do
    [ -d "$bak" ] || continue
    date="$(basename "$bak")"; date="${date#.}"; date="${date%.bak}"
    path="$NAS1_ROOT/$date"
    if [ -L "$path" ]; then
      # Symlink already created -> interrupted cleanup; finish it. (A lingering
      # .nfsXXXX may keep .bak around; the next run removes it once empty.)
      rm -rf "$bak"
      log "reconcile: finished cleanup for $date"
    elif [ -e "$path" ]; then
      warn "reconcile: anomaly for $date (real dir and .bak both present)"
    else
      # Path missing -> crashed mid-swap. Roll forward if NAS2 verified, else back.
      if verify_copy "$bak" "$NAS2_ROOT/$date"; then
        ln -s "${SYMLINK_REL_PREFIX}${date}" "$path"
        rm -rf "$bak"
        log "reconcile: rolled forward $date"
      else
        mv "$bak" "$path"
        rm -rf "${NAS2_ROOT:?}/$date"
        warn "reconcile: rolled back $date (NAS2 copy incomplete)"
      fi
    fi
  done
  shopt -u nullglob dotglob
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/reconcile.bats`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/reconcile.sh sftp-migration/test/unit/reconcile.bats
git commit -m "feat(sftp-migration): stateless idempotent crash reconciliation"
```
