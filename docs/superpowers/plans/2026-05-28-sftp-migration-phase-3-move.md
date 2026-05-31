# Phase 3 — Capacity (df), Fit-Check, Safe-Move

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Measure NAS usage via `df`, enforce the per-partition fit check, and implement the safe move: `rsync` → verify gate (checksum + permission parity) → relative-symlink swap → delete.

**Architecture:** Usage from `df` per mount (never `du` over the tree — symlinks would skew it). The swap sets the real dir aside as `.<date>.bak` (in-flight fds survive) then creates a **relative** symlink resolving through `.nas2/` (works with or without chroot).

**Tech Stack:** Bash 4+, `rsync`, `df`, `stat`, bats-core. Depends on Phases 1–2.

---

### Task 1: Capacity & fit check (`capacity.sh`)

Implements **T1-09, T1-10, T1-11**. Terms: **Fit Check**, **High/Low Watermark**, **NAS2 Reserve**.

**Files:**
- Create: `sftp-migration/lib/capacity.sh`
- Create: `sftp-migration/test/unit/capacity.bats`

- [ ] **Step 1: Write the failing fit-check tests**

Create `sftp-migration/test/unit/capacity.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup

setup()    { setup_roots; load_lib log.sh capacity.sh; }
teardown() { teardown_roots; }

@test "T1-09: partition that fits is accepted" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  run fits_on_nas2 4000
  [ "$status" -eq 0 ]
}

@test "T1-10: partition larger than free-minus-reserve is rejected" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  run fits_on_nas2 6000
  [ "$status" -ne 0 ]
}

@test "T1-11: boundary size == free-minus-reserve fits" {
  nas_free_bytes() { echo $(( NAS2_RESERVE_BYTES + 5000 )); }
  run fits_on_nas2 5000
  [ "$status" -eq 0 ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/capacity.bats`
Expected: FAIL — `source: .../lib/capacity.sh: No such file or directory`.

- [ ] **Step 3: Implement capacity.sh**

Create `sftp-migration/lib/capacity.sh`:

```bash
# shellcheck shell=bash
# Usage is measured with df PER MOUNT — never du over the tree, because the
# date symlinks and the .nas2 bind mount would make du miscount.
#
# Each helper validates that the awk pipeline produced a non-empty integer.
# A failing df/du (stale mount, ENOENT, permission denied) otherwise leaks an
# empty value into arithmetic — the Phase 6 watermark loop would silently
# compare against bogus numbers.

_is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

nas_used_pct() {
  local n
  n="$(df -P "$1" 2>/dev/null | awk 'NR==2 { gsub(/%/,"",$5); print $5 }')"
  _is_uint "$n" || { warn "nas_used_pct: invalid df output for $1"; return 1; }
  echo "$n"
}

nas_free_bytes() {
  local n
  n="$(df -PB1 "$1" 2>/dev/null | awk 'NR==2 { print $4 }')"
  _is_uint "$n" || { warn "nas_free_bytes: invalid df output for $1"; return 1; }
  echo "$n"
}

dir_size_bytes() {
  local n
  n="$(du -sb "$1" 2>/dev/null | awk '{ print $1 }')"
  _is_uint "$n" || { warn "dir_size_bytes: invalid du output for $1"; return 1; }
  echo "$n"
}

# fits_on_nas2 <size_bytes>: 0 if the partition fits while preserving the reserve.
# Propagates failure if the underlying free-bytes read couldn't be obtained.
fits_on_nas2() {
  local size free
  size="$1"
  free="$(nas_free_bytes "$NAS2_ROOT")" || return 1
  [ "$size" -le $(( free - NAS2_RESERVE_BYTES )) ]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/capacity.bats`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/capacity.sh sftp-migration/test/unit/capacity.bats
git commit -m "feat(sftp-migration): df-based usage and NAS2 fit check"
```

---

### Task 2: Symlink-resolution config/harness + rsync & verify gate (`move.sh`)

Implements the verify gate (content checksum + permission parity). Term: **Verify Gate**, **Safe-Move Sequence**.

**Files:**
- Modify: `sftp-migration/lib/config.sh` (add symlink prefix)
- Modify: `sftp-migration/test/helpers/setup.bash` (add `.nas2` resolution link)
- Create: `sftp-migration/lib/move.sh`
- Create: `sftp-migration/test/unit/move.bats`

- [ ] **Step 1: Add the relative symlink prefix to config.sh**

Append to `sftp-migration/lib/config.sh`:

```bash
# Relative target prefix for date symlinks. NAS2 is reachable under the root as
# .nas2 (a bind mount in prod), so a date symlink points to .nas2/<date> and
# resolves inside any chroot. Trailing slash intentional.
: "${SYMLINK_REL_PREFIX:=.nas2/}"
```

- [ ] **Step 2: Make the harness resolve relative symlinks**

In `sftp-migration/test/helpers/setup.bash`, inside `setup_roots`, add a `.nas2`
link under NAS1 pointing at NAS2 (emulates the prod bind mount), immediately
after the `mkdir -p "$NAS1_ROOT" "$NAS2_ROOT"` line:

```bash
  ln -s "$NAS2_ROOT" "$NAS1_ROOT/.nas2"
```

- [ ] **Step 3: Write the failing verify-gate tests**

Create `sftp-migration/test/unit/move.bats`:

```bash
#!/usr/bin/env bats
load ../helpers/setup
load ../helpers/assertions

setup() {
  setup_roots
  load_lib log.sh guard.sh dates.sh capacity.sh move.sh
  sentinel on
}
teardown() { teardown_roots; }

@test "verify gate passes for an identical copy" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  run verify_partition 20260101
  [ "$status" -eq 0 ]
}

@test "verify gate fails when the NAS2 copy is tampered" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  echo tampered >> "$NAS2_ROOT/20260101/catX/catX0001file"
  run verify_partition 20260101
  [ "$status" -ne 0 ]
}

@test "verify gate fails on permission mismatch" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  chmod 600 "$NAS2_ROOT/20260101/catX/catX0001file"
  run verify_partition 20260101
  [ "$status" -ne 0 ]
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/move.bats`
Expected: FAIL — `source: .../lib/move.sh: No such file or directory`.

- [ ] **Step 5: Implement rsync + verify in move.sh**

Create `sftp-migration/lib/move.sh`:

```bash
# shellcheck shell=bash

# rsync_partition <date>: copy NAS1/<date> -> NAS2/<date>, preserving metadata,
# resumable (--partial), bandwidth-capped. Source is read-only; safe to re-run.
#
# All `local`s split so the RHS never reads outer-scope $date (bash quirk).
rsync_partition() {
  local date src dst opts
  date="$1"
  src="$NAS1_ROOT/$date"
  dst="$NAS2_ROOT/$date"
  opts=(-a --delete --partial)
  [ -n "$RSYNC_BWLIMIT" ] && opts+=(--bwlimit="$RSYNC_BWLIMIT")
  mkdir -p "$dst"
  rsync "${opts[@]}" "$src/" "$dst/"
}

# verify_partition <date>: gate before any swap. NAS2 copy must be byte-identical
# (checksum) AND match ownership/mode (permission parity). Non-zero on mismatch,
# INCLUDING a failure to actually run rsync (binary missing, OOM, NFS hang) —
# empty stdout from rsync only means "identical" when rc==0.
# NOTE: -i (--itemize-changes) is load-bearing. Without it, -an emits NOTHING
# on a content mismatch and the gate silently passes corrupt copies.
# NOTE: find -mindepth 1 — the src dir itself isn't matched by `${s#"$src"/}`
# (no trailing slash), causing a spurious "missing" on every call without it.
# All `local`s split to avoid the bash same-statement RHS-reads-outer quirk.
verify_partition() { verify_copy "$NAS1_ROOT/$1" "$NAS2_ROOT/$1"; }

verify_copy() {
  local src dst diff rc rel s d
  src="$1"
  dst="$2"
  diff="$(rsync -ani --checksum --delete "$src/" "$dst/" 2>/dev/null)"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    warn "verify: rsync invocation failed (rc=$rc) for $src vs $dst"
    return 1
  fi
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
  done < <(find "$src" -mindepth 1 -print0)
  return 0
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/move.bats`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add sftp-migration/lib/config.sh sftp-migration/lib/move.sh sftp-migration/test/helpers/setup.bash sftp-migration/test/unit/move.bats
git commit -m "feat(sftp-migration): rsync copy and checksum+permission verify gate"
```

---

### Task 3: Swap and full single-partition migrate

Implements **T1-17, T1-18, T1-19** and the happy path. Terms: **Safe-Move Sequence**, **NAS2 Availability Guard**.

**Files:**
- Modify: `sftp-migration/lib/move.sh` (add `swap_to_symlink`, `migrate_partition`)
- Modify: `sftp-migration/test/unit/move.bats` (add cases)

- [ ] **Step 1: Add the failing swap & migrate tests**

Append to `sftp-migration/test/unit/move.bats`:

```bash
@test "T1-18/19: swap replaces dir with a resolving relative symlink" {
  make_partition 20260101 catX 2048
  rsync_partition 20260101
  swap_to_symlink 20260101
  [ -L "$NAS1_ROOT/20260101" ]
  [ -f "$NAS1_ROOT/.20260101.bak/catX/catX0001file" ]
  [ -f "$NAS1_ROOT/20260101/catX/catX0001file" ]
}

@test "migrate_partition happy path: resolving symlink, no .bak, content intact" {
  make_partition 20260101 catX 2048
  local before; before="$(sha256sum "$NAS1_ROOT/20260101/catX/catX0001file" | awk '{print $1}')"
  run migrate_partition 20260101
  [ "$status" -eq 0 ]
  [ -L "$NAS1_ROOT/20260101" ]
  [ ! -e "$NAS1_ROOT/.20260101.bak" ]
  local after; after="$(sha256sum "$NAS1_ROOT/20260101/catX/catX0001file" | awk '{print $1}')"
  [ "$before" = "$after" ]
}

@test "T1-17: guard failure aborts migrate with no NAS2 write" {
  make_partition 20260101 catX 2048
  sentinel off
  run migrate_partition 20260101
  [ "$status" -ne 0 ]
  run assert_no_local_shadow_growth
  [ "$status" -eq 0 ]
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd sftp-migration && bats test/unit/move.bats`
Expected: FAIL — `swap_to_symlink: command not found`.

- [ ] **Step 3: Implement swap and migrate in move.sh**

Append to `sftp-migration/lib/move.sh`:

```bash
# swap_to_symlink <date>: set the real dir aside as .<date>.bak (so in-flight,
# inode-bound download fds keep reading), then create a RELATIVE symlink that
# resolves through .nas2/ to the NAS2 copy. Relative target works inside chroot.
# Defensive guards refuse to proceed on already-migrated paths or leftover .bak
# (reconciliation territory) so the swap never enters an undefined state.
# All `local`s split (bash same-statement RHS-reads-outer quirk).
swap_to_symlink() {
  local date path bak
  date="$1"
  path="$NAS1_ROOT/$date"
  bak="$NAS1_ROOT/.$date.bak"
  if [ -L "$path" ]; then
    warn "swap: $path is already a symlink; skipping (already migrated)"
    return 0
  fi
  if [ -e "$bak" ]; then
    warn "swap: $bak already exists; reconcile required"
    return 1
  fi
  mv "$path" "$bak"
  ln -s "${SYMLINK_REL_PREFIX}${date}" "$path"
}

# migrate_partition <date>: full safe move for one partition.
# guard -> rsync -> verify gate -> swap -> immediate delete of .bak.
# On NFS, immediate rm is safe (silly-rename preserves any open reader); the
# reconciliation sweep (Phase 4) cleans any .bak left non-empty by a .nfsXXXX.
#
# Idempotency: a no-op when the partition is already a symlink (re-run safe).
# A leftover .<date>.bak signals a prior crash and forces reconcile-first.
migrate_partition() {
  local date path bak
  date="$1"
  path="$NAS1_ROOT/$date"
  bak="$NAS1_ROOT/.$date.bak"
  if [ -L "$path" ]; then
    log "migrate: $date already migrated; skipping"
    return 0
  fi
  if [ -e "$bak" ]; then
    warn "migrate: $bak exists from prior crash; reconcile required"
    return 1
  fi
  check_nas2 || return 1
  rsync_partition "$date"   || { warn "rsync failed for $date";  return 1; }
  verify_partition "$date"  || { warn "verify failed for $date; not swapping"; return 1; }
  swap_to_symlink "$date"
  # rm -rf may fail with ENOTEMPTY if NFS silly-renamed an in-flight file
  # into .bak (.nfsXXXX). That's the expected NFS-safe path: the migration
  # itself has succeeded (symlink in place, NAS2 copy verified); the reconcile
  # sweep (Phase 4) removes the lingering dir once the open fd closes.
  # Treat the rm failure as non-fatal and log loudly so it isn't misread as
  # a real problem.
  rm -rf "$bak" 2>/dev/null \
    || warn "migrate: $bak not fully removed (likely .nfsXXXX held open); reconcile will sweep"
  log "migrated $date"
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd sftp-migration && bats test/unit/move.bats`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add sftp-migration/lib/move.sh sftp-migration/test/unit/move.bats
git commit -m "feat(sftp-migration): relative-symlink swap and full single-partition migrate"
```
