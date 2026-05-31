---
name: maintaining-sftp-migration
description: Design summary and per-file map for sftp-migration. Use when an agent or maintainer needs to orient to the system before changing it, or to find which doc or source file answers a specific question.
---

# sftp-migration — system map

A bash + bats system that migrates aged long-term SFTP date-partitions from a
full NAS1 to a second NAS2 via per-date symlinks, transparently to downstream,
and rewrites the long-term purge to be UTC- and symlink-aware. **No backup;
hardware redundancy only** — a wrong `rm` is permanent loss.

## Core design (the invariants every change must preserve)

1. **Per-date symlink unification.** `NAS1/<date>` is either a real directory
   (hot/recent partitions) or a RELATIVE symlink `.nas2/<date>` resolving via
   a bind mount into NAS2 (migrated partitions). Downstream sees one tree.
2. **The `.nas2` bind mount.** `/mnt/nas2` is bind-mounted at
   `/mnt/nas1/.nas2` so relative symlinks resolve inside any chroot. The
   sentinel is read THROUGH this path — so a bind-drop trips the guard.
3. **No backup → defense in depth on the destructive side.** Every `rm`/`mv`
   path passes through: (a) calendar round-trip date validation, (b) symlink
   target allowlist to `NAS2_ROOT`, (c) sentinel-via-bind-mount NAS2 guard,
   (d) `PURGE_DRY_RUN=1` default on the irreversible side. Each gate caught
   a real bug in review.
4. **Stateless reconciliation.** A crash anywhere in the `mv → ln -s` window
   is repaired by inferring filesystem state on the next run — no journal,
   idempotent, safe to re-run.
5. **Shared local-disk lock.** Migration and purge mutex each other via
   local-disk `flock` (NFS flock is unreliable). Both jobs run on the same
   VM, so a local lock is authoritative.
6. **UTC everywhere.** Folder names are UTC dates; every `date` call in `lib/`
   uses `-u`; a CI lint catches regressions. Under no backup, off-by-one
   from a TZ shift is permanent loss.
7. **Adaptive watermark drain.** When NAS1 > HIGH, drain toward LOW
   oldest-first, fit-checking each partition against NAS2's reserve, yielding
   to live SFTP load via an active-sessions gate.
8. **Atomic Prometheus emission.** `.tmp → mv` textfile; gauges drive
   procurement forecast, fit-check alert, and a dead-man's switch.

## File map — what each script owns

### `bin/`

| File | Owns |
|------|------|
| `sftp-migrate` | Hourly cron entry. Refuses root. Delegates to `migrate_run` under the shared lock. `migrate_run` owns the NAS2 guard so the dead-man's-switch advances on every exit path. |
| `sftp-purge` | Daily cron entry. Refuses root. `check_nas2` runs INSIDE the lock (TOCTOU-safe), then `purge_run`. |

### `lib/` (sourced; never `set -e` here — it leaks to the caller)

| File | Owns |
|------|------|
| `config.sh` | All env-overridable defaults (`${VAR:=default}`). One place for tuning knobs. |
| `log.sh` | `log` / `warn` / `die` printf helpers with UTC timestamps. |
| `guard.sh` | `check_nas2`: reads the sentinel THROUGH the bind-mount path. Catches NAS2-unmounted, bind-dropped, and stale-handle in one syscall. |
| `dates.sh` | UTC age math: `parse_partition_epoch_days`, `partition_age_days`. Round-trip guard rejects month-00 / day-00 / invalid calendar dates. |
| `eligibility.sh` | `is_eligible`, `list_eligible_oldest_first`. Real-dir-only (no re-migration), `age > MIN_MIGRATE_AGE_DAYS` (strict). |
| `capacity.sh` | `df`-based per-mount usage (NEVER `du` over the tree). Numeric-validated. `fits_on_nas2` enforces `NAS2_RESERVE_BYTES`. |
| `move.sh` | The single-partition pipeline: `rsync_partition → verify_copy → swap_to_symlink → migrate_partition`. Plus the drain orchestrator `migrate_run`. |
| `lock.sh` | `with_lock <timeout_secs> <cmd…>`. Closing the fd releases the lock. |
| `reconcile.sh` | 4-state crash recovery; reuses `verify_copy` to gate roll-forward vs rollback. Validates date before any destructive op. |
| `purge.sh` | The IRREVERSIBLE side. Two-phase per partition (per-category at its retention, then date-level cleanup). Allowlists symlink targets to `NAS2_ROOT`. |
| `backfill.sh` | `active_sessions` + `backfill_should_yield`. The migration drain breaks out when load is too high. |
| `metrics.sh` | Atomic textfile emission for the node_exporter textfile collector. |

### `test/`

| File | Owns |
|------|------|
| `helpers/setup.bash` | `setup_roots`, `make_partition`, `sentinel on\|off`, `load_lib`. Emulates the prod `.nas2` bind mount via a symlink. |
| `helpers/assertions.bash` | Custom asserts (e.g. `assert_no_local_shadow_growth`). |
| `unit/*.bats` | Tier-1 (CI/WSL). ~80 tests, all with `T1-*` IDs cited in commits. |
| `nfs/semantics.bats` | Tier-2 (skip-gated by `RUN_NFS_TESTS=1`). The NFS silly-rename test. |
| `nfs/RUNBOOK.md` | Semi-automated `T2-02`..`T2-10` checklist for the NFS-backed VM. |

## Where to dig deeper

| If your question is about… | Look at |
|---|---|
| **How** to operate it (cron, env, prereqs) | `README.md` |
| **What** a term means | `docs/sftp-migration/CONTEXT.md` (glossary — terms only, no specs) |
| **Why** symlinks instead of mergerfs | `docs/sftp-migration/adr/0001-…md` |
| **Which** prod-side facts are still TBD | `docs/sftp-migration/discovery.md` |
| **Which** test pins which property | `docs/sftp-migration/test-plan.md` (`T1-*` / `T2-*` IDs are cited in commits + code comments) |
| **How** a phase was built from scratch | `docs/superpowers/plans/2026-05-28-sftp-migration-phase-{1..6}-*.md` |
| **Why** a particular line is the way it is | `git log -p sftp-migration/lib/<file>.sh` — each commit names the bug class it closes |
| **What** the actual code does | The source. Every non-trivial function has a docstring; the comments are the spec. |

## Meta-conventions (cheap to remember; expensive to violate)

- All `local`s split (`local a b; a=$1; b=$a`) — never `local a=$1 b=$a` (the RHS reads OUTER `$a`).
- All sourced libs start `# shellcheck shell=bash`. No `set -e`.
- Any function using `shopt` wraps its body in `( … )` so the change is subshell-scoped.
- Every commit message names the bug class it closes — `git log` is the canonical "why was this written this way" trail.
