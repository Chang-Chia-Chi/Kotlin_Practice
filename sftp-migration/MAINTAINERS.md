---
name: maintaining-sftp-migration
description: File-by-file map and modification playbook for sftp-migration. Use when an agent or devops engineer needs to modify, extend, or debug code under sftp-migration/, or to understand the design discipline before changing anything. Distinct from README.md (operator/devops) — this file is for code-modifiers.
---

# Maintaining sftp-migration

This is a **destructive, irreversible, no-backup** system. The discipline
below isn't style — it's safety. Read this file before changing code under
`sftp-migration/`. Pair it with `docs/sftp-migration/CONTEXT.md` (design
glossary) and the per-phase plan files for the reasoning behind each decision.

## Where to find what

| If you want to … | Go to |
|---|---|
| Operate the system (cron, env vars) | `README.md` |
| Understand WHAT each term means | `docs/sftp-migration/CONTEXT.md` |
| Understand the symlink-vs-mergerfs choice | `docs/sftp-migration/adr/0001-…md` |
| Discover prod-side facts before deploying | `docs/sftp-migration/discovery.md` |
| Find which test pins which property | `docs/sftp-migration/test-plan.md` |
| See how a phase was built from scratch | `docs/superpowers/plans/2026-05-28-sftp-migration-phase-{1..6}-*.md` |

## File map

**`bin/`** (cron entries; refuse root; `set -uo pipefail` not `-e`)
- `sftp-migrate` — hourly. `with_lock 300 migrate_run`. `migrate_run` owns the NAS2 guard so the dead-man's-switch metric advances even on guard-fail.
- `sftp-purge` — daily. `check_nas2` runs **inside** the lock (TOCTOU-safe).

**`lib/`** (sourced; never `set -e` here — it would leak to the caller)
- `config.sh` — env-overridable defaults. Add new knobs as `: "${VAR:=default}"`.
- `log.sh` — `log` / `warn` / `die` (UTC).
- `guard.sh` — `check_nas2` reads sentinel THROUGH the `.nas2` bind mount.
- `dates.sh` — UTC-forced age math; `parse_partition_epoch_days` round-trips parse→format→compare.
- `eligibility.sh` — real-dir-only, oldest-first; rejects symlinks (already migrated).
- `capacity.sh` — `df` per mount only; numeric-validated; never `du` over the tree.
- `move.sh` — `rsync_partition`, `verify_copy`, `swap_to_symlink`, `migrate_partition`, `migrate_run`.
- `lock.sh` — `with_lock <timeout> <cmd…>` (local-disk flock; closing fd releases).
- `reconcile.sh` — 4-state crash recovery; subshell-scoped `shopt nullglob`; validates date.
- `purge.sh` — IRREVERSIBLE; dry-run default; allowlists symlink targets to `NAS2_ROOT`.
- `backfill.sh` — `active_sessions` + `backfill_should_yield`.
- `metrics.sh` — atomic `.tmp → mv` textfile.

**`test/`**
- `helpers/setup.bash` — `setup_roots`, `make_partition`, `sentinel on|off`, `load_lib`. Emulates the prod `.nas2` bind mount with a symlink so relative symlinks resolve.
- `helpers/assertions.bash` — `assert_no_local_shadow_growth`.
- `unit/*.bats` — Tier-1 (CI, 80 tests in WSL Ubuntu).
- `nfs/semantics.bats` — Tier-2 (skipped unless `RUN_NFS_TESTS=1` on real NFS).
- `nfs/RUNBOOK.md` — semi-automated T2-02..T2-10 manual checklist.

## Modification discipline (each rule came from a real bug)

1. **Split `local` declarations.** `local a=$1 b=$a` reads OUTER `$a`. Write `local a b; a=$1; b=$a`. (Phase 2.)
2. **Check exit codes separately from stdout.** Empty stdout ≠ success. Use `if ! out="$(cmd)"; then …` or capture `rc=$?`. (Phase 3 C1.)
3. **`rsync -an --checksum` is silent on content diff.** Use `-ani`. Without `-i`, a tampered copy passes verify. (Phase 3.)
4. **Negative tests are load-bearing.** Every destructive op needs a failure-path test. Happy-path can't catch "empty output mistaken for success." (Phases 3, 5, end-to-end.)
5. **Read sentinels THROUGH the bind mount** (`$NAS1_ROOT/.nas2/.nas2_sentinel`). Reading directly at `$NAS2_ROOT` misses the bind-dropped case. (Staff review.)
6. **`rm -rf` of an NFS dir can hit ENOTEMPTY** from a `.nfsXXXX` held by a live reader. Tolerate it (warn, continue); reconcile sweeps next cycle.
7. **Wrap any `shopt`-using body in `( … )`** to scope the change. Skip `dotglob` unless you genuinely need `*` to match dotfiles. (Phase 4.)
8. **Validate destructive-helper inputs CALLEE-side.** Don't trust callers. Regex-gate `date`/`cat`/`id`; pass `--` to `rm`. (Phase 5 C1.)
9. **Refuse to follow symlinks outside `NAS2_ROOT`.** `resolve_partition_data_dir` is the allowlist gate; without it a compromised producer can plant a symlink that makes purge `rm -rf` arbitrary paths. (End-to-end SEC-C1.)
10. **Entrypoints refuse to run as root.** Defense in depth.
11. **Date math is always UTC.** A `date` call without `-u` in `lib/` fails the CI lint test.
12. **Don't change `PURGE_DRY_RUN`'s default from `1`.** Arming destruction is an explicit operator action.

## Common modifications

### Add a new long-term category
- Append to `LONGTERM_RETENTIONS` env (`catZ:120`).
- No code change — `purge_run` discovers categories at runtime.
- Validate: `PURGE_DRY_RUN=1 bin/sftp-purge`, look for `would delete … catZ`.

### Add a new metric
- Set a `_M_THING` global at the exit point in `migrate_run` / `purge_run`.
- Add a `printf '… %s\n' "${_M_THING:-0}"` line to `metric_emit`.
- Add a test asserting the gauge value (see T1-36/37/38).
- Wire the Alertmanager rule outside this repo.

### Change a watermark / reserve
- Edit the env var in cron (`HIGH_WATERMARK`, `LOW_WATERMARK`, `NAS2_RESERVE_BYTES`).
- No code change.

### Extend the eligibility rule (e.g., per-category)
- Modify `is_eligible` in `lib/eligibility.sh`. Add tests in `test/unit/eligibility.bats`. Update Phase 2 plan + glossary.

## Debugging a failing run

1. Inspect `$METRICS_FILE` for the textfile contents.
2. `head -c1 $NAS1_ROOT/.nas2/.nas2_sentinel` — confirm guard.
3. `ls -la $NAS1_ROOT/.*.bak` — lingering `.bak`? Run `bin/sftp-migrate` (which calls `reconcile` first).
4. For corruption suspicions, run `verify_partition <date>` interactively against a single partition.
5. For purge concerns, set `PURGE_DRY_RUN=1` and re-run; review log before re-arming.

## Don't

- Don't add `set -e` to `lib/*.sh`.
- Don't call destructive helpers from new code without going through the date-regex + symlink-allowlist gates.
- Don't run any test against a path that isn't under `mktemp -d` (real fs writes leak state across tests).
- Don't add docs to `CONTEXT.md` that aren't glossary terms — it's a glossary, not a spec.
