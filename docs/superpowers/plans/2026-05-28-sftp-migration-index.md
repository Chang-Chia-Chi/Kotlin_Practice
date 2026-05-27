# SFTP NAS-Migration — Implementation Plan Index

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement these plans task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a safe, adaptive system that migrates aged long-term SFTP date-partitions from a full NAS1 to NAS2 using per-date symlinks, transparently to downstream, and rewrites the long-term purge to be symlink- and UTC-aware.

**Why split into phases:** each phase is its own file and produces independently testable software. Implement in order — later phases source modules built in earlier ones.

**Design source of truth:** [`docs/sftp-migration/CONTEXT.md`](../../sftp-migration/CONTEXT.md) (glossary), [`adr/0001`](../../sftp-migration/adr/0001-symlink-per-date-over-union-mount.md), [`discovery.md`](../../sftp-migration/discovery.md), [`test-plan.md`](../../sftp-migration/test-plan.md). Test-case IDs (`T1-*`, `T2-*`) referenced in tasks live in `test-plan.md`.

**Tech stack:** Bash 4+ (CentOS-8), `rsync`, `flock`, `find`, coreutils; tests in **bats-core**; metrics via node_exporter textfile collector.

---

## Phase files (implement in order)

1. [Phase 1 — Scaffolding, config, logging, NAS2 availability guard](2026-05-28-sftp-migration-phase-1-scaffolding.md)
2. [Phase 2 — UTC age computation & eligibility selection](2026-05-28-sftp-migration-phase-2-dates-eligibility.md)
3. [Phase 3 — Capacity (df), fit-check, safe-move (rsync→verify→swap)](2026-05-28-sftp-migration-phase-3-move.md)
4. [Phase 4 — Immediate delete, stateless reconciliation, shared lock](2026-05-28-sftp-migration-phase-4-reconcile-lock.md)
5. [Phase 5 — Purge rewrite (dry-run-first, two-phase, symlink-aware)](2026-05-28-sftp-migration-phase-5-purge.md)
6. [Phase 6 — Metrics, backfill orchestration, Tier-2 NFS tests](2026-05-28-sftp-migration-phase-6-metrics-backfill.md)

---

## Target file structure (built across phases)

```
sftp-migration/
├── bin/
│   ├── sftp-migrate              # migration cron entrypoint        (Phase 3,4,6)
│   └── sftp-purge                # purge cron entrypoint            (Phase 5)
├── lib/
│   ├── config.sh                 # defaults + env overrides         (Phase 1)
│   ├── log.sh                     # log/warn/die                    (Phase 1)
│   ├── guard.sh                   # check_nas2 (sentinel)           (Phase 1)
│   ├── dates.sh                   # UTC age math                    (Phase 2)
│   ├── eligibility.sh             # eligible-partition selection    (Phase 2)
│   ├── capacity.sh                # df usage, fit check, watermark  (Phase 3)
│   ├── move.sh                    # rsync, verify gate, swap, delete(Phase 3,4)
│   ├── lock.sh                    # shared flock                    (Phase 4)
│   ├── reconcile.sh               # stateless reconciliation        (Phase 4)
│   ├── purge.sh                   # two-phase symlink-aware purge   (Phase 5)
│   └── metrics.sh                 # prometheus textfile emission    (Phase 6)
└── test/
    ├── helpers/
    │   ├── setup.bash             # fixtures (make_partition, etc.) (Phase 1)
    │   └── assertions.bash        # custom asserts                  (Phase 1)
    ├── unit/                      # Tier-1 (local dirs, CI)         (Phases 1-6)
    └── nfs/semantics.bats         # Tier-2 (run on NFS VM)          (Phase 6)
```

## Local dev environment (Windows host + WSL) — verified 2026-05-28

The repo lives on a Windows host; tests must run in **WSL Ubuntu 22.04** (`bash`,
`rsync`, `flock`, `ss`, `stat`, `du`, `df`, `find`, `readlink`, `sha256sum`,
`awk` all present). Verified: repo is reachable from WSL, `core.autocrlf` is
unset (files stay LF — required for sourcing), and both symlink creation and
`chmod +x` work on `/mnt/c`, so all Tier-1 tests run correctly off the repo path.

**The only missing tool is `bats`.** One-time setup:

```bash
# From a WSL shell (Start menu "Ubuntu", or `wsl -d Ubuntu` in PowerShell):
sudo apt-get update && sudo apt-get install -y bats   # installs bats-core 1.2.1 (sufficient)
bats --version
```

**Run the suite (from inside WSL, not Git Bash):**

```bash
wsl -d Ubuntu                                   # open the WSL shell
cd "/mnt/c/Users/maxch/OneDrive/文件/GitHub/Kotlin_Practice/sftp-migration"
bats test/unit/                                 # Tier-1 (CI-equivalent)
```

> Work from a real WSL shell. Driving WSL through Git Bash (`wsl.exe -- bash -lc '…'`)
> mangles quoted multi-word commands and `/mnt/c` paths (MSYS path conversion) —
> avoid it for running tests.

> Tier-2 (`test/nfs/`) needs real NFS mounts and `RUN_NFS_TESTS=1`; it is NOT run
> from this dev box. See Phase 6 Task 6.

> Editors: keep `lib/*.sh` and `test/**` at **LF** line endings. `core.autocrlf`
> is unset so git won't rewrite them, but a CRLF-saving editor would break `source`.
> A `.gitattributes` with `*.sh text eol=lf` is a cheap safeguard.

## Shared conventions (all phases)

- **Sourcing:** `lib/*.sh` files are sourced; `bin/*` source the libs they need. Every lib begins with `# shellcheck shell=bash`.
- **Config via env with defaults**, e.g. `: "${NAS1_ROOT:=/mnt/nas1}"`. Tests override these to point at temp dirs.
- **No wall-clock in logic:** date functions read `${NOW_OVERRIDE:-}` (epoch seconds) when set, so tests are deterministic. Production leaves it unset → real UTC.
- **All age math is UTC** (`date -u`), never the VM local timezone.
- **Idempotency:** every destructive function is safe to re-run; reconciliation infers state from the filesystem, no journal.
- **Commits:** one per task (TDD red→green→commit). Conventional-commit prefixes.

## Prerequisites before running in production (not code — gating facts)

From `discovery.md`: SFTP server, chroot, **NFS-vs-CIFS**, NFS version/options, same-appliance-vs-different, network speed, traffic pattern, downstream group GID. Infra-owned: mount persistence (fstab), NAS2 health, backup/DR. See memory `project_sftp_migration_infra_boundary`.
