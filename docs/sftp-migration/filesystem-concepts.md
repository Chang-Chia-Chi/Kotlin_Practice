# SFTP NAS-Migration — Filesystem Concepts & Learning Guide

Reference for understanding the core filesystem concepts behind the migration design,
the trade-offs in each decision, and where to go deeper.

---

## 1. NFS Client Semantics

NFS is not a local filesystem — it is a distributed protocol layered on the kernel's VFS.
Several behaviors that "just work" on ext4/xfs require explicit reasoning on NFS.

### Inode-bound file descriptors

An open file descriptor refers to the **inode**, not the path name. Unlinking (or renaming)
the path does not close the fd — the data stays accessible until the last fd closes and the
inode's reference count drops to zero.

**Why it matters:** an in-flight SFTP download holds an open fd on the file being streamed.
Even after you `rm -rf .D.bak`, the download reads from the inode it already opened. This
is the central safety guarantee validated by test T2-01.

### NFS silly-rename

When a client calls `unlink()` on a file that has an open fd, the NFS client kernel
**renames** the file to `.nfsXXXX` instead of sending a DELETE to the server. The
`.nfsXXXX` name persists until every fd on that inode closes; then the client removes it.

This behavior is implemented in the **NFS client kernel** (`fs/nfs/unlink.c`), not the NAS
appliance. Test fidelity for T2-01 therefore requires matching the *client*: CentOS-8 with
the same NFS version and mount options — a container on a local ext4/overlayfs does **not**
exercise this code path and gives false confidence.

**Why it matters:** silly-rename is what lets the safe-move sequence do an "immediate delete"
of `.D.bak` right after the symlink swap. Without it, you would need an lsof-gated deferred
deletion loop instead.

### CIFS has no silly-rename

SMB/CIFS does not implement this behavior. If discovery reveals the NAS is CIFS, the
safe-move sequence falls back to lsof-gated / defer-and-retry deletion.

### Stale NFS mounts (the NFS-shadow footgun)

If NFS unmounts or the server becomes unreachable, the mount point **directory still exists**
on the local filesystem. `ls /mnt/nas2` may return nothing; yet `ls` itself succeeds because
you are listing the local empty directory that was the mount point. Any writes now land on the
**local root disk** — silently, and potentially filling it.

`mountpoint -q` only checks whether a filesystem is mounted on the path. It does **not** catch
a stale NFS handle (`ESTALE`) where the mount is present but the server is unreachable.

The sentinel-file guard (`head -c1 /mnt/nas2/.nas2_sentinel || exit`) catches both cases:
unmounted (file absent on empty local dir) and stale (read returns `ESTALE`). It is also
trivially testable — tests `touch`/remove the sentinel file in a local temp dir, with no real
NFS or mount privileges needed.

### flock over NFS

`flock(2)` on an NFS-backed file is unreliable. Semantics differ by NFS version, server
implementation, and mount options; locks can silently drop after a server reboot. This is
documented in `man 2 flock`.

**Why it matters:** the shared migration/purge lock uses a **local-disk** flock
(`/run/sftp-migration.lock`). Both jobs run on the same VM, so a local lock gives perfect
mutual exclusion without touching NFS.

---

## 2. Symlinks and chroot Interaction

This is the key design fork. Whether a chroot jail is configured changes which symlink
targets are valid from the SFTP client's perspective.

```
Without chroot:
  NAS1/20260101 → /mnt/nas2/20260101     ← absolute symlink, visible to clients ✓

With chroot (jail root = /mnt/nas1):
  /mnt/nas2 is outside the jail
  → symlink appears broken to clients    ✗

  Fix: bind-mount NAS2 inside NAS1:
    /mnt/nas1/.nas2/  =  bind of /mnt/nas2
  Then use a relative symlink:
    20260101 → .nas2/20260101            ← resolves within the jail ✓
```

The "universal design" — bind NAS2 under the NAS1 root + relative symlinks — works in **both**
the chroot and no-chroot cases. The only cost is a dot-prefixed `.nas2` entry visible in the
SFTP tree. This is why `grep -A20 Match /etc/ssh/sshd_config` is the first discovery command:
the entire symlink strategy depends on the answer.

**ChrootDirectory and internal-sftp:** OpenSSH requires `Subsystem sftp internal-sftp` (not
the external binary) for `ChrootDirectory` to work. The external `sftp-server` binary does not
support chroot.

---

## 3. Atomic-ish Swap (why the flip window exists)

On a local POSIX filesystem, `rename(2)` is **atomic** — the name either points to old or new,
never absent. The swap in this design is **two separate syscalls**:

```bash
mv NAS1/20260101 NAS1/.20260101.bak     # step 1: removes the real path
ln -s /mnt/nas2/20260101 NAS1/20260101  # step 2: creates the symlink
```

Between step 1 and step 2, the path `20260101` does not exist. Any `open()` landing in that
window gets `ENOENT`.

**Why it is accepted:** only aged long-term files (5–70 days old) are ever migrated. Downstream
clients retry. In-flight downloads are inode-bound and are never affected. The window is
microseconds.

**The alternative that eliminates this window entirely is mergerfs** — see §5.

---

## 4. Age Computation and the Timezone Bug

Date partitions are named `YYYYMMDD` in UTC. The subtle bug: the CentOS VM may have a local
timezone configured (e.g., UTC+9). Near midnight, `date +%Y%m%d` returns a date that is off by
one day relative to the UTC folder name. The purge and migration age calculations must use
`date -u` (UTC), **never the local timezone**.

Test T1-02 injects a non-UTC container timezone to prove the code path cannot drift.

The same discipline applies to the purge cutoff: `age > retention` (strict greater-than), never
`>=`. On a system with no backup, keeping a partition one extra day is free; deleting it one
day early is unrecoverable.

---

## 5. Decisions Made and Alternatives

### Decision A: Symlink-per-date vs. mergerfs (union mount)

| | Symlink-per-date *(chosen)* | mergerfs |
|--|---|---|
| How two mounts appear as one | Replace the directory with a symlink at migration time | FUSE union: both mounts appear as one namespace, no per-file action | 
| Flip window | Yes — microsecond ENOENT between `mv` and `ln -s` | None — `mv` between branches is atomic within the union |
| New write placement | Fixed to NAS1 (until a placement policy is added) | `most-free-space` policy auto-balances new writes |
| New runtime dependency | None | FUSE daemon — crash darkens the entire SFTP root |
| Read throughput | Kernel NFS path, no overhead | FUSE adds a user/kernel context switch per read |
| Installability | coreutils | Requires a FUSE package; may be blocked on a corporate VM |

**Revisit trigger:** if load testing shows the flip window causes actual client-visible failures,
or if the migration scripting becomes operationally heavy, mergerfs is the principled fix.

### Decision B: Whole date partition vs. per-category migration unit

Per-category would allow finer-grained space reclaim. Rejected because:
- After 4 days, a partition contains *only* long-term data — no mixed-retention reason to split.
- One symlink per date is simpler to track, purge, and reconcile than N per date.
- A partition split across two mounts mid-life creates a mixed-path purge problem.

### Decision C: Local flock vs. NAS-based lockfile

A NAS lockfile could coordinate across multiple VMs. Rejected because:
- Both jobs run on the same VM — no cross-host coordination needed.
- NFS locking (`lockd` / NFSv4 state) has a history of silent failure after server reboots.
- A local `flock` is kernel-enforced, immediate, and reliable on the same host.

---

## 6. Two-Phase Purge (irreversible-op discipline)

The purge `rm -rf` is the one truly unrecoverable operation — NAS data has no backup. The
two-phase discipline:

1. **Dry-run phase first** — log what *would* be deleted, delete nothing. A human reviews the
   output and confirms it matches expectation.
2. **Armed phase only after review** — with strict `age > retention` bias toward keeping.

This pattern — "log before act, bias toward keeping" — applies to any irreversible batch
operation with no backup.

**Purge / symlink interaction:** only the **long-term 70-day purge** ever encounters a
symlinked (migrated) partition. The short-term (4-day) and file-id (1–2 day) purges operate
exclusively on hot NAS1 real directories and need no changes.

The two-phase long-term purge:
1. Delete each category through the symlink at its own retention age.
2. Remove the date symlink + empty NAS2 dir only after the partition is fully drained at
   `max(retention across categories)`.

---

## 7. Key Metrics and Observability Design

- **NAS2 mount health:** a sentinel-file read on each scrape, wired to Alertmanager. NAS2 is
  now a load-bearing SPOF for all migrated partitions; its health must be monitored at the same
  level as NAS1.
- **Watermarks:** high/low percentage thresholds on NAS1 usage. Migration starts above high,
  stops once below low. The thresholds are fixed; adaptivity comes from reading live usage each
  run.
- **NAS2 reserve:** an absolute free-space floor on NAS2. The per-partition fit check
  (`partition_size > NAS2_free − reserve`) uses it. One knob instead of a percentage cap.
- **Prometheus textfile:** `.prom` written atomically (`.tmp` → `mv`) so a concurrent scrape
  never sees a partial file.

---

## 8. Learning Resources

### NFS fundamentals

- **`man nfs(5)`** — Linux NFS mount options. Understand `hard`/`soft`, `actimeo`, `intr`,
  `vers=`, and `noatime`.
- **`man 2 open`, `man 2 unlink`** — POSIX inode vs. path semantics; foundation for why
  silly-rename works.
- **[linux-nfs.org](https://linux-nfs.org/)** — NFS client development docs; covers behavior of
  open fds across unlink.
- **RFC 7530** (NFSv4) and **RFC 1813** (NFSv3) — protocol specs for when you need to understand
  *why* NFS behaves differently from a local FS at the protocol level.

### Silly-rename

- **Linux kernel source `fs/nfs/unlink.c`** — `nfs_async_unlink()` is where silly-rename lives.
  Searching `nfs silly rename site:lwn.net` surfaces solid kernel-level explanations.
- **Brendan Gregg's tracing posts** — silly-rename appears in NFS `open()`/`unlink()` traces.

### Symlinks and chroot in OpenSSH

- **`man sshd_config(5)`** — `ChrootDirectory`, `internal-sftp` requirement, `Match` block.
- **OpenSSH source `session.c`** — shows how `ChrootDirectory` is applied and why
  `internal-sftp` is required.

### Union filesystems

- **[mergerfs GitHub README](https://github.com/trapexit/mergerfs)** — author's documentation
  is unusually thorough. Covers create policies (`most-free-space`, `ff`), FUSE overhead, and
  NFS-over-FUSE caveats.
- **`man 4 fuse`** and **`man 8 mount.fuse`** — understand the user/kernel round-trip cost per
  syscall that FUSE adds.
- **[OverlayFS kernel docs](https://docs.kernel.org/filesystems/overlayfs.html)** — overlayfs is
  copy-on-write (not pooling like mergerfs), but understanding both clarifies when to use each.

### flock and NFS locking

- **`man 2 flock`** — documents explicitly that behavior over NFS is unspecified.
- **`man 2 fcntl` (F_OFD_SETLK)** — POSIX record locks; different semantics from `flock`, also
  unreliable over NFS.
- **"File locking in Linux"** by Dan Rosenberg — covers `flock` vs. `lockf` vs. `fcntl`
  semantics and their failure modes.

### POSIX filesystem atomicity

- **`man 2 rename`** — the atomicity guarantee: no observer sees the old or new path missing.
  Explains why the two-syscall swap (mv + ln -s) is *not* atomic.
- **"Ensuring data reaches disk"** (lwn.net) — covers `fsync`, `rename` durability, and what
  POSIX actually guarantees vs. what NFS provides.

### rsync

- **`man rsync`** — specifically `--checksum`, `--bwlimit`, `--timeout`, `-a` (archive mode).
  The verify gate uses `rsync -an --checksum`: `-n` is dry-run; with `--checksum` it does a full
  byte comparison without transferring data. That is the verify gate.

### bats-core (test framework)

- **[bats-core docs](https://bats-core.readthedocs.io/)** — `@test`, `setup`, `teardown`, `run`,
  and `assert_*` helpers. Understanding these four is enough to read and write all Tier-1 tests.

---

## Summary: The One Concept That Gates Everything

**Inode-bound fds + NFS silly-rename** is the load-bearing invariant of this design. It is what
makes "immediate delete" safe and what T2-01 tests. Every other decision (symlinks, chroot,
local flock, UTC dates, two-phase purge) is real but secondary. If you only have time to go
deep on one thing before reviewing this codebase, make it this.
