# 1. Symlink-per-date for NAS unification, not a union mount

Date: 2026-05-27

## Status

Accepted

## Context

NAS1 (25T) is 90% full. A second NAS, NAS2 (10T), is being added. Downstream
clients construct SFTP paths themselves from `filename + upload date`
(`/{date}/{category}/{filename}`) — the database does NOT store a physical path.
Therefore both NAS mounts must appear as a single unified tree under the SFTP
root, and no client-side change is acceptable.

Old date partitions (>4 days) contain only long-term data (~200G) because
short-term categories have been purged. These are migrated from NAS1 to NAS2 to
reclaim space.

Two mechanisms can present two physical NAS mounts as one logical tree:

1. **Symlink-per-date** — migrate a date partition's bytes to NAS2, then replace
   `NAS1/<date>` with a symlink pointing to `NAS2/<date>`.
2. **Union/pooled mount (mergerfs)** — point the SFTP root at a FUSE union of
   NAS1 + NAS2; the logical path is decoupled from the physical branch, so moving
   a partition between mounts is invisible to clients.

## Decision

Use **symlink-per-date** (option 1).

The symlink swap is performed as: `rsync` the partition to NAS2 → verify →
`mv NAS1/<date> NAS1/.<date>.bak` then `ln -s /mnt/nas2/<date> NAS1/<date>` →
lsof-gated deferred deletion of the `.bak` copy.

## Consequences

**Positive:**
- No new runtime component. The symlink approach has no daemon that can fail and
  black out the entire SFTP root.
- No FUSE overhead; reads go straight through the kernel NFS client.
- Already validated — symlinks are known to work on this VM.

**Negative:**
- A sub-millisecond window exists between `mv` and `ln -s` where the path
  `/{date}` does not exist. A brand-new `open()` landing exactly in that gap
  fails with ENOENT. **We are explicitly throwing transient ENOENT errors at
  any client unlucky enough to hit that window** — accepted because downstream
  clients already retry and only long-term files (5–70 days old, rarely
  downloaded) are affected. In-flight downloads are never affected (open fds
  are inode-bound and survive the swap).
- The two-syscall floor is unavoidable in plain bash. **Escape hatch if the
  window ever bites in practice:** `renameat2(RENAME_EXCHANGE)` atomically
  swaps two paths in a single syscall — but it requires Linux 3.15+ AND
  filesystem support; **NFS support is implementation-dependent** (often
  returns EINVAL/EOPNOTSUPP on remote exports), so verify on the target NAS
  before committing to it. A tiny C helper or Python wrapper would be needed.
- Migration logic lives in a script that must be correct (atomic-ish swap,
  ENOTEMPTY-tolerant `rm -rf`, sentinel-via-bind-mount guard) rather than
  being handled structurally by the filesystem.

## Alternative considered: union mount (mergerfs)

mergerfs pools both NAS mounts into one namespace; migrating a partition becomes a
plain `mv` on the underlying branch with zero change to the logical path —
**eliminating the flip window entirely** — and a `most-free-space` create policy
would auto-balance new writes.

Rejected for now due to cost: it adds a FUSE layer stacked on top of NFS (real,
must-be-load-tested throughput overhead) and a critical-path daemon whose failure
darkens the whole SFTP root.

**Revisit if:** load testing shows the flip window actually causes client-visible
failures, or if manual placement/migration scripting becomes operationally heavy.
mergerfs is the principled fix at that point.
