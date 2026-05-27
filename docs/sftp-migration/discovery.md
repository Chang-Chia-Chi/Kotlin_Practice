# SFTP Service Discovery Checklist

Run these commands on the CentOS-8 VM as root (or with sudo).
Record the output — it determines which migration strategy is safe.

---

## 1. Identify the SFTP server software

```bash
rpm -qa | grep -E 'openssh|vsftpd|proftpd'
```
Expected: `openssh-server-x.x.x` means OpenSSH. `vsftpd-x.x.x` means vsftpd.

```bash
systemctl list-units --type=service --state=running | grep -E 'ssh|ftp'
```
Expected: `sshd.service` (OpenSSH) or `vsftpd.service`.

```bash
ps aux | grep -E 'sshd|vsftpd|proftpd|sftp-server'
```
Confirms what is actually running right now.

> **Note:** If downstream clients connect on port 22 using the SFTP protocol (not FTP),
> it is OpenSSH — vsftpd does not speak SFTP.

---

## 2. Identify the SFTP subsystem (OpenSSH only)

```bash
grep -E 'Subsystem|sftp' /etc/ssh/sshd_config
```

Two common results:
- `Subsystem sftp /usr/lib/openssh/sftp-server` — external binary
- `Subsystem sftp internal-sftp` — built-in, required for chroot to work

---

## 3. Check for chroot jail configuration (OpenSSH only)

```bash
grep -A 20 'Match' /etc/ssh/sshd_config
```

Look for `ChrootDirectory`. Record the value exactly — it may use `%h` (home dir) or an absolute path.

Examples:
- `ChrootDirectory /data/sftp` — all users jailed to `/data/sftp`
- `ChrootDirectory %h` — each user jailed to their own home directory
- *(absent)* — no chroot, clients see the real filesystem

> **Why this matters:** Symlinks whose targets are outside the chroot directory are
> invisible to SFTP clients. Bind mounts work correctly inside a chroot.

---

## 4. Identify the current NAS mount(s)

```bash
mount | grep -E 'nfs|cifs|nfs4'
```

```bash
nfsstat -m
```
Shows each NFS mount's negotiated options. The `Flags:` line contains `vers=` (e.g., `vers=4.2`) — record it. Determines NFS v3 vs v4 (locking/rename differences) and the options the Option-B test mount must match.

```bash
findmnt -t nfs,nfs4 -o SOURCE,TARGET,FSTYPE,OPTIONS
```

```bash
df -h | grep -v tmpfs
```

```bash
cat /etc/fstab
```

Record:
- Mount point path (e.g., `/mnt/nas1`)
- Filesystem type — **NFS vs CIFS is critical** (CIFS has no silly-rename; changes the safety design)
- NFS version (`vers=3` vs `vers=4.x`)
- Remote server and share path
- Mount options (especially `ro`/`rw`, `noatime`, `soft`/`hard`, `actimeo` for NFS)

> **Test fidelity:** the Option-B self-hosted NFS server must be mounted on the
> test client with the SAME `vers=` and `hard`/`soft` options as prod.

---

## 5. Identify the root path and file structure

```bash
ls -la /path/to/sftp/root/
```
(Replace with the actual root path from the chroot or SFTP config.)

```bash
ls /path/to/sftp/root/ | head -20
```
Shows date-partition folder names and naming convention (e.g., `20240101`, `2024-01-01`).

```bash
du -sh /path/to/sftp/root/*/
```
Shows size per date partition — helps decide which partitions to migrate first.

---

## 6. Check for active SFTP sessions

```bash
who
```

```bash
ss -tnp | grep ':22'
```

```bash
# If using OpenSSH, list open files on the NAS mount:
lsof +D /mnt/nas1 2>/dev/null
```

> **Why this matters:** Confirms whether clients are actively reading files.
> Any migration window should be planned when this is empty or minimal.

### Network capacity (sets `--bwlimit`)

```bash
ip -br link                                   # find the NIC name (e.g., ens192)
ethtool <iface> | grep Speed                  # link speed: 1000Mb/s = 1GbE, 10000Mb/s = 10GbE
```

### Live load signal (for the load-gated backfill) + traffic pattern

```bash
# Active SFTP/SSH connection count — the gate signal:
ss -tn state established '( sport = :22 )' | grep -c ':22'
```

```bash
# Sample over time to learn the daily peak/quiet pattern (NIC throughput):
sar -n DEV 1 5      # if sysstat installed; watch rxkB/s + txkB/s on the NAS-facing NIC
```

> Sample the connection count / `sar` a few times across a day to confirm the
> assumed business-hours-peak / overnight-quiet pattern before scheduling batches.

---

## 6b. Access model, permission parity, same-appliance check

```bash
# Is NAS2 the same appliance as NAS1? Compare the server host/IP of each mount:
nfsstat -m        # or: findmnt -t nfs,nfs4 -o SOURCE,TARGET
```
Same server host/IP for both → same appliance → UID/GID/idmap/squash inherited (parity free).
Different host → must actively align idmap domain, GIDs, squash, NFS version.

```bash
# Numeric ownership + mode of existing files (what NAS2 must reproduce):
ls -ln /path/to/sftp/root/<date>/<category>/ | head
```

```bash
# The common downstream group granting read (resolve its GID — must match on NAS2):
getent group <sftp_group>
```

> **Per-user login:** a GID/idmap mismatch on NAS2 breaks downloads for ALL users.
> Verify gate must stat-compare uid/gid/mode; canary must download as a real user.

## 7. Check OS-level filesystem capabilities

```bash
cat /proc/filesystems | grep -E 'overlay|bind'
```

```bash
uname -r
```

```bash
modprobe overlay && echo "overlay supported"
```

> Bind mounts and overlayfs are the preferred migration mechanism.
> These require kernel support, which CentOS-8 ships with by default (kernel 4.18+).

---

## Summary: What to bring back

| Item | Command | Why needed |
|------|---------|------------|
| SFTP server software | `rpm -qa` | Determines available options |
| Subsystem type | `grep Subsystem sshd_config` | `internal-sftp` required for chroot |
| ChrootDirectory value | `grep -A20 Match sshd_config` | Determines symlink vs bind-mount safety |
| Current mount points | `mount` + `fstab` | Know what we're working with |
| Date partition naming | `ls sftp root` | Migration planning |
| Partition sizes | `du -sh` | Prioritize what to move |
| Active session count | `ss -tnp` | Plan migration window |
