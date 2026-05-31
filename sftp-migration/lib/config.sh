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

# Relative target prefix for date symlinks. NAS2 is reachable under the root as
# .nas2 (a bind mount in prod), so a date symlink points to .nas2/<date> and
# resolves inside any chroot. Trailing slash intentional.
: "${SYMLINK_REL_PREFIX:=.nas2/}"
