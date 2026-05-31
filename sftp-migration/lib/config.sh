# shellcheck shell=bash
: "${NAS1_ROOT:=/mnt/nas1}"
: "${NAS2_ROOT:=/mnt/nas2}"
: "${NAS2_SENTINEL:=${NAS2_ROOT}/.nas2_sentinel}"
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
