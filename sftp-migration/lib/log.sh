# shellcheck shell=bash
log()  { printf '%s [INFO] %s\n'  "$(date -u +%FT%TZ)" "$*"; }
warn() { printf '%s [WARN] %s\n'  "$(date -u +%FT%TZ)" "$*" >&2; }
die()  { printf '%s [ERROR] %s\n' "$(date -u +%FT%TZ)" "$*" >&2; exit 1; }
