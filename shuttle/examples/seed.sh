#!/usr/bin/env sh
# The POSIX twin of seed.ps1. Only the PowerShell one is verified on this machine.
#
#   sh shuttle/examples/seed.sh [file] [directory]
set -eu
here=$(dirname "$0")
file=${1:-"$here/sample/123-order.csv"}
directory=${2:-drop}
container=${SHUTTLE_SFTP_CONTAINER:-shuttle-example-sftp}
name=$(basename "$file")
docker cp "$file" "$container:/home/vendor/$directory/$name"
docker exec "$container" chown vendor "/home/vendor/$directory/$name"
docker exec "$container" ls -l "/home/vendor/$directory"
