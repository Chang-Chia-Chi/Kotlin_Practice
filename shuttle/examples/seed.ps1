# Drops one sample CSV into the vendor SFTP server's /drop directory, the way the vendor would.
#
#   pwsh shuttle/examples/seed.ps1
#   pwsh shuttle/examples/seed.ps1 -File .\my-file.csv -Directory outbound   # feed the mirror route instead
#
# The file name has to match the route's extract regex, `(?<orderNumber>\d+)-.*\.csv`, or the transfer is
# REJECTED at the first step - which is itself worth seeing once.

param(
    [string] $File      = (Join-Path $PSScriptRoot 'sample\123-order.csv'),
    [string] $Directory = 'drop',
    [string] $Container = 'shuttle-example-sftp'
)

$ErrorActionPreference = 'Stop'

$name = Split-Path $File -Leaf
docker cp $File "${Container}:/home/vendor/$Directory/$name"
# docker cp writes as root; the vendor user needs to own it to move it into temp/ on ack.
docker exec $Container chown vendor "/home/vendor/$Directory/$name"
docker exec $Container ls -l "/home/vendor/$Directory"
