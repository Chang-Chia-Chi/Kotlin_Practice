#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."   # soak/
mkdir -p run/logs
nohup bash scripts/sample.sh > run/logs/sample.log 2>&1 &
echo $! > run/sample.pid
disown
sleep 2
echo "sampler pid $(cat run/sample.pid)"
