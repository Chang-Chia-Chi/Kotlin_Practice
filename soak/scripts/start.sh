#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."   # soak/
mkdir -p run/logs
rm -f run/logs/app.log run/app.pid
nohup bash scripts/run-app.sh > run/logs/app.log 2>&1 &
echo $! > run/app.pid
disown
sleep 1
echo "started pid $(cat run/app.pid)"
