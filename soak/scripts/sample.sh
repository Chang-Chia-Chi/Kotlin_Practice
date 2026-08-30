#!/bin/bash
# Samples the running soak every 30s into soak/run/samples.csv. Run detached (nohup) from WSL
# for the full soak duration; the foreground session just checks in on the file periodically.
set -uo pipefail
cd "$(dirname "$0")/.."   # soak/
CSV=run/samples.csv
PIDFILE=run/app.pid

if [ ! -f "$CSV" ]; then
  echo "ts,elapsed_s,ready_state,http_code,rss_kb,open_fds,meter_count,wip_gen_bytes,wip_wal_bytes,equipment_gen_bytes,equipment_wal_bytes,scratch_run_dirs,snapshot_current_gen_wip,snapshot_current_gen_equipment,snapshot_live_gen_wip,snapshot_live_gen_equipment,note" > "$CSV"
fi

T0=$(date +%s)
if [ -f run/t0 ]; then T0=$(cat run/t0); else echo "$T0" > run/t0; fi

while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  now=$(date +%s)
  elapsed=$((now - T0))

  pid=$(cat "$PIDFILE" 2>/dev/null || echo "")
  if [ -n "$pid" ] && [ -d "/proc/$pid" ]; then
    rss_kb=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null || echo "")
    open_fds=$(ls /proc/$pid/fd 2>/dev/null | wc -l)
  else
    rss_kb=""
    open_fds=""
  fi

  ready_body=$(curl -s -m 5 -o /tmp/ready.$$ -w '%{http_code}' http://localhost:8080/health/ready)
  http_code="$ready_body"
  ready_state=$(grep -o '"state":"[a-z-]*"' /tmp/ready.$$ 2>/dev/null | cut -d'"' -f4)
  rm -f /tmp/ready.$$

  metrics=$(curl -s -m 5 http://localhost:8080/q/metrics 2>/dev/null)
  meter_count=$(echo "$metrics" | grep -vc '^#')
  cur_wip=$(echo "$metrics" | grep 'snapshot_current_generation{group="wip"}' | awk '{print $2}')
  cur_equip=$(echo "$metrics" | grep 'snapshot_current_generation{group="equipment"}' | awk '{print $2}')
  live_wip=$(echo "$metrics" | grep 'snapshot_live_generations{group="wip"}' | awk '{print $2}')
  live_equip=$(echo "$metrics" | grep 'snapshot_live_generations{group="equipment"}' | awk '{print $2}')

  wip_gen_bytes=$(du -cb run/state/cache/wip/*.db 2>/dev/null | tail -1 | awk '{print $1}')
  wip_wal_bytes=$(du -cb run/state/cache/wip/*.wal 2>/dev/null | tail -1 | awk '{print $1}')
  equip_gen_bytes=$(du -cb run/state/cache/equipment/*.db 2>/dev/null | tail -1 | awk '{print $1}')
  equip_wal_bytes=$(du -cb run/state/cache/equipment/*.wal 2>/dev/null | tail -1 | awk '{print $1}')
  scratch_dirs=$(ls run/state/scratch 2>/dev/null | wc -l)

  note="${1:-}"
  echo "$ts,$elapsed,$ready_state,$http_code,$rss_kb,$open_fds,$meter_count,${wip_gen_bytes:-0},${wip_wal_bytes:-0},${equip_gen_bytes:-0},${equip_wal_bytes:-0},$scratch_dirs,${cur_wip:-},${cur_equip:-},${live_wip:-},${live_equip:-},$note" >> "$CSV"

  sleep 30
done
