#!/usr/bin/env bash
#
# T47: single-node DynaCache measured against a real redis:7, same client, same flags.
#
# Runs three passes against each target -- plain, pipelined (-P 16) and large values (-d 1024)
# -- with the official redis-benchmark out of the redis:7 image, so the client is identical on
# both sides and nothing has to be installed on the machine. Every pass is written to a CSV
# under $TEMP/dynacache-bench/.
#
# Run it from anywhere in Git Bash:  bash DynaCache/bench/single-node.sh
#
# Why the node runs with fsync NEVER: DynaCache answers a write only once its WAL entry is
# durable (C14). Under EVERY_SECOND that means every write waits for the next second's fsync,
# so write throughput is (clients / 1s) and a 100k-request SET pass would take hours. The
# redis:7 container runs with its own default, which is no AOF at all and replies that never
# wait for the disk, so NEVER is the configuration that compares like with like. The cost of
# EVERY_SECOND is measured separately by DURABILITY_REQUESTS below and reported as a finding.
set -uo pipefail

PORT=6390
REDIS_PORT=6391
CONTAINER=dynacache-bench-redis
IMAGE=redis:7
HOST_FROM_CONTAINER=host.docker.internal
CLIENTS=50
# Overridable so a dry run can prove the plumbing in a minute: REQUESTS=1000 LIST_STEP=1000
# DURABILITY_REQUESTS=100 QUIET_BUDGET=5 BENCH_OUT=/tmp/dry bash single-node.sh
REQUESTS=${REQUESTS:-100000}
# The redis-benchmark tests DynaCache's parser has commands for. SADD, SPOP and ZPOPMIN are
# the ones it does not; they are reported as skipped rather than left out silently.
TESTS=ping_inline,ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,lrange_100,hset,zadd,mset
# A short SET pass under EVERY_SECOND. It runs at about (clients / second), so keep it small.
DURABILITY_REQUESTS=${DURABILITY_REQUESTS:-500}
# One round of the list-length confirmation: LPUSH this many elements onto the same key.
LIST_STEP=${LIST_STEP:-20000}
PASS_TIMEOUT=1800
PING_TIMEOUT=60
# The quiet gate before each pass: wait for QUIET_SECONDS in a row with no Java process but
# our own node and at least IDLE_FLOOR percent CPU idle, giving up after QUIET_BUDGET seconds
# and running anyway. Other sessions build on this machine, so a pass taken under contention
# has to be marked as such rather than silently believed.
QUIET_SECONDS=${QUIET_SECONDS:-10}
IDLE_FLOOR=${IDLE_FLOOR:-70}
QUIET_BUDGET=${QUIET_BUDGET:-600}

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT=${BENCH_OUT:-"$(cygpath -u "${TEMP:-/tmp}")/dynacache-bench"}
DATA="$OUT/data"
JAVA_HOME=${JAVA_HOME:-/c/Users/maxch/.jdks/openjdk-22.0.1}
MVN=${MVN:-/c/Users/maxch/.m2/wrapper/dists/apache-maven-3.9.8/af622e91/bin/mvn}
SERVER_JAR="$ROOT/dynacache-server/target/dynacache-server-0.1.0-SNAPSHOT.jar"
CP_FILE="$ROOT/dynacache-server/target/cp-dynacache-server.txt"

NODE_PID=
# How many java.exe on this machine are ours; the quiet gate subtracts it.
OWN_JAVA=0

fail() { echo "FAILED: $*" >&2; exit 1; }

cleanup() {
  stop_node
  docker rm -f "$CONTAINER" >/dev/null 2>&1
}
trap cleanup EXIT

# The jars plus the runtime classpath. dependency:build-classpath runs in the same reactor
# invocation as package, because the sibling modules are only resolvable once they are built.
build() {
  [ -f "$SERVER_JAR" ] && [ -f "$CP_FILE" ] && return 0
  echo "building DynaCache..."
  JAVA_HOME="$JAVA_HOME" "$MVN" -B -o -q -f "$ROOT/pom.xml" -DskipTests package \
    dependency:build-classpath '-Dmdep.outputFile=target/cp-${project.artifactId}.txt' \
    -Dmdep.includeScope=runtime || fail "maven build"
}

# One node in single-node mode: no --peers, no CP group, 16 partitions, a throwaway data dir.
start_node() {
  local fsync=$1
  rm -rf "$DATA"
  mkdir -p "$DATA"
  "$JAVA_HOME/bin/java" \
    -cp "$(cat "$CP_FILE");$(cygpath -w "$SERVER_JAR")" \
    dynacache.server.DynaCacheServerKt "$PORT" 16 "$(cygpath -w "$DATA")" "$fsync" \
    >"$OUT/node-$fsync.log" 2>&1 &
  NODE_PID=$!
  OWN_JAVA=1
  echo "node started (pid $NODE_PID, fsync $fsync), log $OUT/node-$fsync.log"
}

stop_node() {
  OWN_JAVA=0
  [ -n "$NODE_PID" ] || return 0
  kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  NODE_PID=
}

# Waits for a RESP PING through the same container the benchmark runs in, so a green light
# here means the client can reach the port the same way the measurement will.
wait_for_ping() {
  local port=$1 name=$2 waited=0
  while [ "$waited" -lt "$PING_TIMEOUT" ]; do
    if [ "$(docker run --rm "$IMAGE" redis-cli -h "$HOST_FROM_CONTAINER" -p "$port" ping 2>/dev/null)" = "PONG" ]; then
      echo "$name answers PING on $port"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  [ -n "$NODE_PID" ] && cat "$OUT"/node-*.log >&2
  fail "$name never answered PING on port $port"
}

java_count() { tasklist //FI "IMAGENAME eq java.exe" //NH 2>/dev/null | grep -c java.exe; }

# Percent of one sample of total CPU time, rounded. typeperf takes about a second, which is
# also the poll interval, so the wait loop needs no sleep of its own.
cpu_busy() {
  typeperf '\Processor(_Total)\% Processor Time' -sc 1 2>/dev/null |
    grep -E '^"[0-9]' | tail -1 | awk -F'","' '{gsub(/"/,"",$2); printf "%.0f", $2}'
}

# Blocks until the machine has been quiet for QUIET_SECONDS in a row, then records what it saw
# next to the pass name. $2 is how many java.exe are ours: 1 while our node runs, 0 otherwise.
# On budget exhaustion it returns anyway with quiet=no, because a marked number beats no number.
wait_for_quiet() {
  local label=$1 own=$2 waited=0 streak=0 java=0 idle=0
  while [ "$waited" -lt "$QUIET_BUDGET" ]; do
    java=$(java_count)
    idle=$((100 - $(cpu_busy)))
    if [ "$java" -le "$own" ] && [ "$idle" -ge "$IDLE_FLOOR" ]; then
      streak=$((streak + 1))
      if [ "$streak" -ge "$QUIET_SECONDS" ]; then
        echo "$label other_java=$((java - own)) cpu_idle=$idle% quiet=yes" | tee -a "$OUT/load.txt"
        return 0
      fi
    else
      streak=0
    fi
    waited=$((waited + 1))
  done
  echo "$label other_java=$((java - own)) cpu_idle=$idle% quiet=NO_TAKEN_UNDER_CONTENTION" | tee -a "$OUT/load.txt"
}

# One benchmark pass. $1 names the CSV, $2 is the port, the rest are extra redis-benchmark flags.
pass() {
  local name=$1 port=$2
  shift 2
  wait_for_quiet "$name" "$OWN_JAVA"
  echo "-- $name"
  timeout "$PASS_TIMEOUT" docker run --rm "$IMAGE" redis-benchmark \
    -h "$HOST_FROM_CONTAINER" -p "$port" -c "$CLIENTS" -n "$REQUESTS" -t "$TESTS" --csv "$@" \
    >"$OUT/$name.csv" 2>"$OUT/$name.err"
  local status=$?
  [ "$status" -eq 0 ] || { cat "$OUT/$name.err" >&2; fail "$name (exit $status)"; }
  grep -c '^"' "$OUT/$name.csv" >/dev/null || fail "$name produced no rows"
  cat "$OUT/$name.csv"
}

# The passes every target gets: plain, pipelined, large values, and a spread pass.
#
# The spread pass exists because redis-benchmark leaves `__rand_int__` as a literal string
# unless -r is given, so every SET, GET and INCR in the other three passes names one key and
# MSET names that same key ten times. Ten copies of one key are one partition, so without -r
# nothing here crosses a partition boundary and DynaCache's multi-key fan-out is never
# exercised. With -r the ten keys of an MSET land on up to ten partitions, which is the
# measurement ApEngine.fanOut's marked ceiling actually needs.
three_passes() {
  local target=$1 port=$2
  pass "$target-plain" "$port" -d 3
  pass "$target-pipelined" "$port" -d 3 -P 16
  pass "$target-1024b" "$port" -d 1024
  local keep=$TESTS
  TESTS=set,get,incr,mset
  pass "$target-spread" "$port" -d 3 -r 100000
  TESTS=$keep
}

command -v docker >/dev/null || fail "docker is not on PATH"
docker version >/dev/null 2>&1 || fail "docker is installed but not running"
mkdir -p "$OUT"
: >"$OUT/load.txt"
build

echo "=== environment"
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit: $(git -C "$ROOT" rev-parse --short HEAD)"
  echo "java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
  echo "docker: $(docker version --format '{{.Server.Version}}')"
  echo "image: $IMAGE $(docker image inspect "$IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null)"
  echo "flags: -c $CLIENTS -n $REQUESTS -t $TESTS"
} | tee "$OUT/environment.txt"

echo "=== DynaCache (fsync NEVER)"
start_node NEVER
wait_for_ping "$PORT" DynaCache
three_passes dynacache "$PORT"

echo "=== DynaCache durability cost (fsync EVERY_SECOND, SET only, $DURABILITY_REQUESTS requests)"
stop_node
start_node EVERY_SECOND
wait_for_ping "$PORT" DynaCache
wait_for_quiet dynacache-every-second-set "$OWN_JAVA"
timeout "$PASS_TIMEOUT" docker run --rm "$IMAGE" redis-benchmark \
  -h "$HOST_FROM_CONTAINER" -p "$PORT" -c "$CLIENTS" -n "$DURABILITY_REQUESTS" -d 3 -t set --csv \
  >"$OUT/dynacache-every-second-set.csv" 2>&1 || fail "durability pass"
cat "$OUT/dynacache-every-second-set.csv"
stop_node

# The list tests are the ones that fall furthest behind Redis, and the suspect is the
# per-command memory recount, which is O(elements) of the key the command touched. Four
# identical LPUSH passes against one fresh node push onto the same growing key, so the only
# thing that changes between rounds is how long that key is.
echo "=== DynaCache list-length confirmation (fsync NEVER, four LPUSH passes on one key)"
start_node NEVER
wait_for_ping "$PORT" DynaCache
for round in 1 2 3 4; do
  wait_for_quiet "dynacache-listgrowth-$round" "$OWN_JAVA"
  timeout "$PASS_TIMEOUT" docker run --rm "$IMAGE" redis-benchmark \
    -h "$HOST_FROM_CONTAINER" -p "$PORT" -c "$CLIENTS" -n "$LIST_STEP" -d 3 -t lpush --csv \
    >"$OUT/dynacache-listgrowth-$round.csv" 2>&1 || fail "list-growth round $round"
  echo "round $round (mylist reaches $((round * LIST_STEP))): $(tail -1 "$OUT/dynacache-listgrowth-$round.csv")"
done
stop_node

echo "=== redis:7"
docker rm -f "$CONTAINER" >/dev/null 2>&1
docker run -d --name "$CONTAINER" -p "$REDIS_PORT:6379" "$IMAGE" >/dev/null || fail "starting $CONTAINER"
wait_for_ping "$REDIS_PORT" redis:7
three_passes redis "$REDIS_PORT"
docker rm -f "$CONTAINER" >/dev/null

echo
echo "=== load at each pass"
cat "$OUT/load.txt"
echo
echo "done. CSVs are in $OUT"
