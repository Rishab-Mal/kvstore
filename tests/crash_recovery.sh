#!/usr/bin/env bash
set -euo pipefail

SERVER=${1:?usage: crash_recovery.sh path/to/kvserver [fsync-policy]}
POLICY=${2:-everysec}
PORT=${PORT:-6391}
N=${N:-10000}

DIR=$(mktemp -d)
PID=""
cleanup() {
    if [[ -n "$PID" ]]; then kill -9 "$PID" 2>/dev/null || true; fi
    rm -rf "$DIR"
}
trap cleanup EXIT

start_server() {
    "$SERVER" --port "$PORT" --aof "$DIR/data.aof" --fsync "$POLICY" >>"$DIR/server.log" 2>&1 &
    PID=$!
    for _ in $(seq 50); do
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "FAIL: server did not start"; cat "$DIR/server.log"; exit 1
}

start_server
acks=$(seq 1 "$N" | awk '{ print "SET key:" $1 " value:" $1 }' | nc -N 127.0.0.1 "$PORT" | grep -c '^OK$' || true)
if [[ "$acks" -ne "$N" ]]; then
    echo "FAIL: only $acks/$N writes were acknowledged"; exit 1
fi

{ kill -9 "$PID" && wait "$PID"; } 2>/dev/null || true
PID=""

start_server
found=$(seq 1 "$N" | awk '{ print "GET key:" $1 }' | nc -N 127.0.0.1 "$PORT" |
    awk '$0 == "VALUE value:" NR { ok++ } END { print ok + 0 }')

if [[ "$found" -ne "$N" ]]; then
    echo "FAIL: recovered $found/$N keys after kill -9 (fsync=$POLICY)"
    cat "$DIR/server.log"
    exit 1
fi
echo "PASS: recovered $found/$N keys after kill -9 (fsync=$POLICY)"
