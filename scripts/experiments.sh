#!/usr/bin/env bash
# Producer-throughput experiments on the first 50M ITCH messages, 4 consumers (see PERF_LOG).
set -euo pipefail
F=${1:?ITCH file}
B=${BUILD:-build}
echo "# $(date -u +%FT%TZ) load: $(uptime | sed 's/.*load averages: //')"
run() { echo -n "$* -> "; "$B/shmcast-itch" "$F" --limit 50000000 "$@" | grep -o '"producer_msgs_per_sec": [0-9]*\|"ok": [a-z]*' | tr '\n' ' '; echo; }
run --consumers 0 --slow 0 --slot 80
run --consumers 0 --slow 0 --slot 128
for i in 1 2; do
  run --consumers 4 --slow 0 --slot 80
  run --consumers 4 --slow 0 --slot 128
done
for bo in 0 16 64 256; do run --consumers 4 --slow 0 --slot 80 --backoff $bo; done
