#!/usr/bin/env bash
# camp-bench backfill runner for ONE target. From-empty backfill of a fixed block window
# against a shared RPC; measures wall-clock, peak RSS, CPU-seconds, storage bytes/block.
# Usage: run_target.sh <label> <ampd> <ampctl> <subcmd> <db> <flight> <jsonl> <admin> <start> <window>
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/bin
LABEL=$1; AMPD=$2; AMPCTL=$3; SUBCMD=$4; DB=$5; FLIGHT=$6; JSONL=$7; ADMIN=$8; START=$9; WINDOW=${10}
END=$((START + WINDOW))
ROOT=/tmp/bench/$LABEL
RPCPROV=$ROOT/providers/arbitrum_one_rpc.toml
ADMINURL=http://127.0.0.1:$ADMIN
CFG=$ROOT/ampd.toml
LOG=$ROOT/ampd.log
RESULT=/tmp/bench/result-$LABEL.txt

# fresh state: brand-new data dir + brand-new database (avoids any migration race on reuse)
rm -rf "$ROOT/data"/* "$ROOT/manifests"/* 2>/dev/null
docker exec camp-bench-pg sh -lc "dropdb -U bench --force '$DB' 2>/dev/null; createdb -U bench '$DB'" >/dev/null 2>&1
sleep 1

JSONL_LINE="jsonl_addr         = \"127.0.0.1:$JSONL\""
[ "$JSONL" = "none" ] && JSONL_LINE="# jsonl server not available on this target"
cat > "$CFG" <<EOF
data_dir      = "$ROOT/data"
providers_dir = "$ROOT/providers"
manifests_dir = "$ROOT/manifests"
flight_addr        = "127.0.0.1:$FLIGHT"
$JSONL_LINE
admin_api_addr     = "127.0.0.1:$ADMIN"
poll_interval_secs = 1.0
[metadata_db]
url = "postgres://bench:bench@localhost:15433/$DB"
[writer]
compression = "zstd(1)"
[writer.compactor]
active = true
[writer.collector]
active = true
EOF

echo "[$LABEL] starting daemon ($SUBCMD) pinned to cores 0-7..."
# shellcheck disable=SC2086
taskset -c 0-7 "$AMPD" --config "$CFG" $SUBCMD >"$LOG" 2>&1 &
PID=$!
# wait for admin API
for i in $(seq 1 40); do "$AMPCTL" provider ls --admin-url "$ADMINURL" >/dev/null 2>&1 && break; sleep 0.5; done

echo "[$LABEL] register provider + manifest($START) + dataset + deploy(--end-block $END)"
"$AMPCTL" provider register arbitrum-one "$RPCPROV" --admin-url "$ADMINURL" >>"$LOG" 2>&1
"$AMPCTL" manifest generate --kind evm-rpc --network arbitrum-one --start-block "$START" -o "$ROOT/manifests/m.toml" >>"$LOG" 2>&1
"$AMPCTL" dataset register _/arbitrum_one "$ROOT/manifests/m.toml" --tag 1.0.0 --admin-url "$ADMINURL" >>"$LOG" 2>&1

T0=$(date +%s.%N)
"$AMPCTL" dataset deploy _/arbitrum_one@1.0.0 --end-block "$END" --admin-url "$ADMINURL" >>"$LOG" 2>&1

# poll daemon log for backfill completion ("dump completed successfully"/"job completed"). 30-min guard.
DONE=0
for i in $(seq 1 1800); do
  if grep -qiE "dump completed successfully|job completed successfully" "$LOG" 2>/dev/null; then
    DONE=1; break
  fi
  sleep 1
done
T1=$(date +%s.%N)
ELAPSED=$(echo "$T1 - $T0" | bc)

# resolve the actual daemon pid via the admin port (robust to taskset/fork), capture metrics
DPID=$(ss -ltnp 2>/dev/null | grep ":$ADMIN" | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
DPID=${DPID:-$PID}
VMHWM_KB=$(grep VmHWM /proc/$DPID/status 2>/dev/null | awk '{print $2}')
CLK=$(getconf CLK_TCK)
read -r UT ST < <(awk '{print $14, $15}' /proc/$DPID/stat 2>/dev/null)
CPU_SECS=$(echo "scale=1; ($UT + $ST) / $CLK" | bc 2>/dev/null)
STORAGE=$(du -sb "$ROOT/data" 2>/dev/null | awk '{print $1}')
# blocks actually indexed (from postgres physical_table or trust window); use du-based + window
BPS=$(echo "scale=2; $WINDOW / $ELAPSED" | bc)
BPB=$(echo "scale=1; ${STORAGE:-0} / $WINDOW" | bc)
RSS_MB=$(echo "scale=0; ${VMHWM_KB:-0} / 1024" | bc)

{
  echo "label=$LABEL done=$DONE window=$WINDOW [$START,$END)"
  echo "backfill_secs=$ELAPSED  blocks_per_sec=$BPS"
  echo "peak_rss_mb=$RSS_MB  cpu_secs=$CPU_SECS"
  echo "storage_bytes=$STORAGE  bytes_per_block=$BPB"
} | tee "$RESULT"

kill $PID 2>/dev/null; wait $PID 2>/dev/null
echo "[$LABEL] done (done=$DONE)"
