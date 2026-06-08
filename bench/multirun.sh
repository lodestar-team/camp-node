#!/usr/bin/env bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/bin
START=471200000; WIN=5000; N=3
: > /tmp/bench/multi.csv
echo "target,run,backfill_secs,blocks_per_sec,peak_rss_mb,cpu_secs,bytes_per_block,done" >> /tmp/bench/multi.csv
for run in $(seq 1 $N); do
  for t in camp amp; do
    if [ "$t" = camp ]; then
      bash /tmp/bench/run_target.sh camp /home/pepe/amp-source/target/release/ampd /home/pepe/amp-source/target/release/ampctl "dev" bench_camp 1902 1903 1910 $START $WIN >/dev/null 2>&1
    else
      bash /tmp/bench/run_target.sh amp /usr/local/bin/ampd /usr/local/bin/ampctl "solo --flight-server --admin-server" bench_amp 1922 none 1930 $START $WIN >/dev/null 2>&1
    fi
    R=/tmp/bench/result-$t.txt
    secs=$(grep -oE 'backfill_secs=[0-9.]+' $R | cut -d= -f2)
    bps=$(grep -oE 'blocks_per_sec=[0-9.]+' $R | cut -d= -f2)
    rss=$(grep -oE 'peak_rss_mb=[0-9]+' $R | cut -d= -f2)
    cpu=$(grep -oE 'cpu_secs=[0-9.]+' $R | cut -d= -f2)
    bpb=$(grep -oE 'bytes_per_block=[0-9.]+' $R | cut -d= -f2)
    done=$(grep -oE 'done=[01]' $R | head -1 | cut -d= -f2)
    echo "$t,$run,$secs,$bps,$rss,$cpu,$bpb,$done" >> /tmp/bench/multi.csv
    echo "[$t run $run] secs=$secs bps=$bps rss=$rss done=$done"
  done
done
echo "MULTIRUN_DONE"
