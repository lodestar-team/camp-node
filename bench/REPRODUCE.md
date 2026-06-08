# camp-bench — reproducing the camp-node vs Amp backfill benchmark

MVP harness behind [`../benchmarking-against-amp.md`](../benchmarking-against-amp.md). It runs a
**from-empty backfill of a fixed Arbitrum One window** for one or both indexer binaries, through the
**same RPC**, into a throwaway Postgres + fresh data dir, and reports wall-clock, throughput, peak
RSS, CPU-seconds, on-disk bytes/block, and parquet file count.

> Scope: this is the indicative, single-box MVP — **not** the dedicated-box, n≥5, cold/warm,
> RPC-counting publication run (see RFC #2 / `ROADMAP.md`). A future `crates/bin/camp-bench` Rust CLI
> will add the query-suite (Flight/JSONL/pgwire), an RPC-counting proxy, and cgroup isolation.

## Prerequisites

- Docker (throwaway Postgres).
- The two binaries to compare:
  - **camp-node**: `cargo build -p ampd -p ampctl --release` → `target/release/{ampd,ampctl}`.
  - **amp** (baseline): pull the public image and extract, or install via ampup. The latest *public*
    build is `ghcr.io/edgeandnode/amp:latest` (version label `v0.0.36`); newer builds are private.
- An Arbitrum One RPC endpoint.

## Setup

```sh
# 1) throwaway Postgres on :15433 with two isolated DBs
docker run -d --name camp-bench-pg -p 15433:5432 \
  -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench postgres:16
docker exec camp-bench-pg createdb -U bench bench_camp
docker exec camp-bench-pg createdb -U bench bench_amp

# 2) provider config (set your RPC) for each target's data dir
mkdir -p /tmp/bench/camp/{data,providers,manifests} /tmp/bench/amp/{data,providers,manifests}
cp bench/providers/arbitrum_one_rpc.toml.example /tmp/bench/camp/providers/arbitrum_one_rpc.toml
cp bench/providers/arbitrum_one_rpc.toml.example /tmp/bench/amp/providers/arbitrum_one_rpc.toml
# edit both: set url = "<your arbitrum rpc>"
```

## Run

`run_target.sh <label> <ampd> <ampctl> <subcmd> <db> <flight> <jsonl|none> <admin> <start_block> <window>`

```sh
# camp-node (uses the `dev` subcommand; serves Flight+JSONL+pgwire)
bash bench/run_target.sh camp \
  ./target/release/ampd ./target/release/ampctl "dev" \
  bench_camp 1902 1903 1910 471200000 5000

# amp v0.0.36 (uses the `solo` subcommand; Flight only — pass `none` for jsonl)
bash bench/run_target.sh amp \
  /usr/local/bin/ampd /usr/local/bin/ampctl "solo --flight-server --admin-server" \
  bench_amp 1922 none 1930 471200000 5000
```

Each run prints metrics to stdout and `/tmp/bench/result-<label>.txt`. The driver: resets the DB
(`dropdb --force` + `createdb`) and data dir, starts the daemon pinned to cores 0–7, registers the
provider + a manifest at `--start-block S`, deploys with `--end-block S+window`, polls the daemon log
for `dump/job completed successfully`, then reads peak RSS (`/proc/<pid>/status` `VmHWM`) and
CPU (`/proc/<pid>/stat`), `du`s the data dir, and kills the daemon.

## Multi-run (medians)

`multirun.sh` runs n=3 **interleaved** (camp, amp, camp, amp, …) over a 5,000-block window so both
targets share the same RPC/network conditions, writing `/tmp/bench/multi.csv`:

```sh
bash bench/multirun.sh
column -t -s, /tmp/bench/multi.csv
```

## Notes / honesty

- **RPC-bound**: backfill time is dominated by the shared RPC at identical batch/concurrency
  settings. Interleaving shares conditions, but provider variance remains — report dispersion, not
  just a point. (We saw amp dip ~18% on one run purely from RPC variance.)
- **Same window ⇒ deterministic storage**: bytes/block is exact per target across runs.
- **Capability gap**: amp v0.0.36 serves Flight only (no JSONL/pgwire), so a head-to-head *query*
  benchmark can only use Flight; JSONL/pgwire are camp-only on that baseline.
- Pin + date-stamp the amp version for any published comparison.
