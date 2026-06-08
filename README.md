# camp-node

**The indexing engine behind [camp](https://github.com/lodestar-team/camp) / [engine.camp](https://engine.camp).**
camp-node turns a raw blockchain JSON-RPC endpoint into a queryable, columnar SQL
database — it extracts blocks/transactions/logs, stores them as Parquet, and serves
them over Arrow Flight, JSON Lines, and the Postgres wire protocol. It is built and run **from source we can read,
pin, and patch**, so engine.camp never depends on a prebuilt closed-source binary.

> **Provenance & attribution.** camp-node is built on **Amp**, the blockchain-native
> database by **Edge & Node Ventures, Inc.** ([ampup.sh](https://ampup.sh/docs)), used under
> the Business Source License 1.1. (E&N distributes Amp via ampup.sh; its source is not
> currently published at a public URL — the BUSL `LICENSE` shipped with the code is the
> governing grant.)
> It is an independent, source-built distribution maintained by lodestar-team — **not
> affiliated with, sponsored by, or endorsed by Edge & Node Ventures or The Graph.**
> "Amp" is their name; the executables keep the upstream names `ampd`/`ampctl`/`ampsync`
> for compatibility, but camp-node is versioned independently (see
> [Versioning](#versioning)) and licensed under BUSL-1.1 (see [License](#license)).

---

## What camp-node does

camp-node is a **high-performance ETL engine for blockchain data.** It turns a raw
JSON-RPC endpoint (or other chain data source) into a queryable, columnar analytics
database. The pipeline is four stages:

```
        EXTRACT                TRANSFORM              STORE                 SERVE
   ┌──────────────┐      ┌──────────────────┐   ┌──────────────┐    ┌────────────────────┐
   │  EVM JSON-RPC │ ───▶ │  Apache DataFusion │─▶ │  Apache Parquet│ ─▶ │ Arrow Flight (gRPC) │
   │  (eth_getBlock│      │  SQL + EVM UDFs    │   │  columnar files│    │ JSON Lines (HTTP)   │
   │   / receipts) │      │                    │   │  on local SSD  │    │                     │
   └──────────────┘      └──────────────────┘   └──────────────┘    └────────────────────┘
```

1. **Extract** — a *provider* (e.g. `evm-rpc`, or `pinax` for Firehose→Parquet) pulls
   blocks, transactions, and logs from a
   chain's JSON-RPC endpoint, in batched/concurrent requests, resumable from the last
   indexed block.
2. **Transform** — data is processed with SQL through [Apache DataFusion](https://datafusion.apache.org/),
   extended with blockchain-specific user-defined functions (decode logs, hash event
   signatures, call contracts at query time).
3. **Store** — output is written as [Apache Parquet](https://parquet.apache.org/) (columnar,
   compressed, splittable) on local SSD or any object store — S3, GCS, Azure, or **Cloudflare
   R2** (see [`docs/config.md`](./docs/config.md)) — with a background **compactor** that merges
   small files and a **collector** that garbage-collects superseded ones.
4. **Serve** — query the data over **Arrow Flight** (gRPC, high-throughput binary), a
   **JSON Lines HTTP** endpoint (POST raw SQL, get one JSON object per row), or the
   **Postgres wire protocol** (psql / Grafana / BI tools; opt-in via `--pg-server`).

A PostgreSQL **metadata database** tracks datasets, manifests, providers, jobs, workers,
file metadata, and indexing progress, and coordinates distributed workers via
`LISTEN/NOTIFY`.

### Why a database, not just an indexer

Because the storage layer is Parquet and the query layer is DataFusion SQL, you query
on-chain data the way you'd query any analytics warehouse — `SELECT … WHERE … GROUP BY …`
over `blocks`, `transactions`, and `logs` — with full SQL (joins, aggregates, window
functions) and sub-second columnar scans, instead of writing a bespoke indexer per use case.

---

## Data sources

camp-node extracts via pluggable **provider kinds** (`ampctl manifest generate --kind <kind>`):

- **`evm-rpc`** — pulls blocks/transactions/logs from any Ethereum-compatible JSON-RPC
  endpoint (batched, resumable). The default for tip-following a chain.
- **`pinax`** — reads [Pinax](https://pinax.network)'s public Firehose→Parquet datasets
  (S3) and materialises them in-engine. Serves the standard EVM tables (`blocks`,
  `transactions`, `logs`) **plus** the **full-instrumentation** set a JSON-RPC indexer can't
  produce: `calls` (internal-tx traces), `storage_changes`, `balance_changes` (with reason),
  `code_changes`, and `nonce_changes` — eight tables, one `RawDatasetRows` per block. Files
  overlapping a block range are located by binary search over the chronologically-sorted
  listing (not a full scan), so a range doesn't read years of history. Provider config points
  at the public bucket; no API key. (Non-EVM chains follow on the same path.)
- `firehose`, `solana`, `eth-beacon` — additional source kinds.

Every kind feeds the same store/serve layer, so datasets from different sources are queried
identically.

---

## Data model

Each chain is a **dataset**, addressed as `namespace/name@version` (e.g.
`_/arbitrum_one@1.0.0`). An `evm-rpc` raw dataset materializes three tables. Query them as
`"namespace/name@version".table`.

| Table | One row per | Key columns |
|-------|-------------|-------------|
| **blocks** | block | `block_num`, `timestamp`, `hash`, `parent_hash`, `miner`, `gas_limit`, `gas_used`, `base_fee_per_gas`, `state_root`, `receipt_root`, … |
| **transactions** | transaction | `block_num`, `tx_index`, `tx_hash`, `from`, `to`, `value`, `gas_limit`, `gas_used`, `gas_price`, `max_fee_per_gas`, `input`, `nonce`, `type`, `status` |
| **logs** | event log | `block_num`, `tx_hash`, `tx_index`, `log_index`, `address`, `topic0`–`topic3`, `data` |

Types are native Arrow: addresses/hashes are `FixedSizeBinary` (20/32 bytes), `value`/gas
prices are `Decimal128(38,0)`, `timestamp` is a nanosecond `Timestamp`. A
`generate`d manifest pins the exact Arrow schema, the `start_block`, and whether to index
finalized blocks only.

---

## Query interfaces

All three frontends run the same DataFusion engine; pick the wire format you want.

**JSON Lines over HTTP** — POST a raw SQL string, get one JSON object per result row:

```sh
curl -X POST http://localhost:1603 \
  --data 'SELECT block_num, gas_used FROM "_/arbitrum_one@1.0.0".blocks ORDER BY block_num DESC LIMIT 5'
```

**Arrow Flight (gRPC, FlightSQL)** — wrap SQL in a `CommandStatementQuery`, stream Arrow
record batches back. High-throughput; used by the Python client and BI connectors.

**Postgres wire protocol** *(read-only)* — connect any Postgres client or BI tool (psql,
Grafana, Metabase, DBeaver) directly. Enable with `--pg-server` (off by default):

```sh
ampd --config camp.toml server --pg-server 127.0.0.1:5432
psql -h 127.0.0.1 -p 5432 -d amp \
  -c 'SELECT count(*) FROM "_/arbitrum_one@1.0.0"."blocks";'
```

Each dataset (`namespace/name@version`) is exposed as a Postgres *schema* and each of its
tables as a relation, so qualify with quoted identifiers: `"_/eth@1.0.0"."blocks"`. The
engine's EVM UDFs (e.g. `evm_topic`, `evm_decode_log`) and `pg_catalog`/`information_schema`
introspection are available, so BI tools can browse the schema natively. The catalog refreshes
in the background as the chain advances. Writes (DDL/DML) are rejected.

### EVM user-defined functions

SQL is extended with blockchain primitives, so decoding happens *inside the query*:

| UDF | Purpose |
|-----|---------|
| `evm_topic('Transfer(address,address,uint256)')` | keccak topic0 hash of an event signature |
| `evm_decode_log(topic1, topic2, topic3, data, '<event sig>')` | decode an event log into a struct of its parameters |
| `eth_call(...)` | execute a contract read at query time |
| `evm_decode_params` / `evm_encode_params` | ABI-decode / encode call data |

This is how a single raw `logs` table powers decoded views (ERC-20 transfers, DEX swaps,
protocol events) without deploying a separate decoded dataset — you decode inline:

```sql
SELECT evm_decode_log(topic1, topic2, topic3, data,
         'Transfer(address,address,uint256)') AS transfer
FROM   "_/arbitrum_one@1.0.0".logs
WHERE  topic0 = evm_topic('Transfer(address,address,uint256)')
LIMIT  10
```

---

## Components

One binary, `ampd`, runs any role; `ampctl` is the operator CLI. A single box can run
everything (`ampd dev`); production can split server/controller/workers across hosts.

### `ampd` subcommands

| Command | Role |
|---------|------|
| `ampd dev` | **All-in-one** single process: controller (Admin API) + query servers (Flight + JSON Lines) + an in-process worker. The single-box mode. Add `--pg-server` for the Postgres-wire endpoint. |
| `ampd server` | Query servers only (`--flight-server`, `--jsonl-server`, `--pg-server`). |
| `ampd controller` | Controller + Admin API (scheduling, dataset/provider/job management). |
| `ampd worker --node-id <id>` | A distributed extraction/materialization worker. |
| `ampd migrate` | Run metadata-DB migrations. |

Default ports (configurable): Arrow Flight `1602`, JSON Lines `1603`, Admin API `1610`.
The Postgres-wire endpoint is opt-in via `--pg-server` (bare flag → `127.0.0.1:5432`).

### `ampctl` (operator CLI)

| Group | What it manages |
|-------|-----------------|
| `ampctl provider` | provider configs (RPC endpoints, batching, auth) — `register`, `ls`, `inspect`, `rm` |
| `ampctl manifest` | dataset manifests in content-addressable storage — `generate`, `register`, `list`, … |
| `ampctl dataset` | datasets — `register`, `deploy` (start syncing), `list`, `versions`, `inspect`, `restore` |
| `ampctl job` | extraction/materialization jobs |
| `ampctl worker` | worker nodes |

Typical bring-up of a chain:

```sh
ampctl provider register arbitrum-one ./providers/arbitrum_one_rpc.toml
ampctl manifest generate --kind evm-rpc --network arbitrum-one --start-block <N> -o ./manifest.toml
ampctl dataset register _/arbitrum_one ./manifest.toml --tag 1.0.0
ampctl dataset deploy  _/arbitrum_one@1.0.0          # continuous — tip-follows forever
```

---

## Configuration

`ampd` takes one TOML config (via `--config` or `AMP_CONFIG`):

```toml
data_dir      = "/var/lib/ampd/data"        # Parquet output
providers_dir = "/var/lib/ampd/providers"   # one TOML per data source
manifests_dir = "/var/lib/ampd/manifests"   # dataset manifests

flight_addr    = "127.0.0.1:1602"
jsonl_addr     = "127.0.0.1:1603"
admin_api_addr = "127.0.0.1:1610"

[metadata_db]
url = "postgres://ampd:<pw>@localhost/ampd"  # auto_migrate defaults true

[writer]
compression = "zstd(1)"
[writer.compactor]                            # on by default; shown for clarity
active = true
[writer.collector]                            # GC of superseded files; on by default
active = true
```

A provider config (`providers_dir/arbitrum_one_rpc.toml`):

```toml
name    = "arbitrum-one"
kind    = "evm-rpc"
network = "arbitrum-one"
url     = "https://your-arbitrum-rpc"
rpc_batch_size           = 10   # batched JSON-RPC; >1 enables fast path
concurrent_request_limit = 10
# auth_header / auth_token if your RPC needs them
```

---

## How camp uses camp-node

[camp](https://github.com/lodestar-team/camp) ([engine.camp](https://engine.camp)) is a
free, no-key public REST/SQL API over an Arbitrum One dataset. It runs **this engine's
`ampd`** as the indexer: the `evm-rpc` provider tip-follows Arbitrum One into
`blocks`/`transactions`/`logs`, the camp gateway issues SQL to the JSON Lines server, and
decoded endpoints (transfers, Uniswap V3, Graph Horizon, …) are built from inline
`evm_decode_log` over the raw `logs` table — no separate decoded datasets. camp's free,
non-competitive use is permitted under Amp's BUSL Additional Use Grant (see
[License](#license)).

---

## Build from source

Verified on Linux (Arch/Manjaro), June 2026.

### Prerequisites

| Tool | Why | Arch/Manjaro | Debian/Ubuntu |
|------|-----|--------------|---------------|
| Rust **1.91.0** | pinned by `rust-toolchain.toml` (rustup auto-installs) | `rustup` | `rustup` |
| `protoc` | tonic/prost gRPC codegen | `pacman -S protobuf` | `apt install protobuf-compiler` |
| `cmake` | builds `snmalloc-sys` (default allocator) | `pacman -S cmake` | `apt install cmake` |
| `pkg-config`, C/C++ toolchain | native deps | `pacman -S pkgconf base-devel` | `apt install pkg-config build-essential` |
| `git` | build stamps the commit via `vergen` | (system) | (system) |
| PostgreSQL | metadata DB (local is fine) | `pacman -S postgresql` | `apt install postgresql` |

### Build

```sh
cargo build --release -p ampd -p ampctl     # rustup picks up the pinned 1.91.0
./target/release/ampd --version             # e.g. "ampd v0.1.0"
```

Skip the bundled snmalloc allocator (drops the `cmake` requirement; uses the system
allocator — functionally identical):

```sh
cargo build --release -p ampd -p ampctl --no-default-features
```

A Nix flake is also provided: `nix build .#ampd`.

---

## Running

Bring up a local Postgres (`docker-compose up -d` provides one), point `ampd.toml` at it,
then run the all-in-one mode and register a chain:

```sh
ampd --config ./ampd.toml dev          # migrates the DB, starts all servers + a worker
# then use ampctl (see Components) to register the provider + dataset and deploy
```

The deployed job tip-follows continuously and is **resumable** — on restart the worker
picks the job back up from the metadata DB and continues from the last indexed block.

---

## Roadmap

See [`ROADMAP.md`](./ROADMAP.md) — Parquet layout tuning, materialized decoded views, optional
HyperSync ingest, and more, prioritised for camp's actual situation (one chain, one box, free,
unique data). Reliability (off-box backup) is Phase 0. The Postgres-wire endpoint, default-on
Bloom filters, and default-on compactor have shipped.

---

## Versioning

camp-node is versioned **independently of upstream Amp.** `v0.1.0` is the first release of
camp-node (cut from the Dec-2025 `a1937bf` baseline) — it marks this distribution's own
starting point, not parity with any upstream `0.0.x` release. The binary's version string
comes from `git describe`, so a build at a tagged commit reports that tag
(`ampd v0.1.0`); an untagged build reports the short commit hash.

---

## License

Licensed under the **Business Source License 1.1**. See [LICENSE](LICENSE).

- **Licensor:** Edge & Node Ventures, Inc.
- **Change Date:** three years from publication of each version → **Apache 2.0**.
- **Additional Use Grant:** production use is permitted *provided it does not compete* with
  Edge & Node Ventures' offerings of the Licensed Work. Per the grant, a product offered to
  third parties **not on a paid basis**, or that does not significantly overlap with their
  offering, **is not competitive.** camp is a free, no-key public service → its production
  use is permitted under this grant, for as long as it stays free.

BUSL grants no trademark rights. camp-node is an independent distribution built on Edge &
Node Ventures' Amp ([ampup.sh](https://ampup.sh/docs)) and is not affiliated with, sponsored
by, or endorsed by Edge & Node Ventures or The Graph.
