# camp-node Roadmap

> Engine roadmap for [camp-node](https://github.com/lodestar-team/camp-node), the
> source-built Amp distribution behind [engine.camp](https://engine.camp). The product
> roadmap for the gateway lives in [lodestar-team/camp](https://github.com/lodestar-team/camp/blob/main/docs/ROADMAP-2026.md).

## Framing — read this before the task list

A detailed external research report ("How to decisively beat upstream Amp across sync,
query, coverage, cost, and DX") proposed a full competitive roadmap. It's excellent
engineering and most of it is captured below — but it optimizes camp-node as a *general
indexer product*, and camp's actual situation is narrower and changes the priorities:

- **One chain (Arbitrum One), one box, free, no-key, run by one person.** The lever for
  adoption is **unique data + DX + reliability**, not winning a backfill-speed benchmark.
- **We deliberately don't backfill** — "start at current block, let history accumulate"
  (see camp's 2026 roadmap). So a 2000x *backfill* speedup optimizes something we chose to
  skip. It only becomes a lever if it makes offering history cheap enough to *change* that
  decision.
- **We just de-forked off a closed-source binary** to run an engine we can read/pin/patch.
  Several proposals (notably HyperSync) re-introduce a **vendor dependency** (Envio operates
  the endpoints). That's the same category of dependency we just removed — adopt only with
  eyes open and always keep a no-vendor default path.
- **The report's biggest omission is reliability.** Everything runs on one ThinkPad; a disk
  failure is unrecoverable loss of the accumulating Arbitrum history. That's camp's real #1
  risk and it's Phase 0 here.

So the tiers below re-rank the report's axes for camp: **DX/query reach + reliability first**
(they serve the live service), ingest speed second (optional accelerator, not the headline),
"grow camp-node as a product" third.

Everything stays inside the existing `BlockStreamer` trait, the extractor-crate pattern, and
DataFusion-on-Parquet. **Do not** fork the data model or replace DataFusion. Nothing here
re-builds the `pinax` source (v0.2.0–v0.4.0) — the traces / full-instrumentation path goes
through Pinax; this attacks the `evm-rpc` hot path, the query/serve layer, and DX.

---

## Progress checklist

**Phase 0 — measure & protect**
- [x] Baseline metrics — see [`ops/BASELINE.md`](./ops/BASELINE.md) (⚠️ ~45k files/table; compactor not merging)
- [~] Off-box backup + tested restore — tooling done & verified (`ops/backup.sh`, `ops/restore.sh`); **needs an off-box remote configured** (rclone + `restic init`) to go live

**Tier 1 — serve the live service**
- [ ] Postgres-wire endpoint (`crates/services/pgserver`, pgwire + datafusion-postgres)
- [x] Parquet Bloom filters — default-on, per-column (`address`/`topic0-3`/`tx_hash`/`block_hash`), verified present in written files + query correctness. (Sort-by-address intentionally skipped: bloom subsumes it for equality filters and sorting would hurt `block_num` range-pruning.)
- [ ] Materialized decoded views (`materialized_view` manifest kind + bundled views)
- [ ] Lift streaming-SQL limits (JSON Lines blocking-plan path)
- [ ] Flight SQL on the Flight server
- [x] Enable compactor + collector by default (Rust defaults + sample config + README)
- [ ] Allocator benchmark (mimalloc / jemalloc / snmalloc) + pick winner
- [x] Parquet footer cache — **already implemented** (`catalog/reader.rs`: footers in
  Postgres + `foyer` metadata cache; better than the roadmap assumed → file count is not a
  query-latency problem). Remaining compactor tuning (size-tiered, raise `eager_compaction_limit`
  to coalesce the 16KB-median tail) is a separate, lower-urgency item.

**Tier 2 — cheaper / faster ingest**
- [ ] HyperSync extractor (`crates/extractors/evm-hypersync`, opt-in, no-vendor default kept)
- [ ] Snapshot / `ampctl dataset bootstrap`
- [ ] Parallel range-sharded backfill + cryo-style aligned chunking

**Tier 3 — grow camp-node as a product**
- [ ] Extism/WASM plugin runtime (replace Deno `js-runtime`) + `camp-pdk`
- [ ] `redb` embedded metadata + `--solo` mode
- [ ] `ampctl init <contract>` via Sourcify + TS/Python/Rust codegen
- [ ] Reth ExEx extractor (`crates/extractors/evm-reth-exex`; non-Arbitrum)
- [ ] Observability (`/v1/status` lag/latency, OpenTelemetry) + SIWE token tiers

**Tier 4 — defer until demand**
- [ ] ClickHouse mirror sink in `ampsync`
- [ ] Federation / discovery (Graphcast/Waku gossip)

---

## Phase 0 — Measure & protect (do first)

- **Off-box backup + tested restore (camp's real #1).** Nightly sync of the Parquet data dir
  to object storage + a rehearsed `ampctl dataset restore`/`bootstrap`. The accumulating
  Arbitrum history can't be re-fetched past the RPC pruning window — protect it before
  building features.
- **Baseline metrics.** Backfill blocks/sec split into RPC vs decode vs write; query
  p50/p95/p99 by endpoint; compaction frequency + row-group/file-count distribution. Every
  "Nx" claim below is a hypothesis until measured on our own Arbitrum workload.

## Tier 1 — Serve the live service (highest leverage for camp)

- **Postgres-wire endpoint** — `crates/services/pgserver` via `pgwire` + `datafusion-postgres`
  over the existing SessionContext. Makes engine.camp speak Postgres natively: Grafana,
  Metabase, Superset, DBeaver, psql, every ORM — no JSON Lines glue. This is the single change
  that turns "blockchain ETL engine" into "blockchain DB BI tools already speak." Read-mostly;
  refuse writes. *(report §2.2 — I rank this #1 for camp.)*
- **Parquet layout tuning** — sort `logs` by `(address, block_num)` at compaction + Bloom
  filters on `address`/`topic0`/`tx_hash` + verify page index on + ZSTD level 6 in the
  compactor (keep level 1 for hot writes). Directly speeds camp's actual
  `WHERE address=? AND topic0=? AND block_num BETWEEN…` workload. 5–50x on point lookups,
  ~free on full scans. *(report §2.1)*
- **Materialized decoded views** — a `materialized_view` manifest kind that registers an
  inline DataFusion SQL view and materializes it to Parquet on commit/schedule. Ship
  `erc20_transfers`, `erc721/1155`, `uniswap_v2/v3/v4_swaps`, `aave_v3_*` out of the box.
  Decoded data *is* the product; this removes per-query `evm_decode_log` cost. *(report §3.3)*
- **Lift streaming-SQL limits** in the JSON Lines path (route blocking `ORDER BY`/`GROUP BY`/
  `LIMIT`/window plans through collected execution with a spill budget) + **Flight SQL** on the
  existing Flight server (JDBC/ODBC/Python DBI for free). Cheap, high-reach. *(report §2.3, §2.4)*
- **Free wins:** enable the compactor by default; benchmark `mimalloc`/`jemalloc`/snmalloc and
  pick the winner; cache Parquet footers (LRU) for the small-file tail. *(report §4.3, §4.5, §2.6)*

## Tier 2 — Cheaper / faster ingest (optional, measure first)

- **HyperSync extractor** — `crates/extractors/evm-hypersync` on `hypersync-client` (MPL-2.0):
  columnar Arrow ingest with server-side pushdown, ~10–50x+ backfill on Arbitrum (re-measure).
  **Caveats that matter for camp:** (a) it's a *vendor* dependency (Envio runs the endpoints) —
  keep `evm-rpc` as the no-vendor default and make this opt-in with a `fallback_provider`;
  (b) its main value here is *cheap zero-RPC tip-following* and making historical data cheap
  enough to revisit the no-backfill stance — not the headline benchmark. MPL §3.3 linking from
  BUSL-1.1 is fine if unmodified; document it. *(report §1.1)*
- **Snapshot / bootstrap** — publish periodic public Parquet snapshots of
  `_/arbitrum_one`; `ampctl dataset bootstrap --from <bucket>` takes a new operator from clone
  to a queryable window in minutes. Pairs with the Phase 0 backup pipeline. *(report §1.5)*
- **Parallel range-sharded backfill** + cryo-style aligned chunking on `evm-rpc` as the
  no-vendor fast path (2–5x without HyperSync). *(report §1.3, §1.4)*

## Tier 3 — Grow camp-node as an adoptable product (later)

- **Extism/WASM plugin runtime** replacing the Deno `js-runtime` (smaller, multi-lang,
  better sandbox) + a `camp-pdk`. Big rewrite; only worth it once users want custom decoders
  the materialized-views path can't cover. *(report §3.1)*
- **`redb` embedded metadata + `--solo` mode** — drop the mandatory Postgres for single-box
  self-hosters (`cargo install camp-node && camp-node init && camp-node run`). Helps *other*
  operators adopt, not camp's own ops; single-writer, so Postgres stays for multi-worker.
  *(report §4.8, §5.1)*
- **`ampctl init <contract>`** via Sourcify (ABI → typed materialized views) + TS/Python/Rust
  codegen. Biggest DX delta vs Ponder/Envio/SQD — we generate views from an ABI instead of
  making users write handlers. *(report §3.4, §5.3)*
- **Reth ExEx extractor** — `crates/extractors/evm-reth-exex`, reorg-aware in-process ingest,
  "1–2 orders of magnitude vs JSON-RPC." **Does not help Arbitrum** (Nitro is a Geth fork) — it
  unlocks Ethereum/Base/OP/Reth-backed L2s, i.e. a *multi-chain* future, which camp isn't today.
  *(report §1.2)*
- Observability (`/v1/status` lag/row-counts/latency, OpenTelemetry), SIWE token tiers on the
  edge. *(report §5.5, §5.6)*

## Tier 4 — Defer until there's demand

- **ClickHouse mirror sink** in `ampsync` — only when a user's mix is point-lookups against
  billions of rows. Parquet+DataFusion stays primary. *(report §2.5)*
- **Federation / discovery** (Graphcast/Waku gossip, `bootstrap --from-peer`) — only with ≥3
  operators. *(report §5.7)*

---

## Build notes (de-risked, ready to execute)

**pgwire endpoint (Tier 1 #1) — feasibility confirmed.**
- Use **`datafusion-postgres` 0.12.2** — it requires `datafusion ^50`, an exact match for our
  workspace (DataFusion 50.3.0, arrow 56.2.0). 0.13+ track DF51–53; do **not** bump them
  without a DataFusion upgrade. Pulls `pgwire` transitively.
- Reuse the existing context builder in `crates/core/common/src/query_context.rs`
  (`datafusion_ctx` + `register_table` / `create_catalog_schema` / `register_udfs`) — the same
  machinery `flight`/`jsonl` use — to produce a `SessionContext`, then `setup_pg_catalog(ctx)`
  + `serve(ctx, ServerOptions)`.
- **STATUS: foundation done + verified.** `crates/services/pgserver` exists and compiles;
  `datafusion-postgres 0.12.2` resolves cleanly on our exact DataFusion 50.3.0 (unified, no
  second copy) + arrow 56 / arrow-pg 0.8.1; `serve(ctx, opts, AuthManager)` wrapper wired
  (no-auth default, localhost bind).
- **KEY DESIGN FINDING (the real work).** `serve()` wants ONE static `SessionContext` with all
  tables pre-registered. camp does the opposite: `server::flight::execute_query` runs
  `catalog_for_sql(sql)` → infers which datasets the SQL references → loads ONLY those → builds
  a **fresh `QueryContext` (catalog snapshot) per query**. So a static `serve()` is wrong on
  two counts: freshness (snapshot per query) AND on-demand catalog loading. **Do not** pre-build
  a giant static context.
- **Correct approach:** use datafusion-postgres for the **wire layer only** — `serve_with_handlers`
  with a custom `PgWireServerHandlers` (or a `QueryHook`) whose query path **delegates to camp's
  existing `execute_query`** (catalog_for_sql → QueryContext → execute), then encodes the result
  `RecordBatch`es to the pg wire format via the re-exported `arrow_pg`. This reuses camp's exact,
  correct, fresh query pipeline and borrows only pgwire's protocol + `pg_catalog` emulation.
- Remaining: implement that handler; map `"ns/name@ver".table` → clean `schema.table` for BI
  tools (or rely on quoted identifiers initially); `--pg-server` flag on `ampd dev`/`server`
  (default `127.0.0.1:5432`); **verify against psql + Grafana/DBeaver** (incl. their `pg_catalog`
  startup queries — the known datafusion-postgres rough edge). Read-only; refuse writes.
- **Implementation facts traced (ready to build):** camp's loader is `catalog_for_sql` →
  `get_physical_catalog(store, metadata_db, table_refs, func_refs, env)`; **`TableSnapshot`
  already implements `TableProvider`** (it's passed straight to `ctx.register_table`), so no
  wrapper needed. Two real frictions to handle: (1) `register_table` also calls
  `ctx.register_object_store(table.url(), table.object_store())` per table — a dynamic
  `SchemaProvider` can't touch the ctx, so **pre-register the (few) object stores** on the
  pgwire ctx up front; (2) DataFusion `SchemaProvider::table_names()`/`schema_names()` are
  **sync**, but listing datasets/tables needs an async metadata-DB query — keep a **periodically
  refreshed cached list** for enumeration, and resolve the actual `TableProvider` lazily in the
  async `table()` via `get_physical_catalog`. Decision: **delegate-handler vs dynamic-catalog** —
  dynamic-catalog reuses all of `DfSessionService` (pg_catalog/extended/encoding) and only needs
  the two frictions solved; prefer it. This is a focused build that must be psql-verified before
  shipping — do it in a dedicated run, not bolted onto a long session.

## Always-true constraints

- The real prize remains **Arbitrum full instrumentation** via the `pinax` source the moment
  Pinax ships it (zero code change). Engine speed is secondary to that.
- Keep a **no-vendor default** for every hot path (Envio/Pinax are accelerators, not the floor).
- Treat every external "Nx" figure as a hypothesis to validate in Phase 0.
- Don't fork the data model; don't replace DataFusion; compose, don't rewrite.

*Distilled from an external research report (June 2026) + camp's own constraints. The report's
raw axis-by-axis detail is preserved in the team notes.*
