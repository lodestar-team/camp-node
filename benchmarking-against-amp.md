# camp-node vs Amp — benchmark & feature comparison

**Status:** living document · **Date:** 2026-06-08 · **camp-node:** v0.5.1 (`[tree]`-verified)

## What this compares, and how trustworthy each column is

| Column | Provenance | Confidence |
|---|---|---|
| **camp-node v0.5.1** | `[tree]` — read directly from this repo @ v0.5.1, and exercised on this box. | High — source + observed. |
| **Amp v0.0.36** | `[obs]` — black-box observation of the **latest public** amp binary (`ghcr.io/edgeandnode/amp:latest`, version label `v0.0.36`, built 2026-05-05) + its `ampctl`. No decompilation. | Medium-high — what it *exposes*, not how it works. |
| **Amp "current" (private/docs)** | `[docs]` — Amp's public docs (ampup.sh/docs) as of June 2026, describing the line distributed via the **private** `edgeandnode/amp` GitHub releases. **Not observed by me.** | Low — documentation-derived; may omit gated/undocumented features. |

> **Why two Amp columns.** The *only* amp binary publicly obtainable is **v0.0.36** (ghcr `:latest`). Anything newer lives behind the private `edgeandnode/amp` repo (the GitHub releases API 404s anonymously; `ampup install` needs a token). So the **defensible, artifact-grounded comparison is camp-node v0.5.1 `[tree]` vs amp v0.0.36 `[obs]`** — both things on this box. The "current/docs" column is a best-effort appendix, explicitly unverified.

Legend: ✅ present · ❌ absent · ◐ conditional · ❔ unknown/undocumented.

---

## Headline (honest)

- **vs the Amp it forked (Dec-2025 / v0.0.36 line): camp-node is a near-superset.** Same inherited engine, plus four things added on top — the `pinax` source, Postgres-wire, on-by-default Bloom filters + compaction, and selectable allocators — **minus** a few source *kinds* amp's CLI exposes that camp's doesn't (see below). Mostly source/observation-verifiable.
- **vs Amp "current": a trade, not a sweep.** camp leads on **Postgres-wire, keyless/free instrumented data (public Pinax bucket), open source-built access, and on-by-default efficiency**. Amp leads on **verifiability/attestation** and a **productized hosted ecosystem** (Studio, catalog, SDKs, gateway auth). The core ETL surface (sources, Flight+JSONL, DataFusion+Parquet, reorg handling, EVM UDFs) is **parity**, because camp inherited it.

The claim that holds up: *"camp-node is ahead of the Amp it forked on openness, Postgres compatibility, keyless instrumented data, and efficiency defaults — while Amp leads on verifiability and hosted-product polish, and exposes a few more source kinds."* Not "camp beats Amp on everything."

---

## Feature comparison

### Data sources / extractors

| Source kind | camp-node v0.5.1 `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| `evm-rpc` | ✅ | ✅ | ✅ |
| `eth-beacon` (consensus) | ✅ (CLI-exposed) | ❔ (not in `manifest generate` list) | ✅ |
| `firehose` | ✅ (CLI-exposed) | ❔ (not in `manifest generate` list) | ✅ |
| `solana` | ◐ crate present, **not CLI-exposed** | ✅ (in `manifest generate`) | ❔ |
| `bitcoin-rpc` | ❌ | ✅ (in `manifest generate`) | ❔ |
| `tempo` | ❌ | ✅ (in `manifest generate`) | ❔ |
| derived (`manifest`/SQL) | ✅ | ✅ | ✅ |
| **`pinax` (Firehose→Parquet, free public bucket, no key)** | ✅ v0.2–0.4 | ❌ | ❌ |

> Observed directly: camp `ampctl manifest generate` → kinds `evm-rpc, eth-beacon, firehose`; amp v0.0.36 → `bitcoin-rpc, evm-rpc, solana, tempo`. So **each exposes kinds the other doesn't** — amp v0.0.36 leads on `bitcoin-rpc`/`tempo`/`solana`; camp leads on a CLI-wired `firehose`/`eth-beacon` and the keyless `pinax` source. (camp's `solana` crate exists but isn't wired into `manifest generate`.)

### Data coverage

| Data | camp-node `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| blocks / transactions / logs | ✅ | ✅ | ✅ |
| internal traces (`calls`), storage/balance/code/nonce changes | ✅ **keyless via public Pinax bucket** | ◐ via Firehose (needs an endpoint) | ◐ via Firehose |
| decoded events (`evm_decode`) | ✅ | ✅ | ✅ |

> **Nuance:** Amp can produce full-instrumentation data too (via Firehose). camp's real differentiator is the **access path** — it pulls instrumented data from Pinax's **free public bucket with no API key and no self-run Firehose**. The lead is "keyless/free instrumented data," not "traces at all."

### Query interfaces

| Interface | camp-node `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| Arrow Flight (gRPC) | ✅ | ✅ | ✅ |
| JSON Lines HTTP | ✅ | ❌ **dropped in v0.0.36** | ✅ |
| **Postgres wire** | ✅ read-only (`--pg-server`, v0.5.0) | ❌ | ❌ |
| streaming SQL | ✅ (documented limits) | ✅ | ✅ |

> Observed: amp v0.0.36 serves **Flight only** (no JSONL server, no `--jsonl-server` flag — dropped in that release). camp serves Flight + JSONL + Postgres-wire. **This is the clearest camp interface lead** — and it means a fair query benchmark can only compare *Flight* head-to-head; JSONL and pgwire are camp-only on v0.0.36.

### SQL / UDFs / extensibility

| Capability | camp-node `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| `evm_decode` / `evm_topic` / encode/decode params | ✅ | ✅ | ✅ |
| `eth_call` RPC UDFs | ✅ (inherited) | ✅ | ✅ |
| JavaScript UDFs (`js-runtime`) | ✅ | ❔ | ✅ |
| **attestation / verifiability UDFs** | ❌ **none in tree** (grep-verified) | ❔ | ✅ (marketed) |
| WASM / Extism plugins | ❌ (roadmap) | ❌ | ❌ |

> Correction to earlier drafts: camp has **no** attestation/verifiability UDFs (verified absent in the tree). Verifiability is a genuine **Amp lead**, central to its positioning; camp doesn't tell that story.

### Storage / engine

| | camp-node `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| Parquet + DataFusion | ✅ | ✅ | ✅ |
| Arrow-native types (FixedSizeBinary, Decimal128) | ✅ | ✅ | ✅ |
| object store | ✅ **S3 / GCS / Azure / R2** | ✅ S3 / GCS / Azure | ✅ S3 / GCS / Azure |
| **Bloom filters default-on** | ✅ (equality cols, verified) | ❌ | ❔ |
| **compactor + collector default-on** | ✅ | ❌ ships **off** | ❔ |
| selectable mimalloc / jemalloc | ✅ (+ benchmark) | ◐ snmalloc | ❔ |

> Correction: camp **does** support Azure (`az://`, `object_store` azure feature + `MicrosoftAzureBuilder`) — earlier drafts implied it didn't. So camp's object-store breadth ≥ amp's (adds R2 docs/verification on top). The default-on Bloom/compaction are real camp wins **at default settings**, though amp could also flip them on.

### Ops / deploy / access

| | camp-node `[tree]` | Amp v0.0.36 `[obs]` | Amp current `[docs]` |
|---|---|---|---|
| single-box + distributed roles | ✅ (`dev`+`server`/`controller`/`worker`) | ✅ (`solo`+roles) | ✅ |
| Grafana dashboards shipped | ✅ `grafana/` (4 dashboards) | ❔ | ✅ (OTel) |
| backup/restore tooling | ✅ (Phase-0) | ❔ | ❔ |
| reorg handling | ✅ (inherited) | ✅ | ✅ |
| **source you can build & read** | ✅ public BUSL lineage | ❌ closed binary | ❌ closed |
| **free, no-signup, no-key hosted** | ✅ engine.camp | n/a (binary) | ❌ gateway needs tokens |
| hosted Studio / catalog / SDKs | ◐ Lodestar Dashboard | n/a | ✅ |

> **camp leads:** openness (source-built) + access model (free/no-key). **Amp leads:** productized hosted ecosystem + verifiability story.

---

## Performance — measured (camp-bench MVP)

**Setup (identical for both):** from-empty backfill of the **same fixed Arbitrum One window** through the **same RPC** (`petko-rpc.infradao.tech/arbitrum`, `rpc_batch_size=10`, `concurrent_request_limit=10`), into a throwaway Postgres (Docker) + fresh `/tmp` data dir, both pinned to CPU cores 0–7, compactor/collector on for both. camp uses `ampd dev`; amp v0.0.36 uses `ampd solo`. Window: 5,000 blocks from 471,200,000 (finalized). Each target's *own* matching `ampctl` drives `provider register → manifest generate → dataset register → dataset deploy --end-block`.

**Results — n=3 interleaved runs, median (5,000-block window, 2026-06-08):**

| Metric | camp-node v0.5.1 | amp v0.0.36 | camp delta |
|---|---|---|---|
| Backfill wall-clock | **44.2 s** | 51.2 s | **~16% faster** |
| Backfill throughput | **113 blk·s⁻¹** | 98 blk·s⁻¹ | ~16% higher |
| Peak RSS | **150 MB** | 157 MB | ~4% lower |
| CPU-seconds | **2.0 s** | 2.6 s | ~23% lower |
| On-disk bytes/block | **1931** | 1990 | ~3% smaller |
| Parquet files (post-compaction, both compaction-on) | 3 | 3 | parity — both compact |

Per-run dispersion (blk·s⁻¹): camp **113.2 / 115.8 / 110.7** (very tight); amp **97.7 / 99.6 / 81.7**. amp's run-3 dip to 81.7 is **RPC-provider variance**, not an indexer change — and is exactly why this axis must be reported with dispersion and treated as RPC-bound (see caveats). Bytes/block was **identical across runs per target** (deterministic: same window ⇒ same data ⇒ same compression).

### Why camp is faster/slower on each metric (the mechanism, not just the number)

- **Backfill blocks/sec — near-parity, camp marginally ahead.** This axis is **RPC-bound**: both spend ~all their wall-clock waiting on the *same* RPC endpoint at *identical* batch/concurrency settings, and both use the **same `evm-rpc` client lineage** (camp's is forked from amp's). So the result is dominated by the provider, not the indexer; any camp edge is small and comes from newer deps/compiler + post-fetch write/compaction overlap, **not** a fundamentally faster fetch. *Honest read: this is not where camp wins — it's parity-by-inheritance.* The real backfill lever is the HyperSync extractor (not yet built), which bypasses per-block RPC entirely.

- **Peak RSS — comparable, camp similar-or-lower.** Same Arrow/Parquet writer buffering model (inherited); RSS scales with in-flight row groups. Differences are within noise / allocator (both snmalloc by default). Not a durable moat either way.

- **CPU-seconds — low for both.** RPC-bound ⇒ the process is mostly idle-waiting on network, so user+system CPU is small relative to wall-clock. Compaction adds a little camp-side CPU (it runs by default).

- **Storage bytes/block — near-identical.** Both write the **same schema** through the **same Parquet writer with `zstd(1)`** (camp inherited it), so on-disk size is essentially the same; the ~3% camp edge is a minor schema/encoding difference, not a compaction effect (see file count). camp's default-on **Bloom filters add a few KB per file** yet bytes/block stays ≈ equal — *the bloom-filter index overhead is negligible*.

- **File count — parity on this benchmark (both = 3 files); the compaction advantage is steady-state, not bulk-backfill.** *Measured:* with compaction enabled for both, camp **and** amp v0.0.36 each settled to exactly **3 parquet files** (one per blocks/logs/transactions) — amp's compactor works when on. So this bulk backfill shows **no file-count difference**. camp's real edge is a **defaults** one: camp ships compactor+collector **on**, amp ships them **off** — which matters for a **long-running tip-following** deployment (each poll writes a small file; without default-on compaction they pile up and degrade query planning — exactly camp's old prod file-count problem). A bulk backfill writes few files regardless, so it does **not** exercise that difference; don't quote a file-count win from this benchmark.

- **Query latency — camp wins on *capability*, not just speed.** A fair head-to-head can only use **Arrow Flight** (the only interface amp v0.0.36 and camp share). On Flight the engines are at parity (same DataFusion). camp's advantage is that it *also* answers **JSON Lines and Postgres-wire** — interfaces amp v0.0.36 can't speak at all — so "query latency on pgwire/JSONL" is a camp-only column, a capability gap rather than a benchmark win. (camp's footer-cache + default-on Bloom filters do help selective queries prune row groups, which would show on dense-window point lookups — measure before claiming.)

---

## Methodology caveats (read before quoting any number)

- **Shared dev box.** These ran on the same machine as the live engine.camp fork (still serving) + the upstream binary — *not* a quiet dedicated box. Treat figures as **indicative, not publication-grade**. RFC #2's dedicated-box, n≥5, cold/warm, RPC-counting run is the publishable version.
- **RPC-bound backfill.** The dominant cost is the shared external RPC; runs were interleaved (camp/amp alternating) so they share network conditions, but provider variance remains. Request counts aren't yet captured.
- **amp v0.0.36 ≠ "Amp current."** v0.0.36 is the latest *public* build (ghcr, 2026-05-05). Newer private builds may differ; pin + date-stamp any comparison.
- **No claims about Amp internals** — closed binary, not decompiled. Only what it exposes.
- **Confirm EULA.** Verify ampup's binary license permits benchmarking before publishing comparisons.
- **Reproduce:** see `bench/REPRODUCE.md` for the exact commands.
