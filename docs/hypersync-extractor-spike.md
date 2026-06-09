# Spike: HyperSync as an EVM extractor backend (RFC #1 de-risk)

Status: **spike plan / not yet built**. This is a de-risk for the proposed `evm-hypersync`
extractor — answering "should we build it, and what will bite us" *before* committing extractor
work. It is not a design sign-off.

## Goal

Today camp backfills EVM chains over **JSON-RPC** (`crates/extractors/evm-rpc`), which is
RPC-bound — our own benchmark put backfill at ~12 blocks/sec without batching, and even tuned it is
gated by RPC round-trips and provider rate limits (see `feedback_ampd_rpc_batching`,
`project_camp_bench`). [Envio **HyperSync**](https://docs.envio.dev/docs/HyperSync/overview) is a
columnar, bulk-oriented data source for EVM chains designed for exactly this: pulling wide ranges of
blocks/logs/transactions far faster than `eth_getBlockByNumber` loops. If it slots in cleanly, it
could cut historical backfill time by a large factor and reduce our RPC-provider dependency.

## How it would fit (the seam is small)

camp already abstracts the source behind one trait — `BlockStreamer` in
`crates/core/common/src/lib.rs`:

```rust
pub trait BlockStreamer: Clone + 'static {
    fn block_stream(self, start: BlockNum, end: BlockNum)
        -> impl Future<Output = impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send> + Send;
    fn latest_block(&mut self, finalized: bool)
        -> impl Future<Output = Result<Option<BlockNum>, BoxError>> + Send;
    fn provider_name(&self) -> &str;
}
```

The evm-rpc extractor is just an implementor of this trait plus a `DatasetKind` string
(`"evm-rpc"`) and a provider config. A HyperSync backend is therefore **additive**:

- new crate `crates/extractors/evm-hypersync`, `DatasetKind = "evm-hypersync"`,
- a `HyperSyncStreamer: BlockStreamer` that pulls ranges from HyperSync and **emits the exact same
  `RawDatasetRows`** the RPC extractor produces (same tables: blocks / transactions / logs, same
  per-block invariant + `_block_num`),
- a provider config carrying the HyperSync endpoint URL (+ optional API token).

Crucially: **same output schema → no downstream changes.** Compaction, the catalog, Flight/JSONL/
pgwire, and existing manifests all keep working. A dataset just picks its source by `kind`.

## The risks the spike must retire (in priority order)

1. **Schema fidelity / completeness.** Does HyperSync return *every* field our `blocks` /
   `transactions` / `logs` tables require, with the same semantics (e.g. effective gas price, log
   `removed`, `transactionIndex`, all topics)? Any missing/derived field is the whole ballgame —
   it's the difference between "drop-in" and "rewrite the table builders." **Highest risk; test
   first.** Compare a HyperSync-built block range byte-for-row against the RPC-built one for the same
   range.
2. **Reorg / finality model.** Our pipeline has an explicit reorg path (`docs/reorgs.md`) and a
   `finalized` flag on `latest_block`. Does HyperSync expose finality, and is its tip consistent
   enough to drive tip-following, or is it better used **only for historical backfill** with RPC
   handling the live tip? Likely answer: HyperSync for backfill, RPC for tip — confirm.
3. **Throughput & cost, measured not assumed.** Re-run the `bench/` harness (the same from-empty
   backfill driver used vs. amp) with a HyperSync streamer over a fixed Arbitrum window. Compare
   blocks/sec, peak RSS, and bytes/block against the RPC path. Note any API quota / token cost.
4. **Range/stream semantics & backpressure.** `block_stream` is a `Stream` over `[start, end]`.
   Does HyperSync paginate in a way that maps cleanly to one `RawDatasetRows` per block (our
   single-block invariant), or does it return columnar batches we must re-split per block? Re-splitting
   is fine but is real code — scope it.
5. **Chain coverage.** Which of our target chains does HyperSync support? (Arbitrum One is the live
   fork dataset — confirm it's covered and at what tip lag.)

## Spike plan (timeboxed, ~1–2 days, throwaway code OK)

1. **Schema diff (½ day).** Hand-write a throwaway script (or a minimal `HyperSyncStreamer` that
   only fills blocks+logs) to pull a small fixed Arbitrum range. Diff every column against the RPC
   extractor's output for the same range. **Go/no-go gate:** if fields are missing or semantically
   different and can't be derived, stop and report — the extractor is not a drop-in.
2. **Backfill bench (½ day).** Wire the throwaway streamer behind `BlockStreamer`, run the `bench/`
   from-empty driver over the standard window, record blocks/sec + RSS + bytes/block vs. RPC.
3. **Tip/finality probe (½ day).** Check what HyperSync reports for the head and finality; decide
   backfill-only vs. tip-following.
4. **Write up findings** against risks 1–5 with a build/don't-build recommendation and, if build, a
   crate-level design (provider config shape, error/retry strategy via `BlockStreamerExt::with_retry`,
   per-block re-split if needed).

## Success criteria for "build it"

- Risk 1 retired: all required columns present or cheaply derivable (no fidelity loss).
- Risk 3 shows a **material** backfill speedup over RPC (target: ≥3×; anything under ~1.5× isn't
  worth the new dependency).
- A clear, bounded story for the live tip (even if that's "RPC stays the tip source").

## Non-goals for the spike

- Production-quality error handling, full chain matrix, or manifest schema changes.
- Replacing the RPC extractor — HyperSync is an *additional* backend, selected per dataset by
  `kind`. The RPC path stays the default and the fallback.

## References

- Extension seam: `crates/core/common/src/lib.rs` (`BlockStreamer`), `crates/extractors/evm-rpc`
  (the reference implementor), `crates/extractors/evm-rpc/src/dataset_kind.rs` (kind dispatch).
- Bench harness: `bench/` (`run_target.sh`, `REPRODUCE.md`).
- Reorg handling: `docs/reorgs.md`.
- HyperSync docs: https://docs.envio.dev/docs/HyperSync/overview
