//! `BlockStreamer` over Pinax Parquet in S3 (anonymous Tigris bucket).
//!
//! v1: serves the `calls` table (internal-tx traces — data RPC can't produce).
//! For a block range it selects the overlapping Pinax `calls` parquet partitions
//! (via footer block_num stats), maps their columns onto camp-node's firehose
//! `calls` schema, and yields one `RawDatasetRows` per block. The engine's worker
//! then writes proper camp-node parquet.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Int32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use common::{
    BlockNum, BlockStreamer, BoxError, RawDatasetRows, RawTableRows, Table,
    metadata::segments::BlockRange,
};
use futures::{Stream, StreamExt};
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;

/// Reads Pinax `calls` parquet for a chain and maps it to camp-node rows.
#[derive(Clone)]
pub struct PinaxClient {
    pub name: String,
    /// Public S3/HTTPS base, e.g. "https://pinax.fly.storage.tigris.dev".
    pub endpoint: String,
    /// Chain prefix in the bucket, e.g. "mainnet".
    pub network: String,
}

impl PinaxClient {
    pub fn new(cfg: &crate::ProviderConfig) -> Self {
        Self {
            name: cfg.name.clone(),
            endpoint: cfg.endpoint.clone(),
            network: cfg.network.clone(),
        }
    }

    /// Build an anonymous S3 store for the public Pinax (Tigris) bucket.
    /// `endpoint` is the virtual-hosted public base `https://<bucket>.<host>`.
    fn store(&self) -> Result<Arc<dyn ObjectStore>, BoxError> {
        // Split "https://pinax.fly.storage.tigris.dev" -> bucket="pinax", host="https://fly.storage.tigris.dev".
        // `endpoint` is the public virtual-hosted base, e.g.
        // "https://pinax.fly.storage.tigris.dev" (bucket already in the host).
        // object_store (with endpoint set + vhost) uses the endpoint host as-is,
        // so we point it at the full vhost base and just record the bucket name.
        let bucket = self
            .endpoint
            .strip_prefix("https://")
            .unwrap_or(&self.endpoint)
            .split('.')
            .next()
            .ok_or_else(|| format!("invalid pinax endpoint: {}", self.endpoint))?;
        let s3 = AmazonS3Builder::new()
            .with_region("auto")
            .with_bucket_name(bucket)
            .with_endpoint(self.endpoint.clone())
            .with_virtual_hosted_style_request(true)
            .with_skip_signature(true)
            .build()?;
        Ok(Arc::new(s3))
    }

    fn calls_table(&self) -> Table {
        firehose_datasets::evm::tables::all(&self.network)
            .into_iter()
            .find(|t| t.name() == "calls")
            .expect("firehose defines a `calls` table")
    }
}

impl BlockStreamer for PinaxClient {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        async_stream::try_stream! {
            let store = self.store()?;
            let table = self.calls_table();
            let prefix = Path::from(format!("{}/calls", self.network));

            // 1. Read all overlapping partition files into one Amp-schema batch.
            // Files list chronologically and block_num is monotonic with time, so
            // skip files entirely before the range and stop once we're past it
            // (avoids footer-reading the whole multi-year partition).
            let mut listing = store.list(Some(&prefix));
            let mut amp_batches: Vec<RecordBatch> = Vec::new();
            while let Some(meta) = listing.next().await {
                let meta = meta?;
                let reader = ParquetObjectReader::new(store.clone(), meta.location.clone());
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                let (fmin, fmax) = file_block_bounds(&builder);
                if let Some(mx) = fmax {
                    if mx < start {
                        continue; // file is before the range
                    }
                }
                if let Some(mn) = fmin {
                    if mn > end {
                        break; // sorted listing → no later file overlaps
                    }
                }
                let mut batches = builder.build()?;
                while let Some(batch) = batches.next().await {
                    amp_batches.push(map_calls_batch(&batch?, start, end)?);
                }
            }

            // 2. Emit one RawDatasetRows per block in the range.
            for block in start..=end {
                let cols = filter_block(&amp_batches, block, table.schema().as_ref())?;
                let range = BlockRange {
                    numbers: block..=block,
                    network: self.network.clone(),
                    hash: Default::default(),
                    prev_hash: None,
                };
                let rows = RawTableRows::new(table.clone(), range, cols)?;
                yield RawDatasetRows::new(vec![rows]);
            }
        }
    }

    async fn latest_block(&mut self, _finalized: bool) -> Result<Option<BlockNum>, BoxError> {
        let store = self.store()?;
        let prefix = Path::from(format!("{}/calls", self.network));
        // Find the lexicographically-latest partition file (date-partitioned),
        // then read its footer max block_num.
        let mut listing = store.list(Some(&prefix));
        let mut latest: Option<object_store::path::Path> = None;
        while let Some(meta) = listing.next().await {
            let loc = meta?.location;
            if latest.as_ref().is_none_or(|l| loc > *l) {
                latest = Some(loc);
            }
        }
        let Some(loc) = latest else {
            return Ok(None);
        };
        let reader = ParquetObjectReader::new(store.clone(), loc);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        Ok(column_stats_max(&builder, "block_num"))
    }

    fn provider_name(&self) -> &str {
        &self.name
    }
}

// ---- helpers ----

/// File's `block_num` (min, max) from parquet row-group stats, if available.
fn file_block_bounds(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
) -> (Option<BlockNum>, Option<BlockNum>) {
    let meta = builder.metadata();
    let schema = builder.parquet_schema();
    let Some(idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == "block_num")
    else {
        return (None, None);
    };
    let mut fmin = u64::MAX;
    let mut fmax = 0u64;
    let mut have = false;
    for rg in meta.row_groups() {
        if let Some(stats) = rg.column(idx).statistics() {
            if let (Some(mn), Some(mx)) = (
                stats_as_u64(stats.min_bytes_opt()),
                stats_as_u64(stats.max_bytes_opt()),
            ) {
                fmin = fmin.min(mn);
                fmax = fmax.max(mx);
                have = true;
            }
        }
    }
    if have { (Some(fmin), Some(fmax)) } else { (None, None) }
}

fn stats_as_u64(bytes: Option<&[u8]>) -> Option<u64> {
    let b = bytes?;
    if b.len() == 8 {
        Some(u64::from_le_bytes(b.try_into().ok()?))
    } else {
        None
    }
}

fn column_stats_max(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    col: &str,
) -> Option<BlockNum> {
    let meta = builder.metadata();
    let schema = builder.parquet_schema();
    let idx = (0..schema.num_columns()).find(|&i| schema.column(i).name() == col)?;
    let mut max = None;
    for rg in meta.row_groups() {
        if let Some(stats) = rg.column(idx).statistics() {
            if let Some(mx) = stats_as_u64(stats.max_bytes_opt()) {
                max = Some(max.map_or(mx, |m: u64| m.max(mx)));
            }
        }
    }
    max
}

/// Map a Pinax `calls` RecordBatch onto the firehose `calls` Amp schema,
/// keeping only rows with block_num in [start, end].
fn map_calls_batch(b: &RecordBatch, start: BlockNum, end: BlockNum) -> Result<RecordBatch, BoxError> {
    // Cast to Utf8 so dictionary-encoded / large-utf8 columns (e.g. call_type) work.
    let s = |n: &str| -> Result<StringArray, BoxError> {
        let c = b
            .column_by_name(n)
            .ok_or_else(|| -> BoxError { format!("pinax calls missing column {n}").into() })?;
        let utf8 = arrow::compute::cast(c, &DataType::Utf8)?;
        Ok(utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| -> BoxError { format!("{n} not castable to Utf8").into() })?
            .clone())
    };
    let u64c = |n: &str| -> Result<UInt64Array, BoxError> {
        let c = b
            .column_by_name(n)
            .ok_or_else(|| -> BoxError { format!("missing {n}").into() })?;
        Ok(arrow::compute::cast(c, &DataType::UInt64)?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| -> BoxError { format!("{n} cast u64").into() })?
            .clone())
    };
    let u32c = |n: &str| -> Result<UInt32Array, BoxError> {
        let c = b
            .column_by_name(n)
            .ok_or_else(|| -> BoxError { format!("missing {n}").into() })?;
        Ok(arrow::compute::cast(c, &DataType::UInt32)?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| -> BoxError { format!("{n} cast u32").into() })?
            .clone())
    };

    let block_num = u64c("block_num")?;
    let ts_secs = u64c("timestamp")?;

    let cols: Vec<ArrayRef> = vec![
        Arc::new(block_num.clone()),                                  // _block_num
        Arc::new(fsb_from_hex(&s("block_id")?, 32)?),                 // block_hash
        Arc::new(block_num.clone()),                                  // block_num
        Arc::new(ts_ns_from_secs(&ts_secs)),                          // timestamp
        Arc::new(u32c("tx_index")?),                                  // tx_index
        Arc::new(fsb_from_hex(&s("tx_hash")?, 32)?),                  // tx_hash
        Arc::new(u32c("call_index")?),                                // index
        Arc::new(u32c("parent_index")?),                              // parent_index
        Arc::new(u32c("depth")?),                                     // depth
        Arc::new(call_type_to_i32(&s("call_type")?)),                 // call_type
        Arc::new(fsb_from_hex(&s("caller")?, 20)?),                   // caller
        Arc::new(fsb_from_hex(&s("address")?, 20)?),                  // address
        Arc::new(dec128_from_str(&s("value")?)?),                     // value
        Arc::new(u64c("gas_limit")?),                                 // gas_limit
        Arc::new(u64c("gas_consumed")?),                              // gas_consumed
        Arc::new(bin_from_hex(&s("output")?)),                        // return_data
        Arc::new(bin_from_hex(&s("input")?)),                         // input
        Arc::new(bool_col(b, "suicide")?),                            // selfdestruct
        Arc::new(bool_col(b, "executed_code")?),                      // executed_code
        Arc::new(UInt64Array::from(vec![0u64; b.num_rows()])),        // begin_ordinal
        Arc::new(UInt64Array::from(vec![0u64; b.num_rows()])),        // end_ordinal
    ];

    // Build against the Amp schema, then filter to [start, end].
    let table = firehose_calls_schema();
    let batch = RecordBatch::try_new(table, cols)?;
    let mask: BooleanArray = block_num
        .iter()
        .map(|v| v.map(|n| n >= start && n <= end))
        .collect();
    Ok(arrow::compute::filter_record_batch(&batch, &mask)?)
}

fn firehose_calls_schema() -> arrow::datatypes::SchemaRef {
    // Use the canonical firehose calls table schema.
    firehose_datasets::evm::tables::all("")
        .into_iter()
        .find(|t| t.name() == "calls")
        .expect("calls")
        .schema()
        .clone()
}

/// Slice all mapped batches down to a single block and return columns in schema order.
fn filter_block(
    batches: &[RecordBatch],
    block: BlockNum,
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<ArrayRef>, BoxError> {
    let mut kept: Vec<RecordBatch> = Vec::new();
    for b in batches {
        let bn = b
            .column_by_name(common::SPECIAL_BLOCK_NUM)
            .ok_or_else(|| -> BoxError { "missing _block_num".into() })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| -> BoxError { "_block_num not u64".into() })?;
        let mask: BooleanArray = bn.iter().map(|v| v.map(|n| n == block)).collect();
        kept.push(arrow::compute::filter_record_batch(b, &mask)?);
    }
    let combined = if kept.is_empty() {
        RecordBatch::new_empty(Arc::new(schema.clone()))
    } else {
        arrow::compute::concat_batches(&Arc::new(schema.clone()), &kept)?
    };
    Ok(combined.columns().to_vec())
}

fn fsb_from_hex(a: &StringArray, n: usize) -> Result<FixedSizeBinaryArray, BoxError> {
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            vals.push(None);
        } else {
            let h = a.value(i).strip_prefix("0x").unwrap_or(a.value(i));
            let b = hex::decode(h).map_err(|e| -> BoxError { e.to_string().into() })?;
            vals.push(Some(b));
        }
    }
    Ok(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vals.into_iter(),
        n as i32,
    )?)
}

fn bin_from_hex(a: &StringArray) -> BinaryArray {
    let owned: Vec<Option<Vec<u8>>> = (0..a.len())
        .map(|i| {
            if a.is_null(i) {
                None
            } else {
                let h = a.value(i).strip_prefix("0x").unwrap_or(a.value(i));
                Some(hex::decode(h).unwrap_or_default())
            }
        })
        .collect();
    BinaryArray::from_iter(owned.iter().map(|o| o.as_deref()))
}

fn dec128_from_str(a: &StringArray) -> Result<Decimal128Array, BoxError> {
    let mut out: Vec<Option<i128>> = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) || a.value(i).is_empty() {
            out.push(None);
        } else {
            out.push(a.value(i).parse::<i128>().ok());
        }
    }
    Ok(Decimal128Array::from(out).with_precision_and_scale(38, 0)?)
}

fn ts_ns_from_secs(a: &UInt64Array) -> TimestampNanosecondArray {
    let v: Vec<i64> = (0..a.len())
        .map(|i| (a.value(i) as i64).saturating_mul(1_000_000_000))
        .collect();
    TimestampNanosecondArray::from(v).with_timezone("+00:00")
}

fn call_type_to_i32(a: &StringArray) -> Int32Array {
    let v: Vec<i32> = (0..a.len())
        .map(|i| match a.value(i).to_ascii_uppercase().as_str() {
            "CALL" => 1,
            "CALLCODE" => 2,
            "DELEGATE" | "DELEGATECALL" => 3,
            "STATIC" | "STATICCALL" => 4,
            "CREATE" => 5,
            "SELFDESTRUCT" => 6,
            _ => 0,
        })
        .collect();
    Int32Array::from(v)
}

fn bool_col(b: &RecordBatch, n: &str) -> Result<BooleanArray, BoxError> {
    let c = b
        .column_by_name(n)
        .ok_or_else(|| -> BoxError { format!("missing {n}").into() })?;
    Ok(c.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| -> BoxError { format!("{n} not bool").into() })?
        .clone())
}
