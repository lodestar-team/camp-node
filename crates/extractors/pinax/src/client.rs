//! `BlockStreamer` over Pinax Parquet in S3 (anonymous Tigris bucket).
//!
//! Serves the full-instrumentation tables a JSON-RPC indexer can't produce:
//! `calls` (internal-tx traces) + `storage_changes` / `balance_changes` /
//! `code_changes` / `nonce_changes`. For a block range it locates the overlapping
//! Pinax partition files per table (binary search over the chronologically-sorted
//! file list — not a full scan), maps each onto the Amp schema, and yields one
//! `RawDatasetRows` per block. The engine's worker writes proper camp-node parquet.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Int32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use common::{
    BlockNum, BlockStreamer, BoxError, RawDatasetRows, RawTableRows, Table,
    metadata::segments::BlockRange,
};
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, aws::AmazonS3Builder, path::Path};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;

#[derive(Clone)]
pub struct PinaxClient {
    pub name: String,
    /// Public virtual-hosted base, e.g. "https://pinax.fly.storage.tigris.dev".
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

    fn store(&self) -> Result<Arc<dyn ObjectStore>, BoxError> {
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
}

/// (amp table name, pinax partition dir, mapper) — pinax dir == amp name here.
type MapFn = fn(&RecordBatch) -> Result<RecordBatch, BoxError>;
fn table_specs() -> Vec<(&'static str, MapFn)> {
    vec![
        ("calls", map_calls as MapFn),
        ("storage_changes", map_storage_changes),
        ("balance_changes", map_balance_changes),
        ("code_changes", map_code_changes),
        ("nonce_changes", map_nonce_changes),
    ]
}

impl BlockStreamer for PinaxClient {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        async_stream::try_stream! {
            let store = self.store()?;
            let specs = table_specs();
            // table -> (Table, mapped Amp batches across the range)
            let mut loaded: Vec<(Table, Vec<RecordBatch>)> = Vec::new();
            for (name, map_fn) in &specs {
                let table = crate::tables::all(&self.network)
                    .into_iter()
                    .find(|t| t.name() == *name)
                    .ok_or_else(|| -> BoxError { format!("no table {name}").into() })?;
                let prefix = Path::from(format!("{}/{}", self.network, name));
                let files = files_in_range(&store, &prefix, start, end).await?;
                let mut batches = Vec::new();
                for meta in files {
                    let reader = ParquetObjectReader::new(store.clone(), meta.location);
                    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                    let mut s = builder.build()?;
                    while let Some(b) = s.next().await {
                        batches.push(map_fn(&b?)?);
                    }
                }
                loaded.push((table, batches));
            }

            for block in start..=end {
                let mut rows = Vec::with_capacity(loaded.len());
                for (table, batches) in &loaded {
                    let cols = filter_block(batches, block, table.schema())?;
                    let range = BlockRange {
                        numbers: block..=block,
                        network: self.network.clone(),
                        hash: Default::default(),
                        prev_hash: None,
                    };
                    rows.push(RawTableRows::new(table.clone(), range, cols)?);
                }
                yield RawDatasetRows::new(rows);
            }
        }
    }

    async fn latest_block(&mut self, _finalized: bool) -> Result<Option<BlockNum>, BoxError> {
        let store = self.store()?;
        let prefix = Path::from(format!("{}/calls", self.network));
        let metas: Vec<ObjectMeta> = store.list(Some(&prefix)).try_collect().await?;
        let Some(last) = metas.into_iter().max_by(|a, b| a.location.cmp(&b.location)) else {
            return Ok(None);
        };
        let (_, mx) = file_bounds(&store, &last).await?;
        Ok(mx)
    }

    fn provider_name(&self) -> &str {
        &self.name
    }
}

// ---- file selection (binary search over chronologically-sorted files) ----

async fn file_bounds(
    store: &Arc<dyn ObjectStore>,
    meta: &ObjectMeta,
) -> Result<(Option<BlockNum>, Option<BlockNum>), BoxError> {
    let reader = ParquetObjectReader::new(store.clone(), meta.location.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let m = builder.metadata();
    let schema = builder.parquet_schema();
    let Some(idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == "block_num")
    else {
        return Ok((None, None));
    };
    let (mut lo, mut hi, mut have) = (u64::MAX, 0u64, false);
    for rg in m.row_groups() {
        if let Some(st) = rg.column(idx).statistics() {
            if let (Some(a), Some(b)) = (u64le(st.min_bytes_opt()), u64le(st.max_bytes_opt())) {
                lo = lo.min(a);
                hi = hi.max(b);
                have = true;
            }
        }
    }
    Ok(if have { (Some(lo), Some(hi)) } else { (None, None) })
}

fn u64le(b: Option<&[u8]>) -> Option<u64> {
    let b = b?;
    (b.len() == 8).then(|| u64::from_le_bytes(b.try_into().unwrap()))
}

/// Files overlapping [start,end], via binary search on the sorted listing.
async fn files_in_range(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    start: BlockNum,
    end: BlockNum,
) -> Result<Vec<ObjectMeta>, BoxError> {
    let mut metas: Vec<ObjectMeta> = store.list(Some(prefix)).try_collect().await?;
    metas.sort_by(|a, b| a.location.cmp(&b.location)); // chronological
    if metas.is_empty() {
        return Ok(vec![]);
    }
    // lowest index whose max_block >= start (unknown stats treated as candidate)
    let (mut lo, mut hi) = (0usize, metas.len());
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (_, mx) = file_bounds(store, &metas[mid]).await?;
        match mx {
            Some(mx) if mx < start => lo = mid + 1,
            _ => hi = mid,
        }
    }
    // forward scan, stop once a file starts past end
    let mut out = Vec::new();
    for meta in &metas[lo..] {
        let (mn, _) = file_bounds(store, meta).await?;
        if let Some(mn) = mn {
            if mn > end {
                break;
            }
        }
        out.push(meta.clone());
    }
    Ok(out)
}

/// Slice all of a table's mapped batches to a single block; columns in schema order.
fn filter_block(
    batches: &[RecordBatch],
    block: BlockNum,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, BoxError> {
    let mut kept = Vec::new();
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
        RecordBatch::new_empty(schema.clone())
    } else {
        arrow::compute::concat_batches(schema, &kept)?
    };
    Ok(combined.columns().to_vec())
}

// ---- column helpers ----

/// Get a column as Utf8 (handles dictionary / large-utf8 / utf8view).
fn utf8(b: &RecordBatch, n: &str) -> Result<StringArray, BoxError> {
    let c = b
        .column_by_name(n)
        .ok_or_else(|| -> BoxError { format!("missing column {n}").into() })?;
    Ok(arrow::compute::cast(c, &DataType::Utf8)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| -> BoxError { format!("{n} not Utf8-castable").into() })?
        .clone())
}
fn u64col(b: &RecordBatch, n: &str) -> Result<UInt64Array, BoxError> {
    let c = b.column_by_name(n).ok_or_else(|| -> BoxError { format!("missing {n}").into() })?;
    Ok(arrow::compute::cast(c, &DataType::UInt64)?
        .as_any().downcast_ref::<UInt64Array>().unwrap().clone())
}
fn u32col(b: &RecordBatch, n: &str) -> Result<UInt32Array, BoxError> {
    let c = b.column_by_name(n).ok_or_else(|| -> BoxError { format!("missing {n}").into() })?;
    Ok(arrow::compute::cast(c, &DataType::UInt32)?
        .as_any().downcast_ref::<UInt32Array>().unwrap().clone())
}
fn fsb(a: &StringArray, n: usize) -> Result<FixedSizeBinaryArray, BoxError> {
    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            vals.push(None);
        } else {
            let h = a.value(i).strip_prefix("0x").unwrap_or(a.value(i));
            match hex::decode(h) {
                Ok(b) if b.len() == n => vals.push(Some(b)),
                _ => vals.push(None), // wrong length / invalid → null
            }
        }
    }
    Ok(FixedSizeBinaryArray::try_from_sparse_iter_with_size(vals.into_iter(), n as i32)?)
}
fn bin(a: &StringArray) -> BinaryArray {
    let owned: Vec<Option<Vec<u8>>> = (0..a.len())
        .map(|i| (!a.is_null(i)).then(|| {
            let h = a.value(i).strip_prefix("0x").unwrap_or(a.value(i));
            hex::decode(h).unwrap_or_default()
        }))
        .collect();
    BinaryArray::from_iter(owned.iter().map(|o| o.as_deref()))
}
fn dec(a: &StringArray) -> Result<Decimal128Array, BoxError> {
    let out: Vec<Option<i128>> = (0..a.len())
        .map(|i| (!a.is_null(i) && !a.value(i).is_empty()).then(|| a.value(i).parse::<i128>().ok()).flatten())
        .collect();
    Ok(Decimal128Array::from(out).with_precision_and_scale(38, 0)?)
}
fn ts_ns(a: &UInt64Array) -> TimestampNanosecondArray {
    let v: Vec<i64> = (0..a.len()).map(|i| (a.value(i) as i64).saturating_mul(1_000_000_000)).collect();
    TimestampNanosecondArray::from(v).with_timezone("+00:00")
}

fn sch(table: &str) -> SchemaRef {
    crate::tables::schema_for("", table).expect("schema")
}

/// Shared head columns for change tables: _block_num, block_hash, block_num, timestamp, tx_hash, ordinal, address.
fn head(b: &RecordBatch) -> Result<Vec<ArrayRef>, BoxError> {
    let block_num = u64col(b, "block_num")?;
    Ok(vec![
        Arc::new(block_num.clone()),
        Arc::new(fsb(&utf8(b, "block_id")?, 32)?),
        Arc::new(block_num),
        Arc::new(ts_ns(&u64col(b, "timestamp")?)),
        Arc::new(fsb(&utf8(b, "tx_hash")?, 32)?),
        Arc::new(u64col(b, "ordinal")?),
        Arc::new(fsb(&utf8(b, "address")?, 20)?),
    ])
}

// ---- per-table mappers ----

fn map_storage_changes(b: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let mut c = head(b)?;
    c.push(Arc::new(bin(&utf8(b, "key")?)));
    c.push(Arc::new(bin(&utf8(b, "old_value")?)));
    c.push(Arc::new(bin(&utf8(b, "new_value")?)));
    Ok(RecordBatch::try_new(sch("storage_changes"), c)?)
}
fn map_balance_changes(b: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let mut c = head(b)?;
    c.push(Arc::new(utf8(b, "old_value")?));
    c.push(Arc::new(utf8(b, "new_value")?));
    c.push(Arc::new(utf8(b, "reason")?));
    Ok(RecordBatch::try_new(sch("balance_changes"), c)?)
}
fn map_code_changes(b: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let mut c = head(b)?;
    c.push(Arc::new(fsb(&utf8(b, "old_hash")?, 32)?));
    c.push(Arc::new(fsb(&utf8(b, "new_hash")?, 32)?));
    c.push(Arc::new(bin(&utf8(b, "old_code")?)));
    c.push(Arc::new(bin(&utf8(b, "new_code")?)));
    Ok(RecordBatch::try_new(sch("code_changes"), c)?)
}
fn map_nonce_changes(b: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let mut c = head(b)?;
    c.push(Arc::new(u64col(b, "old_value")?));
    c.push(Arc::new(u64col(b, "new_value")?));
    Ok(RecordBatch::try_new(sch("nonce_changes"), c)?)
}

fn map_calls(b: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let block_num = u64col(b, "block_num")?;
    let ct_src = utf8(b, "call_type")?;
    let ct = Int32Array::from(
        (0..ct_src.len())
            .map(|i| match ct_src.value(i).to_ascii_uppercase().as_str() {
                "CALL" => 1, "CALLCODE" => 2, "DELEGATE" | "DELEGATECALL" => 3,
                "STATIC" | "STATICCALL" => 4, "CREATE" => 5, "SELFDESTRUCT" => 6, _ => 0,
            })
            .collect::<Vec<i32>>(),
    );
    let bool_col = |n: &str| -> Result<BooleanArray, BoxError> {
        Ok(b.column_by_name(n)
            .ok_or_else(|| -> BoxError { format!("missing {n}").into() })?
            .as_any().downcast_ref::<BooleanArray>()
            .ok_or_else(|| -> BoxError { format!("{n} not bool").into() })?.clone())
    };
    let zero = || UInt64Array::from(vec![0u64; b.num_rows()]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(block_num.clone()),
        Arc::new(fsb(&utf8(b, "block_id")?, 32)?),
        Arc::new(block_num),
        Arc::new(ts_ns(&u64col(b, "timestamp")?)),
        Arc::new(u32col(b, "tx_index")?),
        Arc::new(fsb(&utf8(b, "tx_hash")?, 32)?),
        Arc::new(u32col(b, "call_index")?),
        Arc::new(u32col(b, "parent_index")?),
        Arc::new(u32col(b, "depth")?),
        Arc::new(ct),
        Arc::new(fsb(&utf8(b, "caller")?, 20)?),
        Arc::new(fsb(&utf8(b, "address")?, 20)?),
        Arc::new(dec(&utf8(b, "value")?)?),
        Arc::new(u64col(b, "gas_limit")?),
        Arc::new(u64col(b, "gas_consumed")?),
        Arc::new(bin(&utf8(b, "output")?)),
        Arc::new(bin(&utf8(b, "input")?)),
        Arc::new(bool_col("suicide")?),
        Arc::new(bool_col("executed_code")?),
        Arc::new(zero()),
        Arc::new(zero()),
    ];
    Ok(RecordBatch::try_new(sch("calls"), cols)?)
}
