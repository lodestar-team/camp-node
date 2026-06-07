//! Pinax source: reads Pinax's public Firehose→Parquet (Tigris/S3) for a block
//! range and materialises it through camp-node's own writer, reusing the firehose
//! table schemas (blocks/calls/logs/transactions). Because the engine writes the
//! output files itself, they get proper footer metadata + catalog entries (unlike
//! an external `dataset restore`).

pub mod client;
pub mod dataset_kind;
pub mod tables;

pub use dataset_kind::PinaxDatasetKind;

use std::collections::BTreeMap;

use common::Dataset;

/// Provider config (one TOML under `providers_dir`).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    /// Must be "pinax".
    pub kind: String,
    /// Pinax chain name / bucket prefix, e.g. "mainnet", "arbitrum-one".
    pub network: String,
    /// Public S3/HTTPS base, e.g. "https://pinax.fly.storage.tigris.dev".
    pub endpoint: String,
}

/// Pinax dataset manifest (mirrors the firehose manifest shape; reuses firehose
/// table definitions). `tables` is carried for round-trip/canonicalisation but
/// the logical schema is taken from the firehose EVM tables in [`dataset`].
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Manifest {
    pub kind: PinaxDatasetKind,
    pub network: String,
    pub start_block: common::BlockNum,
    #[serde(default)]
    pub finalized_blocks_only: bool,
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, firehose_datasets::dataset::Table>,
}

/// Build a logical dataset from a Pinax manifest. Reuses the firehose EVM table
/// schemas so `calls`/`storage_changes`-class data lands in known columns.
pub fn dataset(manifest_hash: datasets_common::hash::Hash, manifest: Manifest) -> Dataset {
    let network = manifest.network;
    // Full-instrumentation set: calls + storage/balance/code/nonce changes.
    let tables = crate::tables::all(&network);
    Dataset {
        manifest_hash,
        dependencies: BTreeMap::new(),
        kind: PinaxDatasetKind.as_str().to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables,
        network: Some(network),
        functions: vec![],
    }
}
