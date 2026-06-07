//! Amp table schemas for Pinax full-instrumentation tables that the firehose
//! extractor doesn't define (storage/balance/code/nonce changes). Conventions
//! match the firehose tables: `_block_num` partition col, addresses/hashes as
//! FixedSizeBinary, nanosecond timestamps. Variable/uncertain hex and big-int
//! value fields are kept as Binary/Utf8 for robustness across the dataset.

use std::sync::Arc;

use common::{
    BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE, SPECIAL_BLOCK_NUM, Table,
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    timestamp_type,
};

fn mk(name: &str, schema: Schema, network: String) -> Table {
    Table::new(
        name.parse().expect("valid table name"),
        Arc::new(schema),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}

/// Fields shared by every change table.
fn common_head() -> Vec<Field> {
    vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("block_hash", BYTES32_TYPE, false),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("timestamp", timestamp_type(), false),
        Field::new("tx_hash", BYTES32_TYPE, true),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("address", ADDRESS_TYPE, false),
    ]
}

pub fn storage_changes(network: String) -> Table {
    let mut f = common_head();
    f.push(Field::new("key", DataType::Binary, false));
    f.push(Field::new("old_value", DataType::Binary, true));
    f.push(Field::new("new_value", DataType::Binary, true));
    mk("storage_changes", Schema::new(f), network)
}

pub fn balance_changes(network: String) -> Table {
    let mut f = common_head();
    f.push(Field::new("old_value", DataType::Utf8, true));
    f.push(Field::new("new_value", DataType::Utf8, true));
    f.push(Field::new("reason", DataType::Utf8, true));
    mk("balance_changes", Schema::new(f), network)
}

pub fn code_changes(network: String) -> Table {
    let mut f = common_head();
    f.push(Field::new("old_hash", BYTES32_TYPE, true));
    f.push(Field::new("new_hash", BYTES32_TYPE, true));
    f.push(Field::new("old_code", DataType::Binary, true));
    f.push(Field::new("new_code", DataType::Binary, true));
    mk("code_changes", Schema::new(f), network)
}

pub fn nonce_changes(network: String) -> Table {
    let mut f = common_head();
    f.push(Field::new("old_value", DataType::UInt64, false));
    f.push(Field::new("new_value", DataType::UInt64, false));
    mk("nonce_changes", Schema::new(f), network)
}

/// All Amp tables this source serves, in a stable order. `calls` comes from the
/// firehose extractor; the change tables are defined above.
pub fn all(network: &str) -> Vec<Table> {
    let n = || network.to_string();
    vec![
        firehose_datasets::evm::tables::all(network)
            .into_iter()
            .find(|t| t.name() == "calls")
            .expect("firehose calls table"),
        storage_changes(n()),
        balance_changes(n()),
        code_changes(n()),
        nonce_changes(n()),
    ]
}

/// Schema for one Amp table by name (for the mapper to build batches against).
pub fn schema_for(network: &str, table: &str) -> Option<SchemaRef> {
    all(network)
        .into_iter()
        .find(|t| t.name() == table)
        .map(|t| t.schema().clone())
}
