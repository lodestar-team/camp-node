//! Integration tests for the dynamic-catalog refresh against a live metadata DB.
//!
//! These exercise [`crate::catalog::refresh_once`] end-to-end: a real (temporary) Postgres
//! metadata DB, a real [`DatasetStore`] over temp object stores, and camp's actual
//! dataset-resolution path — not the in-memory `CatalogState` shims the unit tests use.
//!
//! The invariant under test is the resilience guarantee the module documents: a dataset that
//! fails to resolve is logged and skipped, never surfacing an error or blanking the catalog.
//!
//! Note: `temp_metadata_db` is a process-global singleton, so every `allow_temp_db` config in
//! this binary shares one Postgres instance. Assertions are therefore written to be
//! order-independent — an unresolvable dataset contributes zero tables regardless of what else
//! is registered, so `n == 0` holds whichever test runs first.

use std::sync::atomic::{AtomicU32, Ordering};

use datafusion::prelude::SessionContext;

use common::config::Config;
use common::query_context::create_query_env;
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use metadata_db::{
    DatasetName, DatasetNamespace, DatasetVersion, ManifestHash, ManifestPath, datasets, manifests,
};

use crate::catalog::{SharedCatalog, refresh_once};

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// Build a fully-wired `Config` over fresh temp dirs plus an auto-provisioned temp metadata DB —
/// mirroring `ampd dev`'s startup wiring exactly (see `dev_cmd::run`).
async fn fixture() -> Config {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("pgserver-it-{}-{}", std::process::id(), n));
    for sub in ["data", "providers", "manifests"] {
        std::fs::create_dir_all(root.join(sub)).expect("create temp subdir");
    }
    let cfg_path = root.join("ampd.toml");
    std::fs::write(
        &cfg_path,
        "data_dir = \"data\"\n\
         providers_dir = \"providers\"\n\
         manifests_dir = \"manifests\"\n\
         poll_interval_secs = 1.0\n",
    )
    .expect("write config");

    // allow_temp_db = true → with no metadata_db.url set, Config provisions (and keeps alive) a
    // process-global temp Postgres and runs migrations against it.
    Config::load(&cfg_path, false, None, true, None)
        .await
        .expect("load config")
}

fn dataset_store(config: &Config, metadata_db: metadata_db::MetadataDb) -> DatasetStore {
    DatasetStore::new(
        metadata_db,
        ProviderConfigsStore::new(config.providers_store.prefixed_store()),
        DatasetManifestsStore::new(config.manifests_store.prefixed_store()),
    )
}

#[tokio::test]
async fn refresh_with_nothing_resolvable_yields_empty_catalog() {
    let config = fixture().await;
    let metadata_db = config.metadata_db().await.expect("metadata db");
    let store = dataset_store(&config, metadata_db.clone());
    let env = create_query_env(0, 0, &[], 64).expect("query env");
    let ctx = SessionContext::new();
    let shared = SharedCatalog::new();

    let n = refresh_once(&shared, &ctx, &store, &metadata_db, &env)
        .await
        .expect("refresh must not error when nothing resolves");
    assert_eq!(n, 0, "no resolvable datasets → empty catalog");
}

#[tokio::test]
async fn refresh_skips_unresolvable_dataset_without_blanking() {
    let config = fixture().await;
    let metadata_db = config.metadata_db().await.expect("metadata db");

    // Register a dataset tag whose manifest file does NOT exist in the manifests store.
    // `list_all_datasets` will return it, but `get_dataset` (manifest load) will fail —
    // exercising refresh_once's warn-and-skip path.
    let ns = DatasetNamespace::from_ref_unchecked("_");
    let name = DatasetName::from_ref_unchecked("ghost");
    let version = DatasetVersion::from_ref_unchecked("1.0.0");
    let hash = ManifestHash::from_ref_unchecked(
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    let path = ManifestPath::from_ref_unchecked("nonexistent/manifest.json");

    let mut tx = metadata_db.begin_txn().await.expect("begin txn");
    manifests::register(&mut tx, &hash, &path)
        .await
        .expect("register manifest");
    datasets::link_manifest(&mut tx, &ns, &name, &hash)
        .await
        .expect("link manifest");
    datasets::register_version_tag(&mut tx, &ns, &name, &version, &hash)
        .await
        .expect("register version tag");
    tx.commit().await.expect("commit");

    let store = dataset_store(&config, metadata_db.clone());
    let env = create_query_env(0, 0, &[], 64).expect("query env");
    let ctx = SessionContext::new();
    let shared = SharedCatalog::new();

    // The bad dataset must be skipped — not surfaced as an error, not left as a poisoned catalog.
    let n = refresh_once(&shared, &ctx, &store, &metadata_db, &env)
        .await
        .expect("refresh must skip the unresolvable dataset, not error");
    assert_eq!(n, 0, "an unresolvable dataset contributes no tables");
}
