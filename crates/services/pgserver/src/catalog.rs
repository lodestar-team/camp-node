//! Dynamic DataFusion catalog backing the Postgres-wire endpoint.
//!
//! camp builds an ephemeral catalog per query from the parsed SQL (see
//! `flight::Service::execute_query`). The pgwire layer (`datafusion-postgres`) instead drives a
//! single long-lived [`SessionContext`] via `ctx.sql(..)`, so it needs a catalog that is always
//! present and resolves camp's datasets on demand.
//!
//! This module provides exactly that: a [`CampCatalog`] / [`CampSchema`] pair backed by a
//! periodically-refreshed snapshot ([`SharedCatalog`]). A background task lists the registered
//! datasets, resolves each table through camp's own `catalog_for_sql` builder, and swaps the
//! resulting [`TableSnapshot`]s into the shared map. Reads (`table_names`, `schema`) are lock-free
//! cheap clones of an `Arc`; query-time `table()` returns the most recently published snapshot.
//!
//! Freshness is bounded by the refresh interval — acceptable for analytics/BI read access, and the
//! same staleness model a warehouse mirror has.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::prelude::SessionContext;

use common::catalog::physical::{CatalogSnapshot, TableSnapshot};
use common::catalog::sql::catalog_for_sql;
use common::query_context::QueryEnv;
use dataset_store::DatasetStore;
use datasets_common::reference::Reference;
use metadata_db::MetadataDb;

/// Immutable snapshot of the available schemas/tables. Published atomically by the refresh task.
#[derive(Debug, Default)]
pub struct CatalogState {
    /// schema name -> (table name -> resolved table snapshot)
    schemas: BTreeMap<String, BTreeMap<String, Arc<TableSnapshot>>>,
}

impl CatalogState {
    fn table_count(&self) -> usize {
        self.schemas.values().map(|m| m.len()).sum()
    }
}

/// Lock-free-read handle to the current [`CatalogState`].
#[derive(Clone, Debug)]
pub struct SharedCatalog {
    inner: Arc<RwLock<Arc<CatalogState>>>,
}

impl SharedCatalog {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(CatalogState::default()))),
        }
    }

    fn load(&self) -> Arc<CatalogState> {
        // Poisoning only happens if a writer panicked; the data is still readable.
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn store(&self, state: CatalogState) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(state);
    }
}

impl Default for SharedCatalog {
    fn default() -> Self {
        Self::new()
    }
}

/// A single camp dataset exposed as a Postgres schema.
#[derive(Debug)]
struct CampSchema {
    shared: SharedCatalog,
    schema: String,
}

#[async_trait]
impl SchemaProvider for CampSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.shared
            .load()
            .schemas
            .get(&self.schema)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .shared
            .load()
            .schemas
            .get(&self.schema)
            .and_then(|m| m.get(name))
            .map(|ts| ts.clone() as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.shared
            .load()
            .schemas
            .get(&self.schema)
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }
}

/// The catalog DataFusion resolves against. Holds camp's dynamic schemas plus any schemas
/// registered by `setup_pg_catalog` (i.e. `pg_catalog`).
#[derive(Debug)]
pub struct CampCatalog {
    shared: SharedCatalog,
    /// Statically-registered schemas (pg_catalog). Kept separate so they survive refreshes.
    registered: RwLock<BTreeMap<String, Arc<dyn SchemaProvider>>>,
}

impl CampCatalog {
    pub fn new(shared: SharedCatalog) -> Self {
        Self {
            shared,
            registered: RwLock::new(BTreeMap::new()),
        }
    }
}

impl CatalogProvider for CampCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.shared.load().schemas.keys().cloned().collect();
        names.extend(
            self.registered
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .keys()
                .cloned(),
        );
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(s) = self
            .registered
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
        {
            return Some(s.clone());
        }
        if self.shared.load().schemas.contains_key(name) {
            Some(Arc::new(CampSchema {
                shared: self.shared.clone(),
                schema: name.to_string(),
            }))
        } else {
            None
        }
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DfResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(self
            .registered
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string(), schema))
    }
}

/// Rebuild the catalog snapshot once: enumerate datasets, resolve each table through camp's own
/// `catalog_for_sql` path, register object stores on `ctx`, then publish the new state.
///
/// Errors on individual datasets/tables are logged and skipped — one bad dataset must not blank the
/// whole catalog.
pub async fn refresh_once(
    shared: &SharedCatalog,
    ctx: &SessionContext,
    store: &DatasetStore,
    metadata_db: &MetadataDb,
    env: &QueryEnv,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let tags = store.list_all_datasets().await?;
    let footer_cache = env.parquet_footer_cache.clone();
    let mut state = CatalogState::default();

    for tag in tags {
        let version = tag.version.to_string();
        // "dev" is a moving system tag; skip it to avoid duplicate/unstable schemas.
        if version == "dev" {
            continue;
        }
        let ds_ref = format!("{}/{}@{}", tag.namespace, tag.name, version);

        let reference = match ds_ref.parse::<Reference>() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(dataset = %ds_ref, error = %e, "pgserver: skipping unparsable dataset reference");
                continue;
            }
        };
        let hashref = match store.resolve_revision(&reference).await {
            Ok(Some(h)) => h,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(dataset = %ds_ref, error = %e, "pgserver: skipping unresolvable dataset");
                continue;
            }
        };
        let dataset = match store.get_dataset(&hashref).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(dataset = %ds_ref, error = %e, "pgserver: skipping dataset (load failed)");
                continue;
            }
        };

        for table in dataset.tables() {
            let table_name = table.name().to_string();
            // Resolve this one table through camp's exact catalog builder so we inherit version
            // resolution, canonical-segment logic and physical parquet locations.
            let sql = format!("SELECT * FROM \"{ds_ref}\".\"{table_name}\"");
            let stmt = match common::sql::parse(common::sql_str::SqlStr::new_unchecked(sql)) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "pgserver: skipping table (parse failed)");
                    continue;
                }
            };
            let catalog = match catalog_for_sql(store, metadata_db, &stmt, env.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "pgserver: skipping table (catalog failed)");
                    continue;
                }
            };
            let snapshot =
                match CatalogSnapshot::from_catalog(catalog, false, footer_cache.clone()).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(table = %table_name, error = %e, "pgserver: skipping table (snapshot failed)");
                        continue;
                    }
                };

            for ts in snapshot.table_snapshots() {
                let pt = ts.physical_table();
                // Object stores must be registered on the serving ctx's runtime, since the table
                // providers reference them by URL during scans.
                ctx.register_object_store(pt.url(), pt.object_store());
                let schema_name = pt.catalog_schema().to_string();
                state
                    .schemas
                    .entry(schema_name)
                    .or_default()
                    .insert(table_name.clone(), ts.clone());
            }
        }
    }

    let count = state.table_count();
    shared.store(state);
    Ok(count)
}
