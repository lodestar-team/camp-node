//! Postgres-wire endpoint for camp-node.
//!
//! Exposes the same DataFusion query layer as the Flight/JSON-Lines servers over the
//! PostgreSQL wire protocol, so BI tools (Grafana, Metabase, DBeaver, psql, …) connect
//! natively. Read-only. Built on `datafusion-postgres` (pinned to the DataFusion-50 line
//! that matches this workspace).
//!
//! camp resolves its catalog per-query from the parsed SQL, whereas the pgwire layer drives a
//! single long-lived [`SessionContext`]. We bridge the two with a dynamic catalog
//! ([`catalog::CampCatalog`]) backed by a periodically-refreshed snapshot, plus `pg_catalog`
//! emulation so BI clients can introspect. See [`catalog`] for details.

mod catalog;

#[cfg(test)]
mod it_refresh;

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve};

use common::memory_pool::{MemoryPoolKind, TieredMemoryPool, make_memory_pool};
use common::query_context::QueryEnv;
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use catalog::{CampCatalog, SharedCatalog};

/// Default interval at which the dynamic catalog is rebuilt from the metadata DB.
const DEFAULT_REFRESH: Duration = Duration::from_secs(30);

/// Max simultaneous Postgres-wire connections (the lib semaphore-gates accepts; 0 = unlimited).
/// Generous — only rejects pathological connection floods; normal BI/psql usage is well under it.
const MAX_PG_CONNECTIONS: usize = 128;

/// Memory ceiling (MB) for the Postgres-wire query pool, used when the config sets no per-query
/// limit (`query_max_mem_mb == 0`, the default). Bounds a runaway pg query (cross-join / huge
/// sort) so it fails with ResourcesExhausted instead of OOM-ing the box. Generous on a 64 GiB host.
const DEFAULT_PG_MEM_MB: usize = 8192;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("pg server: {0}")]
    Serve(String),

    #[error("pg_catalog setup: {0}")]
    PgCatalog(String),
}

/// Build the long-lived serving context: a dynamic camp catalog + pg_catalog emulation + camp UDFs,
/// with a bounded memory pool so a single pg query can't exhaust the box.
fn build_context(shared: SharedCatalog, env: &QueryEnv) -> Result<Arc<SessionContext>, Error> {
    let config = SessionConfig::new()
        .with_information_schema(true)
        // We register our own "amp" catalog below; don't let DataFusion create a memory one.
        .with_create_default_catalog_and_schema(false)
        .with_default_catalog_and_schema("amp", "public");

    // The Flight path bounds memory per-query via a tiered pool; the pgwire path drives a single
    // long-lived context via `ctx.sql`, so we bound it once here. A tiered pool (global parent +
    // a finite pg child) means a runaway pg query (cross-join / huge sort) fails with
    // ResourcesExhausted instead of OOM-ing the node. Reuse the engine's disk manager, footer
    // cache and object-store registry so scans and spill behave exactly as on the other frontends.
    let pg_mem_mb = if env.query_max_mem_mb > 0 {
        env.query_max_mem_mb
    } else {
        DEFAULT_PG_MEM_MB
    };
    let pg_pool = TieredMemoryPool::new(
        env.global_memory_pool.clone(),
        make_memory_pool(MemoryPoolKind::Greedy, pg_mem_mb * 1024 * 1024),
    );
    let runtime_env = Arc::new(RuntimeEnv {
        memory_pool: Arc::new(pg_pool),
        disk_manager: env.disk_manager.clone(),
        cache_manager: env.cache_manager.clone(),
        object_store_registry: env.object_store_registry.clone(),
    });

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // The dynamic camp catalog. pg_catalog registers itself into this same ("amp") catalog.
    ctx.register_catalog("amp", Arc::new(CampCatalog::new(shared)));

    setup_pg_catalog(&ctx, "amp", EmptyContextProvider)
        .map_err(|e| Error::PgCatalog(e.to_string()))?;

    // camp's EVM UDFs (evm_decode_log, evm_topic, encode/decode params, …) so they work over pgwire.
    for udf in common::query_context::udfs() {
        ctx.register_udf(udf);
    }
    for udaf in common::query_context::udafs() {
        ctx.register_udaf(udaf);
    }

    Ok(Arc::new(ctx))
}

/// Serve a Postgres-wire endpoint over camp's query layer. Blocks until the listener stops.
///
/// `store`/`metadata_db`/`env` are the same handles the Flight `Service` holds; share clones.
pub async fn run(
    store: DatasetStore,
    metadata_db: MetadataDb,
    env: QueryEnv,
    addr: &str,
) -> Result<(), Error> {
    run_with_refresh(store, metadata_db, env, addr, DEFAULT_REFRESH).await
}

pub async fn run_with_refresh(
    store: DatasetStore,
    metadata_db: MetadataDb,
    env: QueryEnv,
    addr: &str,
    refresh: Duration,
) -> Result<(), Error> {
    let shared = SharedCatalog::new();
    let ctx = build_context(shared.clone(), &env)?;

    // Prime the catalog before accepting connections so the first client sees tables.
    match catalog::refresh_once(&shared, &ctx, &store, &metadata_db, &env).await {
        Ok(n) => tracing::info!(tables = n, "pgserver: initial catalog loaded"),
        Err(e) => {
            tracing::warn!(error = %e, "pgserver: initial catalog refresh failed (will retry)")
        }
    }

    // Background refresh keeps the catalog fresh as the chain advances / datasets change.
    {
        let shared = shared.clone();
        let ctx = ctx.clone();
        let store = store.clone();
        let metadata_db = metadata_db.clone();
        let env = env.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(refresh);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                match catalog::refresh_once(&shared, &ctx, &store, &metadata_db, &env).await {
                    Ok(n) => tracing::debug!(tables = n, "pgserver: catalog refreshed"),
                    Err(e) => tracing::warn!(error = %e, "pgserver: catalog refresh failed"),
                }
            }
        });
    }

    let (host, port) = split_addr(addr);
    let opts = ServerOptions::new()
        .with_host(host)
        .with_port(port)
        // Bound the connection table so a flood can't exhaust the listener.
        .with_max_connections(MAX_PG_CONNECTIONS);
    // No password auth: the endpoint binds localhost and the public edge (engine.camp) terminates
    // auth + rate-limits, exactly as it does for Flight / JSON Lines. Public exposure (TLS + auth +
    // per-IP rate limit, plus a statement timeout once the lib supports a server default) is a
    // separate, deliberate step — see docs/pgwire-exposure.md.
    let auth = Arc::new(AuthManager::new());
    tracing::info!(%addr, "starting postgres-wire endpoint (read-only)");
    serve(ctx, &opts, auth)
        .await
        .map_err(|e| Error::Serve(e.to_string()))
}

fn split_addr(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(5432)),
        None => (addr.to_string(), 5432),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_context, split_addr};
    use crate::catalog::SharedCatalog;

    #[test]
    fn build_context_registers_amp_catalog_with_bounded_pool() {
        // query_max_mem_mb = 0 → the pg pool falls back to the bounded default (not unbounded).
        let env = common::query_context::create_query_env(0, 0, &[], 64).expect("query env");
        let ctx = build_context(SharedCatalog::new(), &env).expect("context builds");
        // The dynamic camp catalog is registered as "amp" (pg_catalog layers into it).
        assert!(ctx.catalog_names().contains(&"amp".to_string()));
        // information_schema is on so BI tools can introspect.
        assert!(ctx.catalog("amp").is_some());
    }

    #[test]
    fn split_addr_parses_host_and_port() {
        assert_eq!(split_addr("127.0.0.1:5432"), ("127.0.0.1".into(), 5432));
        assert_eq!(split_addr("0.0.0.0:15432"), ("0.0.0.0".into(), 15432));
        // missing port → default 5432
        assert_eq!(split_addr("hostonly"), ("hostonly".into(), 5432));
        // unparseable port → default 5432
        assert_eq!(split_addr("h:notaport"), ("h".into(), 5432));
    }
}
