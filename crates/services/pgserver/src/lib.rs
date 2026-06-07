//! Postgres-wire endpoint for camp-node.
//!
//! Exposes the same DataFusion query layer as the Flight/JSON-Lines servers over the
//! PostgreSQL wire protocol, so BI tools (Grafana, Metabase, DBeaver, psql, …) connect
//! natively. Read-only. Built on `datafusion-postgres` (pinned to the DataFusion-50 line
//! that matches this workspace).

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("pg server: {0}")]
    Serve(String),
}

/// Serve a Postgres-wire endpoint over the given DataFusion context.
/// Blocks until the listener stops.
pub async fn run(ctx: Arc<SessionContext>, addr: &str) -> Result<(), Error> {
    let (host, port) = split_addr(addr);
    let opts = ServerOptions::new().with_host(host).with_port(port);
    // Default = no password auth. The endpoint binds localhost; the public edge
    // (engine.camp) terminates auth + rate-limits, as it does for Flight/JSON Lines.
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
