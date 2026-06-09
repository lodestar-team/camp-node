use common::{BoxError, config::Config};

mod controller_cmd;
mod dev_cmd;
mod migrate_cmd;
mod server_cmd;
mod worker_cmd;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Parser)]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
struct Args {
    /// The configuration file to use. This file defines where to look for dataset definitions and
    /// providers, along with many other configuration options.
    #[arg(long, env = "AMP_CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
    Dev {
        /// Enable Arrow Flight RPC Server.
        #[arg(long, env = "FLIGHT_SERVER")]
        flight_server: bool,
        /// Enable JSON Lines Server.
        #[arg(long, env = "JSONL_SERVER")]
        jsonl_server: bool,
        /// Enable Admin API Server.
        #[arg(long, env = "ADMIN_SERVER")]
        admin_server: bool,
        /// Enable the read-only Postgres-wire Server. Optionally pass a bind address;
        /// defaults to 127.0.0.1:5432 when given as a bare flag.
        #[arg(long, env = "PG_SERVER", num_args = 0..=1, default_missing_value = "127.0.0.1:5432")]
        pg_server: Option<String>,
    },
    Server {
        /// Enable Arrow Flight RPC Server.
        #[arg(long, env = "FLIGHT_SERVER")]
        flight_server: bool,
        /// Enable JSON Lines Server.
        #[arg(long, env = "JSONL_SERVER")]
        jsonl_server: bool,
        /// Enable the read-only Postgres-wire Server. Optionally pass a bind address;
        /// defaults to 127.0.0.1:5432 when given as a bare flag.
        #[arg(long, env = "PG_SERVER", num_args = 0..=1, default_missing_value = "127.0.0.1:5432")]
        pg_server: Option<String>,
    },
    Worker {
        /// The node id of the worker.
        #[arg(long, env = "AMP_NODE_ID")]
        node_id: String,
    },
    Controller,
    /// Run migrations on the metadata database
    Migrate,
}

#[tokio::main]
async fn main() {
    if let Err(err) = main_inner().await {
        // Manually print the error so we can control the format.
        let err = common::utils::error_with_causes(&*err);
        eprintln!("Exiting with error: {err}");
        std::process::exit(1);
    }
}

async fn main_inner() -> Result<(), BoxError> {
    // Initialize tokio-console subscriber if feature is enabled
    #[cfg(feature = "console-subscriber")]
    {
        console_subscriber::init();
        tracing::info!("tokio-console subscriber initialized");
    }

    let Args {
        config: config_path,
        command,
    } = clap::Parser::parse();

    // Log version info
    tracing::info!(
        "version {}, commit {} ({}), built on {}",
        env!("VERGEN_GIT_DESCRIBE"),
        env!("VERGEN_GIT_SHA"),
        env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        env!("VERGEN_BUILD_DATE"),
    );

    match command {
        Command::Dev {
            mut flight_server,
            mut jsonl_server,
            mut admin_server,
            pg_server,
        } => {
            let pg_at = parse_pg_addr(pg_server)?;

            // Flight + JSONL + Admin auto-enable when none is selected explicitly. pgwire is
            // additive (opt-in via --pg-server) and never suppresses them.
            (flight_server, jsonl_server, admin_server) =
                resolve_default_servers(flight_server, jsonl_server, admin_server);

            let config = load_config(config_path.as_ref(), true).await?;

            let (_providers, meter) = monitoring::init(config.opentelemetry.as_ref())?;

            dev_cmd::run(
                config,
                meter,
                flight_server,
                jsonl_server,
                admin_server,
                pg_at,
            )
            .await
            .map_err(Into::into)
        }
        Command::Server {
            mut flight_server,
            mut jsonl_server,
            pg_server,
        } => {
            let pg_at = parse_pg_addr(pg_server)?;

            // Flight + JSONL auto-enable when neither is selected explicitly. pgwire is additive
            // (opt-in via --pg-server) and never suppresses them. (No Admin in `server` mode.)
            (flight_server, jsonl_server, _) =
                resolve_default_servers(flight_server, jsonl_server, false);

            let config = load_config(config_path.as_ref(), false).await?;
            let addrs = config.addrs.clone();

            let (_providers, meter) = monitoring::init(config.opentelemetry.as_ref())?;

            server_cmd::run(config, meter, &addrs, flight_server, jsonl_server, pg_at)
                .await
                .map_err(Into::into)
        }
        Command::Worker { node_id } => {
            let node_id = node_id.parse()?;

            let config = load_config(config_path.as_ref(), false).await?;

            let (_providers, meter) = monitoring::init(config.opentelemetry.as_ref())?;

            worker_cmd::run(config, meter, node_id)
                .await
                .map_err(Into::into)
        }
        Command::Controller => {
            let config = load_config(config_path.as_ref(), false).await?;
            let admin_api_addr = config.addrs.admin_api_addr;

            let (_providers, meter) = monitoring::init(config.opentelemetry.as_ref())?;

            controller_cmd::run(config, meter, admin_api_addr)
                .await
                .map_err(Into::into)
        }
        Command::Migrate => {
            let config = load_config(config_path.as_ref(), false).await?;

            let (_providers, _meter) = monitoring::init(config.opentelemetry.as_ref())?;

            migrate_cmd::run(config).await.map_err(Into::into)
        }
    }
}

/// Resolve which of the default servers (Flight / JSON-Lines / Admin) to enable.
///
/// Convenience default: if the user enabled *none* of them explicitly, enable all three. Any
/// explicit selection is honoured verbatim. This is intentionally independent of `--pg-server`:
/// the Postgres-wire endpoint is additive and must never suppress the default servers (enabling
/// pgwire on a node that otherwise relies on the default trio is the common case). For the
/// `server` command (no Admin), pass `admin = false` and ignore the third return value.
fn resolve_default_servers(flight: bool, jsonl: bool, admin: bool) -> (bool, bool, bool) {
    if !flight && !jsonl && !admin {
        (true, true, true)
    } else {
        (flight, jsonl, admin)
    }
}

/// Parse the optional `--pg-server` address. `None` (flag absent) disables the endpoint.
fn parse_pg_addr(pg_server: Option<String>) -> Result<Option<std::net::SocketAddr>, BoxError> {
    pg_server
        .map(|s| {
            s.parse::<std::net::SocketAddr>()
                .map_err(|e| format!("invalid --pg-server address {s:?}: {e}").into())
        })
        .transpose()
}

async fn load_config(
    config_path: Option<&String>,
    allow_temp_db: bool,
) -> Result<Config, BoxError> {
    let Some(config) = config_path else {
        return Err("--config parameter is mandatory".into());
    };

    // Gather build info from environment variables set by vergen
    let build_info = common::config::BuildInfo {
        version: env!("VERGEN_GIT_DESCRIBE").to_string(),
        commit_sha: env!("VERGEN_GIT_SHA").to_string(),
        commit_timestamp: env!("VERGEN_GIT_COMMIT_TIMESTAMP").to_string(),
        build_date: env!("VERGEN_BUILD_DATE").to_string(),
    };

    let config = Config::load(config, true, None, allow_temp_db, build_info).await?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::{parse_pg_addr, resolve_default_servers};

    #[test]
    fn no_explicit_flags_enables_default_trio() {
        // The convenience default: a bare `dev`/`server` runs the standard servers.
        assert_eq!(resolve_default_servers(false, false, false), (true, true, true));
    }

    #[test]
    fn explicit_selection_is_honoured_verbatim() {
        assert_eq!(resolve_default_servers(true, false, false), (true, false, false));
        assert_eq!(resolve_default_servers(false, true, false), (false, true, false));
        assert_eq!(resolve_default_servers(false, false, true), (false, false, true));
        assert_eq!(resolve_default_servers(true, true, false), (true, true, false));
    }

    #[test]
    fn pgwire_is_additive_and_never_suppresses_defaults() {
        // Regression: `--pg-server` must NOT disable Flight/JSONL/Admin. resolve_default_servers
        // is independent of pgwire, so enabling pgwire alongside a bare command still yields the
        // full default trio (pgwire is then layered on by the caller via `pg_at`).
        let pg_at = parse_pg_addr(Some("127.0.0.1:1705".to_string())).expect("valid addr");
        assert!(pg_at.is_some());
        // No flight/jsonl/admin flags set → all three still enabled regardless of pgwire.
        assert_eq!(resolve_default_servers(false, false, false), (true, true, true));
    }

    #[test]
    fn parse_pg_addr_handles_absent_and_invalid() {
        assert!(parse_pg_addr(None).expect("none ok").is_none());
        assert!(parse_pg_addr(Some("127.0.0.1:5432".into())).expect("valid").is_some());
        assert!(parse_pg_addr(Some("not-an-addr".into())).is_err());
    }
}
