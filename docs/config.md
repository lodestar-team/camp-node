# Configuration

The main configuration, used for both writing and serving datasets, is specified in a toml file which
is exemplified and documented in the [sample config](config.sample.toml). Its path should be passed
in as the `AMP_CONFIG` environment variable.

Configuring datasets to be extracted and served requires three different object storage directories:

- `manifests_dir`: Contains the dataset definitions. This is the input to the extraction process.
- `providers_dir`: Auxiliary to the dataset definitions, configures providers for external services
  like Firehose.
- `data_dir`: Where the actual dataset parquet tables are stored once extracted. Can be initially empty.

Although the initial setup with three directories may seem cumbersome, it allows for a highly
flexible configuration.

# Using env vars

Note that the values in the `AMP_CONFIG` file can be overridden from the environment, by prefixing
the env var name with `AMP_CONFIG_`. For example, to override the `data_dir` value, you can set a
`AMP_CONFIG_DATA_DIR` env var to the desired path.

For nested configuration values, use double underscores (`__`) to represent the nesting hierarchy. For example:

- To override `metadata_db.url`, set `AMP_CONFIG_METADATA_DB__URL`
- To override `metadata_db.pool_size`, set `AMP_CONFIG_METADATA_DB__POOL_SIZE`
- To override `writer.compression`, set `AMP_CONFIG_WRITER__COMPRESSION`

# Service addresses

The following optional configuration keys allow you to control the hostname and port that each service binds to:

- `flight_addr`: Arrow Flight RPC server address (default: `0.0.0.0:1602`)
- `jsonl_addr`: JSON Lines server address (default: `0.0.0.0:1603`)
- `admin_api_addr`: Admin API server address (default: `0.0.0.0:1610`)

## Logging

Simplified control of the logging verbosity level is offered by the `AMP_LOG` env var. It accepts
the values `error`, `warn`, `info`, `debug` or `trace`. The default value is `debug`. The standard
`RUST_LOG` env var can be used for finer-grained log filtering.

# Configuring object stores

All directory configurations (the `*_dir` keys) support both filesystem and object store locations.
So they can either be a filesystem path, for local storage, or a URL for object an store. For
production usage, an object store is recommended. Object store URLs can be in one of the following
formats:

### S3-compatible stores

URL format: `s3://<bucket>`

Sessions can be configured through the following environment variables:

- `AWS_ACCESS_KEY_ID`: access key ID
- `AWS_SECRET_ACCESS_KEY`: secret access key
- `AWS_DEFAULT_REGION` / `AWS_REGION`: region (use `auto` for non-AWS providers)
- `AWS_ENDPOINT`: endpoint (set this for any non-AWS S3-compatible provider)
- `AWS_SESSION_TOKEN`: session token
- `AWS_ALLOW_HTTP`: allow non-TLS connections (e.g. a local MinIO over `http://`)
- `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`: `false` to force path-style addressing (needed by MinIO; fine for R2)

Any S3-compatible store works via `AWS_ENDPOINT` — verified end-to-end against MinIO (write + read +
list through the same store layer the indexer uses).

#### Cloudflare R2

R2 speaks the S3 API, so it's just an endpoint + credentials. Put the parquet store on R2 instead of
local disk — useful when you don't want to manage local storage, want to share the data, or want to
front it with Cloudflare's CDN for fast public reads:

```toml
# camp.toml
data_dir      = "s3://my-bucket/amp/"
providers_dir = "s3://my-bucket/providers/"
manifests_dir = "s3://my-bucket/manifests/"
```

```sh
# R2 credentials are an "S3 API token" from the Cloudflare dashboard
export AWS_ENDPOINT="https://<ACCOUNT_ID>.r2.cloudflarestorage.com"
export AWS_ACCESS_KEY_ID="<R2_ACCESS_KEY_ID>"
export AWS_SECRET_ACCESS_KEY="<R2_SECRET_ACCESS_KEY>"
export AWS_REGION="auto"
```

The indexer writes parquet straight to R2 and the query engine reads it back. Reads stay fast because
parquet footers are cached (in Postgres + an in-process `foyer` cache) and per-column **Bloom filters**
let the engine fetch only the row groups a query actually touches — so a selective query pulls a few
ranges from R2, not whole files. For local testing, MinIO works with the same config plus
`AWS_ENDPOINT=http://localhost:9000`, `AWS_ALLOW_HTTP=true`, and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`.

### Keeping the parquet file count down (compaction)

The RPC extractor writes small parquet files as it follows the chain, which would otherwise pile up.
camp-node compacts them for you — **the compactor and collector are on by default** (since v0.5.0): the
compactor merges small files into larger ones and the collector garbage-collects the superseded
originals. You don't need to compact or reindex by hand. The defaults are equivalent to:

```toml
[writer.compactor]
active = true
[writer.collector]
active = true
```

On a pre-v0.5.0 build, set those explicitly. Tuning knobs (`target_size`, compaction interval,
`eager_compaction_limit`, concurrency) are in `docs/config.sample.toml`.

### Google Cloud Storage (GCS)

URL format: `gs://<bucket>`

GCS Authorization can be configured through one of the following environment variables:

- `GOOGLE_SERVICE_ACCOUNT_PATH`: location of service account file, or
- `GOOGLE_SERVICE_ACCOUNT_KEY`: JSON serialized service account key, or
- Application Default Credentials.

## Datasets

### Dataset Identity and Versioning

Datasets in Amp are identified by three components:

- **Namespace**: An organizational grouping (e.g., `my_org`, `edgeandnode`, or `_` for the default namespace)
- **Name**: The dataset name (e.g., `eth_mainnet`, `uniswap_v3`)
- **Version/Revision**: A version tag, special tag, or manifest hash (e.g., `1.0.0`, `latest`, `dev`, or a hash)

The complete dataset reference follows the format: `namespace/name@revision`

Examples:

- `my_org/eth_mainnet@1.0.0` - Production release 1.0.0
- `my_org/eth_mainnet@latest` - Latest semantic version
- `my_org/eth_mainnet@dev` - Development version
- `_/eth_mainnet@latest` - Using default namespace

### SQL Schema Names

In SQL queries, datasets are referenced using their fully qualified name (namespace/name) with quotes:

```sql
SELECT * FROM "namespace/name".table_name
```

Example: A dataset with namespace `my_org` and name `eth_mainnet` will have tables referenced in SQL as:

- `"my_org/eth_mainnet".blocks`
- `"my_org/eth_mainnet".logs`

**Note**: SQL references must be quoted because the schema name contains a forward slash (`/`).

### Dataset Categories

Conceptually there are two categories of datasets:

- **Raw datasets**: Extracted from external systems such as Firehose or EVM RPC endpoints
- **Derived datasets**: Defined as SQL transformations over other datasets

## Raw datasets

Details for the raw datasets currently implemented:

- EVM RPC [dataset docs](../crates/extractors/evm-rpc/README.md)
- Firehose [dataset docs](../crates/extractors/firehose/README.md)

### Generating Raw Dataset Manifests

The `ampctl gen-manifest` command provides a convenient way to generate manifest JSON files for raw datasets. These manifests define the schema and configuration that will be used during extraction.

#### Usage

```bash
# Generate manifest for EVM RPC dataset
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet

# Generate manifest for EVM RPC dataset with custom start block
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet --start-block 1000000

# Generate manifest for Firehose dataset
ampctl gen-manifest --network mainnet --kind firehose --name eth_firehose

# Output to file
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet -o ./manifests_dir/eth_mainnet.json

# Output to directory (will create ./manifests/evm-rpc.json)
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet -o ./manifests/

# Only include finalized blocks
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet --finalized-blocks-only
```

#### Parameters

- `--network`: Network name (e.g., mainnet, goerli, polygon, anvil)
- `--kind`: Dataset type (evm-rpc, firehose, eth-beacon)
- `--name`: Dataset name (must be a valid dataset identifier, without namespace)
- `--out` (or `-o`): Optional output file or directory path. If a directory is specified, the file will be named `{kind}.json`. If not specified, the manifest is printed to stdout.
- `--start-block`: Starting block number for extraction (defaults to 0). Applies to evm-rpc, firehose, and eth-beacon datasets.
- `--finalized-blocks-only`: Only include finalized block data (flag, defaults to false)

The generated manifest includes the complete schema definition with all tables and columns for the specified dataset type and network.

#### Registration and Deployment

After generating a manifest, you need to register it with a namespace and version:

```bash
# Register the dataset (creates/updates dev tag)
ampctl dataset register my_namespace/eth_mainnet ./manifest.json

# Register with a specific version (creates version tag and updates latest if higher)
ampctl dataset register my_namespace/eth_mainnet ./manifest.json --tag 1.0.0

# Deploy the dataset for extraction
ampctl dataset deploy my_namespace/eth_mainnet@1.0.0
```

# Providers

Providers are external data source configurations that raw datasets connect to for extracting blockchain data. Each provider is configured as a separate TOML file in the `providers_dir` directory.

Environment variable substitution is supported using `${VAR_NAME}` syntax for any field value.

## Provider Kinds

The `kind` field in a provider configuration must be one of the following:

- **`evm-rpc`**: Ethereum-compatible JSON-RPC endpoints (supports HTTP, WebSocket, and IPC connections)
- **`firehose`**: Firehose gRPC endpoints
- **`eth-beacon`**: Ethereum Beacon Chain (consensus layer) REST API endpoints

Each kind has its own set of required and optional configuration fields.

## Provider Configuration Structure

All provider configurations must define:

- `kind`: The provider type (must be one of the four kinds listed above)
- `network`: The blockchain network identifier (e.g., "mainnet", "goerli", "polygon")

Additional fields vary by provider kind. The provider name is derived from the filename (without the `.toml` extension), though it can be explicitly set with a `name` field.

## Sample Provider Configurations

Complete sample configuration files for each provider kind are available in the [docs/providers/](providers/) directory:

- **[evm-rpc.sample.toml](providers/evm-rpc.sample.toml)** - Configuration for Ethereum-compatible JSON-RPC endpoints. Includes fields for URL (HTTP/WebSocket/IPC), concurrent request limits, RPC batching, rate limiting, and receipt fetching options.

- **[firehose.sample.toml](providers/firehose.sample.toml)** - Configuration for StreamingFast Firehose gRPC endpoints. Includes fields for gRPC URL and authentication token.

- **[eth-beacon.sample.toml](providers/eth-beacon.sample.toml)** - Configuration for Ethereum Beacon Chain REST API endpoints. Includes fields for API URL, concurrent request limits, and rate limiting.

These sample files document all available configuration fields for each provider kind, including both required and optional parameters with their default values.
