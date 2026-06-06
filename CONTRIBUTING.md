# Contributing to camp-node

camp-node is a source-built distribution of [Amp](https://ampup.sh/docs),
Edge & Node Ventures' blockchain-native database, packaged so that
[camp](https://github.com/lodestar-team/camp) / [engine.camp](https://engine.camp) can run
an indexing engine it controls. It tracks upstream Amp under BUSL-1.1 (see [LICENSE](LICENSE)).

## Where to send what

- **Engine internals** — extraction, DataFusion UDFs, Parquet, the Flight / JSON Lines
  servers, the `ampd` / `ampctl` / `ampsync` binaries — originate in Edge & Node's upstream
  Amp. Their public source isn't currently published, so engine fixes here are carried as
  local patches on the BUSL baseline; report engine issues through the official Amp channels
  ([ampup.sh](https://ampup.sh/docs)) where possible.
- **camp-node packaging** — this repo's README, build prerequisites, versioning, and
  distribution — open an issue or PR here.

## Build & checks

See the [README](README.md#build-from-source) for prerequisites. Before opening a PR:

```sh
cargo build --release -p ampd -p ampctl
cargo fmt --all
cargo clippy --workspace
```

## License of contributions

There is **no CLA**. Contributions are accepted under this repository's license
([BUSL-1.1](LICENSE)); the underlying Licensed Work remains © Edge & Node Ventures, Inc.
By contributing, you confirm you have the right to submit the change under that license.
