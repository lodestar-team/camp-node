# Phase 0 baseline — engine.camp Arbitrum One (2026-06-07)

Measured on the prod box (single ThinkPad) against `_/arbitrum_one@4.0.1`, to anchor
every "Nx" the roadmap chases. Re-measure at each phase boundary.

## Storage / file layout
| table | files | size |
|---|---|---|
| blocks | 44,727 | 2.4G |
| transactions | 45,560 | 7.2G |
| logs | 45,847 | 3.3G |
| **data dir total** | ~135k | **13G** |

**⚠️ Standout finding: ~45k tiny Parquet files per table.** The compactor is not merging
(known: it *plans* but doesn't merge — hourly reindex was the workaround). This is the
dominant query-latency drag (footer reads across 135k files) and the strongest argument to
prioritise **enable-compactor** + **footer cache** + (later) a real size-tiered compaction
fix. It also makes restic backup heavier (lots of small objects).

## Query latency (JSON Lines :1703, warm)
| query | latency |
|---|---|
| `COUNT(*) blocks` | ~300 ms |
| `MAX(block_num) logs` | ~206 ms |
| `block_num, gas_used ORDER BY block_num DESC LIMIT 5` | ~192 ms |

Targets after Tier-1 Parquet layout tuning + footer cache: point lookups
(`WHERE address=? AND topic0=?`) should drop sharply once `logs` is sorted by
`(address, block_num)` with Bloom filters; full-table aggregates are already fine.

## Tip / freshness
Arbitrum head tracked ~2–7 blocks behind chain tip (~real-time). History accumulating
~1 day/day since the 2026-06-06 cutover (deliberate no-backfill stance).

## Backup tooling
`restic` + `rclone` present. `ops/backup.sh` (restic data dir + pg_dump) and `ops/restore.sh`
**tested end-to-end** (backup→restore→checksum match). Off-box protection pending: configure
an `rclone` remote + `RESTIC_REPOSITORY` + `restic init`, then enable the systemd timer.
