#!/usr/bin/env bash
#
# Off-box backup for camp-node — the Parquet data dir (restic, dedup+incremental)
# plus the Postgres metadata catalog (pg_dump). Parquet files are immutable once
# written, so snapshotting a live data dir is safe; the catalog dump captures the
# manifest/file/job state needed to make the restored files queryable.
#
# Enable real off-box protection by pointing RESTIC_REPOSITORY at a remote
# (e.g. an rclone backend or S3):
#   rclone config                       # add a 'tigris'/'b2'/'s3' remote, once
#   export RESTIC_REPOSITORY=rclone:tigris:camp-backups
#   export RESTIC_PASSWORD_FILE=~/.config/camp-node/restic.pass
#   export PG_DUMP_URL=postgres://user:pass@localhost/ampd_fork   # optional
#   restic init                         # first time only
#   ops/backup.sh
#
set -euo pipefail

: "${DATA_DIR:=/home/pepe/amp-fork/data}"
: "${STAGING:=/home/pepe/amp-fork/backup-staging}"
: "${RESTIC_REPOSITORY:?set RESTIC_REPOSITORY (e.g. rclone:tigris:camp-backups or s3:https://...)}"
: "${RESTIC_PASSWORD_FILE:?set RESTIC_PASSWORD_FILE (a file holding the repo password)}"
: "${PG_DUMP_URL:=}"
: "${KEEP_DAILY:=7}"
: "${KEEP_WEEKLY:=4}"

export RESTIC_REPOSITORY RESTIC_PASSWORD_FILE

mkdir -p "$STAGING"
if [ -n "$PG_DUMP_URL" ]; then
  echo "[backup] dumping metadata catalog…"
  pg_dump --no-owner "$PG_DUMP_URL" | zstd -q -19 -o "$STAGING/metadata.sql.zst" -f \
    || echo "[backup] WARN: pg_dump failed (continuing with data-dir only)"
fi

echo "[backup] $DATA_DIR + $STAGING -> $RESTIC_REPOSITORY"
restic backup --tag camp-node "$DATA_DIR" "$STAGING"
restic forget --tag camp-node --keep-daily "$KEEP_DAILY" --keep-weekly "$KEEP_WEEKLY" --prune
restic snapshots --tag camp-node --latest 3
echo "[backup] done"
