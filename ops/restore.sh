#!/usr/bin/env bash
#
# Restore a camp-node backup made by ops/backup.sh.
#   export RESTIC_REPOSITORY=rclone:tigris:camp-backups
#   export RESTIC_PASSWORD_FILE=~/.config/camp-node/restic.pass
#   ops/restore.sh /home/pepe/amp-fork-restored [snapshot-id]
#
# Then: point the engine's data_dir at the restored path (or rsync it back),
# restore metadata.sql.zst into Postgres if present, and run
# `ampctl dataset restore` to re-register files in the catalog.
#
set -euo pipefail

: "${RESTIC_REPOSITORY:?set RESTIC_REPOSITORY}"
: "${RESTIC_PASSWORD_FILE:?set RESTIC_PASSWORD_FILE}"
export RESTIC_REPOSITORY RESTIC_PASSWORD_FILE

TARGET="${1:?usage: restore.sh <target-dir> [snapshot-id]}"
SNAP="${2:-latest}"

echo "[restore] $RESTIC_REPOSITORY @ $SNAP -> $TARGET"
mkdir -p "$TARGET"
restic restore "$SNAP" --target "$TARGET"
echo "[restore] done. Files under $TARGET. Next:"
echo "  - point data_dir at the restored data/ (or rsync back)"
echo "  - if metadata.sql.zst is present: zstd -d < .../metadata.sql.zst | psql <db>"
echo "  - ampctl dataset restore   # re-index restored files into the catalog"
