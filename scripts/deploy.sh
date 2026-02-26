#!/usr/bin/env bash
set -euo pipefail

SSH_KEY="/home/jbeck/git/claude/ssh/linode"
HOST="root@192.46.223.21"
DEST="/opt/labelwatch/"

echo "==> Syncing code to ${HOST}:${DEST}"
rsync -az --delete \
  --filter='P config.toml' \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude '*.db' \
  --exclude '*.db-journal' \
  --exclude '*.db-shm' \
  --exclude '*.db-wal' \
  --exclude '.git' \
  --exclude '.pytest_cache' \
  -e "ssh -i ${SSH_KEY}" \
  ./ "${HOST}:${DEST}"

echo "==> Restarting labelwatch service"
ssh -i "${SSH_KEY}" "${HOST}" 'systemctl restart labelwatch.service'

echo "==> Waiting for startup..."
sleep 8

STATUS=$(ssh -i "${SSH_KEY}" "${HOST}" 'systemctl is-active labelwatch.service')
if [ "${STATUS}" = "active" ]; then
    echo "==> Service is active"
    ssh -i "${SSH_KEY}" "${HOST}" "sqlite3 /var/lib/labelwatch/labelwatch.db \"SELECT 'schema_version=' || value FROM meta WHERE key='schema_version';\""
else
    echo "!!! Service status: ${STATUS}"
    echo "==> Recent logs:"
    ssh -i "${SSH_KEY}" "${HOST}" 'journalctl -u labelwatch.service -n 20 --no-pager'
    exit 1
fi
