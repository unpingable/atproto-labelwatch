#!/usr/bin/env bash
# checkin.sh — Quick observatory health check-in.
# Usage: ./scripts/checkin.sh [ssh_key] [host]
set -euo pipefail

SSH_KEY="${1:-${DEPLOY_SSH_KEY:-$HOME/git/claude/ssh/linode}}"
HOST="${2:-jbeck@192.46.223.21}"
DB="/var/lib/labelwatch/labelwatch.db"
DW_DB="/opt/driftwatch/deploy/data/labeler.sqlite"
FACTS_DB="/opt/driftwatch/deploy/data/facts.sqlite"

ssh_cmd() { ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" "$@"; }
sql()     { ssh_cmd "sqlite3 '$1' \"$2\"" 2>/dev/null; }

echo "=== DISK ==="
ssh_cmd "df -h /"

echo ""
echo "=== SERVICES ==="
ssh_cmd "docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null; echo '---'; systemctl is-active labelwatch labelwatch-discovery labelwatch-api 2>/dev/null"

echo ""
echo "=== DRIFTWATCH HEALTH ==="
ssh_cmd "curl -s http://localhost:8422/health/extended 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(f\"  status:     {d[\"status\"]}\")
print(f\"  build:      {d[\"build_sha\"]}\")
print(f\"  eps:        {d[\"events_per_sec\"]} (baseline {d[\"baseline_eps\"]})\")
print(f\"  coverage:   {d[\"coverage_pct\"]*100:.0f}%\")
print(f\"  drops:      {d[\"drop_frac\"]}\")
print(f\"  queue:      {d[\"queue_depth\"]}\")
print(f\"  lag:        {d[\"stream_lag_s\"]}s\")
print(f\"  wal:        {d[\"wal\"][\"wal_size_mb\"]}MB (busy: {d[\"wal\"][\"checkpoint_busy\"]})\")
r = d.get(\"resolver\", {})
if r:
    print(f\"  resolver:   {r[\"ok\"]}/{r[\"total\"]} ({r[\"ok\"]/max(r[\"total\"],1)*100:.1f}%) pending={r[\"pending\"]}\")
'"

echo ""
echo "=== LABELWATCH HEALTH ==="
ssh_cmd "curl -s http://localhost:8423/health 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(f\"  ok:         {d[\"ok\"]}\")
sig = d.get(\"signals\", {})
if sig:
    print(f\"  signals:    {sig[\"verdict\"]}\")
    c = sig.get(\"classifications\", {})
    print(f\"  active={c.get(\"active\",0)} degrading={c.get(\"degrading\",0)} gone_dark={c.get(\"gone_dark\",0)} surging={c.get(\"surging\",0)}\")
    for ref in sig.get(\"reference_issues\", []):
        print(f\"  !! {ref[\"handle\"]}: {ref[\"signal\"]} ({ref[\"events_7d\"]} 7d / {ref[\"events_30d\"]} 30d)\")
'"

echo ""
echo "=== DB SIZES ==="
ssh_cmd "ls -lh $DB $DW_DB ${FACTS_DB} 2>/dev/null | awk '{print \"  \" \$NF \": \" \$5}'"

echo ""
echo "=== COUNTS ==="
LW_EVENTS=$(sql "$DB" "SELECT COUNT(*) FROM label_events")
LW_LABELERS=$(sql "$DB" "SELECT COUNT(*) FROM labelers")
LW_BOUNDARY=$(sql "$DB" "SELECT COUNT(*) FROM boundary_targets")
FACTS_ACTORS=$(sql "$FACTS_DB" "SELECT COUNT(*) FROM actor_identity_facts")
FACTS_WITH_PDS=$(sql "$FACTS_DB" "SELECT COUNT(*) FROM actor_identity_facts WHERE pds_host IS NOT NULL AND pds_host != ''")
echo "  label_events:         $LW_EVENTS"
echo "  labelers:             $LW_LABELERS"
echo "  boundary_targets:     $LW_BOUNDARY"
echo "  actor_identities:     $FACTS_ACTORS (${FACTS_WITH_PDS} with PDS)"
