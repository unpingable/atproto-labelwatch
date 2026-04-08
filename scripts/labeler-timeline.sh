#!/usr/bin/env bash
# labeler-timeline.sh — Daily event counts for a labeler over the last N days.
# Usage: ./scripts/labeler-timeline.sh <labeler_did_or_handle> [days=45]
set -euo pipefail

SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/git/claude/ssh/linode}"
HOST="jbeck@192.46.223.21"
DB="/var/lib/labelwatch/labelwatch.db"
IDENTIFIER="${1:?Usage: labeler-timeline.sh <did_or_handle> [days]}"
DAYS="${2:-45}"

# If it looks like a handle, resolve to DID
if [[ "$IDENTIFIER" != did:* ]]; then
    DID=$(ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
        "sqlite3 '$DB' \"SELECT did FROM labelers WHERE handle='$IDENTIFIER' LIMIT 1\"" 2>/dev/null)
    if [ -z "$DID" ]; then
        echo "Could not resolve handle '$IDENTIFIER' to DID" >&2
        exit 1
    fi
    echo "Resolved $IDENTIFIER → $DID"
else
    DID="$IDENTIFIER"
fi

CUTOFF=$(date -d "-${DAYS} days" +%Y-%m-%d 2>/dev/null || date -v-${DAYS}d +%Y-%m-%d)

echo ""
echo "Daily events for $DID (last ${DAYS}d):"
echo "---"
ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
    "sqlite3 '$DB' \"SELECT date(ts) as day, COUNT(*) as n FROM label_events WHERE labeler_did='$DID' AND ts > '$CUTOFF' GROUP BY day ORDER BY day\"" 2>/dev/null | \
    while IFS='|' read -r day count; do
        bar=$(printf '%*s' $((count / 1000)) '' | tr ' ' '▓')
        printf "  %s  %7d  %s\n" "$day" "$count" "$bar"
    done
