#!/usr/bin/env bash
# probe-labeler.sh — Check if a labeler is alive: endpoint, DID doc, profile.
# Usage: ./scripts/probe-labeler.sh <did>
set -euo pipefail

SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/git/claude/ssh/linode}"
HOST="jbeck@192.46.223.21"
DID="${1:?Usage: probe-labeler.sh <did>}"

echo "=== PLC Directory ==="
ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
    "curl -sf --max-time 10 'https://plc.directory/$DID' | python3 -c '
import json, sys
d = json.load(sys.stdin)
handles = [a.replace(\"at://\",\"\") for a in d.get(\"alsoKnownAs\",[])]
print(f\"  handle:   {handles[0] if handles else \"(none)\"}\" )
for svc in d.get(\"service\",[]):
    print(f\"  service:  {svc[\"id\"]} → {svc[\"serviceEndpoint\"]}\")
for vm in d.get(\"verificationMethod\",[]):
    print(f\"  key:      {vm[\"id\"]}\")
'" 2>/dev/null || echo "  (failed to reach PLC directory)"

# Extract labeler endpoint from DID doc
LABELER_EP=$(ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
    "curl -sf --max-time 10 'https://plc.directory/$DID' | python3 -c '
import json, sys
d = json.load(sys.stdin)
for svc in d.get(\"service\",[]):
    if svc[\"id\"] == \"#atproto_labeler\":
        print(svc[\"serviceEndpoint\"])
        break
'" 2>/dev/null)

echo ""
echo "=== Labeler Endpoint ==="
if [ -n "$LABELER_EP" ]; then
    echo "  endpoint: $LABELER_EP"
    RESP=$(ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
        "curl -sf --max-time 10 '${LABELER_EP}/xrpc/com.atproto.label.queryLabels?uriPatterns=*&limit=1' 2>&1" || true)
    if [ -n "$RESP" ]; then
        echo "  status:   ALIVE"
        echo "  sample:   $(echo "$RESP" | head -c 200)"
    else
        echo "  status:   UNREACHABLE"
    fi
else
    echo "  (no #atproto_labeler service in DID doc)"
fi

echo ""
echo "=== Bluesky Profile ==="
ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$HOST" \
    "curl -sf --max-time 10 'https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor=$DID' | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(f\"  handle:      {d.get(\"handle\",\"?\")}\")
print(f\"  display:     {d.get(\"displayName\",\"?\")}\")
assoc = d.get(\"associated\",{})
print(f\"  labeler:     {assoc.get(\"labeler\", False)}\")
print(f\"  followers:   {d.get(\"followersCount\",\"?\")}\")
print(f\"  description: {(d.get(\"description\",\"\") or \"\")[:120]}\")
'" 2>/dev/null || echo "  (failed to fetch profile)"
