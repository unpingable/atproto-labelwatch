#!/bin/bash
# labelwatch-lock-watcher.sh
#
# Incident-bundled capture for SQLite write-lock contention on labelwatch.
# Tails journald for trigger events and, on match, snapshots both processes'
# stacks + cheap context into a per-incident bundle directory.
#
# Per chatty's design:
#   - systemctl MainPID (not pgrep)
#   - cheap metadata first (status/ps/lsof)
#   - per-incident bundle dirs
#   - cross-unit journal ±30s
#   - event-driven (systemd service, not timer)
#   - snapshot-after-trigger is honest about sometimes missing the holder
#
# Incident bundle layout:
#   /var/log/labelwatch-lock-traces/<ISO8601>/
#     context.txt
#     systemctl-status.txt
#     ps.txt
#     lsof.txt
#     main.stack
#     discovery.stack
#     restart-state.txt
#     journal-snippet.txt

set -u

CAPTURE_DIR=/var/log/labelwatch-lock-traces
DB_PATH=/var/lib/labelwatch/labelwatch.db

# Trigger phrases — include related fatal/crash lines, not just the headline
TRIGGER_REGEX='DB locked during startup|database is locked|DB write failure|Exiting due to fatal error'

mkdir -p "$CAPTURE_DIR"

logger -t labelwatch-lock-watcher "watcher started, capture_dir=$CAPTURE_DIR"

# Track last bundle time to coalesce rapid-fire retries into one bundle
# (e.g. "retrying 1/6" + "retrying 2/6" within seconds — same incident)
LAST_BUNDLE_TS=0
COALESCE_WINDOW_SEC=10

capture_incident() {
    local trigger_line="$1"
    local now_epoch
    now_epoch=$(date +%s)

    if (( now_epoch - LAST_BUNDLE_TS < COALESCE_WINDOW_SEC )); then
        logger -t labelwatch-lock-watcher "coalescing trigger within ${COALESCE_WINDOW_SEC}s window"
        return
    fi
    LAST_BUNDLE_TS=$now_epoch

    local ts
    ts=$(date -u +%Y-%m-%dT%H-%M-%SZ)
    local bundle="$CAPTURE_DIR/$ts"
    mkdir -p "$bundle"

    local main_pid disc_pid
    main_pid=$(systemctl show -p MainPID --value labelwatch 2>/dev/null)
    disc_pid=$(systemctl show -p MainPID --value labelwatch-discovery 2>/dev/null)

    # 1. Context file — cheap, always succeeds
    {
        echo "=== trigger ==="
        echo "$trigger_line"
        echo ""
        echo "=== timestamp ==="
        date -u
        echo ""
        echo "=== MainPIDs (from systemctl show, not pgrep) ==="
        echo "labelwatch           = $main_pid"
        echo "labelwatch-discovery = $disc_pid"
    } > "$bundle/context.txt"

    # 2. systemctl status for both units
    systemctl status labelwatch labelwatch-discovery labelwatch-api --no-pager \
        > "$bundle/systemctl-status.txt" 2>&1

    # 3. ps for both pids
    if [[ -n "$main_pid" && "$main_pid" != "0" && -n "$disc_pid" && "$disc_pid" != "0" ]]; then
        ps -fp "$main_pid" "$disc_pid" > "$bundle/ps.txt" 2>&1
    else
        echo "skipped — missing pid(s): main=$main_pid disc=$disc_pid" > "$bundle/ps.txt"
    fi

    # 4. lsof on the DB — who has it open?
    lsof "$DB_PATH" > "$bundle/lsof.txt" 2>&1

    # 4b. /proc state — cheap, often gold if py-spy misses the holder.
    # wchan in particular can tell us whether the process is sleeping
    # in something rude (e.g. fsync, write, futex) even without a stack.
    {
        for pid in "$main_pid" "$disc_pid"; do
            [[ -z "$pid" || "$pid" == "0" ]] && continue
            label="pid=$pid"
            [[ "$pid" == "$main_pid" ]] && label="$label (labelwatch)"
            [[ "$pid" == "$disc_pid" ]] && label="$label (discovery)"
            echo "=== $label ==="
            echo "--- cmdline ---"
            tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null; echo
            echo "--- wchan ---"
            cat "/proc/$pid/wchan" 2>/dev/null; echo
            echo "--- status (head) ---"
            head -20 "/proc/$pid/status" 2>/dev/null
            echo ""
        done
    } > "$bundle/proc.txt"

    # 5. Restart cadence state
    {
        echo "=== labelwatch-discovery restart state ==="
        systemctl show labelwatch-discovery \
            -p NRestarts \
            -p ExecMainStartTimestamp \
            -p ExecMainExitTimestamp \
            -p ActiveEnterTimestamp \
            --no-pager
        echo ""
        echo "=== labelwatch main restart state ==="
        systemctl show labelwatch \
            -p NRestarts \
            -p ExecMainStartTimestamp \
            -p ActiveEnterTimestamp \
            --no-pager
    } > "$bundle/restart-state.txt"

    # 6. Stacks — launch both in parallel with timeout, the race is unavoidable
    if [[ -n "$main_pid" && "$main_pid" != "0" ]]; then
        timeout 15 py-spy dump --pid "$main_pid" > "$bundle/main.stack" 2>&1 &
        local main_bg=$!
    fi
    if [[ -n "$disc_pid" && "$disc_pid" != "0" ]]; then
        timeout 15 py-spy dump --pid "$disc_pid" > "$bundle/discovery.stack" 2>&1 &
        local disc_bg=$!
    fi
    wait

    # 7. Cross-unit journal snippet — ±60s around the event (1min each side)
    journalctl -u labelwatch -u labelwatch-discovery \
        --since "60 seconds ago" \
        --no-pager \
        > "$bundle/journal-snippet.txt" 2>&1

    logger -t labelwatch-lock-watcher "captured incident: $bundle"
}

# Tail journal starting from NOW (-n 0), not from any historical lines.
# Without -n 0, journalctl -f replays a few recent lines, which would
# produce fake archaeology from the 2276-restart history on boot.
journalctl -u labelwatch -u labelwatch-discovery -f -n 0 -o short-iso --no-pager \
    | grep --line-buffered -E "$TRIGGER_REGEX" \
    | while IFS= read -r line; do
        capture_incident "$line"
    done
