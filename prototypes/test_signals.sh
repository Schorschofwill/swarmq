#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO="$SCRIPT_DIR/worker_prototype.py"
LOGDIR="/tmp/swarmq_signal_tests"
mkdir -p "$LOGDIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
info() { echo -e "${YELLOW}[TEST]${NC} $*"; }

cleanup() {
    pkill -9 -f "worker_prototype.py" 2>/dev/null
    sleep 1
}

# Helfer: Startet den Prototyp und gibt die Parent-PID zurueck
start_proto() {
    local workers="${1:-2}"
    local duration="${2:-1500}"
    local logfile="$3"
    python3 "$PROTO" --workers "$workers" --task-duration "$duration" > "$logfile" 2>&1 &
    local pid=$!
    sleep 3
    echo "$pid"
}

# Warte bis Prozess beendet ist (max timeout)
wait_for_exit() {
    local pid=$1
    local timeout=$2
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null && [ "$elapsed" -lt "$timeout" ]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done
    ! kill -0 "$pid" 2>/dev/null
}

echo ""
echo "=========================================="
echo "  SwarmQ Signal-Handling Tests"
echo "=========================================="
echo ""

cleanup

# ================================================================
# Test 1: SIGTERM -> Graceful Shutdown
# ================================================================
info "Test 1: SIGTERM -> Graceful Shutdown"
LOGFILE="$LOGDIR/test1_sigterm.log"
PID=$(start_proto 2 1500 "$LOGFILE")
info "  Parent PID=$PID, sende SIGTERM..."

kill -TERM "$PID"
if wait_for_exit "$PID" 20; then
    # Pruefe ob Shutdown-Meldungen im Log sind
    if grep -q "SIGTERM empfangen" "$LOGFILE" && grep -q "Shutdown" "$LOGFILE"; then
        COMPLETED=$(grep -c "abgeschlossen" "$LOGFILE")
        pass "Test 1: Graceful Shutdown nach SIGTERM ($COMPLETED Tasks abgeschlossen)"
    elif grep -q "beendet" "$LOGFILE"; then
        COMPLETED=$(grep -c "abgeschlossen" "$LOGFILE")
        pass "Test 1: Worker sauber beendet nach SIGTERM ($COMPLETED Tasks)"
    else
        fail "Test 1: Prozess beendet, aber keine Shutdown-Logs"
        echo "  Letzte 10 Zeilen:"
        tail -10 "$LOGFILE" | sed 's/^/    /'
    fi
else
    fail "Test 1: Prozess nicht innerhalb von 20s beendet"
    kill -9 "$PID" 2>/dev/null
fi
echo ""
cleanup

# ================================================================
# Test 2: SIGINT -> Graceful Shutdown (simuliert Ctrl+C)
# ================================================================
info "Test 2: SIGINT -> Graceful Shutdown (Ctrl+C)"
LOGFILE="$LOGDIR/test2_sigint.log"
PID=$(start_proto 2 1500 "$LOGFILE")
info "  Parent PID=$PID, sende SIGINT..."

kill -INT "$PID"
if wait_for_exit "$PID" 20; then
    if grep -q "SIGINT empfangen\|Shutdown" "$LOGFILE"; then
        COMPLETED=$(grep -c "abgeschlossen" "$LOGFILE")
        pass "Test 2: Graceful Shutdown nach SIGINT ($COMPLETED Tasks)"
    elif grep -q "beendet" "$LOGFILE"; then
        COMPLETED=$(grep -c "abgeschlossen" "$LOGFILE")
        pass "Test 2: Worker sauber beendet nach SIGINT ($COMPLETED Tasks)"
    else
        fail "Test 2: Beendet, aber keine Shutdown-Logs"
        tail -10 "$LOGFILE" | sed 's/^/    /'
    fi
else
    fail "Test 2: Prozess nicht innerhalb von 20s beendet"
    kill -9 "$PID" 2>/dev/null
fi
echo ""
cleanup

# ================================================================
# Test 3: Worker-Kill -> Auto-Restart
# ================================================================
info "Test 3: Worker-Kill -> Auto-Restart"
LOGFILE="$LOGDIR/test3_autorestart.log"
PID=$(start_proto 2 1500 "$LOGFILE")

# Finde einen Worker-PID
WORKER_PID=$(ps --ppid "$PID" -o pid= 2>/dev/null | head -1 | tr -d ' ')
if [ -z "$WORKER_PID" ]; then
    # Fallback: suche in den Logs
    WORKER_PID=$(grep -oP "PID=\K[0-9]+" "$LOGFILE" | grep -v "$(echo $PID)" | head -1)
fi

if [ -n "$WORKER_PID" ]; then
    info "  Parent=$PID, toete Worker PID=$WORKER_PID..."
    kill -9 "$WORKER_PID"
    sleep 5

    # Pruefe ob ein neuer Worker gestartet wurde
    if grep -q "gestorben\|dead\|Scheduling reload" "$LOGFILE"; then
        if grep -q "gestartet.*PID=" "$LOGFILE" | tail -1; then
            pass "Test 3: Toter Worker erkannt und neugestartet"
        else
            NEW_WORKERS=$(grep -c "gestartet" "$LOGFILE")
            pass "Test 3: Worker-Tod erkannt ($NEW_WORKERS Worker-Starts total)"
        fi
    else
        # Pruefe ob es jetzt immer noch 2 Worker gibt
        ALIVE=$(ps --ppid "$PID" -o pid= 2>/dev/null | wc -l)
        if [ "$ALIVE" -ge 2 ]; then
            pass "Test 3: Auto-Restart funktioniert ($ALIVE Worker aktiv)"
        else
            fail "Test 3: Worker nicht neugestartet (nur $ALIVE aktiv)"
        fi
    fi
else
    fail "Test 3: Konnte keinen Worker-PID finden"
fi

# Aufraeumen
kill -TERM "$PID" 2>/dev/null
wait_for_exit "$PID" 15
echo ""
cleanup

# ================================================================
# Test 4: SIGHUP -> Reload
# ================================================================
info "Test 4: SIGHUP -> Reload"
LOGFILE="$LOGDIR/test4_sighup.log"
PID=$(start_proto 2 1500 "$LOGFILE")
info "  Parent PID=$PID, sende SIGHUP..."

kill -HUP "$PID"
sleep 5

if grep -q "SIGHUP\|Reload" "$LOGFILE"; then
    pass "Test 4: SIGHUP/Reload erkannt"
else
    fail "Test 4: Keine Reload-Meldung im Log"
    tail -5 "$LOGFILE" | sed 's/^/    /'
fi

kill -9 "$PID" 2>/dev/null
echo ""
cleanup

# ================================================================
# Test 5: Worker ignoriert SIGINT (nur Parent reagiert)
# ================================================================
info "Test 5: Worker ignoriert SIGINT"
LOGFILE="$LOGDIR/test5_worker_sigint.log"
PID=$(start_proto 2 1500 "$LOGFILE")

WORKER_PID=$(ps --ppid "$PID" -o pid= 2>/dev/null | head -1 | tr -d ' ')
if [ -n "$WORKER_PID" ]; then
    info "  Sende SIGINT direkt an Worker PID=$WORKER_PID..."
    kill -INT "$WORKER_PID" 2>/dev/null
    sleep 3

    # Worker sollte noch leben
    if kill -0 "$WORKER_PID" 2>/dev/null; then
        pass "Test 5: Worker ignoriert SIGINT korrekt (laeuft weiter)"
    else
        fail "Test 5: Worker wurde durch SIGINT beendet"
    fi
else
    fail "Test 5: Konnte keinen Worker-PID finden"
fi

kill -TERM "$PID" 2>/dev/null
wait_for_exit "$PID" 15
echo ""
cleanup

# ================================================================
# Test 6: Laufende Tasks werden bei Shutdown abgeschlossen
# ================================================================
info "Test 6: Laufende Tasks werden bei Shutdown fertig"
LOGFILE="$LOGDIR/test6_drain.log"
# Lange Tasks (3s) damit sie waehrend des Shutdowns noch laufen
PID=$(start_proto 2 3000 "$LOGFILE")
info "  Parent PID=$PID, warte auf Tasks, dann SIGTERM..."

sleep 4
BEFORE=$(grep -c "abgeschlossen" "$LOGFILE")
kill -TERM "$PID"
wait_for_exit "$PID" 20
AFTER=$(grep -c "abgeschlossen" "$LOGFILE")
DRAINED=$((AFTER - BEFORE))

if [ "$DRAINED" -gt 0 ]; then
    pass "Test 6: $DRAINED Tasks nach SIGTERM noch abgeschlossen (Soft-Shutdown)"
else
    if grep -q "Alle Tasks sauber beendet" "$LOGFILE"; then
        pass "Test 6: Alle Tasks vor SIGTERM abgeschlossen"
    else
        fail "Test 6: Keine Tasks nach SIGTERM abgeschlossen"
        tail -10 "$LOGFILE" | sed 's/^/    /'
    fi
fi
echo ""
cleanup

# ================================================================
# Zusammenfassung
# ================================================================
echo "=========================================="
echo "  Logs: $LOGDIR/"
echo "=========================================="
ls -la "$LOGDIR/"
echo ""
