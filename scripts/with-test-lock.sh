#!/usr/bin/env bash
# Fuehrt einen Befehl unter der gemeinsamen Testsperre aus (Plan
# 2026-10-13-0902, KTD10): Integration, e2e und opt-in-Suiten teilen
# RabbitMQ und Valkey; parallele Sitzungen stoerten sich gegenseitig.
#
# -o: Kinder erben die Sperre nicht, ein verwaister Worker haelt sie nicht.
# -w: nach SWARMQ_TEST_LOCK_WAIT Sekunden Abbruch mit Hinweis.
# Ein Sammelrezept sperrt einmal und setzt SWARMQ_TEST_LOCK_HELD; aufgerufene
# Rezepte sperren dann nicht erneut. In der CI (CI gesetzt) entfaellt die Sperre.
set -euo pipefail

if [[ -n "${CI:-}" || -n "${SWARMQ_TEST_LOCK_HELD:-}" ]]; then
    exec "$@"
fi

lock="${SWARMQ_TEST_LOCK:-${XDG_RUNTIME_DIR:-/tmp}/swarmq-itest.lock}"
wait_s="${SWARMQ_TEST_LOCK_WAIT:-3600}"
export SWARMQ_TEST_LOCK_HELD=1
rc=0
flock -o -E 75 -w "$wait_s" "$lock" "$@" || rc=$?
if [[ $rc -eq 75 ]]; then
    echo "Testsperre ${lock} nach ${wait_s} s nicht frei: eine andere Sitzung faehrt" \
        "Tests gegen RabbitMQ/Valkey. Warten oder SWARMQ_TEST_LOCK_WAIT erhoehen." >&2
fi
exit "$rc"
