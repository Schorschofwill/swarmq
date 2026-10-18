#!/usr/bin/env bash
# Einstieg des Mutmut-Abbilds (Plan 2026-10-17-0941, U2/KTD4).
#
# MUTMUT_LAUF          a = nur tests/unit, b = zusätzlich tests/integration/backend
#                      gegen die Compose-Dienste rabbitmq und valkey
# MUTMUT_MUSTER        Mutanten-Muster für `mutmut run` (fnmatch über
#                      swarmq.<modulpfad>.<funktion>__mutmut_N); leer = alle.
#                      Standard für b: swarmq.backend.valkey.*
# MUTMUT_MAX_CHILDREN  Kindprozesse; Standard a: 3. b: immer 1, ein anderer Wert
#                      wird abgewiesen (ohne xdist teilen alle Kinder vhost und
#                      Valkey-DB, die Flush-Fixture leert sie)
#
# mutmut 3.8 liest die Testauswahl nur aus [tool.mutmut] der pyproject.toml;
# deshalb schreibt das Skript den Schlüssel in die Kopie im Container.
set -euo pipefail
cd /app

lauf="${MUTMUT_LAUF:-}"
case "$lauf" in
    a)
        auswahl='["tests/unit"]'
        muster="${MUTMUT_MUSTER:-}"
        kinder="${MUTMUT_MAX_CHILDREN:-3}"
        ;;
    b)
        auswahl='["tests/unit", "tests/integration/backend"]'
        muster="${MUTMUT_MUSTER:-swarmq.backend.valkey.*}"
        kinder="${MUTMUT_MAX_CHILDREN:-1}"
        # KTD4: genau ein Kind, weil alle Kinder vhost und Valkey-DB teilen.
        if [[ "$kinder" != 1 ]]; then
            echo "mutmut-entrypoint: Lauf b verlangt genau ein Kind, MUTMUT_MAX_CHILDREN ist '${kinder}'" >&2
            exit 2
        fi
        # Nur im Container: die Dienste sind Wegwerf-Container dieses
        # Compose-Projekts, kein fremder Stack auf 5672/6379.
        export SWARMQ_TEST_RABBITMQ_HOST=rabbitmq
        export SWARMQ_TEST_RABBITMQ_PORT=5672
        export SWARMQ_TEST_RABBITMQ_MGMT_PORT=15672
        export SWARMQ_TEST_VALKEY_HOST=valkey
        export SWARMQ_TEST_VALKEY_PORT=6379
        export SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1
        ;;
    *)
        echo "mutmut-entrypoint: MUTMUT_LAUF muss a oder b sein, ist '${lauf}'" >&2
        exit 2
        ;;
esac

# Ein Muster ohne * trifft in mutmut 3.8 keinen Mutanten (AssertionError
# bzw. KeyError statt einer Meldung); Schlüssel beginnen immer mit swarmq.
if [[ -n "$muster" && ( "$muster" != *"*"* || "$muster" != swarmq.* ) ]]; then
    echo "mutmut-entrypoint: Muster '${muster}' ungültig; Form: swarmq.<modulpfad>.* (mit *)" >&2
    exit 2
fi

python - "$auswahl" <<'PY'
import json
import pathlib
import re
import sys
import tomllib

path = pathlib.Path("pyproject.toml")
text = path.read_text("utf-8")
line = f"pytest_add_cli_args_test_selection = {json.dumps(json.loads(sys.argv[1]))}"
text, count = re.subn(r"^pytest_add_cli_args_test_selection = .*$", line, text, flags=re.MULTILINE)
if count != 1:
    sys.exit(f"mutmut-entrypoint: Schlüssel in pyproject.toml {count}-mal gefunden, erwartet 1")
path.write_text(text, "utf-8")
selection = tomllib.loads(text)["tool"]["mutmut"]["pytest_add_cli_args_test_selection"]
print(f"mutmut-entrypoint: Testauswahl {selection}")
PY

if [[ "$lauf" == b ]]; then
    python - <<'PY'
import socket
import sys
import time

ziele = [("rabbitmq", 5672), ("rabbitmq", 15672), ("valkey", 6379)]
frist = time.monotonic() + 180
for host, port in ziele:
    while True:
        try:
            socket.create_connection((host, port), timeout=2).close()
            break
        except OSError:
            if time.monotonic() > frist:
                sys.exit(f"mutmut-entrypoint: {host}:{port} nicht erreichbar")
            time.sleep(1)
print("mutmut-entrypoint: Dienste erreichbar")
PY
    # Vorlauf: check_docker überspringt Tests, wenn ein Dienst fehlt, statt
    # zu scheitern; ein grüner Lauf mit Übersprüngen hieße eine leere
    # Bewertung. Darum bricht jeder übersprungene Test den Start ab.
    echo "mutmut-entrypoint: Vorlauf pytest tests/integration/backend"
    vorlauf="$(mktemp)"
    uv run --no-sync pytest tests/integration/backend -x -q -rs -p no:randomly | tee "$vorlauf"
    if grep -q "skipped" "$vorlauf"; then
        echo "mutmut-entrypoint: Vorlauf hat Tests übersprungen; Dienste prüfen" >&2
        exit 1
    fi
    rm -f "$vorlauf"
fi

args=(--max-children "$kinder")
if [[ -n "$muster" ]]; then
    args+=("$muster")
fi
echo "mutmut-entrypoint: Lauf ${lauf}, mutmut run ${args[*]}"
start=$(date +%s)
status=0
uv run --no-sync mutmut run "${args[@]}" || status=$?
echo "mutmut-entrypoint: mutmut run beendet mit ${status} nach $(( $(date +%s) - start )) s"
exit "$status"
