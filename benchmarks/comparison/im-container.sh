#!/usr/bin/env bash
# Einstieg des Vergleichs-Images (Dockerfile, scripts/vergleich-entfernt.sh).
#
#   im-container.sh performance|garantie|ab [argumente ...]
#
# Ein Lauf je Container. Die Dienste startet der Lauf über den Docker-Socket
# des Hosts selbst und räumt sie am Ende ab, auch nach einem Fehler.
# Ergebnisse und Ausgang landen in VERGLEICH_ERGEBNISSE (Standard
# /ergebnisse), von wo ``vergleich-entfernt.sh abholen`` sie kopiert.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
ergebnisse="${VERGLEICH_ERGEBNISSE:-/ergebnisse}"
lauf="${1:?Lauf fehlt: performance, garantie oder ab}"
shift
mkdir -p "$ergebnisse"
# Dienste des Performance-Vergleichs (RabbitMQ, Valkey); den Garantie-Stack verwaltet stoerungen/garantie.py.
stack=(compose -p swarmq-vergleich -f docker-compose.yml)

case "$lauf" in
    performance)
        docker "${stack[@]}" up -d --wait
        .venvs/swarmq/bin/python runner.py "$@"
        ausgang=$?
        docker "${stack[@]}" down -v
        cp -r results "$ergebnisse/"
        ;;
    ab)
        # Der Messbefehl kann die Dienste brauchen, etwa der Runner selbst.
        docker "${stack[@]}" up -d --wait
        mkdir -p "$ergebnisse/ab"
        (cd ../.. && uv run --no-default-groups --group messung python scripts/ab_messung.py --ergebnis "$ergebnisse/ab/ergebnis.json" "$@")
        ausgang=$?
        docker "${stack[@]}" down -v
        ;;
    garantie)
        .venvs/swarmq/bin/python -m stoerungen.garantie --ergebnisse "$ergebnisse/garantie" "$@"
        ausgang=$?
        ;;
    *)
        echo "unbekannter Lauf: $lauf" >&2
        exit 2
        ;;
esac

echo "$ausgang" >"$ergebnisse/ausgang"
exit "$ausgang"
