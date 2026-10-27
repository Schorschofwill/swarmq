#!/usr/bin/env bash
# Vergleich auf einem entfernten Docker-Host (benchmarks/comparison/METHODIK.md).
#
# Der Messhost startet nur Container: Repo, Umgebungen und Ergebnisse liegen
# im Image und im Container, nie auf dem Host. Gesteuert wird von hier über
# den Docker-Kontext des Hosts (etwa ``celsius``).
#
#   scripts/vergleich-entfernt.sh starten <kontext> performance|garantie|ab [argumente ...]
#   scripts/vergleich-entfernt.sh status  <kontext>
#   scripts/vergleich-entfernt.sh abholen <kontext> <ordner>
#
# ``starten`` baut das Image aus einem sauberen Klon des aktuellen Commits und
# startet den Lauf im Hintergrund; der eigene Rechner darf danach schlafen.
# Der Lauf-Container nutzt das Netz des Hosts und dessen Docker-Socket, weil
# er die Dienste selbst startet und für die Störungen neu startet.
# ``abholen`` kopiert /ergebnisse und das Log in <ordner> und entfernt
# Container und Image. VERGLEICH_REPO nennt ein anderes Repo als dieses.
#
# CI-Jobs auf dem Messhost verfälschen die Messung. Läuft dort der Container
# ``gitlab-runner``, hält ``starten`` ihn vor dem Lauf an und vermerkt das am
# Lauf-Container; ``abholen`` startet ihn dann wieder. Der Runner-Container
# trägt ``--stop-signal SIGQUIT``, so laufen angefangene Jobs noch zu Ende.
set -euo pipefail

NAME=swarmq-vergleich
RUNNER=gitlab-runner
repo="${VERGLEICH_REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
befehl="${1:?Befehl fehlt: starten, status oder abholen}"
kontext="${2:?Docker-Kontext fehlt, etwa celsius}"
shift 2

d() { docker --context "$kontext" "$@"; }

case "$befehl" in
    starten)
        lauf="${1:?Lauf fehlt: performance, garantie oder ab}"
        shift
        if [ -n "$(git -C "$repo" status --porcelain)" ]; then
            echo "Arbeitsbaum mit Änderungen: gemessen wird nur ein Commit" >&2
            exit 2
        fi
        if [ -n "$(d ps -aq --filter "name=^${NAME}\$")" ]; then
            echo "auf $kontext liegt schon ein Lauf ($NAME); erst abholen" >&2
            exit 2
        fi
        commit="$(git -C "$repo" rev-parse --short HEAD)"
        kopie="$(mktemp -d)"
        trap 'rm -rf "$kopie"' EXIT
        git clone -q --no-hardlinks "$repo" "$kopie/repo"
        git -C "$kopie/repo" checkout -q --detach "$commit"
        d build -q -t "$NAME:$commit" -f "$kopie/repo/benchmarks/comparison/Dockerfile" "$kopie/repo"
        vermerk=()
        if [ "$(d inspect -f '{{.State.Running}}' "$RUNNER" 2>/dev/null)" = true ]; then
            echo "halte $RUNNER auf $kontext an; laufende Jobs enden vorher"
            d stop "$RUNNER" >/dev/null
            vermerk=(--label "swarmq.runner=$RUNNER")
        fi
        d run -d --name "$NAME" --network host -v /var/run/docker.sock:/var/run/docker.sock \
            "${vermerk[@]}" "$NAME:$commit" "$lauf" "$@"
        echo "gestartet: $NAME:$commit auf $kontext"
        ;;
    status)
        d ps -a --filter "name=^${NAME}\$" --format '{{.Status}}'
        d logs --tail 20 "$NAME"
        ;;
    abholen)
        ziel="${1:?Zielordner fehlt}"
        if [ "$(d inspect -f '{{.State.Running}}' "$NAME")" = true ]; then
            echo "$NAME läuft noch auf $kontext; Stand mit: $0 status $kontext" >&2
            exit 2
        fi
        mkdir -p "$ziel"
        d cp "$NAME:/ergebnisse/." "$ziel/"
        d logs "$NAME" >"$ziel/lauf.log" 2>&1
        image="$(d inspect -f '{{.Config.Image}}' "$NAME")"
        runner="$(d inspect -f '{{index .Config.Labels "swarmq.runner"}}' "$NAME")"
        d rm "$NAME"
        d rmi "$image"
        echo "abgeholt nach $ziel; $NAME und $image auf $kontext entfernt"
        if [ -n "$runner" ]; then
            d start "$runner" >/dev/null
            echo "$runner auf $kontext wieder gestartet"
        fi
        ;;
    *)
        echo "unbekannter Befehl: $befehl" >&2
        exit 2
        ;;
esac
