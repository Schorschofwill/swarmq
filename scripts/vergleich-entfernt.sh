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
# CI-Jobs auf dem Messhost verfälschen die Messung. ``starten`` setzt deshalb
# den Pausenschalter der GitHub-Runner (ops/github-runner/, Plan
# 2026-10-28-1308, KTD6) und wartet, bis kein Slot mehr einen Job hat;
# angefangene Jobs laufen zu Ende. Erreicht wird der Schalter über einen
# kurzlebigen Helfer-Container mit dem Zustandsverzeichnis als Bind-Mount,
# also über denselben Docker-Kontext, ohne Token und ohne SSH. Fehlt das
# Verzeichnis, scheitert der Mount, und es wird nicht ungeschützt gemessen.
# Nur ein Schalter, den ``starten`` selbst gesetzt hat, wird am Lauf-Container
# vermerkt; nur dann entfernt ``abholen`` ihn wieder.
#
# Übergangszeit: Läuft noch der Container ``gitlab-runner``, hält ``starten``
# ihn ebenfalls an und vermerkt das; ``abholen`` startet ihn dann wieder. Der
# Runner-Container trägt ``--stop-signal SIGQUIT``, so laufen angefangene
# Jobs noch zu Ende.
set -euo pipefail

NAME=swarmq-vergleich
RUNNER=gitlab-runner
ZUSTAND=/var/lib/github-runner
HELFER=busybox:1.37
repo="${VERGLEICH_REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
befehl="${1:?Befehl fehlt: starten, status oder abholen}"
kontext="${2:?Docker-Kontext fehlt, etwa celsius}"
shift 2

d() { docker --context "$kontext" "$@"; }

# Führt ein sh-Skript im Zustandsverzeichnis der GitHub-Runner auf dem Host aus.
zustand() { d run --rm --mount "type=bind,src=$ZUSTAND,dst=/zustand" "$HELFER" sh -c "$1"; }

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
        echo "pausiere die GitHub-Runner auf $kontext; laufende Jobs enden vorher"
        # Erst unter der Sperre der Slots setzen, dann in einem zweiten Aufruf
        # warten, bis jeder Slot pausiert: So ist vor dem Warten bekannt, ob
        # dieser Lauf den Schalter gesetzt hat.
        pause="$(zustand 'cd /zustand && flock lock sh -c "if [ -e pause ]; then echo schon; else touch pause; echo gesetzt; fi"')" || {
            echo "Pausenschalter auf $kontext nicht erreichbar ($ZUSTAND); ohne Pause wird nicht gemessen" >&2
            exit 2
        }
        if [ "$(echo "$pause" | head -1)" = gesetzt ]; then
            vermerk+=(--label "swarmq.pause=github-runner")
            # Scheitert etwas vor dem Lauf-Container (oder bricht jemand das Warten
            # ab), entsteht kein Vermerk für ``abholen``; dann hier freigeben,
            # sonst blieben alle Runner pausiert. Ein schon gesetzter Schalter bleibt.
            trap 'zustand "cd /zustand && flock lock rm -f pause" >/dev/null || true; rm -rf "$kopie"' EXIT
            trap 'exit 130' INT TERM
        fi
        zustand 'cd /zustand && while grep -qv "^pausiert\$" slots/*.state 2>/dev/null; do sleep 10; done' >/dev/null || {
            echo "Warten auf die Pause der Runner auf $kontext gescheitert" >&2
            exit 2
        }
        if [ "$(d inspect -f '{{.State.Running}}' "$RUNNER" 2>/dev/null)" = true ]; then
            echo "halte $RUNNER auf $kontext an; laufende Jobs enden vorher"
            d stop "$RUNNER" >/dev/null
            vermerk+=(--label "swarmq.runner=$RUNNER")
        fi
        d run -d --name "$NAME" --network host -v /var/run/docker.sock:/var/run/docker.sock \
            "${vermerk[@]}" "$NAME:$commit" "$lauf" "$@"
        # Ab jetzt trägt der Lauf-Container den Vermerk; ``abholen`` gibt frei.
        trap 'rm -rf "$kopie"' EXIT
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
        pausiert="$(d inspect -f '{{index .Config.Labels "swarmq.pause"}}' "$NAME")"
        d rm "$NAME"
        d rmi "$image"
        echo "abgeholt nach $ziel; $NAME und $image auf $kontext entfernt"
        if [ -n "$pausiert" ]; then
            zustand 'cd /zustand && flock lock rm -f pause'
            echo "GitHub-Runner auf $kontext fortgesetzt"
        fi
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
