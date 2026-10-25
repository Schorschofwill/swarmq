#!/usr/bin/env bash
# Legt je System eine eigene Vergleichsumgebung an (Plan 2026-10-24-1118).
#
# Eigene Umgebungen, weil sich die Abhängigkeiten ausschließen (arq verlangt
# redis<6, taskiq-redis redis>=8). Installiert wird exakt nach
# requirements-<system>.txt; runner.py startet jede Kombination mit dem
# Python der Umgebung ihres Systems.
#
# Aufruf: ./einrichten.sh [system ...]   (ohne Argument alle fünf)
# VENVS=<verzeichnis> legt die Umgebungen woanders an (Standard: .venvs).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
venvs="${VENVS:-.venvs}"
systeme=("$@")
if [ ${#systeme[@]} -eq 0 ]; then
    systeme=(swarmq celery dramatiq arq taskiq)
fi

for system in "${systeme[@]}"; do
    uv venv --clear --python 3.14 "$venvs/$system"
    VIRTUAL_ENV="$venvs/$system" uv pip install --exact -r "requirements-$system.txt"
done
