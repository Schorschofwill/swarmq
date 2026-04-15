#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Farben ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# --- Docker starten ---
info "Starte RabbitMQ + Valkey via Docker Compose..."
docker compose up -d

info "Warte auf RabbitMQ Health-Check..."
until docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping 2>/dev/null; do
    sleep 1
done
info "RabbitMQ bereit."

info "Warte auf Valkey Health-Check..."
until docker compose exec -T valkey valkey-cli ping 2>/dev/null | grep -q PONG; do
    sleep 1
done
info "Valkey bereit."

# --- Python-Umgebung ---
if [ ! -d ".venv" ]; then
    info "Erstelle virtuelle Umgebung..."
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q -r requirements.txt

# --- Benchmark ausfuehren ---
echo ""
info "Starte Benchmark..."
echo "============================================================"
python bench.py "$@"
echo "============================================================"
info "Benchmark abgeschlossen."

# --- Aufraeumen (optional) ---
echo ""
read -rp "Docker-Container stoppen und aufraeumen? [y/N] " answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
    docker compose down -v
    info "Container gestoppt und Volumes entfernt."
else
    info "Container laufen weiter. Stoppen mit: docker compose down -v"
fi
