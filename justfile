# swarmq-Aufgaben (Plan 2026-10-13-0902, U7). Aufruf: uv run just <rezept>
#
# Test-Rezepte reichen weitere Argumente an pytest durch, etwa
# ``uv run just unit -k logging``. Ein Pfad grenzt die Suite ein
# (``uv run just integration tests/integration/test_x.py``, auch als Node-ID),
# Optionen wie ``-k`` nicht (scripts/run-tests.sh --suite). Der Seed fuer
# pytest-randomly wird ausgegeben; ``SWARMQ_TEST_SEED=<seed> uv run just
# <rezept>`` stellt einen Lauf nach. Rezepte mit RabbitMQ/Valkey laufen unter einer gemeinsamen
# Testsperre (scripts/with-test-lock.sh).

set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

# Standard sind die swarmq-Testcontainer, nicht 5672/6379: Dort laufen
# oertlich fremde Stacks, und die Fixtures flushen Valkey.
export SWARMQ_TEST_RABBITMQ_PORT := env("SWARMQ_TEST_RABBITMQ_PORT", "5699")
export SWARMQ_TEST_VALKEY_PORT := env("SWARMQ_TEST_VALKEY_PORT", "6399")

uv_run := "uv run --extra test --extra reload"

# Prozesse (pytest-xdist) fuer Unit, Integration und e2e: 8 war fuer Unit
# auf 24 Kernen das Optimum (15 s statt 50 s), mehr brachte nichts. Fuer die
# Dienst-Suiten hoechstens 15 (eine Valkey-Datenbank je Prozess).
workers := env("SWARMQ_TEST_WORKERS", "8")

# Rezepte auflisten
_default:
    @{{just_executable()}} --justfile {{justfile()}} --list

# Format und Lint wie der CI-Job lint-and-types
lint:
    {{uv_run}} ruff format --check .
    {{uv_run}} ruff check src/ tests/ typings/

# Typpruefung wie der CI-Job lint-and-types (mypy auch ueber tests)
types:
    {{uv_run}} mypy src/swarmq tests
    {{uv_run}} basedpyright
    {{uv_run}} python -m mypy.stubtest msgpack croniter --mypy-config-file pyproject.toml --ignore-missing-stub

# Bekannte Schwachstellen in den Abhängigkeiten aus uv.lock (OSV), wie der
# CI-Job abhaengigkeiten-pruefen (Plan 2026-10-24-1118, U2, KTD2). Ausnahmen
# kommen als ``--ignore <ID>`` dazu, je mit Kommentar aus Grund und Signal-ID.
audit:
    uv audit --frozen

# Unit-Suite (ohne Dienste): parallel auf SWARMQ_TEST_WORKERS Prozessen, danach
# die ``timing``-Tests (enges Zeitbudget gegen die echte Uhr) allein
unit *args:
    scripts/run-tests.sh --no-services --parallel {{workers}} --suite tests/unit "$@"

# Property-Tests (ohne Dienste)
property *args:
    scripts/run-tests.sh --no-services --suite tests/property -m property "$@"

# Fuzz-Ziele ohne Dienste: alle Dateien unter tests/fuzz ausser
# test_external_publish_fuzz.py, die chaos_stack braucht (Plan 2026-10-17-0941, R21)
fuzz *args:
    scripts/run-tests.sh --no-services --suite tests/fuzz --ignore tests/fuzz/test_external_publish_fuzz.py -m fuzz "$@"

# Integration gegen RabbitMQ und Valkey: parallel auf SWARMQ_TEST_WORKERS Prozessen,
# je in eigener vhost und Valkey-Datenbank (tests/support/services.py), danach
# die exclusive-Tests allein (scripts/run-tests.sh --parallel)
integration *args:
    scripts/run-tests.sh --parallel {{workers}} --suite tests/integration "$@"

# e2e-Suite gegen RabbitMQ und Valkey
e2e *args:
    scripts/run-tests.sh --parallel {{workers}} --suite tests/e2e "$@"

# Chaos-Suiten (eigene Compose-Stacks, Toxiproxy; docs/testing/test-strategy.md).
# Ohne Pfad gilt testpaths; ``uv run just chaos tests/chaos`` grenzt ein.
chaos *args:
    scripts/run-tests.sh -m chaos "$@"

# Stress-Suite
stress *args:
    scripts/run-tests.sh --suite tests/stress -m stress "$@"

# Alle Suiten der CI, unter einer Sperre
all:
    scripts/with-test-lock.sh {{just_executable()}} --justfile {{justfile()}} _all

_all: unit property fuzz integration e2e

# Unit, Integration und e2e unter Coverage, zusammengefuehrt (unter einer Sperre)
coverage:
    scripts/with-test-lock.sh {{just_executable()}} --justfile {{justfile()}} _coverage

_coverage:
    {{uv_run}} coverage erase
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh --no-services --parallel {{workers}} tests/unit
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh --parallel {{workers}} tests/integration
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh --parallel {{workers}} tests/e2e
    {{uv_run}} coverage combine
    {{uv_run}} coverage report

# Vorhandene Messdaten zusammenfuehren; coverage.xml fuer GitLab, Bericht im Terminal
coverage-report:
    {{uv_run}} coverage combine
    {{uv_run}} coverage xml
    {{uv_run}} coverage report

# Ratchet gegen den Merge-Base mit main und diff-cover (nach coverage-report; scripts/coverage_gate.py)
coverage-gate:
    {{uv_run}} --group coverage python scripts/coverage_gate.py

# fail_under aus drei main-Laeufen anheben (ohne Werte per GitLab-API; scripts/coverage_ratchet.py)
ratchet *values:
    {{uv_run}} python scripts/coverage_ratchet.py "$@"

# A/B-Messung zweier Refs mit Urteil nach R5 (scripts/ab_messung.py --help); Wertungslaeufe nur auf celsius
ab *args:
    {{uv_run}} --group messung python scripts/ab_messung.py "$@"

# --- Mutation im Container (Plan 2026-10-17-0941, U2; docker/mutation/README.md) ---
# Docker-Kontext je Aufruf (nie `docker context use`); Standard ist der Lastserver.
mutation_ctx := env("SWARMQ_MUTATION_CONTEXT", "lastserver")
mutation_docker := "docker --context " + quote(mutation_ctx)
mutation_compose := mutation_docker + " compose -f docker/mutation/compose.yml"

# Mutmut-Lauf a (tests/unit) oder b (zusaetzlich tests/integration/backend) bauen und abgekoppelt starten
mutation-starten lauf:
    case "$1" in a|b) ;; *) echo "LAUF muss a oder b sein" >&2; exit 2 ;; esac
    {{mutation_docker}} info > /dev/null || { echo "Kontext {{mutation_ctx}} nicht erreichbar" >&2; exit 1; }
    if [ -n "$({{mutation_docker}} ps -aq --filter label=swarmq-mutation)$({{mutation_docker}} volume ls -q --filter label=swarmq-mutation)" ]; then echo "Kontext {{mutation_ctx}}: Container oder Volumes eines Mutmut-Laufs vorhanden; erst mutation-holen, dann mutation-abbauen" >&2; exit 1; fi
    if [ -n "$({{mutation_docker}} ps -q --filter name=loadsuite-runner)" ]; then echo "Kontext {{mutation_ctx}}: ein Loadsuite-Lauf laeuft (KTD10)" >&2; exit 1; fi
    MUTMUT_LAUF="$1" {{mutation_compose}} --profile "$1" up -d --build --force-recreate mutmut
    {{mutation_compose}} --profile "$1" ps -a
    @echo "Status: {{mutation_compose}} logs --tail 20 mutmut"

# Ergebnisse eines Laufs nach mutants-LAUF/ holen und mit mutmut results lokal auswerten
mutation-holen lauf:
    case "$1" in a|b) ;; *) echo "LAUF muss a oder b sein" >&2; exit 2 ;; esac
    rm -rf "mutants-$1.neu" && mkdir "mutants-$1.neu"
    {{mutation_compose}} cp mutmut:/app/mutants "mutants-$1.neu/mutants"
    # mutmut results liest pyproject.toml und mutants/<pfad>.meta relativ zum Arbeitsverzeichnis.
    cp "mutants-$1.neu/mutants/pyproject.toml" "mutants-$1.neu/pyproject.toml"
    ln -s mutants/src "mutants-$1.neu/src"
    # Erst auswerten, dann die alte Kopie ersetzen: mutmut schreibt .meta nicht atomar.
    cd "mutants-$1.neu" && {{uv_run}} --project "{{justfile_directory()}}" mutmut results --all true > ergebnisse.txt
    rm -rf "mutants-$1" && mv "mutants-$1.neu" "mutants-$1"
    @echo "mutants-$1/ergebnisse.txt:"
    sed 's/.*: //' "mutants-$1/ergebnisse.txt" | sort | uniq -c

# Container, Volumes, Netz und gebautes Abbild des Mutmut-Laufs entfernen und pruefen, dass nichts uebrig ist
mutation-abbauen:
    {{mutation_compose}} --profile a --profile b down -v --rmi local --remove-orphans
    rest="$({{mutation_docker}} ps -aq --filter label=swarmq-mutation; {{mutation_docker}} volume ls -q --filter label=swarmq-mutation; {{mutation_docker}} network ls -q --filter label=swarmq-mutation; {{mutation_docker}} image ls -q --filter label=swarmq-mutation)"; if [ -n "$rest" ]; then echo "Uebrig mit Label swarmq-mutation:" >&2; echo "$rest" >&2; exit 1; fi
    @echo "Kontext {{mutation_ctx}}: keine Objekte mit Label swarmq-mutation"
