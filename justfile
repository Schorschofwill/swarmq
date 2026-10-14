# swarmq-Aufgaben (Plan 2026-10-13-0902, U7). Aufruf: uv run just <rezept>
#
# Test-Rezepte reichen weitere Argumente an pytest durch, etwa
# ``uv run just unit -k logging``. Der Seed fuer pytest-randomly wird
# ausgegeben; ``SWARMQ_TEST_SEED=<seed> uv run just <rezept>`` stellt einen
# Lauf nach. Rezepte mit RabbitMQ/Valkey laufen unter einer gemeinsamen
# Testsperre (scripts/with-test-lock.sh).

set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

# Standard sind die swarmq-Testcontainer, nicht 5672/6379: Dort laufen
# oertlich fremde Stacks, und die Fixtures flushen Valkey.
export SWARMQ_TEST_RABBITMQ_PORT := env("SWARMQ_TEST_RABBITMQ_PORT", "5699")
export SWARMQ_TEST_VALKEY_PORT := env("SWARMQ_TEST_VALKEY_PORT", "6399")

uv_run := "uv run --extra test --extra reload"

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

# Unit-Suite (ohne Dienste)
unit *args:
    scripts/run-tests.sh --no-services tests/unit "$@"

# Property-Tests (ohne Dienste)
property *args:
    scripts/run-tests.sh --no-services tests/property -m property "$@"

# Parser-Fuzz (ohne Dienste)
fuzz *args:
    scripts/run-tests.sh --no-services tests/fuzz/test_parser_fuzz.py -m fuzz "$@"

# Integration gegen RabbitMQ und Valkey
integration *args:
    scripts/run-tests.sh tests/integration "$@"

# e2e-Suite gegen RabbitMQ und Valkey
e2e *args:
    scripts/run-tests.sh tests/e2e "$@"

# Chaos-Suiten (eigene Compose-Stacks, Toxiproxy; docs/testing/test-strategy.md)
chaos *args:
    scripts/run-tests.sh tests -m chaos "$@"

# Stress-Suite
stress *args:
    scripts/run-tests.sh tests/stress -m stress "$@"

# Alle Suiten der CI, unter einer Sperre
all:
    scripts/with-test-lock.sh {{just_executable()}} --justfile {{justfile()}} _all

_all: unit property fuzz integration e2e

# Unit, Integration und e2e unter Coverage, zusammengefuehrt (unter einer Sperre)
coverage:
    scripts/with-test-lock.sh {{just_executable()}} --justfile {{justfile()}} _coverage

_coverage:
    {{uv_run}} coverage erase
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh --no-services tests/unit
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh tests/integration
    SWARMQ_TEST_COVERAGE=1 scripts/run-tests.sh tests/e2e
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
