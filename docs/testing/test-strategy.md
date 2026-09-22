# Test-Strategie (Phase 1+2 Stand)

Das Test-Universum von SwarmQ teilt sich in **sechs** Suiten mit
unterschiedlichen Ausführungs-Stufen.

## Suiten und Marker

| Verzeichnis | Marker | Default | Stufe | Zweck |
|---|---|---|---|---|
| `tests/unit/` | `unit` | ✓ | pre-merge | reine Logik, keine externe Infra |
| `tests/integration/` | `integration` | ✓ | pre-merge | Broker / Backend mit echtem Docker |
| `tests/e2e/` | `e2e` | ✓ | pre-merge | Vollstack (Client + Worker + RabbitMQ + Valkey) |
| `tests/property/` | `property` | – | nightly (`thorough`) / pre-merge (`quick`) | Hypothesis property-based |
| `tests/fuzz/` | `fuzz` | – | nightly | Byte-Fuzz auf Parser, AMQP-Header-Fuzz |
| `tests/chaos/` | `chaos` | – | nightly | Toxiproxy + Container-Restart + Crash |
| `tests/stress/` | `stress` | – | nightly | hohe Parallelität (10 worker × 1000 tasks) |
| `tests/soak/` | `soak` | – | weekly | 1h Last-Test mit Leak-Detection |

`addopts` in `pyproject.toml` schliesst opt-in-Marker per Default aus,
damit `pytest tests/unit` ≤ 15s bleibt.

## CI-Stufen

- **Pre-merge** (`.github/workflows/ci.yml`) — ≤ 5 min: Unit + Integration + E2E + Property-Quick + Static.
- **Nightly** (`.github/workflows/nightly.yml`, cron `0 2 * * *`) — ≤ 30 min: Chaos + Stress + Fuzz + Property-Thorough + Bandit.
- **Weekly** (`.github/workflows/weekly.yml`, cron `0 2 * * 0`) — ≤ 4h: Mutation-Testing + 1h-Soak.

## Chaos und Fuzz — örtlich, vor jedem Release

Beide Suiten laufen **nicht** in der Pipeline. Der Chaos-Job dort trug
`allow_failure: false` und hielt Branch-Pipelines blockiert, bis er grün
war; geprüft hat er nie etwas. Die beiden einzigen Läufe (2026-09-15 und
2026-09-22) meldeten „1 passed, 40 skipped" in 1,17 Sekunden und gingen
grün durch: `docker:dind` braucht einen privilegierten Container, der
Runner dieses Projekts fährt mit `privileged = false`, und die Fixture
überspringt dann jeden Test.

Die Alternativen wogen schwerer als der Gewinn: einen eigenen Runner
privilegiert zu fahren gibt jedem Job dort die Kontrolle über den Rechner,
und die SaaS-Runner weisen den Job mit `ci_quota_exceeded` ab, weil der
Free-Plan keine Minuten mehr hat. Das ist eine bewusste Abweichung von
KTD15 aus Plan U2.

**Vor jedem Release örtlich fahren**, mit laufendem Docker:

```bash
uv run --extra test --extra reload pytest -m chaos tests/chaos -q
uv run --extra test --extra reload pytest -m fuzz tests/fuzz/test_external_publish_fuzz.py -q
```

Erwartung (Stand 2026-09-22): **37 passed, 4 skipped** in rund vier Minuten,
und **2 passed** für den Fuzz-Lauf. Die vier Skips sind begründet und
sollen so bleiben — `libfaketime` fehlt (auf die U3.x-Stressreihe vertagt),
und die drei Disk-Pressure-Tests stehen hinter `SWARMQ_RUN_DISK_TESTS=1`,
weil sie unter geteilter Last flattern.

Wer die Suite in einer Umgebung fährt, in der ein fehlender Stack ein
Fehler sein soll statt eines Skips, setzt `SWARMQ_REQUIRE_CHAOS=1`. Dann
wird aus jedem übersprungenen Test ein Fehlschlag — gedacht für den Tag, an
dem das Gate wieder in eine Pipeline zurückkehrt.

## Lokale Ausführung

```bash
# Dev-Loop
uv run --extra test pytest tests/unit -q

# E2E gegen Dev-Stack
SWARMQ_TEST_RABBITMQ_PORT=5673 SWARMQ_TEST_VALKEY_PORT=6380 \
  uv run --extra test pytest tests/e2e -q

# Chaos-Suite (startet eigene Compose-Stacks)
uv run --extra test pytest tests/chaos -m chaos

# Property-Tests im thorough-Profil
HYPOTHESIS_PROFILE=thorough uv run --extra test pytest tests/property -m property

# Soak (default 60s; opt-in 1h via env)
SWARMQ_SOAK_HOURS=1 uv run --extra test pytest tests/soak -m soak
```

## Chaos-Architektur

Hybrid-Isolation:
- **E2E** nutzt das langlebige Dev-Stack (`docker-compose.dev.yml` auf Ports `:5673` / `:6380`).
- **Chaos** startet pro Test-Klasse einen frischen Compose-Stack (`tests/chaos/compose.yml`) auf randomisierten Ports. Toxiproxy sitzt als Layer-7-Proxy davor.
- **Stress + Soak** nutzen das Dev-Stack — sie testen Throughput, nicht Failure-Modes.

## Flaky-Quarantäne (U4.4 Phase-1)

Manuell. `scripts/flaky_quarantine.py` liest pytest-JSON-Reports der
letzten N Nightly-Läufe und identifiziert Tests die ≥3× konsekutiv
gescheitert sind. Operator entscheidet über Skip / Fix.

## Spike-Findings (geschützt durch Tests)

- ValkeyBackend Retry-Policy → `tests/unit/backend/test_valkey.py` + chaos-recovery
- Pub/Sub Resubscribe → `tests/unit/test_client.py::TestClientProgress::test_*_resubscribes_*` + `tests/chaos/test_network_chaos.py::test_pubsub_disconnect_resubscribe`
- aio_pika Channel-Recreate → `tests/integration/broker/test_rabbitmq_reconnect.py`
