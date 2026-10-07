# Test-Strategie (Phase 1+2 Stand)

Das Test-Universum von SwarmQ teilt sich in **sechs** Suiten mit
unterschiedlichen Ausführungs-Stufen.

## Suiten und Marker

| Verzeichnis | Marker | Default | Stufe | Zweck |
|---|---|---|---|---|
| `tests/unit/` | `unit` | ✓ | pre-merge | reine Logik, keine externe Infra |
| `tests/integration/` | `integration` | ✓ | pre-merge | Broker / Backend mit echtem Docker |
| `tests/e2e/` | `e2e` | ✓ | pre-merge | Vollstack (Client + Worker + RabbitMQ + Valkey) |
| `tests/property/` | `property` | – | jeder Push (im `unit-tests`-Job) | Hypothesis property-based |
| `tests/fuzz/test_parser_fuzz.py` | `fuzz` | – | jeder Push (im `unit-tests`-Job) | Byte-Fuzz auf Parser |
| `tests/fuzz/test_external_publish_fuzz.py` | `fuzz` | – | örtlich vor Releases | AMQP-Header-Fuzz, braucht den Chaos-Stack |
| `tests/chaos/` | `chaos` | – | örtlich vor Releases | Toxiproxy + Container-Restart + Crash |
| `tests/stress/` | `stress` | – | manuell in der Pipeline | hohe Parallelität (10 worker × 1000 tasks) |
| `tests/soak/` | `soak` | – | manuell in der Pipeline | 1h Last-Test mit Leak-Detection |

`addopts` in `pyproject.toml` schliesst opt-in-Marker per Default aus,
damit der Dev-Loop schnell bleibt. Für Unit-Tests gilt die Regel aus
`AGENTS.md`: jeder Unit-Test < 100 ms, Richtwert rund 15 ms im Mittel. Eine
feste Grenze für die Gesamtlaufzeit gibt es nicht — sie darf mit der Zahl der
Tests wachsen. Ein Unit-Test, der dauerhaft länger braucht (echte Prozesse,
Timer, Fork), wandert nach `tests/integration/`, statt abgeschwächt zu werden.
Langsame Tests findet `pytest tests/unit --durations=20`.

## CI-Stufen

Die einzige CI dieses Projekts ist `.gitlab-ci.yml`. Was dort steht, läuft;
alles andere läuft örtlich oder gar nicht — und das steht hier, damit es
niemand für eine Zusicherung hält.

**Bei jedem Push**, alle blockierend:

| Job | Was |
|---|---|
| `lint-and-types` | `ruff check src/ tests/`, `mypy src/swarmq tests/support tests/contracts` |
| `lock-check` | `uv.lock` ist zum `pyproject.toml` synchron |
| `unit-tests` | `tests/unit`, dazu `tests/property` und `tests/fuzz/test_parser_fuzz.py` |
| `integration-tests` | `tests/integration` gegen RabbitMQ und Valkey |
| `e2e-tests` | `tests/e2e` |
| `secret_detection` | GitLab Secret Detection (Template) |

**Manuell in der Pipeline**, nicht blockierend: `stress-tests`, `soak-tests`.
Beide werden faktisch nie gestartet; wer Last messen will, fährt sie von
Hand an oder örtlich.

**Nicht in der Pipeline**: Chaos und der Chaos-gestützte Fuzz-Lauf — siehe
den Abschnitt darüber. Mutation-Testing gibt es als `scripts/run_mutation.sh`
und läuft nur von Hand.

**Es gibt keine Zeitpläne.** Weder nightly noch weekly: `pipeline_schedules`
ist leer. Frühere Fassungen dieses Dokuments nannten
`.github/workflows/{ci,nightly,weekly}.yml` als Quelle dieser Stufen — die
Dateien lagen im Repo, das Projekt liegt aber auf GitLab, und sie sind nie
gelaufen. Sie sind entfernt.

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

**Vor jedem Release örtlich fahren**, mit laufendem Docker. Zuerst die
Standard-Suite (Unit, Integration, E2E) gegen den Teststack:

```bash
SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_VALKEY_PORT=6399 \
  uv run --extra test --extra reload pytest tests -q
```

Erwartung (Stand 2026-10-07): **3476 passed, 2 skipped**.

Dann das vollständige Chaos-Gate über alle opt-in-Marker. Es umfasst
`tests/chaos` (eigene Compose-Stacks), den Chaos-gestützten Fuzz-Lauf
`tests/fuzz/test_external_publish_fuzz.py`, die Chaos-Kombinationen in
`tests/e2e/combinations` (Prozess-Abstürze, Scheduler-Failover,
Backend-/Broker-Ausfälle über Toxiproxy) sowie Stress- und Property-Tests.
Die Kombinationen brauchen einen Toxiproxy neben dem Teststack: Ohne ihn
überspringen sie die zwölf Toxiproxy-Tests, und das Gate ist nicht
vollständig. `docker-compose.chaos.yml` startet den Haupt-Stack auf
5672/6379 mit; ist das belegt, einen eigenen Toxiproxy im Host-Netz starten
und Proxy-Ports sowie Upstreams per Env setzen:

```bash
docker run -d --name swarmq-test-toxiproxy --network host \
  ghcr.io/shopify/toxiproxy:2.9.0 -host 127.0.0.1 -port 8474
SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_VALKEY_PORT=6399 \
SWARMQ_TEST_TOXIPROXY_HOST=127.0.0.1 SWARMQ_TEST_TOXIPROXY_PORT=8474 \
SWARMQ_TEST_TOXIPROXY_RMQ_PORT=5698 SWARMQ_TEST_TOXIPROXY_VALKEY_PORT=6398 \
SWARMQ_TEST_TOXIPROXY_RMQ_UPSTREAM=127.0.0.1:5699 \
SWARMQ_TEST_TOXIPROXY_VALKEY_UPSTREAM=127.0.0.1:6399 \
  uv run --extra test --extra reload pytest tests -q \
  -m "chaos or stress or property or fuzz"
```

Erwartung (Stand 2026-10-07): **113 passed, 4 skipped** (117 Tests). Die
vier Skips sind begründet und sollen so bleiben — `libfaketime` fehlt (auf
die U3.x-Stressreihe vertagt), und die drei Disk-Pressure-Tests stehen
hinter `SWARMQ_RUN_DISK_TESTS=1`, weil sie unter geteilter Last flattern.
Jeder weitere Skip (etwa ein nicht erreichbarer Toxiproxy) heißt: Gate nicht
bestanden.

Einzeln, etwa zum Nachfahren eines roten Teils:

```bash
uv run --extra test --extra reload pytest -m chaos tests/chaos -q        # 41 Tests, 37 passed + 4 skipped
uv run --extra test --extra reload pytest -m fuzz tests/fuzz/test_external_publish_fuzz.py -q  # 2 passed
# Chaos-Kombinationen, mit den Toxiproxy-Variablen von oben: 28 passed
SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_VALKEY_PORT=6399 \
  uv run --extra test pytest -m chaos tests/e2e/combinations -q
```

Die Chaos-Kombinationen in `tests/e2e/combinations` waren von Mai bis
Oktober 2026 nie in einem Gate und deshalb unbemerkt rot (SIGUSR1 ohne
Handler, eine nicht existierende CLI-Option, Supervisor- statt Kind-Kill).
Ohne Toxiproxy überspringt die Suite zwölf Toxiproxy-Tests; die übrigen
sechzehn müssen grün sein. Die Ports 5699/6399 sind Pflicht: Die Suite leert
Valkey nach jedem Test, auf 6379 träfe das einen fremden Stack.

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
