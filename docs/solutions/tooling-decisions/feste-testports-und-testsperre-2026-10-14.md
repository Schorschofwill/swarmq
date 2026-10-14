---
title: Feste Testports und gemeinsame Testsperre für Integration und e2e
date: 2026-10-14
category: docs/solutions/tooling-decisions
module: justfile / scripts/with-test-lock.sh / tests/support/services.py
problem_type: tooling_decision
component: testing
severity: high
applies_when:
  - Integrations-, e2e-, Chaos- oder Stress-Tests lokal gestartet werden
  - mehrere Sitzungen (Menschen, Agenten, Worktrees) dieselben Testcontainer nutzen
  - ein Testlauf viele scheinbar zufällige Timeouts oder ResultNotReady meldet
tags: [testing, ports, flock, rabbitmq, valkey, just]
---

## Problem

Drei Vorfälle mit derselben Wurzel, alle aus örtlichen Testläufen:

- **2026-10-09:** Ein Subagent startete Integrationstests gegen die Standardports.
  Die Fixtures flushen Valkey; geflusht wurde die Datenbank 0 eines fremden
  Stacks (phoenix) auf 6379.
- **2026-10-11:** Eine volle Suite meldete 26 falsche Fehlschläge, weil eine
  zweite Sitzung im Worktree `swarmq-impl` zeitgleich gegen dieselben Container
  testete. Einzeln liefen alle grün.
- **2026-10-13:** Tests löschten ihre RabbitMQ-Queues nie; der Test-Broker wuchs
  auf 31 GB bis zum Kernel-OOM, nach dem Neuanlegen mit Limit in den
  Speicheralarm (alle Verleger blockiert, 179 Timeouts in einem Lauf).

## Lösung

1. **Feste Testports.** Standard sind die swarmq-Testcontainer 5699 (RabbitMQ,
   Management 15699) und 6399 (Valkey), nicht 5672/6379. Das `justfile` setzt
   sie, wenn die Umgebung nichts anderes vorgibt; `tests/support/services.py`
   verweigert einen lokalen Host auf 5672/6379, außer
   `SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1`.
2. **Eine Sperre für alle Sitzungen.** Rezepte mit Diensten laufen unter
   `scripts/with-test-lock.sh`: `flock -o -w` auf
   `${XDG_RUNTIME_DIR:-/tmp}/swarmq-itest.lock` (`SWARMQ_TEST_LOCK`).
   - `-o`: Kindprozesse erben die Sperre nicht; ein verwaister Worker hält sie
     nicht fest.
   - `-w`: nach `SWARMQ_TEST_LOCK_WAIT` Sekunden (Standard 3600) Abbruch mit
     Hinweis statt endlosem Warten.
   - Sammelrezepte (`all`, `coverage`) sperren einmal und setzen
     `SWARMQ_TEST_LOCK_HELD`; die aufgerufenen Rezepte sperren nicht erneut
     (`flock` auf derselben Datei würde sich sonst selbst blockieren).
   - In der CI (`CI` gesetzt) entfällt die Sperre; jeder Job hat eigene Dienste.
3. **Aufräumen je Sitzung.** `delete_session_queues` löscht am Sitzungsende
   über die Management-API die Queues, die in der Sitzung entstanden sind.
   Integrationstests leeren Valkey vor und nach jedem Test.

## Anwendung

```bash
uv run just integration          # Ports 5699/6399, Sperre
uv run just e2e -k scheduler      # Argumente gehen an pytest
SWARMQ_TEST_SEED=123 uv run just e2e   # Reihenfolge eines Laufs nachstellen
```

Massenhaft Timeouts in e2e: zuerst `pgrep -af pytest` (fremder Lauf ohne
Sperre?) und `docker exec swarmq-test-rabbit rabbitmq-diagnostics alarms`
(Speicheralarm?), dann erst die Tests verdächtigen.

## Übergang

Sitzungen, die noch per Hand `flock` auf eine eigene Datei nutzen, sehen die
neue Sperre nicht. Bis alle über `just` laufen, kann `SWARMQ_TEST_LOCK` auf die
bisher benutzte Datei zeigen.
