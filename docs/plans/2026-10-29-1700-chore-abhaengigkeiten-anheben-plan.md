---
title: Abhängigkeiten anheben (Oktober 2026) - Plan
type: chore
date: 2026-10-29
topic: abhaengigkeiten-anheben
artifact_contract: ce-unified-plan/v1
product_contract_source: ce-plan-bootstrap
execution: code
---

# Abhängigkeiten anheben (Oktober 2026) - Plan

## Goal Capsule

- **Objective:** `uv.lock` auf den neuesten Stand aller Pakete bringen, die sich ohne Verhaltensänderung in swarmq anheben lassen. Die Tests laufen danach gegen die Versionen, die eine frische Installation von swarmq bekommt.
- **Means:** `uv lock --upgrade`, aio-pika und aiormq ausgenommen (KTD1); nachgezogene Pins für Benchmark und Vergleichsmessung; Typanpassungen in den Tests; neue Kosten-Basislinie für die Module aus python-dateutil 3.0 (KTD2).
- **Product authority:** Entscheidungsregister `compound-packs/swarmq-entscheidungen/` > dieser Plan. AGENTS.md (TDD, Qualitäts-Gates, Testports) gilt durchgehend.
- **Execution profile:** Auf Georgs Auftrag vom 2026-10-29 („prüfe alle dependencies und schaue welche sich auf den neusten stand bringen lassen“). Ein PR; Merge nur mit grüner CI.

## Ergebnis der Prüfung

- 43 Pakete in `uv.lock` hatten neuere Versionen. 41 davon sind angehoben, darunter zur Laufzeit msgspec 0.22.0, valkey 6.2.0, multidict 7.2.1, yarl 1.25.1, python-dateutil 3.0.0 und tzdata 2026.6.
- markdown-it-py (3.0.0, neu 4.2.0) und mdformat (0.7.22, neu 1.0.0) bleiben: mkdocs-llmstxt hält sie über mdformat-tables. Beide gehören nur zur Doku.
- aio-pika (10.0.1, neu 10.2.1) und aiormq (7.0.0, neu 7.2.4) bleiben (KTD1).

## Key Technical Decisions

- **KTD1 — aio-pika und aiormq bleiben auf 10.0.1 und 7.0.0.** aiormq 7.2 hat `Channel.basic_publish` umgebaut (Abbruch bleibt im eigenen Task, `_raise_on_channel_close`, Rücknahme nicht gesendeter Frames). Mit 10.2.1/7.2.4 sind 50 Unit-Tests in `tests/unit/broker/` und `tests/unit/worker/` rot; die Test-Doubles in `tests/support/aiormq_transport.py` bilden die neuen Interna nicht ab (`'FakeTransport' object has no attribute '_raise_on_channel_close'`). Dazu schließt RabbitMQ mit 7.2.4 beim abgebrochenen `Channel.Close` die ganze Verbindung (`504 CHANNEL_ERROR - expected 'channel.open'`, `bench.py` Szenario 6, 2 von 3 Läufen; Familie F4-28). Swarmqs Umgehungen um aiormq (FS-08, F4-20, F4-22, F4-24, F4-28) müssen gegen 7.2 neu bewertet werden. Das ist ein eigenes Vorhaben. Bis dahin installiert eine frische Installation von swarmq 7.2.x, ungetestet (`aio-pika>=9` ohne Obergrenze); ob eine Obergrenze gesetzt wird, entscheidet Georg.
- **KTD2 — Kosten-Basislinie für python-dateutil 3.0.** croniter zieht python-dateutil; 3.0 lädt beim Import eigene tzdata-Zugriffe sowie `importlib.resources`, `tempfile` und `pkgutil`. Das sind +9 Module bei `import swarmq.cli` und +2 beim Worker-Start. Eine frische Installation bekommt 3.0 ohnehin; der Anstieg kommt aus der Abhängigkeit, nicht aus swarmq.
- **KTD3 — Typen in den Tests.** hypothesis 6.170 typisiert `run_state_machine_as_test(state_machine_factory=...)` als `type[RuleBasedStateMachine]`, nimmt laut Doku und Code aber jede Factory. `tests/support/stateful.py` bekommt `run_machine` mit einem einzigen `cast`, statt zehn Aufrufe einzeln zu unterdrücken.
- **KTD4 — uv 0.11.6 → 0.13.6 überall zugleich.** CI-Container (`ci.yml`, `load.yml`), `setup-uv` im Release und die Basis der beiden Dockerfiles nennen dieselbe Version, damit Lock-Prüfung, Tests und Release mit einem uv laufen. Dependabots Einzel-PR für die Dockerfiles (#14) hätte sie getrennt. uv 0.13.6 nimmt `uv.lock` (revision 3) unverändert an; SBOM-Export, `uv build` und die Unit-Suite sind damit lokal geprüft. Lokal installiertes uv hebt `uv self update` an.
- **KTD5 — Dependabot lässt aio-pika und aiormq aus.** Die regelmäßigen uv-Updates ignorieren beide, bis KTD1 bearbeitet ist; Sicherheitsupdates kommen weiter.
