---
title: Allokationen sind nicht im Kosten-Gate; die Basislinie enthält nur Zählmetriken (Vertragsaufrufe, AMQP-Frames, Valkey-Befehle und Roundtrips, geladene Module)
applies_when:
  - Änderungen an tests/kosten/ (conftest.py, basislinie.toml) oder an scripts/kosten_basislinie.py
  - eine Fehlersuche oder ein Review meldet, dass das Kosten-Gate Allokationen oder Speicher je Task nicht prüft
  - ein Plan will tracemalloc-Messungen ins Gate der Pipeline aufnehmen
tags: [kosten, ci, performance, tracemalloc, qualitaetsablauf]
id: E-085
datum: 2026-10-25
status: gilt
quelle:
  - "Georg 2026-10-25: ‚ich übernehme deine Empfehlung‘ (Herausnahme der Allokationen ins Register)"
  - docs/plans/2026-10-24-1118-chore-qualitaetsablauf-plan.md (R7, KTD5, Stop condition)
  - tests/kosten/conftest.py (Modul-Docstring, Ergebnis der Determinismusprobe)
tests:
  - tests/kosten/test_basislinie_waechter.py::test_the_baseline_holds_only_counted_metrics_and_no_allocations
---

**Entscheidung.** Das Kosten-Gate vergleicht nur Zählmetriken exakt mit `tests/kosten/basislinie.toml`: Vertragsaufrufe an Broker und Backend, AMQP-Frames, Valkey-Befehle und Roundtrips sowie geladene Module. Allokationen (`tracemalloc`) sind nicht im Gate. Sie bleiben ein Signal für die tiefen Runden (Methode M-P-01, Profil und A/B-Messung).

**Begründung.** KTD5 erlaubt eine Metrik im Gate nur, wenn 20 Läufe identische Zahlen liefern. Die Determinismusprobe vor dem Bau ergab für Allokationen über 20 Läufe keine identischen Zahlen. Ein exaktes Gate auf eine unstete Zahl wäre ein Flake (AGENTS.md: Tests sind deterministisch); ein Toleranzband widerspräche R7 („ein Anstieg ist rot“).

**Bewusste Folgen.**
- Ein Anstieg der Allokationen je Task fällt in der Pipeline nicht auf. Er zeigt sich erst in der Loadsuite (gehaltener Speicher) oder in einer tiefen Runde.
- Wer Allokationen später ins Gate holen will, braucht zuerst den Nachweis: 20 identische Läufe lokal und in der CI.

**Überdenken nur mit neuem Argument.** Etwa einer deterministischen Zählweise (z. B. Zählung von Objekten bestimmter Typen statt Bytes) mit bestandener 20-Lauf-Probe.
