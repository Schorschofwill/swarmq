---
title: TaskInfo lehnt max_retries < 0 mit ValueError ab
applies_when:
  - Änderungen an der Prüfung von TaskInfo (task.py, _validate_identity_and_retry)
  - eine Fehlersuche meldet, dass max_retries=-1 abgelehnt wird oder wie 0 wirken sollte
tags: [task, retry, validierung]
id: E-087
datum: 2026-10-26
status: gilt
quelle:
  - "Georg 2026-10-26: Produktfrage V3b, Option „Ja, ValueError“"
  - docs/befunde/doku-pruefung-2026-10.md (LÜCKE V3b)
tests:
  - tests/unit/test_befund_dp_03_taskinfo_negative_values.py::test_negative_max_retries_is_rejected
  - tests/unit/test_befund_dp_03_taskinfo_negative_values.py::test_zero_max_retries_stays_valid
---

**Entscheidung.** `TaskInfo(max_retries=n)` mit `n < 0` wirft `ValueError` („max_retries must be >= 0“) beim Bau, also beim Import des Task-Moduls. `0` bleibt gültig und heißt „keine Wiederholung“.

**Begründung.** Ein negativer Wert wirkte bis 0.14.4 genau wie `0` (`retry_count >= max_retries`). Das war harmlos, aber ein stiller Tippfehler. Die Nachbarfelder `retry_base_delay` und `max_age` lehnen negative Werte schon ab; dieselbe Regel für alle Felder derselben Klasse.

**Bewusste Folgen.** Ein Task-Modul mit `max_retries=-1` lässt sich nicht mehr importieren; der Worker startet dann nicht. Der Changelog nennt das.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
