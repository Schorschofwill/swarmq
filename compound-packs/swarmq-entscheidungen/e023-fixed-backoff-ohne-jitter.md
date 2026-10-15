---
title: retry_backoff "fixed" liefert genau base_delay, höchstens retry_max_delay, ohne Jitter; behoben sind nur Deckel-Jitter und Overflow
applies_when:
  - Änderungen an backoff.calculate_backoff
  - eine Fehlersuche meldet, dass fixed-Retries vieler Tasks synchron laufen (Thundering Herd)
  - eine Fehlersuche meldet, dass Retries oberhalb von max_delay nicht exakt max_delay warten
  - eine Fehlersuche meldet, dass fixed-Retries mit base_delay über retry_max_delay nur retry_max_delay warten
tags: [retry, backoff, jitter, bz-26]
id: E-023
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:140 (Entscheidungen, BZ-26)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:285 (MR 2, BZ-26)
  - Orchestrator nach Gegenprüfung G4 (Fehlersuche Runde 3, F3-16, Präzisierung 2026-10-15), Georg kann widersprechen
tests:
  - tests/unit/test_backoff.py::test_fixed_strategy_returns_base_delay
  - tests/unit/test_befund_bz26_backoff.py::test_exponential_backoff_keeps_jitter_at_the_cap
  - tests/unit/test_befund_bz26_backoff.py::test_linear_backoff_keeps_jitter_at_the_cap
  - tests/unit/test_befund_bz26_backoff.py::test_exponential_backoff_with_huge_attempt_stays_at_cap
---

**Entscheidung.** `fixed` liefert genau `base_delay`, höchstens `retry_max_delay` (`min(base_delay, max_delay)`, präzisiert 2026-10-15, siehe Nachtrag F3-16), ohne Jitter. Behoben sind nur: Exponent gedeckelt (`min(attempt, 64)`, kein `OverflowError`) und Jitter am Deckel (`max_delay · (0,6 + 0,2u)` aus derselben Ziehung).

**Begründung.** `fixed` ist ausdrücklich eine feste Wartezeit; wer Streuung will, nimmt `exponential` oder `linear`. Der Nachweis für „fixed ohne Jitter“ ist deshalb entfallen.

**Bewusste Folgen.** Viele gleichzeitig scheiternde `fixed`-Tasks wiederholen sich synchron; das ist kein Fehler. Oberhalb von `max_delay` liegt die Wartezeit bei 60–80 % von `max_delay`.

**Nachtrag 2026-10-15 (Fehlersuche Runde 3, F3-16): Präzisierung des Deckels.** Die Entscheidung hat nie geregelt, ob `retry_max_delay` auch für `fixed` gilt. Präzisiert: `fixed` liefert genau `base_delay`, höchstens `retry_max_delay`, ohne Jitter, also `min(base_delay, max_delay)`. Damit hält auch die Invariante `scheduler_dedup_ttl >= retry_max_delay` für Cron-Retries mit `fixed`. Dass `calculate_backoff(0, "fixed", 7200, 3600)` heute 7200 liefert, ist der Befund F3-16 (Fix in 0.12.3, Test `tests/unit/test_befund_f3_16_fixed_backoff_cap.py`). Der festschreibende Test `test_backoff.py::test_fixed_strategy_returns_base_delay` (`max_delay > base_delay`) bleibt unverändert grün.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
